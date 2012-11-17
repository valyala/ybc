package memcache

import (
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func newBenchClientServerCache_Ext(buffersSize, osBuffersSize, maxPendingRequestsCount int, b *testing.B) (c *Client, s *Server, cache *ybc.Cache) {
	config := ybc.Config{
		MaxItemsCount: 1000 * 1000,
		DataFileSize:  10 * 1000 * 1000,
	}

	cache, err := config.OpenCache(true)
	if err != nil {
		b.Fatal(err)
	}

	s = &Server{
		Cache:             cache,
		ListenAddr:        testAddr,
		ReadBufferSize:    buffersSize,
		WriteBufferSize:   buffersSize,
		OSReadBufferSize:  osBuffersSize,
		OSWriteBufferSize: osBuffersSize,
	}
	s.Start()

	c = &Client{
		ServerAddr:              testAddr,
		ReadBufferSize:          buffersSize,
		WriteBufferSize:         buffersSize,
		OSReadBufferSize:        osBuffersSize,
		OSWriteBufferSize:       osBuffersSize,
		MaxPendingRequestsCount: maxPendingRequestsCount,
	}
	c.Start()

	return
}

func newBenchClientServerCache(b *testing.B) (c *Client, s *Server, cache *ybc.Cache) {
	return newBenchClientServerCache_Ext(0, 0, 0, b)
}

func BenchmarkClientServer_Set(b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	item := Item{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	for i := 0; i < b.N; i++ {
		err := c.Set(&item)
		if err != nil {
			b.Fatalf("Error in client.Set(): [%s]", err)
		}
	}
}

func BenchmarkClientServer_GetHit(b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	key := []byte("key")
	item := Item{
		Key:   key,
		Value: []byte("value"),
	}
	if err := c.Set(&item); err != nil {
		b.Fatalf("Error in client.Set(): [%s]", err)
	}

	for i := 0; i < b.N; i++ {
		if err := c.Get(&item); err != nil {
			b.Fatalf("Error in client.Get(): [%s]", err)
		}
	}
}

func BenchmarkClientServer_GetMiss(b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	item := Item{
		Key: []byte("key"),
	}

	for i := 0; i < b.N; i++ {
		if err := c.Get(&item); err != ErrCacheMiss {
			b.Fatalf("Unexpected error in client.Get(): [%s]", err)
		}
	}
}

func getMulti(batchSize int, b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	key := []byte("key")
	item := Item{
		Key:   key,
		Value: []byte("value"),
	}
	if err := c.Set(&item); err != nil {
		b.Fatalf("Error in client.Set(): [%s]", err)
	}

	var items []Item
	for i := 0; i < batchSize; i++ {
		items = append(items, item)
	}

	for i := 0; i < b.N; i += batchSize {
		if err := c.GetMulti(items); err != nil {
			b.Fatalf("Error in client.GetMulti(): [%s]", err)
		}
	}
}

func BenchmarkClientServer_GetMulti_1Items(b *testing.B) {
	getMulti(1, b)
}

func BenchmarkClientServer_GetMulti_2Items(b *testing.B) {
	getMulti(2, b)
}

func BenchmarkClientServer_GetMulti_4Items(b *testing.B) {
	getMulti(4, b)
}

func BenchmarkClientServer_GetMulti_8Items(b *testing.B) {
	getMulti(8, b)
}

func BenchmarkClientServer_GetMulti_16Items(b *testing.B) {
	getMulti(16, b)
}

func BenchmarkClientServer_GetMulti_32Items(b *testing.B) {
	getMulti(32, b)
}

func BenchmarkClientServer_GetMulti_64Items(b *testing.B) {
	getMulti(64, b)
}

func setNowait(buffersSize, osBuffersSize, maxPendingRequestsCount int, b *testing.B) {
	c, s, cache := newBenchClientServerCache_Ext(buffersSize, osBuffersSize, maxPendingRequestsCount, b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	item := Item{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	for i := 0; i < b.N; i++ {
		c.SetNowait(&item)
	}

	// this operation is required for waiting until all other operations are complete.
	if err := c.Set(&item); err != nil {
		b.Fatalf("Error in c.Set(): [%s]", err)
	}
}

func BenchmarkClientServer_SetNowait_4KBuffers(b *testing.B) {
	setNowait(4*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_8KBuffers(b *testing.B) {
	setNowait(8*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_16KBuffers(b *testing.B) {
	setNowait(16*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_32KBuffers(b *testing.B) {
	setNowait(32*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_64KBuffers(b *testing.B) {
	setNowait(64*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_128KBuffers(b *testing.B) {
	setNowait(128*1024, 0, 0, b)
}

func BenchmarkClientServer_SetNowait_8KOSBuffers(b *testing.B) {
	setNowait(0, 8*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_16KOSBuffers(b *testing.B) {
	setNowait(0, 16*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_32KOSBuffers(b *testing.B) {
	setNowait(0, 32*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_64KOSBuffers(b *testing.B) {
	setNowait(0, 64*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_128KOSBuffers(b *testing.B) {
	setNowait(0, 128*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_256KOSBuffers(b *testing.B) {
	setNowait(0, 256*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_32PendingRequests(b *testing.B) {
	setNowait(0, 0, 32, b)
}

func BenchmarkClientServer_SetNowait_64PendingRequests(b *testing.B) {
	setNowait(0, 0, 64, b)
}

func BenchmarkClientServer_SetNowait_128PendingRequests(b *testing.B) {
	setNowait(0, 0, 128, b)
}

func BenchmarkClientServer_SetNowait_256PendingRequests(b *testing.B) {
	setNowait(0, 0, 256, b)
}

func BenchmarkClientServer_SetNowait_512PendingRequests(b *testing.B) {
	setNowait(0, 0, 512, b)
}

func BenchmarkClientServer_SetNowait_1KPendingRequests(b *testing.B) {
	setNowait(0, 0, 1024, b)
}

type WorkerFunc func(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B)

type SetupFunc func(c *Client)

func concurrentOps(setupFunc SetupFunc, workerFunc WorkerFunc, workersCount int, b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	if setupFunc != nil {
		setupFunc(c)
	}
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ch := make(chan int, b.N)
	defer close(ch)
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go workerFunc(c, ch, wg, i, b)
	}

	for i := 0; i < b.N; i++ {
		ch <- i
	}
}

func setWorker(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	item := Item{
		Key:   []byte(fmt.Sprintf("key_%d", i)),
		Value: []byte(fmt.Sprintf("value_%d", i)),
	}
	for _ = range ch {
		if err := c.Set(&item); err != nil {
			b.Fatalf("Error when calling channel.Set(): [%s]", err)
		}
	}
}

func concurrentSet(workersCount int, b *testing.B) {
	concurrentOps(nil, setWorker, workersCount, b)
}

func BenchmarkClientServer_ConcurrentSet_1Workers(b *testing.B) {
	concurrentSet(1, b)
}

func BenchmarkClientServer_ConcurrentSet_2Workers(b *testing.B) {
	concurrentSet(2, b)
}

func BenchmarkClientServer_ConcurrentSet_4Workers(b *testing.B) {
	concurrentSet(4, b)
}

func BenchmarkClientServer_ConcurrentSet_8Workers(b *testing.B) {
	concurrentSet(8, b)
}

func BenchmarkClientServer_ConcurrentSet_16Workers(b *testing.B) {
	concurrentSet(16, b)
}

func BenchmarkClientServer_ConcurrentSet_32Workers(b *testing.B) {
	concurrentSet(32, b)
}

func BenchmarkClientServer_ConcurrentSet_64Workers(b *testing.B) {
	concurrentSet(64, b)
}

func BenchmarkClientServer_ConcurrentSet_128Workers(b *testing.B) {
	concurrentSet(128, b)
}

func getWorker(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	item := Item{
		Key: []byte(fmt.Sprintf("key_%d", i)),
	}
	for _ = range ch {
		if err := c.Get(&item); err != nil {
			b.Fatalf("Error when calling channel.Get(): [%s]", err)
		}
	}
}

func concurrentGet(workersCount int, b *testing.B) {
	setupFunc := func(c *Client) {
		for i := 0; i < workersCount; i++ {
			item := Item{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("value_%d", i)),
			}
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOps(setupFunc, getWorker, workersCount, b)
}

func BenchmarkClientServer_ConcurrentGet_1Workers(b *testing.B) {
	concurrentGet(1, b)
}

func BenchmarkClientServer_ConcurrentGet_2Workers(b *testing.B) {
	concurrentGet(2, b)
}

func BenchmarkClientServer_ConcurrentGet_4Workers(b *testing.B) {
	concurrentGet(4, b)
}

func BenchmarkClientServer_ConcurrentGet_8Workers(b *testing.B) {
	concurrentGet(8, b)
}

func BenchmarkClientServer_ConcurrentGet_16Workers(b *testing.B) {
	concurrentGet(16, b)
}

func BenchmarkClientServer_ConcurrentGet_32Workers(b *testing.B) {
	concurrentGet(32, b)
}

func BenchmarkClientServer_ConcurrentGet_64Workers(b *testing.B) {
	concurrentGet(64, b)
}

func BenchmarkClientServer_ConcurrentGet_128Workers(b *testing.B) {
	concurrentGet(128, b)
}

func getDeWorker(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	item := Item{
		Key: []byte(fmt.Sprintf("key_%d", i)),
	}
	grace := 100 * time.Millisecond
	for _ = range ch {
		if err := c.GetDe(&item, grace); err != nil {
			b.Fatalf("Error when calling channel.GetDe(): [%s]", err)
		}
	}
}

func concurrentGetDe(workersCount int, b *testing.B) {
	setupFunc := func(c *Client) {
		for i := 0; i < workersCount; i++ {
			item := Item{
				Key:   []byte(fmt.Sprintf("key_%d", i)),
				Value: []byte(fmt.Sprintf("value_%d", i)),
			}
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOps(setupFunc, getDeWorker, workersCount, b)
}

func BenchmarkClientServer_ConcurrentGetDe_1Workers(b *testing.B) {
	concurrentGetDe(1, b)
}

func BenchmarkClientServer_ConcurrentGetDe_2Workers(b *testing.B) {
	concurrentGetDe(2, b)
}

func BenchmarkClientServer_ConcurrentGetDe_4Workers(b *testing.B) {
	concurrentGetDe(4, b)
}

func BenchmarkClientServer_ConcurrentGetDe_8Workers(b *testing.B) {
	concurrentGetDe(8, b)
}

func BenchmarkClientServer_ConcurrentGetDe_16Workers(b *testing.B) {
	concurrentGetDe(16, b)
}

func BenchmarkClientServer_ConcurrentGetDe_32Workers(b *testing.B) {
	concurrentGetDe(32, b)
}

func BenchmarkClientServer_ConcurrentGetDe_64Workers(b *testing.B) {
	concurrentGetDe(64, b)
}

func BenchmarkClientServer_ConcurrentGetDe_128Workers(b *testing.B) {
	concurrentGetDe(128, b)
}

var keyChars = []byte("0123456789qwertyuiopasdfghjklzxcvbnm")

func randFill(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = keyChars[rand.Int()%len(keyChars)]
	}
}

func getSetWorker(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	item := Item{
		Key:   make([]byte, 16),
		Value: make([]byte, 16),
	}
	randFill(item.Value)

	for _ = range ch {
		randFill(item.Key)
		if (rand.Int() & 0x1) == 0 {
			// get operation
			if err := c.Get(&item); err == ErrCommunicationFailure {
				b.Fatalf("Error when calling Client.Get(): [%s]", err)
			}
		} else {
			// set operation
			if err := c.Set(&item); err == ErrCommunicationFailure {
				b.Fatalf("Error when calling Client.Set(): [%s]", err)
			}
		}
	}
}

func concurrentGetSet(workersCount int, b *testing.B) {
	concurrentOps(nil, getSetWorker, workersCount, b)
}

func BenchmarkClientServer_ConcurrentGetSet_1Workers(b *testing.B) {
	concurrentGetSet(1, b)
}

func BenchmarkClientServer_ConcurrentGetSet_2Workers(b *testing.B) {
	concurrentGetSet(2, b)
}

func BenchmarkClientServer_ConcurrentGetSet_4Workers(b *testing.B) {
	concurrentGetSet(4, b)
}

func BenchmarkClientServer_ConcurrentGetSet_8Workers(b *testing.B) {
	concurrentGetSet(8, b)
}

func BenchmarkClientServer_ConcurrentGetSet_16Workers(b *testing.B) {
	concurrentGetSet(16, b)
}

func BenchmarkClientServer_ConcurrentGetSet_32Workers(b *testing.B) {
	concurrentGetSet(32, b)
}

func BenchmarkClientServer_ConcurrentGetSet_64Workers(b *testing.B) {
	concurrentGetSet(64, b)
}

func BenchmarkClientServer_ConcurrentGetSet_128Workers(b *testing.B) {
	concurrentGetSet(128, b)
}
