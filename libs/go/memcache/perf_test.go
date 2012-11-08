package memcache

import (
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"sync"
	"testing"
)

func newBenchClientServerCache_Ext(buffersSize, maxPendingRequestsCount int, b *testing.B) (c *Client, s *Server, cache *ybc.Cache) {
	config := ybc.Config{
		MaxItemsCount: 1000 * 1000,
		DataFileSize:  10 * 1000 * 1000,
	}

	cache, err := config.OpenCache(true)
	if err != nil {
		b.Fatal(err)
	}

	s = &Server{
		Cache:           cache,
		ListenAddr:      testAddr,
		ReadBufferSize:  buffersSize,
		WriteBufferSize: buffersSize,
	}
	s.Start()

	c = &Client{
		ConnectAddr:             testAddr,
		ReadBufferSize:          buffersSize,
		WriteBufferSize:         buffersSize,
		MaxPendingRequestsCount: maxPendingRequestsCount,
	}
	c.Start()

	return
}

func newBenchClientServerCache(b *testing.B) (c *Client, s *Server, cache *ybc.Cache) {
	return newBenchClientServerCache_Ext(0, 0, b)
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

	var keys [][]byte
	for i := 0; i < batchSize; i++ {
		keys = append(keys, key)
	}

	for i := 0; i < b.N; i += batchSize {
		if _, err := c.GetMulti(keys); err != nil {
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

func setNowait(buffersSize, maxPendingRequestsCount int, b *testing.B) {
	c, s, cache := newBenchClientServerCache_Ext(buffersSize, maxPendingRequestsCount, b)
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
}

func BenchmarkClientServer_SetNowait_256Buffers(b *testing.B) {
	setNowait(256, 0, b)
}

func BenchmarkClientServer_SetNowait_512Buffers(b *testing.B) {
	setNowait(512, 0, b)
}

func BenchmarkClientServer_SetNowait_1KBuffers(b *testing.B) {
	setNowait(1*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_2KBuffers(b *testing.B) {
	setNowait(2*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_4KBuffers(b *testing.B) {
	setNowait(4*1024, 0, b)
}

func BenchmarkClientServer_SetNowait_32PendingRequests(b *testing.B) {
	setNowait(0, 32, b)
}

func BenchmarkClientServer_SetNowait_64PendingRequests(b *testing.B) {
	setNowait(0, 64, b)
}

func BenchmarkClientServer_SetNowait_128PendingRequests(b *testing.B) {
	setNowait(0, 128, b)
}

func BenchmarkClientServer_SetNowait_256PendingRequests(b *testing.B) {
	setNowait(0, 256, b)
}

func BenchmarkClientServer_SetNowait_512PendingRequests(b *testing.B) {
	setNowait(0, 512, b)
}

func BenchmarkClientServer_SetNowait_1KPendingRequests(b *testing.B) {
	setNowait(0, 1024, b)
}

type WorkerFunc func(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B)

func concurrentOps(workerFunc WorkerFunc, workersCount int, b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

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
	concurrentOps(setWorker, workersCount, b)
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

func getWorker(c *Client, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	item := Item{
		Key:   []byte(fmt.Sprintf("key_%d", i)),
		Value: []byte(fmt.Sprintf("value_%d", i)),
	}
	if err := c.Set(&item); err != nil {
		b.Fatalf("Error when calling channel.Set(): [%s]", err)
	}
	for _ = range ch {
		if err := c.Get(&item); err != nil {
			b.Fatalf("Error when calling channel.Get(): [%s]", err)
		}
	}
}

func concurrentGet(workersCount int, b *testing.B) {
	concurrentOps(getWorker, workersCount, b)
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
