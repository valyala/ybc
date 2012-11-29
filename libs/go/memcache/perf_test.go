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

func newBenchCachingClientServerCache(b *testing.B) (cc *CachingClient, s *Server, cache *ybc.Cache) {
	c, s, cache := newBenchClientServerCache(b)

	config := ybc.Config{
		MaxItemsCount: 1000 * 1000,
		DataFileSize:  10 * 1000 * 1000,
	}

	localCache, err := config.OpenCache(true)
	if err != nil {
		b.Fatal(err)
	}

	cc = &CachingClient{
		Client: c,
		Cache:  localCache,
	}
	return
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

type WorkerFunc func(c MemcacherDe, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B)

type SetupFunc func(c MemcacherDe)

func concurrentOps(setupFunc SetupFunc, workerFunc WorkerFunc, workersCount int, c MemcacherDe, b *testing.B) {
	if setupFunc != nil {
		setupFunc(c)
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	ch := make(chan int, b.N)
	defer close(ch)
	for i := 0; i < workersCount; i++ {
		wg.Add(1)
		go workerFunc(c, ch, &wg, i, b)
	}

	for i := 0; i < b.N; i++ {
		ch <- i
	}
}

func concurrentOpsForCachingClient(setupFunc SetupFunc, workerFunc WorkerFunc, workersCount int, b *testing.B) {
	c, s, cache := newBenchCachingClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Cache.Close()
	defer c.Client.(Cacher).Stop()

	concurrentOps(setupFunc, workerFunc, workersCount, c, b)
}

func concurrentOpsForClient(setupFunc SetupFunc, workerFunc WorkerFunc, workersCount int, b *testing.B) {
	c, s, cache := newBenchClientServerCache(b)
	defer cache.Close()
	defer s.Stop()
	defer c.Stop()

	concurrentOps(setupFunc, workerFunc, workersCount, c, b)
}

func setWorker(c MemcacherDe, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	var item Item
	item.Key = []byte(fmt.Sprintf("key_%d", i))
	item.Value = []byte(fmt.Sprintf("value_%d", i))

	for _ = range ch {
		if err := c.Set(&item); err != nil {
			b.Fatalf("Error when calling channel.Set(): [%s]", err)
		}
	}
}

func concurrentSet(workersCount int, b *testing.B) {
	concurrentOpsForClient(nil, setWorker, workersCount, b)
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

func concurrentSetForCachingClient(workersCount int, b *testing.B) {
	concurrentOpsForCachingClient(nil, setWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_1Workers(b *testing.B) {
	concurrentSetForCachingClient(1, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_2Workers(b *testing.B) {
	concurrentSetForCachingClient(2, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_4Workers(b *testing.B) {
	concurrentSetForCachingClient(4, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_8Workers(b *testing.B) {
	concurrentSetForCachingClient(8, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_16Workers(b *testing.B) {
	concurrentSetForCachingClient(16, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_32Workers(b *testing.B) {
	concurrentSetForCachingClient(32, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_64Workers(b *testing.B) {
	concurrentSetForCachingClient(64, b)
}

func BenchmarkCachingClientServer_ConcurrentSet_128Workers(b *testing.B) {
	concurrentSetForCachingClient(128, b)
}

func getWorker(c MemcacherDe, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	var item Item
	item.Key = []byte(fmt.Sprintf("key_%d", i))
	for _ = range ch {
		if err := c.Get(&item); err != nil {
			b.Fatalf("Error when calling channel.Get(): [%s]", err)
		}
	}
}

func concurrentGet(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForClient(setupFunc, getWorker, workersCount, b)
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

func concurrentGetForCachingClient(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForCachingClient(setupFunc, getWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_1Workers(b *testing.B) {
	concurrentGetForCachingClient(1, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_2Workers(b *testing.B) {
	concurrentGetForCachingClient(2, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_4Workers(b *testing.B) {
	concurrentGetForCachingClient(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_8Workers(b *testing.B) {
	concurrentGetForCachingClient(8, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_16Workers(b *testing.B) {
	concurrentGetForCachingClient(16, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_32Workers(b *testing.B) {
	concurrentGetForCachingClient(32, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_64Workers(b *testing.B) {
	concurrentGetForCachingClient(64, b)
}

func BenchmarkCachingClientServer_ConcurrentGet_128Workers(b *testing.B) {
	concurrentGetForCachingClient(128, b)
}

func concurrentGetForCachingClientWithValidateTtl(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		validateTtl := time.Millisecond * 100
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.(*CachingClient).SetWithValidateTtl(&item, validateTtl); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForCachingClient(setupFunc, getWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_1Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(1, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_2Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_4Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_8Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(8, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_16Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(16, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_32Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(32, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_64Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(64, b)
}

func BenchmarkCachingClientServer_ConcurrentGetWithValidateTtl_128Workers(b *testing.B) {
	concurrentGetForCachingClientWithValidateTtl(128, b)
}

func getDeWorker(c MemcacherDe, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	var item Item
	item.Key = []byte(fmt.Sprintf("key_%d", i))
	grace := 100 * time.Millisecond
	for _ = range ch {
		if err := c.GetDe(&item, grace); err != nil {
			b.Fatalf("Error when calling channel.GetDe(): [%s]", err)
		}
	}
}

func concurrentGetDe(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForClient(setupFunc, getDeWorker, workersCount, b)
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

func concurrentGetDeWithValidateTtl(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		validateTtl := time.Millisecond * 100
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.(*CachingClient).SetWithValidateTtl(&item, validateTtl); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForCachingClient(setupFunc, getDeWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_1Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(1, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_2Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(2, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_4Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_8Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(8, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_16Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(16, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_32Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(32, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_64Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(64, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDeWithValidateTtl_128Workers(b *testing.B) {
	concurrentGetDeWithValidateTtl(128, b)
}

func concurrentGetDeForCachingClient(workersCount int, b *testing.B) {
	setupFunc := func(c MemcacherDe) {
		var item Item
		for i := 0; i < workersCount; i++ {
			item.Key = []byte(fmt.Sprintf("key_%d", i))
			item.Value = []byte(fmt.Sprintf("value_%d", i))
			if err := c.Set(&item); err != nil {
				b.Fatalf("Error when calling channel.Set(): [%s]", err)
			}
		}
	}
	concurrentOpsForCachingClient(setupFunc, getDeWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_1Workers(b *testing.B) {
	concurrentGetDeForCachingClient(1, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_2Workers(b *testing.B) {
	concurrentGetDeForCachingClient(2, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_4Workers(b *testing.B) {
	concurrentGetDeForCachingClient(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_8Workers(b *testing.B) {
	concurrentGetDeForCachingClient(8, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_16Workers(b *testing.B) {
	concurrentGetDeForCachingClient(16, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_32Workers(b *testing.B) {
	concurrentGetDeForCachingClient(32, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_64Workers(b *testing.B) {
	concurrentGetDeForCachingClient(64, b)
}

func BenchmarkCachingClientServer_ConcurrentGetDe_128Workers(b *testing.B) {
	concurrentGetDeForCachingClient(128, b)
}

var keyChars = []byte("0123456789qwertyuiopasdfghjklzxcvbnm")

func randFill(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = keyChars[rand.Int()%len(keyChars)]
	}
}

func getSetWorker(c MemcacherDe, ch <-chan int, wg *sync.WaitGroup, i int, b *testing.B) {
	defer wg.Done()
	var item Item
	item.Key = make([]byte, 16)
	item.Value = make([]byte, 16)
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
	concurrentOpsForClient(nil, getSetWorker, workersCount, b)
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

func concurrentGetSetForCachingClient(workersCount int, b *testing.B) {
	concurrentOpsForCachingClient(nil, getSetWorker, workersCount, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_1Workers(b *testing.B) {
	concurrentGetSetForCachingClient(1, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_2Workers(b *testing.B) {
	concurrentGetSetForCachingClient(2, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_4Workers(b *testing.B) {
	concurrentGetSetForCachingClient(4, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_8Workers(b *testing.B) {
	concurrentGetSetForCachingClient(8, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_16Workers(b *testing.B) {
	concurrentGetSetForCachingClient(16, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_32Workers(b *testing.B) {
	concurrentGetSetForCachingClient(32, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_64Workers(b *testing.B) {
	concurrentGetSetForCachingClient(64, b)
}

func BenchmarkCachingClientServer_ConcurrentGetSet_128Workers(b *testing.B) {
	concurrentGetSetForCachingClient(128, b)
}
