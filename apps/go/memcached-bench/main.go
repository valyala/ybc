// Benchmark for memcache servers.
//
// Supports simultaneous benchmarking of multiple servers.
package main

import (
	"flag"
	"fmt"
	memcache_org "github.com/bradfitz/gomemcache/memcache"
	memcache_new "github.com/valyala/ybc/libs/go/memcache"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	clientType = flag.String("clientType", "new", "Client type. May be 'new' or 'original'.\n"+
		"'original' is https://github.com/bradfitz/gomemcache/tree/master/memcache,\n"+
		"'new' is https://github.com/valyala/ybc/tree/master/libs/go/memcache")
	connectionsCount = flag.Int("connectionsCount", 4, "The number of TCP connections to memcache server")
	getRatio         = flag.Float64("getRatio", 0.9, "Ratio of 'get' requests for workerMode=GetSetRand.\n"+
		"0.0 means 'no get requests'. 1.0 means 'no set requests'")
	goMaxProcs                = flag.Int("goMaxProcs", 4, "The maximum number of simultaneous worker threads in go")
	itemsCount                = flag.Int("itemsCount", 1000*1000, "The number of items in working set to test in 'GetSetRand' and 'Set' workerModes")
	keySize                   = flag.Int("keySize", 16, "Key size in bytes")
	maxPendingRequestsCount   = flag.Int("maxPendingRequestsCount", 1024, "Maximum number of pending requests. Makes sense only for clientType=new")
	maxResponseTime           = flag.Duration("maxResponseTime", time.Millisecond*20, "Maximum response time shown on response time histogram")
	osReadBufferSize          = flag.Int("osReadBufferSize", 224*1024, "The size of read buffer in bytes in OS. Makes sense only for clientType=new")
	osWriteBufferSize         = flag.Int("osWriteBufferSize", 224*1024, "The size of write buffer in bytes in OS. Makes sense only for clientType=new")
	requestsCount             = flag.Int("requestsCount", 1000*1000, "The number of requests to send to memcache")
	readBufferSize            = flag.Int("readBufferSize", 4096, "The size of read buffer in bytes. Makes sense only for clientType=new")
	responseTimeHistogramSize = flag.Int("responseTimeHistogramSize", 10, "The size of response time histogram")
	serverAddrs               = flag.String("serverAddrs", "localhost:11211", "Comma-delimited addresses of memcache servers to test")
	valueSize                 = flag.Int("valueSize", 200, "Value size in bytes")
	workerMode                = flag.String("workerMode", "GetMiss", "Worker mode. May be 'GetMiss', 'GetHit', 'Set', 'GetSetRand'")
	workersCount              = flag.Int("workersCount", 512, "The number of workers to send requests to memcache")
	writeBufferSize           = flag.Int("writeBufferSize", 4096, "The size of write buffer in bytes. Makes sense only for clientType=new")
)

var (
	key, value []byte

	maxHistogramResponseTime time.Duration
)

type Stats struct {
	responseTimeHistogram []uint32
	cacheMissCount        uint64
	cacheHitCount         uint64
}

func updateResponseTimeHistogram(responseTimeHistogram []uint32, startTime time.Time) {
	n := *responseTimeHistogramSize
	t := time.Since(startTime)
	if t > maxHistogramResponseTime {
		maxHistogramResponseTime = t
	}
	i := int(float64(t) / float64(*maxResponseTime) * float64(n))
	if i > n-1 {
		i = n - 1
	} else if i < 0 {
		i = 0
	}
	responseTimeHistogram[i]++
}

func dashBar(percent float64) string {
	return strings.Repeat("#", int(percent/100.0*60.0))
}

func printStats(stats []Stats) {
	n := *responseTimeHistogramSize
	var totalStats Stats
	totalStats.responseTimeHistogram = make([]uint32, n)
	for j := 0; j < *workersCount; j++ {
		for i := 0; i < n; i++ {
			totalStats.responseTimeHistogram[i] += stats[j].responseTimeHistogram[i]
		}
		totalStats.cacheMissCount += stats[j].cacheMissCount
		totalStats.cacheHitCount += stats[j].cacheHitCount
	}

	fmt.Printf("Response time histogram\n")
	interval := *maxResponseTime / time.Duration(n)
	var meanHistogramResponseTime float64
	for i := 0; i < n; i++ {
		startDuration := interval * time.Duration(i)
		endDuration := interval * time.Duration(i+1)
		if i == n-1 {
			endDuration = time.Hour
		}
		percent := float64(totalStats.responseTimeHistogram[i]) / float64(*requestsCount)
		meanHistogramResponseTime += float64(startDuration+interval/2) * percent
		percent *= 100.0
		fmt.Printf("%6.6s -%6.6s: %8.3f%% %s\n", startDuration, endDuration, percent, dashBar(percent))
	}
	fmt.Printf("Mean response time: %s\n", time.Duration(meanHistogramResponseTime))
	fmt.Printf("Max response time:  %s\n", maxHistogramResponseTime)
	fmt.Printf("Cache miss count: %10d\n", totalStats.cacheMissCount)
	fmt.Printf("Cache hit count:  %10d\n", totalStats.cacheHitCount)

	requestsCount := totalStats.cacheMissCount + totalStats.cacheHitCount
	cacheMissRatio := 0.0
	if requestsCount > 0 {
		cacheMissRatio = float64(totalStats.cacheMissCount) / float64(requestsCount)
	}
	fmt.Printf("Cache miss ratio: %10.3f%%\n", cacheMissRatio*100.0)
}

func workerGetMissOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()

	for _ = range ch {
		n := rand.Intn(*itemsCount)
		keyStr := fmt.Sprintf("miss_%s_%d", key, n)
		startTime := time.Now()
		if _, err := client.Get(keyStr); err != memcache_org.ErrCacheMiss {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
		stats.cacheMissCount++
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerGetMissNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_new.Item

	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = []byte(fmt.Sprintf("miss_%s_%d", key, n))
		startTime := time.Now()
		if err := client.Get(&item); err != memcache_new.ErrCacheMiss {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
		stats.cacheMissCount++
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerGetHitOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()

	for _ = range ch {
		n := rand.Intn(*itemsCount)
		keyStr := fmt.Sprintf("%s_%d", key, n)
		startTime := time.Now()
		_, err := client.Get(keyStr)
		if err == memcache_org.ErrCacheMiss {
			stats.cacheMissCount++
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
			continue
		}
		if err != nil {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
		stats.cacheHitCount++
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerGetHitNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_new.Item

	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = []byte(fmt.Sprintf("%s_%d", key, n))
		startTime := time.Now()
		err := client.Get(&item)
		if err == memcache_new.ErrCacheMiss {
			stats.cacheMissCount++
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
			continue
		}
		if err != nil {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
		stats.cacheHitCount++
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerSetOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_org.Item
	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = fmt.Sprintf("%s_%d", key, n)
		item.Value = value
		startTime := time.Now()
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerSetNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_new.Item
	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = []byte(fmt.Sprintf("%s_%d", key, n))
		item.Value = value
		startTime := time.Now()
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
		updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
	}
}

func workerGetSetRandOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_org.Item
	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = fmt.Sprintf("%s_%d", key, n)
		startTime := time.Now()
		if rand.Float64() < *getRatio {
			_, err := client.Get(item.Key)
			if err == memcache_org.ErrCacheMiss {
				stats.cacheMissCount++
				updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
				continue
			}
			if err != nil {
				log.Fatalf("Error in Client.Get(): [%s]", err)
			}
			stats.cacheHitCount++
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
		} else {
			item.Value = value
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
		}
	}
}

func workerGetSetRandNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()
	var item memcache_new.Item
	for _ = range ch {
		n := rand.Intn(*itemsCount)
		item.Key = []byte(fmt.Sprintf("%s_%d", key, n))
		startTime := time.Now()
		if rand.Float64() < *getRatio {
			err := client.Get(&item)
			if err == memcache_new.ErrCacheMiss {
				stats.cacheMissCount++
				updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
				continue
			}
			if err != nil {
				log.Fatalf("Error in Client.Get(): [%s]", err)
			}
			stats.cacheHitCount++
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
		} else {
			item.Value = value
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
			updateResponseTimeHistogram(stats.responseTimeHistogram, startTime)
		}
	}
}

var keyChars = []byte("1234567890qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM-+,./<>?;':\"[]{}=_()*&^%$#@!\\|`~")

func getRandomKey(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = keyChars[rand.Int()%len(keyChars)]
	}
	return buf
}

func getRandomValue(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte(rand.Int())
	}
	return buf
}

func precreateItemsOrg(client *memcache_org.Client) {
	var wg sync.WaitGroup
	defer wg.Wait()

	n := *itemsCount / *workersCount
	workerFunc := func(start int) {
		defer wg.Done()
		item := memcache_org.Item{
			Value: value,
		}
		for i := start; i < start+n; i++ {
			item.Key = fmt.Sprintf("%s_%d", key, i)
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
		}
	}
	for i := 0; i < *workersCount; i++ {
		go workerFunc(i * n)
	}
}

func precreateItemsNew(client memcache_new.Cacher) {
	item := memcache_new.Item{
		Value: value,
	}
	for i := 0; i < *itemsCount; i++ {
		item.Key = []byte(fmt.Sprintf("%s_%d", key, i))
		client.SetNowait(&item)
	}
}

func getWorkerOrg(serverAddrs_ []string) func(wg *sync.WaitGroup, ch chan int, stats *Stats) {
	client := memcache_org.New(serverAddrs_...)

	worker := workerGetMissOrg
	switch *workerMode {
	case "GetHit":
		precreateItemsOrg(client)
		worker = workerGetHitOrg
	case "GetMiss":
		worker = workerGetMissOrg
	case "Set":
		worker = workerSetOrg
	case "GetSetRand":
		precreateItemsOrg(client)
		worker = workerGetSetRandOrg
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}
	return func(wg *sync.WaitGroup, ch chan int, stats *Stats) {
		worker(client, wg, ch, stats)
	}
}

func getWorkerNew(serverAddrs_ []string) func(wg *sync.WaitGroup, ch chan int, stats *Stats) {
	config := memcache_new.ClientConfig{
		ConnectionsCount:        *connectionsCount,
		MaxPendingRequestsCount: *maxPendingRequestsCount,
		ReadBufferSize:          *readBufferSize,
		WriteBufferSize:         *writeBufferSize,
		OSReadBufferSize:        *osReadBufferSize,
		OSWriteBufferSize:       *osWriteBufferSize,
	}
	var client memcache_new.Cacher
	if len(serverAddrs_) < 2 {
		client = &memcache_new.Client{
			ServerAddr:   *serverAddrs,
			ClientConfig: config,
		}
		client.Start()
	} else {
		c := &memcache_new.DistributedClient{
			ClientConfig: config,
		}
		c.StartStatic(serverAddrs_)
		client = c
	}

	worker := workerGetMissNew
	switch *workerMode {
	case "GetHit":
		precreateItemsNew(client)
		worker = workerGetHitNew
	case "GetMiss":
		client.Delete(key)
		worker = workerGetMissNew
	case "Set":
		worker = workerSetNew
	case "GetSetRand":
		precreateItemsNew(client)
		worker = workerGetSetRandNew
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}
	return func(wg *sync.WaitGroup, ch chan int, stats *Stats) {
		worker(client, wg, ch, stats)
	}

}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(*goMaxProcs)

	serverAddrs_ := strings.Split(*serverAddrs, ",")

	fmt.Printf("Config:\n")
	fmt.Printf("clientType=[%s]\n", *clientType)
	fmt.Printf("connectionsCount=[%d]\n", *connectionsCount)
	fmt.Printf("getRatio=[%f]\n", *getRatio)
	fmt.Printf("goMaxProcs=[%d]\n", *goMaxProcs)
	fmt.Printf("itemsCount=[%d]\n", *itemsCount)
	fmt.Printf("keySize=[%d]\n", *keySize)
	fmt.Printf("maxPendingRequestsCount=[%d]\n", *maxPendingRequestsCount)
	fmt.Printf("maxResponseTime=[%s]\n", *maxResponseTime)
	fmt.Printf("osReadBufferSize=[%d]\n", *osReadBufferSize)
	fmt.Printf("osWriteBufferSize=[%d]\n", *osWriteBufferSize)
	fmt.Printf("requestsCount=[%d]\n", *requestsCount)
	fmt.Printf("readBufferSize=[%d]\n", *readBufferSize)
	fmt.Printf("responseTimeHistogramSize=[%d]\n", *responseTimeHistogramSize)
	fmt.Printf("serverAddrs=[%s]\n", *serverAddrs)
	fmt.Printf("valueSize=[%d]\n", *valueSize)
	fmt.Printf("workerMode=[%s]\n", *workerMode)
	fmt.Printf("workersCount=[%d]\n", *workersCount)
	fmt.Printf("writeBufferSize=[%d]\n", *writeBufferSize)
	fmt.Printf("\n")

	fmt.Printf("Preparing...")
	key = getRandomKey(*keySize)
	value = getRandomValue(*valueSize)

	ch := make(chan int, *requestsCount)
	for i := 0; i < *requestsCount; i++ {
		ch <- i
	}
	close(ch)

	stats := make([]Stats, *workersCount)
	for i := 0; i < *workersCount; i++ {
		stats[i].responseTimeHistogram = make([]uint32, *responseTimeHistogramSize)
	}

	wg := sync.WaitGroup{}
	var startTime time.Time
	defer func() {
		wg.Wait()
		duration := float64(time.Since(startTime)) / float64(time.Second)
		fmt.Printf("done! %.3f seconds, %.0f qps\n", duration, float64(*requestsCount)/duration)
		printStats(stats)
	}()

	var worker func(wg *sync.WaitGroup, ch chan int, stats *Stats)
	switch *clientType {
	case "original":
		worker = getWorkerOrg(serverAddrs_)
	case "new":
		worker = getWorkerNew(serverAddrs_)
	default:
		log.Fatalf("Unknown clientType=[%s]. Expected 'new' or 'original'", *clientType)
	}

	fmt.Printf("done\n")
	fmt.Printf("starting...")
	startTime = time.Now()
	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go worker(&wg, ch, &stats[i])
	}
}
