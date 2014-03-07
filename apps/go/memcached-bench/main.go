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
	numCpu = runtime.NumCPU()

	defaultConnectionsCount        = numCpu
	defaultMaxProcs                = numCpu
	defaultWorkersCount            = 1024 * numCpu
	defaultMaxPendingRequestsCount = defaultWorkersCount
)

var (
	clientType = flag.String("clientType", "new", "Client type. May be 'new' or 'original'.\n"+
		"'original' is https://github.com/bradfitz/gomemcache/tree/master/memcache,\n"+
		"'new' is https://github.com/valyala/ybc/tree/master/libs/go/memcache")
	connectionsCount = flag.Int("connectionsCount", defaultConnectionsCount, "The number of TCP connections to memcache server. Makes sense only for clientType=new")
	getRatio         = flag.Float64("getRatio", 0.9, "Ratio of 'get' requests for workerMode=GetSet.\n"+
		"0.0 means 'no get requests'. 1.0 means 'no set requests'")
	goMaxProcs                = flag.Int("goMaxProcs", defaultMaxProcs, "The maximum number of simultaneous worker threads in go")
	itemsCount                = flag.Int("itemsCount", 100*1000, "The number of items in working set")
	ioTimeout                 = flag.Duration("ioTimeout", time.Second*10, "Timeout for IO operations")
	keySize                   = flag.Int("keySize", 30, "Key size in bytes")
	maxPendingRequestsCount   = flag.Int("maxPendingRequestsCount", defaultMaxPendingRequestsCount, "Maximum number of pending requests. Makes sense only for clientType=new")
	maxResponseTime           = flag.Duration("maxResponseTime", time.Millisecond*20, "Maximum response time shown on response time histogram")
	osReadBufferSize          = flag.Int("osReadBufferSize", 224*1024, "The size of read buffer in bytes in OS. Makes sense only for clientType=new")
	osWriteBufferSize         = flag.Int("osWriteBufferSize", 224*1024, "The size of write buffer in bytes in OS. Makes sense only for clientType=new")
	requestsCount             = flag.Int("requestsCount", 1000*1000, "The number of requests to send to memcache")
	readBufferSize            = flag.Int("readBufferSize", 56*1024, "The size of read buffer in bytes. Makes sense only for clientType=new")
	responseTimeHistogramSize = flag.Int("responseTimeHistogramSize", 10, "The size of response time histogram")
	serverAddrs               = flag.String("serverAddrs", "localhost:11211", "Comma-delimited addresses of memcache servers to test")
	valueSize                 = flag.Int("valueSize", 200, "Value size in bytes")
	workerMode                = flag.String("workerMode", "GetMiss", "Worker mode. May be 'GetMiss', 'GetHit', 'Set', 'GetSet'")
	workersCount              = flag.Int("workersCount", defaultWorkersCount, "The number of workers to send requests to memcache")
	writeBufferSize           = flag.Int("writeBufferSize", 56*1024, "The size of write buffer in bytes. Makes sense only for clientType=new")
)

var (
	key, value []byte
)

type Stats struct {
	responseTimeHistogram []uint32
	totalResponseTime     time.Duration
	minResponseTime       time.Duration
	maxResponseTime       time.Duration
	cacheMissCount        uint32
	cacheHitCount         uint32
	errorsCount           uint32
}

func updateResponseTimeHistogram(stats *Stats, startTime time.Time) {
	n := *responseTimeHistogramSize
	t := time.Since(startTime)
	stats.totalResponseTime += t
	if t < stats.minResponseTime {
		stats.minResponseTime = t
	}
	if t > stats.maxResponseTime {
		stats.maxResponseTime = t
	}
	i := int(float64(t) / float64(*maxResponseTime) * float64(n))
	if i > n-1 {
		i = n - 1
	} else if i < 0 {
		i = 0
	}
	stats.responseTimeHistogram[i]++
}

func dashBar(percent float64) string {
	return strings.Repeat("#", int(percent/100.0*60.0))
}

func printStats(stats []Stats, startTime *time.Time) {
	fmt.Printf("done\n")
	testDuration := time.Since(*startTime)
	n := *responseTimeHistogramSize
	var totalStats Stats
	totalStats.responseTimeHistogram = make([]uint32, n)
	totalStats.minResponseTime = time.Hour * 24 * 365
	for i := 0; i < *workersCount; i++ {
		s := &stats[i]
		for j := 0; j < n; j++ {
			totalStats.responseTimeHistogram[j] += s.responseTimeHistogram[j]
		}
		totalStats.totalResponseTime += s.totalResponseTime
		if totalStats.minResponseTime > s.minResponseTime {
			totalStats.minResponseTime = s.minResponseTime
		}
		if totalStats.maxResponseTime < s.maxResponseTime {
			totalStats.maxResponseTime = s.maxResponseTime
		}
		totalStats.cacheMissCount += s.cacheMissCount
		totalStats.cacheHitCount += s.cacheHitCount
		totalStats.errorsCount += s.errorsCount
	}

	var totalRequestsCount uint32
	for i := 0; i < n; i++ {
		totalRequestsCount += totalStats.responseTimeHistogram[i]
	}
	if totalRequestsCount == 0 {
		fmt.Printf("There are no successful requests performed\n")
		return
	}

	var avgResponseTime time.Duration
	if totalRequestsCount > 0 {
		avgResponseTime = totalStats.totalResponseTime / time.Duration(totalRequestsCount)
	}

	var requestsPerSecond float64
	if testDuration > 0 {
		requestsPerSecond = float64(totalRequestsCount) / (float64(testDuration) / float64(time.Second))
	}

	fmt.Printf("Response time histogram\n")
	interval := *maxResponseTime / time.Duration(n)
	for i := 0; i < n; i++ {
		startDuration := interval * time.Duration(i)
		endDuration := interval * time.Duration(i+1)
		if i == n-1 {
			endDuration = time.Hour
		}
		percent := float64(totalStats.responseTimeHistogram[i]) / float64(totalRequestsCount)
		percent *= 100.0
		fmt.Printf("%6.6s -%6.6s: %8.3f%% %s\n", startDuration, endDuration, percent, dashBar(percent))
	}
	fmt.Printf("Requests per second: %10.0f\n", requestsPerSecond)
	fmt.Printf("Test duration:       %10s\n", testDuration)
	fmt.Printf("Avg response time:   %10s\n", avgResponseTime)
	fmt.Printf("Min response time:   %10s\n", totalStats.minResponseTime)
	fmt.Printf("Max response time:   %10s\n", totalStats.maxResponseTime)
	fmt.Printf("Cache miss count:    %10d\n", totalStats.cacheMissCount)
	fmt.Printf("Cache hit count:     %10d\n", totalStats.cacheHitCount)

	hitMissrequestsCount := totalStats.cacheMissCount + totalStats.cacheHitCount
	cacheMissRatio := 0.0
	if hitMissrequestsCount > 0 {
		cacheMissRatio = float64(totalStats.cacheMissCount) / float64(hitMissrequestsCount)
	}
	fmt.Printf("Cache miss ratio:    %10.3f%%\n", cacheMissRatio*100.0)
	fmt.Printf("Errors count:        %10d\n", totalStats.errorsCount)
}

func workerGetMissOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
	defer wg.Done()

	for _ = range ch {
		n := rand.Intn(*itemsCount)
		keyStr := fmt.Sprintf("miss_%s_%d", key, n)
		startTime := time.Now()
		if _, err := client.Get(keyStr); err != memcache_org.ErrCacheMiss {
			stats.errorsCount++
			continue
		}
		stats.cacheMissCount++
		updateResponseTimeHistogram(stats, startTime)
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
			stats.errorsCount++
			continue
		}
		stats.cacheMissCount++
		updateResponseTimeHistogram(stats, startTime)
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
			continue
		}
		if err != nil {
			stats.errorsCount++
			continue
		}
		stats.cacheHitCount++
		updateResponseTimeHistogram(stats, startTime)
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
			continue
		}
		if err != nil {
			stats.errorsCount++
			continue
		}
		stats.cacheHitCount++
		updateResponseTimeHistogram(stats, startTime)
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
			stats.errorsCount++
			continue
		}
		updateResponseTimeHistogram(stats, startTime)
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
			stats.errorsCount++
			continue
		}
		updateResponseTimeHistogram(stats, startTime)
	}
}

func workerGetSetOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
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
				continue
			}
			if err != nil {
				stats.errorsCount++
				continue
			}
			stats.cacheHitCount++
			updateResponseTimeHistogram(stats, startTime)
		} else {
			item.Value = value
			if err := client.Set(&item); err != nil {
				stats.errorsCount++
				continue
			}
			updateResponseTimeHistogram(stats, startTime)
		}
	}
}

func workerGetSetNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int, stats *Stats) {
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
				continue
			}
			if err != nil {
				stats.errorsCount++
				continue
			}
			stats.cacheHitCount++
			updateResponseTimeHistogram(stats, startTime)
		} else {
			item.Value = value
			if err := client.Set(&item); err != nil {
				stats.errorsCount++
				continue
			}
			updateResponseTimeHistogram(stats, startTime)
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
	n := *itemsCount / *workersCount
	workerFunc := func(wg *sync.WaitGroup, start int) {
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

	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go workerFunc(&wg, i*n)
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
	client.Timeout = *ioTimeout

	worker := workerGetMissOrg
	switch *workerMode {
	case "GetHit":
		precreateItemsOrg(client)
		worker = workerGetHitOrg
	case "GetMiss":
		worker = workerGetMissOrg
	case "Set":
		worker = workerSetOrg
	case "GetSet":
		precreateItemsOrg(client)
		worker = workerGetSetOrg
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
	case "GetSet":
		precreateItemsNew(client)
		worker = workerGetSetNew
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}
	return func(wg *sync.WaitGroup, ch chan int, stats *Stats) {
		worker(client, wg, ch, stats)
	}

}

func main() {
	flag.Parse()
	fmt.Printf("Config:\n")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("%s=%v\n", f.Name, f.Value)
	})
	fmt.Printf("\n")

	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(*goMaxProcs)

	serverAddrs_ := strings.Split(*serverAddrs, ",")

	fmt.Printf("Preparing...")
	key = getRandomKey(*keySize)
	value = getRandomValue(*valueSize)

	stats := make([]Stats, *workersCount)
	for i := 0; i < *workersCount; i++ {
		stats[i].responseTimeHistogram = make([]uint32, *responseTimeHistogramSize)
		stats[i].minResponseTime = time.Hour * 24 * 365
	}

	var startTime time.Time
	defer printStats(stats, &startTime)

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

	ch := make(chan int, 1000000)
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go worker(&wg, ch, &stats[i])
	}

	for i := 0; i < *requestsCount; i++ {
		ch <- i
	}
	close(ch)
}
