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
		"'original' is http://github.com/bradfitz/gomemcache/memcache,\n"+
		"'new' is http://github.com/valyala/ybc/libs/go/memcache")
	connectionsCount = flag.Int("connectionsCount", 4, "The number of TCP connections to memcache server")
	getRatio         = flag.Float64("getRatio", 0.9, "Ratio of 'get' requests for workerMode=GetSetRand.\n"+
		"0.0 means 'no get requests'. 1.0 means 'no set requests'")
	goMaxProcs              = flag.Int("goMaxProcs", 4, "The maximum number of simultaneous worker threads in go")
	keySize                 = flag.Int("keySize", 16, "Key size in bytes")
	maxPendingRequestsCount = flag.Int("maxPendingRequestsCount", 1024, "Maximum number of pending requests. Makes sense only for clientType=new")
	osReadBufferSize        = flag.Int("osReadBufferSize", 224*1024, "The size of read buffer in bytes in OS. Makes sense only for clientType=new")
	osWriteBufferSize       = flag.Int("osWriteBufferSize", 224*1024, "The size of write buffer in bytes in OS. Makes sense only for clientType=new")
	requestsCount           = flag.Int("requestsCount", 1000*1000, "The number of requests to send to memcache")
	readBufferSize          = flag.Int("readBufferSize", 4096, "The size of read buffer in bytes. Makes sense only for clientType=new")
	serverAddrs             = flag.String("serverAddrs", "localhost:11211", "Comma-delimited addresses of memcache servers to test")
	valueSize               = flag.Int("valueSize", 100, "Value size in bytes")
	workerMode              = flag.String("workerMode", "GetMiss", "Worker mode. May be 'GetMiss', 'GetHit', 'Set', 'GetSetRand'")
	workersCount            = flag.Int("workersCount", 512, "The number of workers to send requests to memcache")
	writeBufferSize         = flag.Int("writeBufferSize", 4096, "The size of write buffer in bytes. Makes sense only for clientType=new")
)

var (
	key, value []byte
)

func workerGetMissOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	keyStr := string(key)

	for _ = range ch {
		if _, err := client.Get(keyStr); err != memcache_org.ErrCacheMiss {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
	}
}

func workerGetMissNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_new.Item
	item.Key = key

	for _ = range ch {
		if err := client.Get(&item); err != memcache_new.ErrCacheMiss {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
	}
}

func workerGetHitOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	keyStr := string(key)

	for _ = range ch {
		if _, err := client.Get(keyStr); err != nil {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
	}
}

func workerGetHitNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_new.Item
	item.Key = key

	for _ = range ch {
		if err := client.Get(&item); err != nil {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
	}
}

func workerSetOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_org.Item
	for i := range ch {
		item.Key = fmt.Sprintf("%s_%d", key, i)
		item.Value = []byte(fmt.Sprintf("%s_%d", value, i))
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
	}
}

func workerSetNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_new.Item
	for i := range ch {
		item.Key = []byte(fmt.Sprintf("%s_%d", key, i))
		item.Value = []byte(fmt.Sprintf("%s_%d", value, i))
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
	}
}

func workerGetSetRandOrg(client *memcache_org.Client, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_org.Item
	itemsCount := *requestsCount / *workersCount
	for _ = range ch {
		n := rand.Intn(itemsCount)
		item.Key = fmt.Sprintf("%s_%d", key, n)
		if rand.Float64() < *getRatio {
			_, err := client.Get(item.Key)
			if err == memcache_org.ErrCacheMiss {
				continue
			}
			if err != nil {
				log.Fatalf("Error in Client.Get(): [%s]", err)
			}
		} else {
			item.Value = []byte(fmt.Sprintf("%s_%d", value, n))
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
		}
	}
}

func workerGetSetRandNew(client memcache_new.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache_new.Item
	itemsCount := *requestsCount / *workersCount
	for _ = range ch {
		n := rand.Intn(itemsCount)
		item.Key = []byte(fmt.Sprintf("%s_%d", key, n))
		if rand.Float64() < *getRatio {
			err := client.Get(&item)
			if err == memcache_new.ErrCacheMiss {
				continue
			}
			if err != nil {
				log.Fatalf("Error in Client.Get(): [%s]", err)
			}
		} else {
			item.Value = []byte(fmt.Sprintf("%s_%d", value, n))
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
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

func getWorkerOrg(serverAddrs_ []string, wg *sync.WaitGroup, ch chan int) func() {
	client := memcache_org.New(serverAddrs_...)
	keyStr := string(key)

	worker := workerGetMissOrg
	switch *workerMode {
	case "GetHit":
		item := memcache_org.Item{
			Key:   keyStr,
			Value: value,
		}
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
		worker = workerGetHitOrg
	case "GetMiss":
		client.Delete(keyStr)
		worker = workerGetMissOrg
	case "Set":
		worker = workerSetOrg
	case "GetSetRand":
		worker = workerGetSetRandOrg
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}
	return func() {
		worker(client, wg, ch)
	}
}

func getWorkerNew(serverAddrs_ []string, wg *sync.WaitGroup, ch chan int) func() {
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
			ServerAddr:                *serverAddrs,
			memcache_new.ClientConfig: config,
		}
		client.Start()
	} else {
		c := &memcache_new.DistributedClient{
			memcache_new.ClientConfig: config,
		}
		c.StartStatic(serverAddrs_)
		client = c
	}

	worker := workerGetMissNew
	switch *workerMode {
	case "GetHit":
		item := memcache_new.Item{
			Key:   key,
			Value: value,
		}
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
		worker = workerGetHitNew
	case "GetMiss":
		client.Delete(key)
		worker = workerGetMissNew
	case "Set":
		worker = workerSetNew
	case "GetSetRand":
		worker = workerGetSetRandNew
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}
	return func() {
		worker(client, wg, ch)
	}

}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	serverAddrs_ := strings.Split(*serverAddrs, ",")

	fmt.Printf("Config:\n")
	fmt.Printf("clientType=[%s]\n", *clientType)
	fmt.Printf("connectionsCount=[%d]\n", *connectionsCount)
	fmt.Printf("getRatio=[%f]\n", *getRatio)
	fmt.Printf("goMaxProcs=[%d]\n", *goMaxProcs)
	fmt.Printf("keySize=[%d]\n", *keySize)
	fmt.Printf("maxPendingRequestsCount=[%d]\n", *maxPendingRequestsCount)
	fmt.Printf("osReadBufferSize=[%d]\n", *osReadBufferSize)
	fmt.Printf("osWriteBufferSize=[%d]\n", *osWriteBufferSize)
	fmt.Printf("requestsCount=[%d]\n", *requestsCount)
	fmt.Printf("readBufferSize=[%d]\n", *readBufferSize)
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
	fmt.Printf("done\n")

	fmt.Printf("starting...")
	startTime := time.Now()
	wg := sync.WaitGroup{}
	defer func() {
		wg.Wait()
		duration := float64(time.Since(startTime)) / float64(time.Second)
		fmt.Printf("done! %.3f seconds, %.0f qps\n", duration, float64(*requestsCount)/duration)
	}()

	var worker func()
	switch *clientType {
	case "original":
		worker = getWorkerOrg(serverAddrs_, &wg, ch)
	case "new":
		worker = getWorkerNew(serverAddrs_, &wg, ch)
	default:
		log.Fatalf("Unknown clientType=[%s]. Expected 'new' or 'original'", *clientType)
	}
	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go worker()
	}
	wg.Wait()
}
