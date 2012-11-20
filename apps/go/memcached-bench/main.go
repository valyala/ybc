// Benchmark for memcache servers.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/valyala/ybc/libs/go/memcache"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	serverAddrs             = flag.String("serverAddrs", ":11211", "Comma-delimited addresses of memcache servers to test")
	connectionsCount        = flag.Int("connectionsCount", 4, "The number of TCP connections to memcache server")
	goMaxProcs              = flag.Int("goMaxProcs", 4, "The maximum number of simultaneous worker threads in go")
	key                     = flag.String("key", "key", "The key to query in memcache")
	maxPendingRequestsCount = flag.Int("maxPendingRequestsCount", 1024, "Maximum number of pending requests")
	osReadBufferSize        = flag.Int("osReadBufferSize", 224*1024, "The size of read buffer in bytes in OS")
	osWriteBufferSize       = flag.Int("osWriteBufferSize", 224*1024, "The size of write buffer in bytes in OS")
	requestsCount           = flag.Int("requestsCount", 1000*1000, "The number of requests to send to memcache")
	readBufferSize          = flag.Int("readBufferSize", 4096, "The size of read buffer in bytes")
	value                   = flag.String("value", "value", "Value to store in memcache")
	workerMode              = flag.String("workerMode", "GetMiss", "Worker mode. May be 'GetMiss', 'GetHit', 'Set', 'GetSetRand'")
	workersCount            = flag.Int("workersCount", 512, "The number of workers to send requests to memcache")
	writeBufferSize         = flag.Int("writeBufferSize", 4096, "The size of write buffer in bytes")
)

func workerGetMiss(client memcache.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache.Item
	item.Key = []byte(*key)

	for _ = range ch {
		if err := client.Get(&item); err != memcache.ErrCacheMiss {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
	}
}

func workerGetHit(client memcache.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache.Item
	item.Key = []byte(*key)
	item.Value = []byte(*value)
	valueOrig := item.Value
	item.Value = nil

	for _ = range ch {
		if err := client.Get(&item); err != nil {
			log.Fatalf("Error in Client.Get(): [%s]", err)
		}
		if !bytes.Equal(valueOrig, item.Value) {
			log.Fatalf("Unexpected value read=[%s]. Expected=[%s]", item.Value, valueOrig)
		}
	}
}

func workerSet(client memcache.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache.Item
	for i := range ch {
		item.Key = []byte(fmt.Sprintf("%s_%d", *key, i))
		item.Value = []byte(fmt.Sprintf("%s_%d", *value, i))
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
	}
}

func workerGetSetRand(client memcache.Cacher, wg *sync.WaitGroup, ch <-chan int) {
	defer wg.Done()
	var item memcache.Item
	itemsCount := *requestsCount / *workersCount
	for _ = range ch {
		n := rand.Intn(itemsCount)
		item.Key = []byte(fmt.Sprintf("%s_%d", *key, n))
		v := []byte(fmt.Sprintf("%s_%d", *value, n))
		if rand.Float32() < 0.5 {
			err := client.Get(&item)
			if err == memcache.ErrCacheMiss {
				continue
			}
			if err != nil {
				log.Fatalf("Error in Client.Get(): [%s]", err)
			}
			if !bytes.Equal(item.Value, v) {
				log.Fatalf("Unexpected value=[%s] obtained. Expected [%s]", item.Value, v)
			}
		} else {
			item.Value = v
			if err := client.Set(&item); err != nil {
				log.Fatalf("Error in Client.Set(): [%s]", err)
			}
		}
	}
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	serverAddrs_ := strings.Split(*serverAddrs, ",")
	var client memcache.Cacher
	if len(serverAddrs_) < 2 {
		client = &memcache.Client{
			ServerAddr:              *serverAddrs,
			ConnectionsCount:        *connectionsCount,
			MaxPendingRequestsCount: *maxPendingRequestsCount,
			ReadBufferSize:          *readBufferSize,
			WriteBufferSize:         *writeBufferSize,
			OSReadBufferSize:        *osReadBufferSize,
			OSWriteBufferSize:       *osWriteBufferSize,
		}
		client.Start()
	} else {
		c := &memcache.DistributedClient{
			ConnectionsCount:        *connectionsCount,
			MaxPendingRequestsCount: *maxPendingRequestsCount,
			ReadBufferSize:          *readBufferSize,
			WriteBufferSize:         *writeBufferSize,
			OSReadBufferSize:        *osReadBufferSize,
			OSWriteBufferSize:       *osWriteBufferSize,
		}
		c.StartStatic(serverAddrs_)
		client = c
	}
	defer client.Stop()

	fmt.Printf("Config:\n")
	fmt.Printf("serverAddrs=[%s]\n", *serverAddrs)
	fmt.Printf("connectionsCount=[%d]\n", *connectionsCount)
	fmt.Printf("goMaxProcs=[%d]\n", *goMaxProcs)
	fmt.Printf("key=[%s]\n", *key)
	fmt.Printf("maxPendingRequestsCount=[%d]\n", *maxPendingRequestsCount)
	fmt.Printf("osReadBufferSize=[%d]\n", *osReadBufferSize)
	fmt.Printf("osWriteBufferSize=[%d]\n", *osWriteBufferSize)
	fmt.Printf("requestsCount=[%d]\n", *requestsCount)
	fmt.Printf("readBufferSize=[%d]\n", *readBufferSize)
	fmt.Printf("value=[%s]\n", *value)
	fmt.Printf("workerMode=[%s]\n", *workerMode)
	fmt.Printf("workersCount=[%d]\n", *workersCount)
	fmt.Printf("writeBufferSize=[%d]\n", *writeBufferSize)
	fmt.Printf("\n")

	fmt.Printf("Preparing...")
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

	item := memcache.Item{
		Key:   []byte(*key),
		Value: []byte(*value),
	}

	worker := workerGetMiss
	switch *workerMode {
	case "GetHit":
		if err := client.Set(&item); err != nil {
			log.Fatalf("Error in Client.Set(): [%s]", err)
		}
		worker = workerGetHit
	case "GetMiss":
		if err := client.Delete(item.Key); err != nil && err != memcache.ErrCacheMiss {
			log.Fatalf("Cannot delete item with key=[%s]: [%s]", item.Key, err)
		}
		worker = workerGetMiss
	case "Set":
		worker = workerSet
	case "GetSetRand":
		worker = workerGetSetRand
	default:
		log.Fatalf("Unknown workerMode=[%s]", *workerMode)
	}

	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go worker(client, &wg, ch)
	}
}
