package main

import (
	"flag"
	"github.com/valyala/ybc/bindings/go/ybc"
	"github.com/valyala/ybc/libs/go/memcache"
	"log"
)

var (
	listenAddr      = flag.String("listenAddr", ":12221", "TCP address the daemon will listen to")
	maxItemsCount   = flag.Uint64("maxItemsCount", 1000*1000, "Maximum number of items to store in the cache")
	cacheSize       = flag.Uint64("cacheSize", 100*1000*1000, "Cache size in bytes")
	cacheFilePath   = flag.String("cacheFilePath", "", "Path to cache file. Leave empty for anonymous cache")
	readBufferSize  = flag.Int("readBufferSize", 4096, "Buffer size in bytes for incoming requests")
	writeBufferSize = flag.Int("writeBufferSize", 4096, "Buffer size in bytes for outgoing responses")
)

func main() {
	flag.Parse()

	config := ybc.NewConfig(ybc.SizeT(*maxItemsCount), ybc.SizeT(*cacheSize))
	defer config.Close()

	if *cacheFilePath != "" {
		config.SetDataFile(*cacheFilePath + ".data")
		config.SetIndexFile(*cacheFilePath + ".index")
	}

	cache, err := config.OpenCache(true)
	if err != nil {
		log.Fatal("Cannot open cache: [%s]", err)
	}
	defer cache.Close()

	s := &memcache.Server{
		Cache:           cache,
		ListenAddr:      *listenAddr,
		ReadBufferSize:  *readBufferSize,
		WriteBufferSize: *writeBufferSize,
	}
	if err := s.Serve(); err != nil {
		log.Fatal("Cannot serve traffic: [%s]", err)
	}
}
