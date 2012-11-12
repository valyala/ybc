// Memcache server, which uses YBC library as caching backend.
//
// Thanks to YBC, the server has the following features missing
// in the original memcached:
//   * Cache content survives server restarts if it is backed by files.
//   * Cache size can exceed available RAM size by multiple orders of magnitude.
//     The server should remain fast until the total size of frequently accessed
//     items exceeds RAM size. It may remain relatively fast even if frequently
//     accessed items don't fit RAM if cache files are located on fast SSDs.
//   * The maximum value size is limited by 2Gb.
//   * The maximum key size is limited by 2Gb.
package main

import (
	"../../../bindings/go/ybc"
	"../../../libs/go/memcache"
	"flag"
	"log"
	"runtime"
	"time"
)

var (
	cacheFilePath     = flag.String("cacheFilePath", "", "Path to cache file. Leave empty for anonymous non-persistent cache")
	cacheSize         = flag.Uint64("cacheSize", 100*1000*1000, "Cache size in bytes")
	deHashtableSize   = flag.Int("deHashtableSize", 16, "Dogpile effect hashtable size")
	goMaxProcs        = flag.Int("goMaxProcs", 4, "Maximum number of simultaneous Go threads")
	hotDataSize       = flag.Uint64("hotDataSize", 0, "Hot data size in bytes. 0 disables hot data optimization")
	hotItemsCount     = flag.Uint64("hotItemsCount", 0, "The number of hot items. 0 disables hot items optimization")
	listenAddr        = flag.String("listenAddr", ":11211", "TCP address the server will listen to")
	maxItemsCount     = flag.Uint64("maxItemsCount", 1000*1000, "Maximum number of items the server can cache")
	syncInterval      = flag.Int("syncInterval", 0, "Sync interval in milliseconds. 0 disables data syncing")
	osReadBufferSize  = flag.Int("osReadBufferSize", 224*1024, "Buffer size in bytes for incoming requests in OS")
	osWriteBufferSize = flag.Int("osWriteBufferSize", 224*1024, "Buffer size in bytes for outgoing responses in OS")
	readBufferSize    = flag.Int("readBufferSize", 4096, "Buffer size in bytes for incoming requests")
	writeBufferSize   = flag.Int("writeBufferSize", 4096, "Buffer size in bytes for outgoing responses")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	syncInterval_ := time.Duration(*syncInterval) * time.Millisecond
	if syncInterval_ <= 0 {
		syncInterval_ = ybc.ConfigDisableSync
	}
	config := ybc.Config{
		MaxItemsCount:   ybc.SizeT(*maxItemsCount),
		DataFileSize:    ybc.SizeT(*cacheSize),
		HotItemsCount:   ybc.SizeT(*hotItemsCount),
		HotDataSize:     ybc.SizeT(*hotDataSize),
		DeHashtableSize: *deHashtableSize,
		SyncInterval:    syncInterval_,
	}

	if *cacheFilePath != "" {
		config.DataFile = *cacheFilePath + ".data"
		config.IndexFile = *cacheFilePath + ".index"
	}

	cache, err := config.OpenCache(true)
	if err != nil {
		log.Fatalf("Cannot open cache: [%s]", err)
	}
	defer cache.Close()

	s := memcache.Server{
		Cache:             cache,
		ListenAddr:        *listenAddr,
		ReadBufferSize:    *readBufferSize,
		WriteBufferSize:   *writeBufferSize,
		OSReadBufferSize:  *osReadBufferSize,
		OSWriteBufferSize: *osWriteBufferSize,
	}
	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot serve traffic: [%s]", err)
	}
}
