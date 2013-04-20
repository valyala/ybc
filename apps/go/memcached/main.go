// Memcache server, which uses YBC library as caching backend.
//
// Thanks to YBC, the server has the following features missing
// in the original memcached:
//   * Cache content survives server restarts if it is backed by files.
//   * Cache size may exceed available RAM size by multiple orders of magnitude.
//     The server should remain fast until the total size of frequently accessed
//     items exceeds RAM size. It may remain relatively fast even if frequently
//     accessed items don't fit RAM if cache files are located on fast SSDs.
//   * The maximum value size is limited by 2Gb.
//   * There is no 250 byte limit on key size.
//   * Support for 'dogpile effect' handling - see Client.GetDe()
//     at github.com/valyala/ybc/libs/go/memcache for details.
//   * Support for 'conditional get' command - see Client.Cget()
//     at github.com/valyala/ybc/libs/go/memcache for details.
package main

import (
	"flag"
	"github.com/valyala/ybc/bindings/go/ybc"
	"github.com/valyala/ybc/libs/go/memcache"
	"log"
	"runtime"
	"strings"
	"time"
)

var (
	numCpu          = runtime.NumCPU()
	defaultMaxProcs = 2 * numCpu
)

var (
	cacheFilesPath = flag.String("cacheFilesPath", "",
		"Path to cache file. Leave empty for anonymous non-persistent cache.\n"+
			"Enumerate multiple files delimited by comma for creating a cluster of caches.\n"+
			"This can increase performance only if frequently accessed items don't fit RAM\n"+
			"and each cache file is located on a distinct physical storage.")
	cacheSize         = flag.Uint64("cacheSize", 64, "Total cache capacity in Megabytes")
	deHashtableSize   = flag.Int("deHashtableSize", 16, "Dogpile effect hashtable size")
	goMaxProcs        = flag.Int("goMaxProcs", defaultMaxProcs, "Maximum number of simultaneous Go threads")
	hotDataSize       = flag.Uint64("hotDataSize", 0, "Hot data size in bytes. 0 disables hot data optimization")
	hotItemsCount     = flag.Uint64("hotItemsCount", 0, "The number of hot items. 0 disables hot items optimization")
	listenAddr        = flag.String("listenAddr", ":11211", "TCP address the server will listen to")
	maxItemsCount     = flag.Uint64("maxItemsCount", 1000*1000, "Maximum number of items the server can cache")
	syncInterval      = flag.Duration("syncInterval", time.Second*10, "Interval for data syncing. 0 disables data syncing")
	osReadBufferSize  = flag.Int("osReadBufferSize", 224*1024, "Buffer size in bytes for incoming requests in OS")
	osWriteBufferSize = flag.Int("osWriteBufferSize", 224*1024, "Buffer size in bytes for outgoing responses in OS")
	readBufferSize    = flag.Int("readBufferSize", 56*1024, "Buffer size in bytes for incoming requests")
	writeBufferSize   = flag.Int("writeBufferSize", 56*1024, "Buffer size in bytes for outgoing responses")
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	syncInterval_ := *syncInterval
	if syncInterval_ <= 0 {
		syncInterval_ = ybc.ConfigDisableSync
	}
	config := ybc.Config{
		MaxItemsCount:   ybc.SizeT(*maxItemsCount),
		DataFileSize:    ybc.SizeT(*cacheSize) * ybc.SizeT(1024*1024),
		HotItemsCount:   ybc.SizeT(*hotItemsCount),
		HotDataSize:     ybc.SizeT(*hotDataSize),
		DeHashtableSize: *deHashtableSize,
		SyncInterval:    syncInterval_,
	}

	var cache ybc.Cacher
	var err error

	cacheFilesPath_ := strings.Split(*cacheFilesPath, ",")
	cacheFilesCount := len(cacheFilesPath_)
	log.Printf("Opening data files. This can take a while for the first time if files are big")
	if cacheFilesCount < 2 {
		if cacheFilesPath_[0] != "" {
			config.DataFile = cacheFilesPath_[0] + ".data"
			config.IndexFile = cacheFilesPath_[0] + ".index"
		}
		cache, err = config.OpenCache(true)
		if err != nil {
			log.Fatalf("Cannot open cache: [%s]", err)
		}
	} else if cacheFilesCount > 1 {
		config.MaxItemsCount /= ybc.SizeT(cacheFilesCount)
		config.DataFileSize /= ybc.SizeT(cacheFilesCount)
		var configs ybc.ClusterConfig
		configs = make([]*ybc.Config, cacheFilesCount)
		for i := 0; i < cacheFilesCount; i++ {
			cfg := config
			cfg.DataFile = cacheFilesPath_[i] + ".data"
			cfg.IndexFile = cacheFilesPath_[i] + ".index"
			configs[i] = &cfg
		}
		cache, err = configs.OpenCluster(true)
		if err != nil {
			log.Fatalf("Cannot open cache cluster: [%s]", err)
		}
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
	log.Printf("Starting the server")
	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot serve traffic: [%s]", err)
	}
}
