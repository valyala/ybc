// CDN booster
//
// This is a dumb HTTP proxy, which caches files obtained from upstreamHost.
//
// Currently go-cdn-booster has the following limitations:
//   * Supports only GET requests.
//   * Doesn't respect HTTP headers received from both the client and
//     the upstream host.
//   * Optimized for small static files aka images, js and css with sizes
//     not exceeding few Mb each.
//   * It caches all files without expiration time.
//     Actually this is a feature :)
//
// Thanks to YBC it has the following features:
//   * Should be extremely fast.
//   * Cached items survive CDN booster restart if backed by cacheFilesPath.
//   * Cache size isn't limited by RAM size.
//   * Optimized for SSDs and HDDs.
//   * Performance shouldn't depend on the number of cached items.
//   * It is deadly simple in configuration and maintenance.
//
package main

import (
	"flag"
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"io"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	cacheFilesPath = flag.String("cacheFilesPath", "",
		"Path to cache file. Leave empty for anonymous non-persistent cache.\n"+
			"Enumerate multiple files delimited by comma for creating a cluster of caches.\n"+
			"This can increase performance only if frequently accessed items don't fit RAM\n"+
			"and each cache file is located on a distinct physical storage.")
	cacheSize     = flag.Int("cacheSize", 100*1000*1000, "The total cache size in bytes")
	goMaxProcs    = flag.Int("goMaxProcs", runtime.NumCPU(), "Maximum number of simultaneous Go threads")
	listenAddr    = flag.String("listenAddr", ":8098", "TCP address to listen to")
	maxItemsCount = flag.Int("maxItemsCount", 100*1000, "The maximum number of items in the cache")
	upstreamHost  = flag.String("upstreamHost", "www.google.com", "Upstream host to proxy data from")
)

type ProxyHandler struct {
	UpstreamHost string
	Cache        ybc.Cacher
}

func (ph *ProxyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	respH := w.Header()
	respH.Set("Server", "go-cdn-booster")
	respH.Set("ETag", "W/\"CacheForever\"")

	if req.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	reqH := req.Header
	if reqH.Get("If-None-Match") != "" {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	key := []byte(req.RequestURI)
	item, err := ph.Cache.GetDeItem(key, time.Second)
	if err != nil {
		if err != ybc.ErrCacheMiss {
			log.Fatalf("Unexpeced error=[%s] when obtaining cache value by key=[%s]\n", err, key)
		}

		item = fetchFromUpstream(ph.Cache, w, ph.UpstreamHost, key)
		if item == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
	defer item.Close()

	contentType, err := loadContentType(item)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	respH.Set("Cache-Control", "public, max-age=31536000")
	respH.Set("Content-Length", fmt.Sprintf("%d", item.Available()))
	respH.Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	if _, err = io.Copy(w, item); err != nil {
		log.Printf("Error=[%s] when sending value for key=[%s] to client\n", err, key)
	}
}

func fetchFromUpstream(cache ybc.Cacher, w http.ResponseWriter, upstreamHost string, key []byte) *ybc.Item {
	requestUrl := fmt.Sprintf("http://%s%s", upstreamHost, key)
	resp, err := http.Get(requestUrl)
	if err != nil {
		log.Printf("Error=[%s] when doing request to %s\n", err, requestUrl)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Unexpected status code=[%d]. request to %s\n", resp.StatusCode, requestUrl)
		return nil
	}

	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		log.Printf("Cannot cache response for requestUrl=[%s] without content-length\n", requestUrl)
		return nil
	}
	contentLengthN, err := strconv.Atoi(contentLength)
	if err != nil {
		log.Printf("Error=[%s] when parsing contentLength=[%s] for request to [%s]\n", err, contentLength, requestUrl)
		return nil
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	itemSize := contentLengthN + len(contentType) + 1
	txn, err := cache.NewSetTxn(key, itemSize, ybc.MaxTtl)
	if err != nil {
		log.Printf("Error=[%s] when starting set txn for key=[%s]. itemSize=[%d]\n", err, key, itemSize)
		return nil
	}

	if err = storeContentType(txn, contentType); err != nil {
		log.Printf("Cannot store content-type for key=[%s] in cache\n", key)
		txn.Rollback()
		return nil
	}

	n, err := txn.ReadFrom(resp.Body)
	if err != nil {
		log.Printf("Error=[%s] when copying body with size=%d to cache. key=[%s]\n", err, contentLengthN, key)
		txn.Rollback()
		return nil
	}
	if n != int64(contentLengthN) {
		log.Printf("Unexpected number of bytes copied=%d from response to requestUrl=[%s]. Expected %d\n", n, requestUrl, contentLengthN)
		txn.Rollback()
		return nil
	}
	item, err := txn.CommitItem()
	if err != nil {
		log.Printf("Error=[%s] when commiting set txn for key=[%s]\n", err, key)
		return nil
	}
	return item
}

func storeContentType(w io.Writer, contentType string) (err error) {
	strBuf := []byte(contentType)
	strSize := len(strBuf)
	if strSize > 255 {
		log.Printf("Too long content-type=[%s]. Its length=%d should fit one byte\n", contentType, strSize)
		err = fmt.Errorf("Too long content-type")
		return
	}
	var sizeBuf [1]byte
	sizeBuf[0] = byte(strSize)
	if _, err = w.Write(sizeBuf[:]); err != nil {
		log.Printf("Error=[%s] when storing content-type length\n", err)
		return
	}
	if _, err = w.Write(strBuf); err != nil {
		log.Printf("Error=[%s] when writing content-type string with length=%d\n", err, strSize)
		return
	}
	return
}

func loadContentType(r io.Reader) (contentType string, err error) {
	var sizeBuf [1]byte
	if _, err = r.Read(sizeBuf[:]); err != nil {
		log.Printf("Error=[%s] when extracting content-type length\n", err)
		return
	}
	strSize := int(sizeBuf[0])
	strBuf := make([]byte, strSize)
	if _, err = r.Read(strBuf); err != nil {
		log.Printf("Error=[%s] when extracting content-type string with length=%d\n", err, strSize)
		return
	}
	contentType = string(strBuf)
	return
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	config := ybc.Config{
		MaxItemsCount: ybc.SizeT(*maxItemsCount),
		DataFileSize:  ybc.SizeT(*cacheSize),
	}

	var cache ybc.Cacher
	var err error

	cacheFilesPath_ := strings.Split(*cacheFilesPath, ",")
	cacheFilesCount := len(cacheFilesPath_)
	log.Printf("Opening data files. This can take a while for the first time if files are big\n")
	if cacheFilesCount < 2 {
		if cacheFilesPath_[0] != "" {
			config.DataFile = cacheFilesPath_[0] + ".cdn-booster.data"
			config.IndexFile = cacheFilesPath_[0] + ".cdn-booster.index"
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
			cfg.DataFile = cacheFilesPath_[i] + ".cdn-booster.data"
			cfg.IndexFile = cacheFilesPath_[i] + ".cdn-booster.index"
			configs[i] = &cfg
		}
		cache, err = configs.OpenCluster(true)
		if err != nil {
			log.Fatalf("Cannot open cache cluster: [%s]", err)
		}
	}
	defer cache.Close()
	log.Printf("Data files have been opened\n")

	h := &ProxyHandler{
		UpstreamHost: *upstreamHost,
		Cache:        cache,
	}
	if err := http.ListenAndServe(*listenAddr, h); err != nil {
		log.Fatalf("Error=[%s] when starting proxy at listenAddr=[%s]\n", err, *listenAddr)
	}
}
