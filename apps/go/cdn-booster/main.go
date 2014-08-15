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
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"github.com/vharitonsky/iniflags"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	cacheFilesPath = flag.String("cacheFilesPath", "",
		"Path to cache file. Leave empty for anonymous non-persistent cache.\n"+
			"Enumerate multiple files delimited by comma for creating a cluster of caches.\n"+
			"This can increase performance only if frequently accessed items don't fit RAM\n"+
			"and each cache file is located on a distinct physical storage.")
	cacheSize            = flag.Int("cacheSize", 100, "The total cache size in Mbytes")
	goMaxProcs           = flag.Int("goMaxProcs", runtime.NumCPU(), "Maximum number of simultaneous Go threads")
	httpsCertFile        = flag.String("httpsCertFile", "/etc/ssl/certs/ssl-cert-snakeoil.pem", "Path to HTTPS server certificate. Used only if listenHttpsAddr is set")
	httpsKeyFile         = flag.String("httpsKeyFile", "/etc/ssl/private/ssl-cert-snakeoil.key", "Path to HTTPS server key. Used only if listenHttpsAddr is set")
	httpsListenAddrs     = flag.String("httpsListenAddrs", "", "A list of TCP addresses to listen to HTTPS requests. Leave empty if you don't need https")
	listenAddrs          = flag.String("listenAddrs", ":8098", "A list of TCP addresses to listen to HTTP requests. Leave empty if you don't need http")
	maxConnsPerIp        = flag.Int("maxConnsPerIp", 32, "The maximum number of concurrent connections from a single ip")
	maxIdleUpstreamConns = flag.Int("maxIdleUpstreamConns", 50, "The maximum idle connections to upstream host")
	maxItemsCount        = flag.Int("maxItemsCount", 100*1000, "The maximum number of items in the cache")
	readBufferSize       = flag.Int("readBufferSize", 1024, "The size of read buffer for incoming connections")
	statsRequestPath     = flag.String("statsRequestPath", "/static_proxy_stats", "Path to page with statistics")
	upstreamHost         = flag.String("upstreamHost", "www.google.com", "Upstream host to proxy data from. May include port in the form 'host:port'")
	useClientRequestHost = flag.Bool("useClientRequestHost", false, "If set to true, then use 'Host' header from client requests in requests to upstream host. Otherwise use upstreamHost as a 'Host' header in upstream requests")
	writeBufferSize      = flag.Int("writeBufferSize", 4096, "The size of write buffer for incoming connections")
)

var (
	ifNoneMatchResponseHeader         = []byte("HTTP/1.1 304 Not Modified\nServer: go-cdn-booster\nEtag: W/\"CacheForever\"\n\n")
	internalServerErrorResponseHeader = []byte("HTTP/1.1 500 Internal Server Error\nServer: go-cdn-booster\n\n")
	notAllowedResponseHeader          = []byte("HTTP/1.1 405 Method Not Allowed\nServer: go-cdn-booster\n\n")
	okResponseHeader                  = []byte("HTTP/1.1 200 OK\nServer: go-cdn-booster\nCache-Control: public, max-age=31536000\nETag: W/\"CacheForever\"\n")
	serviceUnavailableResponseHeader  = []byte("HTTP/1.1 503 Service Unavailable\nServer: go-cdn-booster\n\n")
	statsResponseHeader               = []byte("HTTP/1.1 200 OK\nServer: go-cdn-booster\nContent-Type: text/plain\n\n")
)

var (
	cache            ybc.Cacher
	perIpConnTracker = createPerIpConnTracker()
	stats            Stats
	upstreamClient   http.Client
)

func main() {
	iniflags.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	cache = createCache()
	defer cache.Close()

	upstreamClient = http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *maxIdleUpstreamConns,
		},
	}

	var addr string
	for _, addr = range strings.Split(*httpsListenAddrs, ",") {
		go serveHttps(addr)
	}
	for _, addr = range strings.Split(*listenAddrs, ",") {
		go serveHttp(addr)
	}

	waitForeverCh := make(chan int)
	<-waitForeverCh
}

func createCache() ybc.Cacher {
	config := ybc.Config{
		MaxItemsCount: ybc.SizeT(*maxItemsCount),
		DataFileSize:  ybc.SizeT(*cacheSize) * ybc.SizeT(1024*1024),
	}

	var err error
	var cache ybc.Cacher

	cacheFilesPath_ := strings.Split(*cacheFilesPath, ",")
	cacheFilesCount := len(cacheFilesPath_)
	logMessage("Opening data files. This can take a while for the first time if files are big")
	if cacheFilesCount < 2 {
		if cacheFilesPath_[0] != "" {
			config.DataFile = cacheFilesPath_[0] + ".cdn-booster.data"
			config.IndexFile = cacheFilesPath_[0] + ".cdn-booster.index"
		}
		cache, err = config.OpenCache(true)
		if err != nil {
			logFatal("Cannot open cache: [%s]", err)
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
			logFatal("Cannot open cache cluster: [%s]", err)
		}
	}
	logMessage("Data files have been opened")
	return cache
}

func serveHttps(addr string) {
	if addr == "" {
		return
	}
	cert, err := tls.LoadX509KeyPair(*httpsCertFile, *httpsKeyFile)
	if err != nil {
		logFatal("Cannot load certificate: [%s]", err)
	}
	c := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	ln := tls.NewListener(listen(addr), c)
	logMessage("Listening https on [%s]", addr)
	serve(ln)
}

func serveHttp(addr string) {
	if addr == "" {
		return
	}
	ln := listen(addr)
	logMessage("Listening http on [%s]", addr)
	serve(ln)
}

func listen(addr string) net.Listener {
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		logFatal("Cannot listen [%s]: [%s]", addr, err)
	}
	return ln
}

func serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				logMessage("Cannot accept connections due to temporary network error: [%s]", err)
				time.Sleep(time.Second)
				continue
			}
			logFatal("Cannot accept connections due to permanent error: [%s]", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	clientAddr := conn.RemoteAddr().(*net.TCPAddr).IP.To4()
	ipUint32 := ip4ToUint32(clientAddr)
	if perIpConnTracker.registerIp(ipUint32) > *maxConnsPerIp {
		logMessage("Too many concurrent connections (more than %d) from ip=%s. Denying new connection from the ip", *maxConnsPerIp, clientAddr)
		perIpConnTracker.unregisterIp(ipUint32)
		return
	}
	defer perIpConnTracker.unregisterIp(ipUint32)

	r := bufio.NewReaderSize(conn, *readBufferSize)
	w := bufio.NewWriterSize(conn, *writeBufferSize)
	clientAddrStr := clientAddr.String()
	for {
		req, err := http.ReadRequest(r)
		if err != nil {
			if err != io.EOF {
				logMessage("Error when reading http request from ip=%s: [%s]", clientAddr, err)
			}
			return
		}
		req.RemoteAddr = clientAddrStr
		ok := handleRequest(req, w)
		w.Flush()
		if !ok || !req.ProtoAtLeast(1, 1) || req.Header.Get("Connection") == "close" {
			return
		}
	}
}

func handleRequest(req *http.Request, w io.Writer) bool {
	if req.Method != "GET" {
		w.Write(notAllowedResponseHeader)
		return false
	}

	if req.RequestURI == *statsRequestPath {
		w.Write(statsResponseHeader)
		stats.WriteToStream(w)
		return false
	}

	if req.Header.Get("If-None-Match") != "" {
		_, err := w.Write(ifNoneMatchResponseHeader)
		atomic.AddInt64(&stats.IfNoneMatchHitsCount, 1)
		return err == nil
	}

	key := append([]byte(getRequestHost(req)), []byte(req.RequestURI)...)
	item, err := cache.GetDeItem(key, time.Second)
	if err != nil {
		if err != ybc.ErrCacheMiss {
			logFatal("Unexpected error when obtaining cache value by key=[%s]: [%s]", key, err)
		}

		atomic.AddInt64(&stats.CacheMissesCount, 1)
		item = fetchFromUpstream(req, key)
		if item == nil {
			w.Write(serviceUnavailableResponseHeader)
			return false
		}
	} else {
		atomic.AddInt64(&stats.CacheHitsCount, 1)
	}
	defer item.Close()

	contentType, err := loadContentType(req, item)
	if err != nil {
		w.Write(internalServerErrorResponseHeader)
		return false
	}

	if _, err = w.Write(okResponseHeader); err != nil {
		return false
	}
	if _, err = fmt.Fprintf(w, "Content-Type: %s\nContent-Length: %d\n\n", contentType, item.Available()); err != nil {
		return false
	}
	var bytesSent int64
	if bytesSent, err = item.WriteTo(w); err != nil {
		logRequestError(req, "Cannot send file with key=[%s] to client: %s", key, err)
		return false
	}
	atomic.AddInt64(&stats.BytesSentToClients, bytesSent)
	return true
}

func fetchFromUpstream(req *http.Request, key []byte) *ybc.Item {
	upstreamUrl := fmt.Sprintf("http://%s%s", *upstreamHost, req.RequestURI)
	upstreamReq, err := http.NewRequest("GET", upstreamUrl, nil)
	if err != nil {
		logRequestError(req, "Cannot create request structure for [%s]: [%s]", key, err)
		return nil
	}
	upstreamReq.Host = getRequestHost(req)
	resp, err := upstreamClient.Do(upstreamReq)
	if err != nil {
		logRequestError(req, "Cannot make request for [%s]: [%s]", key, err)
		return nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logRequestError(req, "Cannot read response for [%s]: [%s]", key, err)
		return nil
	}

	if resp.StatusCode != http.StatusOK {
		logRequestError(req, "Unexpected status code=%d for the response [%s]", resp.StatusCode, key)
		return nil
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	contentLength := len(body)
	itemSize := contentLength + len(contentType) + 1
	txn, err := cache.NewSetTxn(key, itemSize, ybc.MaxTtl)
	if err != nil {
		logRequestError(req, "Cannot start set txn for response [%s], itemSize=%d: [%s]", key, itemSize, err)
		return nil
	}

	if err = storeContentType(req, txn, contentType); err != nil {
		txn.Rollback()
		return nil
	}

	n, err := txn.Write(body)
	if err != nil {
		logRequestError(req, "Cannot read response [%s] body with size=%d to cache: [%s]", key, contentLength, err)
		txn.Rollback()
		return nil
	}
	if n != contentLength {
		logRequestError(req, "Unexpected number of bytes copied=%d from response [%s] to cache. Expected %d", n, key, contentLength)
		txn.Rollback()
		return nil
	}
	item, err := txn.CommitItem()
	if err != nil {
		logRequestError(req, "Cannot commit set txn for response [%s], size=%d: [%s]", key, contentLength, err)
		return nil
	}
	atomic.AddInt64(&stats.BytesReadFromUpstream, int64(n))
	return item
}

func storeContentType(req *http.Request, w io.Writer, contentType string) (err error) {
	strBuf := []byte(contentType)
	strSize := len(strBuf)
	if strSize > 255 {
		logRequestError(req, "Too long content-type=[%s]. Its' length=%d should fit one byte", contentType, strSize)
		err = fmt.Errorf("Too long content-type")
		return
	}
	var sizeBuf [1]byte
	sizeBuf[0] = byte(strSize)
	if _, err = w.Write(sizeBuf[:]); err != nil {
		logRequestError(req, "Cannot store content-type length in cache: [%s]", err)
		return
	}
	if _, err = w.Write(strBuf); err != nil {
		logRequestError(req, "Cannot store content-type string with length=%d in cache: [%s]", strSize, err)
		return
	}
	return
}

func loadContentType(req *http.Request, r io.Reader) (contentType string, err error) {
	var sizeBuf [1]byte
	if _, err = r.Read(sizeBuf[:]); err != nil {
		logRequestError(req, "Cannot read content-type length from cache: [%s]", err)
		return
	}
	strSize := int(sizeBuf[0])
	strBuf := make([]byte, strSize)
	if _, err = r.Read(strBuf); err != nil {
		logRequestError(req, "Cannot read content-type string with length=%d from cache: [%s]", strSize, err)
		return
	}
	contentType = string(strBuf)
	return
}

func getRequestHost(req *http.Request) string {
	if *useClientRequestHost {
		return req.Host
	}
	return *upstreamHost
}

func logRequestError(req *http.Request, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	logMessage("%s - %s - %s - %s. %s", req.RemoteAddr, req.RequestURI, req.Referer(), req.UserAgent(), msg)
}

func logMessage(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("%s\n", msg)
}

func logFatal(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Fatalf("%s\n", msg)
}

func ip4ToUint32(ip4 net.IP) uint32 {
	return (uint32(ip4[0]) << 24) | (uint32(ip4[1]) << 16) | (uint32(ip4[2]) << 8) | uint32(ip4[3])
}

type PerIpConnTracker struct {
	mutex          sync.Mutex
	perIpConnCount map[uint32]int
}

func (ct *PerIpConnTracker) registerIp(ipUint32 uint32) int {
	ct.mutex.Lock()
	ct.perIpConnCount[ipUint32] += 1
	connCount := ct.perIpConnCount[ipUint32]
	ct.mutex.Unlock()
	return connCount
}

func (ct *PerIpConnTracker) unregisterIp(ipUint32 uint32) {
	ct.mutex.Lock()
	ct.perIpConnCount[ipUint32] -= 1
	ct.mutex.Unlock()
}

func createPerIpConnTracker() *PerIpConnTracker {
	return &PerIpConnTracker{
		perIpConnCount: make(map[uint32]int),
	}
}

type Stats struct {
	CacheHitsCount        int64
	CacheMissesCount      int64
	IfNoneMatchHitsCount  int64
	BytesReadFromUpstream int64
	BytesSentToClients    int64
}

func (s *Stats) WriteToStream(w io.Writer) {
	fmt.Fprintf(w, "Command-line flags\n")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Fprintf(w, "%s=%v\n", f.Name, f.Value)
	})
	fmt.Fprintf(w, "\n")

	requestsCount := s.CacheHitsCount + s.CacheMissesCount + s.IfNoneMatchHitsCount
	var cacheHitRatio float64
	if requestsCount > 0 {
		cacheHitRatio = float64(s.CacheHitsCount+s.IfNoneMatchHitsCount) / float64(requestsCount) * 100.0
	}
	fmt.Fprintf(w, "Requests count: %d\n", requestsCount)
	fmt.Fprintf(w, "Cache hit ratio: %.3f%%\n", cacheHitRatio)
	fmt.Fprintf(w, "Cache hits: %d\n", s.CacheHitsCount)
	fmt.Fprintf(w, "Cache misses: %d\n", s.CacheMissesCount)
	fmt.Fprintf(w, "If-None-Match hits: %d\n", s.IfNoneMatchHitsCount)
	fmt.Fprintf(w, "Read from upstream: %.3f MBytes\n", float64(s.BytesReadFromUpstream)/1000000)
	fmt.Fprintf(w, "Sent to clients: %.3f MBytes\n", float64(s.BytesSentToClients)/1000000)
	fmt.Fprintf(w, "Upstream traffic saved: %.3f MBytes\n", float64(s.BytesSentToClients-s.BytesReadFromUpstream)/1000000)
	fmt.Fprintf(w, "Upstream requests saved: %d\n", s.CacheHitsCount+s.IfNoneMatchHitsCount)
}
