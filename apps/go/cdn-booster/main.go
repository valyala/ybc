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
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
	upstreamHost         = flag.String("upstreamHost", "www.google.com", "Upstream host to proxy data from")
)

var (
	ifNoneMatchResponseHeader         = []byte("HTTP/1.1 304 Not Modified\nServer: go-cdn-booster\nEtag: W/\"CacheForever\"\n\n")
	internalServerErrorResponseHeader = []byte("HTTP/1.1 500 Internal Server Error\nServer: go-cdn-booster\n\n")
	notAllowedResponseHeader          = []byte("HTTP/1.1 405 Method Not Allowed\nServer: go-cdn-booster\n\n")
	okResponseHeader                  = []byte("HTTP/1.1 200 OK\nServer: go-cdn-booster\nCache-Control: public, max-age=31536000\nETag: W/\"CacheForever\"\n")
	serviceUnavailableResponseHeader  = []byte("HTTP/1.1 503 Service Unavailable\nServer: go-cdn-booster\n\n")
)

var (
	cache            ybc.Cacher
	client           http.Client
	perIpConnTracker = createPerIpConnTracker()
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*goMaxProcs)

	cache = createCache()
	defer cache.Close()
	client = http.Client{
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
				logMessage("Cannot accept connections due temporary network error: [%s]", err)
				time.Sleep(time.Second)
				continue
			}
			logFatal("Cannot accept connections due permanent error: [%s]", err)
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

	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
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
		if !handleBufferedRequest(req, w) {
			return
		}
		if !req.ProtoAtLeast(1, 1) || req.Header.Get("Connection") == "close" {
			return
		}
	}
}

func handleBufferedRequest(req *http.Request, w *bufio.Writer) bool {
	defer w.Flush()
	return handleRequest(req, w)
}

func handleRequest(req *http.Request, w io.Writer) bool {
	if req.Method != "GET" {
		w.Write(notAllowedResponseHeader)
		return false
	}

	reqH := req.Header
	if reqH.Get("If-None-Match") != "" {
		_, err := w.Write(ifNoneMatchResponseHeader)
		return err == nil
	}

	key := []byte(req.RequestURI)
	item, err := cache.GetDeItem(key, time.Second)
	if err != nil {
		if err != ybc.ErrCacheMiss {
			logFatal("Unexpeced error when obtaining cache value by key=[%s]: [%s]", key, err)
		}

		item = fetchFromUpstream(req, key)
		if item == nil {
			w.Write(serviceUnavailableResponseHeader)
			return false
		}
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
	if _, err = io.Copy(w, item); err != nil {
		logRequestError(req, "Cannot send file with key=[%s] to client: %s", key, err)
		return false
	}
	return true
}

func fetchFromUpstream(req *http.Request, key []byte) *ybc.Item {
	requestUrl := fmt.Sprintf("http://%s%s", *upstreamHost, key)
	resp, err := client.Get(requestUrl)
	if err != nil {
		logRequestError(req, "Cannot fetch data from [%s]: [%s]", requestUrl, err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logRequestError(req, "Unexpected status code=%d for the response [%s]", resp.StatusCode, requestUrl)
		return nil
	}

	contentLength := resp.Header.Get("Content-Length")
	if contentLength == "" {
		logRequestError(req, "Cannot cache response [%s] without content-length", requestUrl)
		return nil
	}
	contentLengthN, err := strconv.Atoi(contentLength)
	if err != nil {
		logRequestError(req, "Cannot parse contentLength=[%s] for response [%s]: [%s]", contentLength, requestUrl, err)
		return nil
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	itemSize := contentLengthN + len(contentType) + 1
	txn, err := cache.NewSetTxn(key, itemSize, ybc.MaxTtl)
	if err != nil {
		logRequestError(req, "Cannot start set txn for response [%s], key=[%s], itemSize=%d: [%s]", requestUrl, key, itemSize, err)
		return nil
	}

	if err = storeContentType(req, txn, contentType); err != nil {
		txn.Rollback()
		return nil
	}

	n, err := txn.ReadFrom(resp.Body)
	if err != nil {
		logRequestError(req, "Cannot read response [%s] body with size=%d to cache, key=[%s]: [%s]", requestUrl, contentLengthN, key, err)
		txn.Rollback()
		return nil
	}
	if n != int64(contentLengthN) {
		logRequestError(req, "Unexpected number of bytes copied=%d from response [%s], key=[%s] to cache. Expected %d", n, requestUrl, key, contentLengthN)
		txn.Rollback()
		return nil
	}
	item, err := txn.CommitItem()
	if err != nil {
		logRequestError(req, "Cannot commit set txn for response [%s], key=[%s], size=%d: [%s]", requestUrl, key, contentLengthN, err)
		return nil
	}
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
