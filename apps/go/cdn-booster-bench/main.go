// Benchmark tool for go-cdn-booster or any other http/1.1 server
//
// This tool is similar to the well known 'ab' tool. The main difference
// is that this tool issues HTTP/1.1 'Connection: Keep-Alive' requests
// over the limited number of open connections. The number of such connections
// is equivalent to workersCount.
//
// Known limitations:
//   * It cannot test HTTP servers without HTTP/1.1 keep-alive connections
//     support.
//   * It doesn't parse server responses and so doesn't gather stats regarding
//     response status codes.
//   * Currently it shows only the following stats:
//         * time taken for the test
//         * qps - average queries per second
//         * Kbps - average Kbytes per second received from the server.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	numCpu = runtime.NumCPU()

	filesCount    = flag.Int("filesCount", 500, "The number of distinct files to cache. Random query parameter is added to testUrl for generating distinct file urls")
	goMaxProcs    = flag.Int("goMaxProcs", numCpu, "The number of go procs")
	requestsCount = flag.Int("requestsCount", 100000, "The number of requests to perform")
	testUrl       = flag.String("testUrl", "http://localhost:8098/", "Url to test")
	workersCount  = flag.Int("workersCount", 8*numCpu, "The number of workers")
)

func main() {
	flag.Parse()
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("%s=%v\n", f.Name, f.Value)
	})

	runtime.GOMAXPROCS(*goMaxProcs)

	testUri, err := url.Parse(*testUrl)
	if err != nil {
		log.Fatalf("Error=[%s] when parsing testUrl=[%s]\n", err, *testUrl)
	}

	ch := make(chan int, 100000)
	bytesRead := make([]int64, *workersCount)
	wg := &sync.WaitGroup{}

	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go worker(ch, wg, testUri, &bytesRead[i])
	}

	log.Printf("Test started\n")
	startTime := time.Now()
	for i := 0; i < *requestsCount; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
	duration := time.Since(startTime)
	seconds := float64(duration) / float64(time.Second)

	var totalBytesRead int64
	for i := 0; i < *workersCount; i++ {
		totalBytesRead += bytesRead[i]
	}
	qps := float64(*requestsCount) / seconds
	kbps := float64(totalBytesRead) / seconds / float64(1000)
	log.Printf("Done\n")
	log.Printf("%d requests from %d workers in %s\n", *requestsCount, *workersCount, duration)
	log.Printf("%.0f qps, %.0f Kbps\n", qps, kbps)
}

func worker(ch <-chan int, wg *sync.WaitGroup, testUri *url.URL, bytesRead *int64) {
	defer wg.Done()

	hostPort := testUri.Host
	if !strings.Contains(hostPort, ":") {
		hostPort = net.JoinHostPort(hostPort, "80")
	}

	conn, err := net.Dial("tcp", hostPort)
	if err != nil {
		log.Fatalf("Error=[%s] when connecting to [%s]\n", err, hostPort)
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	bytesReadChan := make(chan int64)
	defer func() { *bytesRead = <-bytesReadChan }()

	go responsesReader(tcpConn, bytesReadChan)

	requestsWriter(tcpConn, ch, testUri)
}

func requestsWriter(conn *net.TCPConn, ch <-chan int, testUri *url.URL) {
	defer conn.CloseWrite()
	w := bufio.NewWriter(conn)
	defer w.Flush()
	requestUri := testUri.RequestURI()
	for n := range ch {
		requestStr := []byte(fmt.Sprintf("GET %s?%d HTTP/1.1\nHost: %s\n\n", requestUri, rand.Intn(*filesCount), testUri.Host))
		if _, err := w.Write(requestStr); err != nil {
			log.Fatalf("Error=[%s] when writing HTTP request [%d] to connection\n", err, n)
		}
	}
}

func responsesReader(r io.Reader, bytesReadChan chan<- int64) {
	var bytesRead int64
	var err error
	defer func() { bytesReadChan <- bytesRead }()
	if bytesRead, err = io.Copy(ioutil.Discard, r); err != nil {
		log.Fatalf("Error when reading HTTP response: [%s]\n", err)
	}
}
