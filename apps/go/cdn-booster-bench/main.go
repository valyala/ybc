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
//   * Currently it shows only the following stats:
//         * time taken for the test
//         * Kbytes read - the total size of responses' body
//         * qps - average queries per second
//         * Kbps - average Kbytes per second received from the server.
package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	numCpu = runtime.NumCPU()

	filesCount                 = flag.Int("filesCount", 500, "The number of distinct files to cache. Random query parameter is added to testUrl for generating distinct file urls")
	goMaxProcs                 = flag.Int("goMaxProcs", numCpu, "The number of go procs")
	requestsCount              = flag.Int("requestsCount", 100000, "The number of requests to perform")
	requestsPerConnectionCount = flag.Int("requestsPerConnectionCount", 1000, "The maximum number of requests per established connection to the testUrl")
	testUrl                    = flag.String("testUrl", "http://localhost:8098/", "Url to test")
	workersCount               = flag.Int("workersCount", 8*numCpu, "The number of workers")
)

var (
	tlsConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
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
		ch <- 1
	}
	close(ch)
	wg.Wait()
	duration := time.Since(startTime)
	seconds := float64(duration) / float64(time.Second)

	var totalBytesRead int64
	for i := 0; i < *workersCount; i++ {
		totalBytesRead += bytesRead[i]
	}
	kbytesRead := float64(totalBytesRead) / float64(1000)
	qps := float64(*requestsCount) / seconds
	kbps := kbytesRead / seconds
	log.Printf("Done\n")
	log.Printf("%d requests from %d workers in %s\n", *requestsCount, *workersCount, duration)
	log.Printf("%.0f Kbytes read, %.0f qps, %.0f Kbps\n", kbytesRead, qps, kbps)
}

func worker(ch <-chan int, wg *sync.WaitGroup, testUri *url.URL, bytesRead *int64) {
	defer wg.Done()

	hostPort := testUri.Host
	if !strings.Contains(hostPort, ":") {
		port := "80"
		if testUri.Scheme == "https" {
			port = "443"
		}
		hostPort = net.JoinHostPort(hostPort, port)
	}

	for {
		n := issueRequestsPerConnection(ch, hostPort, testUri)
		if n == 0 {
			break
		}
		*bytesRead += n
	}
}

func issueRequestsPerConnection(ch <-chan int, hostPort string, testUri *url.URL) int64 {
	conn, err := net.Dial("tcp", hostPort)
	if err != nil {
		log.Fatalf("Error=[%s] when connecting to [%s]\n", err, hostPort)
	}
	defer conn.Close()

	if testUri.Scheme == "https" {
		conn = tls.Client(conn, tlsConfig)
		if err = conn.(*tls.Conn).Handshake(); err != nil {
			log.Fatalf("Error during tls handshake: [%s]\n", err)
		}
	}

	statsChan := make(chan int64)
	requestsChan := make(chan int, 1000)
	go readResponses(conn, statsChan, requestsChan)
	writeRequests(conn, ch, requestsChan, testUri)
	close(requestsChan)
	return <-statsChan
}

func writeRequests(conn net.Conn, ch <-chan int, requestsChan chan<- int, testUri *url.URL) {
	var requestsWritten int
	w := bufio.NewWriter(conn)
	requestUri := testUri.RequestURI()
	for _ = range ch {
		requestStr := []byte(fmt.Sprintf("GET %s?%d HTTP/1.1\nHost: %s\n\n", requestUri, rand.Intn(*filesCount), testUri.Host))
		if _, err := w.Write(requestStr); err != nil {
			log.Fatalf("Error=[%s] when writing HTTP request [%d] to connection\n", err, requestsWritten)
		}
		requestsWritten += 1
		requestsChan <- requestsWritten
		if requestsWritten == *requestsPerConnectionCount {
			break
		}
	}
	if err := w.Flush(); err != nil {
		log.Fatalf("Error when flushing requests' buffer: [%s]\n", err)
	}
}

func readResponses(r io.Reader, statsChan chan<- int64, requestsChan <-chan int) {
	var bytesRead int64
	rb := bufio.NewReader(r)
	for n := range requestsChan {
		resp, err := http.ReadResponse(rb, nil)
		if err != nil {
			log.Fatalf("Error when reading response %d: [%s]\n", n, err)
		}
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("Unexpected status code for the response %d: [%d]\n", n, resp.StatusCode)
		}
		n, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			log.Fatalf("Error when reading response %d body: [%s]\n", n, err)
		}
		bytesRead += int64(n)
		resp.Body.Close()
	}
	statsChan <- bytesRead
}
