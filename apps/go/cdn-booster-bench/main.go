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

	filesCount    = flag.Int("filesCount", 500, "The number of distinct files to cache. Random query parameter is added to testUrl for generating distinct file urls")
	goMaxProcs    = flag.Int("goMaxProcs", numCpu, "The number of go procs")
	requestsCount = flag.Int("requestsCount", 100000, "The number of requests to perform")
	testUrl       = flag.String("testUrl", "http://localhost:8098/", "Url to test")
	workersCount  = flag.Int("workersCount", 8*numCpu, "The number of workers")
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

	conn, err := net.Dial("tcp", hostPort)
	if err != nil {
		log.Fatalf("Error=[%s] when connecting to [%s]\n", err, hostPort)
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	if testUri.Scheme == "https" {
		conn = tls.Client(conn, tlsConfig)
		if err = conn.(*tls.Conn).Handshake(); err != nil {
			log.Fatalf("Error during tls handshake: [%s]\n", err)
		}
	}

	statsChan := make(chan int64, 2)
	go readResponses(conn, statsChan)
	requestsWritten := writeRequests(conn, ch, testUri)
	tcpConn.CloseWrite()
	responsesRead := <-statsChan
	if responsesRead != requestsWritten {
		log.Fatalf("Unexpected number of responses read: %d. Expected %d\n", responsesRead, requestsWritten)
	}
	*bytesRead = <-statsChan
}

func writeRequests(conn net.Conn, ch <-chan int, testUri *url.URL) int64 {
	var requestsWritten int64
	w := bufio.NewWriter(conn)
	requestUri := testUri.RequestURI()
	for _ = range ch {
		requestStr := []byte(fmt.Sprintf("GET %s?%d HTTP/1.1\nHost: %s\n\n", requestUri, rand.Intn(*filesCount), testUri.Host))
		if _, err := w.Write(requestStr); err != nil {
			log.Fatalf("Error=[%s] when writing HTTP request [%d] to connection\n", err, requestsWritten)
		}
		requestsWritten += 1
	}
	if err := w.Flush(); err != nil {
		log.Fatalf("Error when flushing requests' buffer: [%s]\n", err)
	}
	return requestsWritten
}

func readResponses(r io.Reader, statsChan chan<- int64) {
	var bytesRead int64
	var responsesRead int64
	rb := bufio.NewReader(r)
	for {
		resp, err := http.ReadResponse(rb, nil)
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				break
			}
			log.Fatalf("Error when reading response: [%s]\n", err)
		}
		n, err := io.Copy(ioutil.Discard, resp.Body)
		if err != nil {
			log.Fatalf("Error when reading response body: [%s]\n", err)
		}
		bytesRead += int64(n)
		resp.Body.Close()
		responsesRead += 1
	}
	statsChan <- responsesRead
	statsChan <- bytesRead
}
