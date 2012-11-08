package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	ErrCacheMiss            = errors.New("cache miss")
	ErrCommunicationFailure = errors.New("communication failure")
)

type tasker interface {
	WriteRequest(w *bufio.Writer) bool
	ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool
	Done(ok bool)
	Wait() bool
}

func requestsSender(w *bufio.Writer, requests <-chan tasker, responses chan<- tasker, c net.Conn, done *sync.WaitGroup) {
	defer done.Done()
	defer w.Flush()
	defer close(responses)
	for {
		var t tasker
		var ok bool

		// Flush w only if there are no pending requests.
		select {
		case t, ok = <-requests:
		default:
			w.Flush()
			t, ok = <-requests
		}
		if !ok {
			break
		}
		if !t.WriteRequest(w) {
			t.Done(false)
			break
		}
		responses <- t
	}
	for t := range requests {
		t.Done(false)
	}
}

func responsesReceiver(r *bufio.Reader, responses <-chan tasker, c net.Conn, done *sync.WaitGroup) {
	defer done.Done()
	line := make([]byte, 0, 1024)
	for t := range responses {
		if !t.ReadResponse(r, &line) {
			t.Done(false)
			c.Close()
			break
		}
		t.Done(true)
	}
	for t := range responses {
		t.Done(false)
	}
}

func handleAddr(addr string, readBufferSize, writeBufferSize, maxPendingResponsesCount int, reconnectTimeout time.Duration, requests <-chan tasker) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("Cannot establish tcp connection to addr=[%s]: [%s]", addr, err)
		for t := range requests {
			t.Done(false)
		}
		time.Sleep(reconnectTimeout)
		return
	}
	defer c.Close()

	r := bufio.NewReaderSize(c, readBufferSize)
	w := bufio.NewWriterSize(c, writeBufferSize)

	responses := make(chan tasker, maxPendingResponsesCount)
	sendRecvDone := &sync.WaitGroup{}
	defer sendRecvDone.Wait()
	sendRecvDone.Add(2)
	go requestsSender(w, requests, responses, c, sendRecvDone)
	go responsesReceiver(r, responses, c, sendRecvDone)
}

func addrHandler(addr string, readBufferSize, writeBufferSize, maxPendingResponsesCount int, reconnectTimeout time.Duration, requests chan tasker, done *sync.WaitGroup) {
	defer done.Done()
	for {
		handleAddr(addr, readBufferSize, writeBufferSize, maxPendingResponsesCount, reconnectTimeout, requests)
		// Check whether the requests channel is drained and closed.
		select {
		case t, ok := <-requests:
			if !ok {
				// The requests channel is drained and closed.
				return
			}
			requests <- t
		default:
			// The requests channel is drained, but not closed.
		}
	}
}

const (
	defaultConnectionsCount        = 1
	defaultMaxPendingRequestsCount = 1024
	defaultReconnectTimeout        = time.Second
)

type Client struct {
	ConnectAddr             string
	ConnectionsCount        int
	MaxPendingRequestsCount int
	ReadBufferSize          int
	WriteBufferSize         int
	ReconnectTimeout        time.Duration

	requests chan tasker
	done     *sync.WaitGroup
}

type Item struct {
	Key        string
	Value      []byte
	Expiration int
}

func (c *Client) init() {
	if c.ConnectionsCount == 0 {
		c.ConnectionsCount = defaultConnectionsCount
	}
	if c.MaxPendingRequestsCount == 0 {
		c.MaxPendingRequestsCount = defaultMaxPendingRequestsCount
	}
	if c.ReadBufferSize == 0 {
		c.ReadBufferSize = defaultReadBufferSize
	}
	if c.WriteBufferSize == 0 {
		c.WriteBufferSize = defaultWriteBufferSize
	}
	if c.ReconnectTimeout == time.Duration(0) {
		c.ReconnectTimeout = defaultReconnectTimeout
	}

	c.requests = make(chan tasker, c.MaxPendingRequestsCount)
	c.done = &sync.WaitGroup{}
	c.done.Add(1)
}

func (c *Client) run() {
	defer c.done.Done()

	connsDone := &sync.WaitGroup{}
	defer connsDone.Wait()
	for i := 0; i < c.ConnectionsCount; i++ {
		connsDone.Add(1)
		go addrHandler(c.ConnectAddr, c.ReadBufferSize, c.WriteBufferSize, c.MaxPendingRequestsCount, c.ReconnectTimeout, c.requests, connsDone)
	}
}

func (c *Client) do(t tasker) bool {
	c.requests <- t
	return t.Wait()
}

func (c *Client) Start() {
	if c.requests != nil || c.done != nil {
		panic("Did you forget calling Client.Stop() before calling Client.Start()?")
	}
	c.init()
	go c.run()
}

func (c *Client) Stop() {
	close(c.requests)
	c.done.Wait()
	c.requests = nil
	c.done = nil
}

type taskSync struct {
	done chan bool
}

func (t *taskSync) Done(ok bool) {
	t.done <- ok
}

func (t *taskSync) Wait() bool {
	return <-t.done
}

type taskGetMulti struct {
	keys  []string
	items map[string]*Item
	taskSync
}

func readValueResponse(line []byte) (key []byte, size int, ok bool) {
	if !bytes.Equal(line[:6], strValue) {
		log.Printf("Unexpected start of line: [%s]. Expected [VALUE ]", line[:6])
		ok = false
		return
	}
	line = line[6:]

	n := -1

	key, n = nextToken(line, n, "key")
	if key == nil {
		ok = false
		return
	}
	flagsUnused, n := nextToken(line, n, "flags")
	if flagsUnused == nil {
		ok = false
		return
	}
	sizeStr, n := nextToken(line, n, "size")
	if sizeStr == nil {
		ok = false
		return
	}
	size, ok = parseSize(sizeStr)
	if !ok {
		return
	}
	casidUnused, n := nextToken(line, n, "casid")
	if casidUnused == nil {
		ok = false
		return
	}
	ok = (n == len(line))
	return
}

func readItem(r *bufio.Reader, keys []string, lineBuf *[]byte) (item *Item, ok bool) {
	if !readLine(r, lineBuf) {
		ok = false
		return
	}
	line := *lineBuf
	if bytes.Equal(line, strEnd) {
		ok = true
		return
	}

	var key []byte
	var size int
	key, size, ok = readValueResponse(line)
	if !ok {
		return
	}
	value, err := ioutil.ReadAll(io.LimitReader(r, int64(size)+2))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", key, err)
		ok = false
		return
	}
	if !bytes.HasSuffix(value, strCrLf) {
		log.Printf("There is no crlf found in 'get' response value for key=[%s]", item.Key)
		ok = false
		return
	}
	item = &Item{
		Key:   string(key),
		Value: value[:size],
	}
	ok = true
	return
}

func (t *taskGetMulti) WriteRequest(w *bufio.Writer) bool {
	_, err := w.Write(strGets)
	if err != nil {
		log.Printf("Cannot issue 'gets' request for keys=[%s]: [%s]", t.keys, err)
		return false
	}
	for _, key := range t.keys {
		_, err = w.Write([]byte(key))
		if err != nil {
			log.Printf("Cannot send key=[%s] in 'gets' request: [%s]", key, err)
			return false
		}
	}
	_, err = w.Write(strCrLf)
	if err != nil {
		log.Printf("Cannot write \\r\\n in the end of 'gets' request for keys=[%s]: [%s]", t.keys, err)
		return false
	}
	return true
}

func (t *taskGetMulti) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	items := t.items
	for {
		item, ok := readItem(r, t.keys, lineBuf)
		if !ok {
			return false
		}
		if item == nil {
			break
		}
		items[item.Key] = item
	}
	return true
}

func (c *Client) GetMulti(keys []string) (items map[string]*Item, err error) {
	t := &taskGetMulti{
		keys:  keys,
		items: make(map[string]*Item, len(keys)),
		taskSync: taskSync{
			done: make(chan bool, 1),
		},
	}
	if !c.do(t) {
		err = ErrCommunicationFailure
		return
	}
	items = t.items
	return
}

type taskGet struct {
	key  string
	item *Item
	taskSync
}

func (t *taskGet) WriteRequest(w *bufio.Writer) bool {
	_, err := w.Write(strGet)
	if err != nil {
		log.Printf("Cannot issue 'get' request for key=[%s]: [%s]", t.key, err)
		return false
	}
	_, err = w.Write([]byte(t.key))
	if err != nil {
		log.Printf("Cannot issue key=[%s] for 'get' request: [%s]", t.key, err)
		return false
	}
	_, err = w.Write(strCrLf)
	if err != nil {
		log.Printf("Cannot issue \\r\\n in 'get' request for key=[%s]: [%s]", t.key, err)
		return false
	}
	return true
}

func (t *taskGet) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	if !readLine(r, lineBuf) {
		return false
	}
	line := *lineBuf
	if bytes.Equal(line, strEnd) {
		return true
	}
	key, size, ok := readValueResponse(line)
	if !ok {
		return false
	}
	if !bytes.Equal(key, []byte(t.key)) {
		log.Printf("Unexpected key=[%s] obtained from response. Expected [%s]", key, t.key)
		return false
	}
	value, err := ioutil.ReadAll(io.LimitReader(r, int64(size)+7))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", t.key, err)
		return false
	}
	if !bytes.HasSuffix(value, strCrLfEndCrLf) {
		log.Printf("There is no crlf found in 'get' response value for key=[%s]", t.key)
		return false
	}
	t.item = &Item{
		Key:   t.key,
		Value: value[:size],
	}
	return true
}

func (c *Client) Get(key string) (item *Item, err error) {
	t := &taskGet{
		key: key,
		taskSync: taskSync{
			done: make(chan bool, 1),
		},
	}
	if !c.do(t) {
		err = ErrCommunicationFailure
		return
	}
	if t.item == nil {
		err = ErrCacheMiss
		return
	}
	item = t.item
	return
}

type taskSet struct {
	item *Item
	taskSync
}

func writeSetRequest(w *bufio.Writer, item *Item, noreply bool) bool {
	_, err := w.Write(strSet)
	if err != nil {
		log.Printf("Cannot issue 'set' request for key=[%s]: [%s]", item.Key, err)
		return false
	}
	_, err = w.Write([]byte(item.Key))
	if err != nil {
		log.Printf("Cannot write key=[%s] into 'set' request: [%s]", item.Key, err)
		return false
	}
	_, err = w.Write(strZero)
	if err != nil {
		log.Printf("Cannot write ' 0 ' into 'set' request: [%s]", err)
		return false
	}
	_, err = w.Write([]byte(strconv.Itoa(item.Expiration)))
	if err != nil {
		log.Printf("Cannot write expiration into 'set' request: [%s]", err)
		return false
	}
	_, err = w.Write([]byte{' '})
	if err != nil {
		log.Printf("Cannot write ' ' into 'set' request: [%s]", err)
		return false
	}
	size := len(item.Value)
	_, err = w.Write([]byte(strconv.Itoa(size)))
	if err != nil {
		log.Printf("Cannot write iltem size=%d into 'set' request: [%s]", size, err)
		return false
	}
	if noreply {
		_, err = w.Write(strWsNoreply)
		if err != nil {
			log.Printf("Cannot write ' noreply' into 'set' request: [%s]", err)
			return false
		}
	}
	_, err = w.Write(strCrLf)
	if err != nil {
		log.Printf("Cannot write \\r\\n into 'set' request: [%s]", err)
		return false
	}
	if _, err = w.Write(item.Value); err != nil {
		log.Printf("Error when sending value in 'set' request for key=[%s]. len(value)=%d: [%s]", item.Key, len(item.Value), err)
		return false
	}
	if _, err = w.Write(strCrLf); err != nil {
		log.Printf("Error when sending crlf for 'set' request for key=[%s]: [%s]", item.Key, err)
		return false
	}
	return true
}

func (t *taskSet) WriteRequest(w *bufio.Writer) bool {
	return writeSetRequest(w, t.item, false)
}

func (t *taskSet) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	if !readLine(r, lineBuf) {
		return false
	}
	line := *lineBuf
	if !bytes.Equal(line, strStored) {
		log.Printf("Unexpected response obtained for 'set' request for key=[%s], len(value)=%d: [%s]", t.item.Key, len(t.item.Value), line)
		return false
	}
	return true
}

func (c *Client) Set(item *Item) (err error) {
	t := &taskSet{
		item: item,
		taskSync: taskSync{
			done: make(chan bool, 1),
		},
	}
	if !c.do(t) {
		err = ErrCommunicationFailure
		return
	}
	return
}

type taskAsync struct{}

func (t *taskAsync) Done(ok bool) {}

func (t *taskAsync) Wait() bool {
	return true
}

type taskSetNowait struct {
	item *Item
	taskAsync
}

func (t *taskSetNowait) WriteRequest(w *bufio.Writer) bool {
	return writeSetRequest(w, t.item, true)
}

func (t *taskSetNowait) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	return true
}

func (c *Client) SetNowait(item *Item) {
	t := &taskSetNowait{
		item: item,
	}
	c.do(t)
}
