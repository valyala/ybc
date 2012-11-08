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
	Key        []byte
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


var doneChansPool = make(chan (chan bool), 1024)

func acquireDoneChan() chan bool {
	select {
	case done := <-doneChansPool:
		return done
	default:
		return make(chan bool, 1)
	}
	panic("unreachable")
}

func releaseDoneChan(done chan bool) {
	select {
	case doneChansPool <- done:
	default:
	}
}

type taskSync struct {
	done chan bool
}

func (t *taskSync) Done(ok bool) {
	t.done <- ok
}

func (t *taskSync) Wait() bool {
	t.done = acquireDoneChan()
	ok := <-t.done
	releaseDoneChan(t.done)
	return ok
}

type taskGetMulti struct {
	keys  [][]byte
	items []Item
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

func readItem(r *bufio.Reader, keys [][]byte, lineBuf *[]byte, item *Item) (ok bool, eof bool) {
	if !readLine(r, lineBuf) {
		ok = false
		return
	}
	line := *lineBuf
	if bytes.Equal(line, strEnd) {
		ok = true
		eof = true
		return
	}

	key, size, ok := readValueResponse(line)
	if !ok {
		ok = false
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
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	item.Key = keyCopy
	item.Value = value[:size]
	ok = true
	return
}

func (t *taskGetMulti) WriteRequest(w *bufio.Writer) bool {
	if _, err := w.Write(strGets); err != nil {
		log.Printf("Cannot issue 'gets' request for keys=[%s]: [%s]", t.keys, err)
		return false
	}
	for _, key := range t.keys {
		if _, err := w.Write(key); err != nil {
			log.Printf("Cannot send key=[%s] in 'gets' request: [%s]", key, err)
			return false
		}
	}
	if _, err := w.Write(strCrLf); err != nil {
		log.Printf("Cannot write \\r\\n in the end of 'gets' request for keys=[%s]: [%s]", t.keys, err)
		return false
	}
	return true
}

func (t *taskGetMulti) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	var item Item
	for {
		ok, eof := readItem(r, t.keys, lineBuf, &item)
		if !ok {
			return false
		}
		if eof {
			break
		}
		t.items = append(t.items, item)
	}
	return true
}

func (c *Client) GetMulti(keys [][]byte) (items []Item, err error) {
	t := taskGetMulti{
		keys:  keys,
		items: make([]Item, 0, len(keys)),
	}
	if !c.do(&t) {
		err = ErrCommunicationFailure
		return
	}
	items = t.items
	return
}

type taskGet struct {
	item      *Item
	itemFound bool
	taskSync
}

func (t *taskGet) WriteRequest(w *bufio.Writer) bool {
	if _, err := w.Write(strGet); err != nil {
		log.Printf("Cannot issue 'get' request for key=[%s]: [%s]", t.item.Key, err)
		return false
	}
	if _, err := w.Write(t.item.Key); err != nil {
		log.Printf("Cannot issue key=[%s] for 'get' request: [%s]", t.item.Key, err)
		return false
	}
	if _, err := w.Write(strCrLf); err != nil {
		log.Printf("Cannot issue \\r\\n in 'get' request for key=[%s]: [%s]", t.item.Key, err)
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
	if !bytes.Equal(key, t.item.Key) {
		log.Printf("Unexpected key=[%s] obtained from response. Expected [%s]", key, t.item.Key)
		return false
	}
	value, err := ioutil.ReadAll(io.LimitReader(r, int64(size)+7))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", key, err)
		return false
	}
	if !bytes.HasSuffix(value, strCrLfEndCrLf) {
		log.Printf("There is no crlf found in 'get' response value for key=[%s]", key)
		return false
	}
	t.item.Value = value[:size]
	t.itemFound = true
	return true
}

func (c *Client) Get(item *Item) error {
	t := taskGet{
		item:      item,
		itemFound: false,
	}
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	if !t.itemFound {
		return ErrCacheMiss
	}
	return nil
}

type taskSet struct {
	item *Item
	taskSync
}

func writeSetRequest(w *bufio.Writer, item *Item, noreply bool) bool {
	if _, err := w.Write(strSet); err != nil {
		log.Printf("Cannot issue 'set' request for key=[%s]: [%s]", item.Key, err)
		return false
	}
	if _, err := w.Write(item.Key); err != nil {
		log.Printf("Cannot write key=[%s] into 'set' request: [%s]", item.Key, err)
		return false
	}
	if _, err := w.Write(strZero); err != nil {
		log.Printf("Cannot write ' 0 ' into 'set' request: [%s]", err)
		return false
	}
	if _, err := w.Write([]byte(strconv.Itoa(item.Expiration))); err != nil {
		log.Printf("Cannot write expiration into 'set' request: [%s]", err)
		return false
	}
	if _, err := w.Write([]byte{' '}); err != nil {
		log.Printf("Cannot write ' ' into 'set' request: [%s]", err)
		return false
	}
	size := len(item.Value)
	if _, err := w.Write([]byte(strconv.Itoa(size))); err != nil {
		log.Printf("Cannot write iltem size=%d into 'set' request: [%s]", size, err)
		return false
	}
	if noreply {
		if _, err := w.Write(strWsNoreply); err != nil {
			log.Printf("Cannot write ' noreply' into 'set' request: [%s]", err)
			return false
		}
	}
	if _, err := w.Write(strCrLf); err != nil {
		log.Printf("Cannot write \\r\\n into 'set' request: [%s]", err)
		return false
	}
	if _, err := w.Write(item.Value); err != nil {
		log.Printf("Error when sending value in 'set' request for key=[%s]. len(value)=%d: [%s]", item.Key, len(item.Value), err)
		return false
	}
	if _, err := w.Write(strCrLf); err != nil {
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
	t := taskSet{
		item: item,
	}
	if !c.do(&t) {
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
	t := taskSetNowait{
		item: item,
	}
	c.do(&t)
}
