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
	OSReadBufferSize        int
	OSWriteBufferSize       int
	ReconnectTimeout        time.Duration

	requests chan tasker
	done     *sync.WaitGroup
}

type Item struct {
	Key        []byte
	Value      []byte
	Expiration int
}

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

func handleAddr(c *Client) bool {
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.ConnectAddr)
	if err != nil {
		log.Printf("Cannot resolve tcp address=[%s]: [%s]", c.ConnectAddr, err)
		return false
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Printf("Cannot establish tcp connection to addr=[%s]: [%s]", tcpAddr, err)
		for t := range c.requests {
			t.Done(false)
		}
		return false
	}
	defer conn.Close()

	if err = conn.SetReadBuffer(c.OSReadBufferSize); err != nil {
		log.Fatal("Cannot set TCP read buffer size to %d: [%s]", c.OSReadBufferSize, err)
	}
	if err = conn.SetWriteBuffer(c.OSWriteBufferSize); err != nil {
		log.Fatal("Cannot set TCP write buffer size to %d: [%s]", c.OSWriteBufferSize, err)
	}

	r := bufio.NewReaderSize(conn, c.ReadBufferSize)
	w := bufio.NewWriterSize(conn, c.WriteBufferSize)

	responses := make(chan tasker, c.MaxPendingRequestsCount)
	sendRecvDone := &sync.WaitGroup{}
	defer sendRecvDone.Wait()
	sendRecvDone.Add(2)
	go requestsSender(w, c.requests, responses, conn, sendRecvDone)
	go responsesReceiver(r, responses, conn, sendRecvDone)

	return true
}

func addrHandler(c *Client, done *sync.WaitGroup) {
	defer done.Done()
	for {
		if !handleAddr(c) {
			time.Sleep(c.ReconnectTimeout)
		}

		// Check whether the requests channel is drained and closed.
		select {
		case t, ok := <-c.requests:
			if !ok {
				// The requests channel is drained and closed.
				return
			}
			c.requests <- t
		default:
			// The requests channel is drained, but not closed.
		}
	}
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
	if c.OSReadBufferSize == 0 {
		c.OSReadBufferSize = defaultOSReadBufferSize
	}
	if c.OSWriteBufferSize == 0 {
		c.OSWriteBufferSize = defaultOSWriteBufferSize
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
		go addrHandler(c, connsDone)
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

func readKeyValue(r *bufio.Reader, line []byte) (key []byte, value []byte, ok bool) {
	var size int
	key, size, ok = readValueResponse(line)
	if !ok {
		return
	}

	var err error
	value, err = ioutil.ReadAll(io.LimitReader(r, int64(size)))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", key, err)
		ok = false
		return
	}
	ok = matchBytes(r, strCrLf)
	return
}

func readItem(r *bufio.Reader, lineBuf *[]byte, item *Item) (ok bool, eof bool, wouldBlock bool) {
	if ok = readLine(r, lineBuf); !ok {
		return
	}
	line := *lineBuf
	if bytes.Equal(line, strEnd) {
		ok = true
		eof = true
		return
	}
	if bytes.Equal(line, strWouldBlock) {
		ok = true
		eof = true
		wouldBlock = true
		return
	}

	var key, value []byte
	key, value, ok = readKeyValue(r, line)
	if !ok {
		return
	}

	item.Key = key
	item.Value = value
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
		ok, eof, _ := readItem(r, lineBuf, &item)
		if !ok {
			return false
		}
		if eof {
			break
		}

		keyCopy := make([]byte, len(item.Key))
		copy(keyCopy, item.Key)
		item.Key = keyCopy
		t.items = append(t.items, item)
	}
	return true
}

func (c *Client) GetMulti(keys [][]byte) (items []Item, err error) {
	t := taskGetMulti{
		keys:  keys,
		items: make([]Item, 0, len(keys)),
		taskSync: taskSync{
			done: acquireDoneChan(),
		},
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

func readSingleItem(r *bufio.Reader, lineBuf *[]byte, item *Item) (ok bool, eof bool, wouldBlock bool) {
	keyOriginal := item.Key
	ok, eof, wouldBlock = readItem(r, lineBuf, item)
	if !ok || eof || wouldBlock {
		return
	}
	if ok = matchBytes(r, strEndCrLf); !ok {
		return
	}
	if ok = bytes.Equal(keyOriginal, item.Key); !ok {
		log.Printf("Key mismatch! Expected [%s], but server returned [%s]", keyOriginal, item.Key)
		return
	}
	item.Key = keyOriginal
	return
}

func (t *taskGet) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	ok, eof, _ := readSingleItem(r, lineBuf, t.item)
	if !ok {
		return false
	}
	if !eof {
		t.itemFound = true
	}
	return true
}

func (c *Client) Get(item *Item) error {
	t := taskGet{
		item:      item,
		itemFound: false,
		taskSync: taskSync{
			done: acquireDoneChan(),
		},
	}
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	if !t.itemFound {
		return ErrCacheMiss
	}
	return nil
}

type taskGetDe struct {
	item       *Item
	grace      int
	itemFound  bool
	wouldBlock bool
	taskSync
}

func (t *taskGetDe) WriteRequest(w *bufio.Writer) bool {
	if _, err := w.Write(strGetDe); err != nil {
		log.Printf("Cannot issue 'getde' request for key=[%s]: [%s]", t.item.Key, err)
		return false
	}
	if _, err := w.Write(t.item.Key); err != nil {
		log.Printf("Cannot issue key=[%s] for 'getde' request: [%s]", t.item.Key, err)
		return false
	}
	if _, err := w.Write(strWs); err != nil {
		log.Printf("Cannot write whitespace in 'getde' request: [%s]", err)
		return false
	}
	if _, err := w.Write([]byte(strconv.Itoa(t.grace))); err != nil {
		log.Printf("Cannot write grace=[%d] to 'getde' request: [%s]", t.grace, err)
		return false
	}
	if _, err := w.Write(strCrLf); err != nil {
		log.Printf("Cannot issue \\r\\n in 'get' request for key=[%s]: [%s]", t.item.Key, err)
		return false
	}
	return true
}

func (t *taskGetDe) ReadResponse(r *bufio.Reader, lineBuf *[]byte) bool {
	ok, eof, wouldBlock := readSingleItem(r, lineBuf, t.item)
	if !ok {
		return false
	}
	if wouldBlock {
		t.wouldBlock = true
		return true
	}
	if !eof {
		t.itemFound = true
	}
	return true
}

func (c *Client) GetDe(item *Item, grace int) error {
	for {
		t := taskGetDe{
			item:       item,
			grace:      grace,
			itemFound:  false,
			wouldBlock: false,
			taskSync: taskSync{
				done: acquireDoneChan(),
			},
		}
		if !c.do(&t) {
			return ErrCommunicationFailure
		}
		if t.wouldBlock {
			time.Sleep(time.Millisecond * time.Duration(100))
			continue
		}
		if !t.itemFound {
			return ErrCacheMiss
		}
		return nil
	}
	panic("unreachable")
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
	if _, err := w.Write(strWs); err != nil {
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
		taskSync: taskSync{
			done: acquireDoneChan(),
		},
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
