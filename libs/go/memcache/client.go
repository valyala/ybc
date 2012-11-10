package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
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
	WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool
	ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool
	Done(ok bool)
	Wait() bool
}

func requestsSender(w *bufio.Writer, requests <-chan tasker, responses chan<- tasker, c net.Conn, done *sync.WaitGroup) {
	defer done.Done()
	defer w.Flush()
	defer close(responses)
	scratchBuf := make([]byte, 0, 1024)
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
		if !t.WriteRequest(w, &scratchBuf) {
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
		log.Fatalf("Cannot set TCP read buffer size to %d: [%s]", c.OSReadBufferSize, err)
	}
	if err = conn.SetWriteBuffer(c.OSWriteBufferSize); err != nil {
		log.Fatalf("Cannot set TCP write buffer size to %d: [%s]", c.OSWriteBufferSize, err)
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

func (t *taskSync) Init() {
	t.done = acquireDoneChan()
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
	ok = false

	if !bytes.Equal(line[:len(strValue)], strValue) {
		log.Printf("Unexpected start of line: [%s]. Expected [%s]", line[:len(strValue)], strValue)
		return
	}
	line = line[len(strValue):]

	n := -1

	key, n = nextToken(line, n, "key")
	if key == nil {
		return
	}
	flagsUnused, n := nextToken(line, n, "flags")
	if flagsUnused == nil {
		return
	}
	sizeStr, n := nextToken(line, n, "size")
	if sizeStr == nil {
		return
	}
	if size, ok = parseInt(sizeStr); !ok {
		return
	}

	if n == len(line) {
		return
	}

	casidUnused, n := nextToken(line, n, "casid")
	if casidUnused == nil {
		ok = false
		return
	}
	ok = expectEof(line, n)
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
		log.Printf("Error when reading value from 'get' response for key=[%s]: [%s]", key, err)
		ok = false
		return
	}
	ok = matchBytes(r, strCrLf)
	return
}

func readItem(r *bufio.Reader, scratchBuf *[]byte, item *Item) (ok bool, eof bool, wouldBlock bool) {
	if ok = readLine(r, scratchBuf); !ok {
		return
	}
	line := *scratchBuf
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

func (t *taskGetMulti) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	if !writeStr(w, strGets) {
		return false
	}
	keysCount := len(t.keys)
	if keysCount > 0 {
		if !writeStr(w, t.keys[0]) {
			return false
		}
	}
	for i := 1; i < keysCount; i++ {
		if writeStr(w, strWs) && !writeStr(w, t.keys[i]) {
			return false
		}
	}
	return writeCrLf(w)
}

func (t *taskGetMulti) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	var item Item
	for {
		ok, eof, _ := readItem(r, scratchBuf, &item)
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
	}
	t.Init()
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

func (t *taskGet) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeStr(w, strGet) && writeStr(w, t.item.Key) && writeCrLf(w)
}

func readSingleItem(r *bufio.Reader, scratchBuf *[]byte, item *Item) (ok bool, eof bool, wouldBlock bool) {
	keyOriginal := item.Key
	ok, eof, wouldBlock = readItem(r, scratchBuf, item)
	if !ok || eof || wouldBlock {
		return
	}
	if ok = matchBytes(r, strEnd); !ok {
		return
	}
	if ok = matchBytes(r, strCrLf); !ok {
		return
	}
	if ok = bytes.Equal(keyOriginal, item.Key); !ok {
		log.Printf("Key mismatch! Expected [%s], but server returned [%s]", keyOriginal, item.Key)
		return
	}
	item.Key = keyOriginal
	return
}

func (t *taskGet) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	ok, eof, _ := readSingleItem(r, scratchBuf, t.item)
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
	}
	t.Init()
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

func (t *taskGetDe) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return (writeStr(w, strGetDe) && writeStr(w, t.item.Key) && writeStr(w, strWs) &&
		writeInt(w, t.grace, scratchBuf) && writeCrLf(w))
}

func (t *taskGetDe) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	ok, eof, wouldBlock := readSingleItem(r, scratchBuf, t.item)
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

// grace period in milliseconds
func (c *Client) GetDe(item *Item, grace int) error {
	for {
		t := taskGetDe{
			item:       item,
			grace:      grace,
			itemFound:  false,
			wouldBlock: false,
		}
		t.Init()
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

func writeSetRequest(w *bufio.Writer, item *Item, noreply bool, scratchBuf *[]byte) bool {
	size := len(item.Value)
	if !writeStr(w, strSet) || !writeStr(w, item.Key) || !writeStr(w, strZero) ||
		!writeInt(w, item.Expiration, scratchBuf) || !writeStr(w, strWs) || !writeInt(w, size, scratchBuf) {
		return false
	}
	if noreply {
		if !writeNoreply(w) {
			return false
		}
	}
	return writeCrLf(w) && writeStr(w, item.Value) && writeCrLf(w)
}

func (t *taskSet) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeSetRequest(w, t.item, false, scratchBuf)
}

func (t *taskSet) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	if !readLine(r, scratchBuf) {
		return false
	}
	line := *scratchBuf
	if !bytes.Equal(line, strStored) {
		log.Printf("Unexpected response obtained for 'set' request for key=[%s], len(value)=%d: [%s]", t.item.Key, len(t.item.Value), line)
		return false
	}
	return true
}

func (c *Client) Set(item *Item) error {
	t := taskSet{
		item: item,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	return nil
}

type taskNowait struct{}

func (t *taskNowait) Done(ok bool) {}

func (t *taskNowait) Wait() bool {
	return true
}

func (t *taskNowait) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	return true
}

type taskSetNowait struct {
	item *Item
	taskNowait
}

func (t *taskSetNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeSetRequest(w, t.item, true, scratchBuf)
}

func (c *Client) SetNowait(item *Item) {
	t := taskSetNowait{
		item: item,
	}
	c.do(&t)
}

type taskDelete struct {
	key         []byte
	itemDeleted bool
	taskSync
}

func writeDeleteRequest(w *bufio.Writer, key []byte, noreply bool) bool {
	if !writeStr(w, strDelete) || !writeStr(w, key) {
		return false
	}
	if noreply {
		if !writeNoreply(w) {
			return false
		}
	}
	return writeCrLf(w)
}

func (t *taskDelete) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeDeleteRequest(w, t.key, false)
}

func (t *taskDelete) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	if !readLine(r, scratchBuf) {
		return false
	}
	line := *scratchBuf
	if bytes.Equal(line, strDeleted) {
		t.itemDeleted = true
		return true
	}
	if bytes.Equal(line, strNotFound) {
		t.itemDeleted = false
		return true
	}
	log.Printf("Unexpected response for 'delete' request: [%s]", line)
	return false
}

func (c *Client) Delete(key []byte) error {
	t := taskDelete{
		key: key,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	if !t.itemDeleted {
		return ErrCacheMiss
	}
	return nil
}

type taskDeleteNowait struct {
	key []byte
	taskNowait
}

func (t *taskDeleteNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeDeleteRequest(w, t.key, true)
}

func (c *Client) DeleteNowait(key []byte) {
	t := taskDeleteNowait{
		key: key,
	}
	c.do(&t)
}
