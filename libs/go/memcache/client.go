package memcache

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

var (
	ErrCacheMiss            = errors.New("cache miss")
	ErrCommunicationFailure = errors.New("communication failure")
)

type tasker interface {
	WriteRequest(*bufio.Writer) bool
	ReadResponse(*bufio.Reader) bool
	Done(bool)
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
	for t := range responses {
		if !t.ReadResponse(r) {
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
	sendRecvDone.Add(2)
	go requestsSender(w, requests, responses, c, sendRecvDone)
	go responsesReceiver(r, responses, c, sendRecvDone)

	sendRecvDone.Wait()
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
	defaultMaxPendingRequestsCount = 100
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
	Flags      int
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
	for i := 0; i < c.ConnectionsCount; i++ {
		connsDone.Add(1)
		go addrHandler(c.ConnectAddr, c.ReadBufferSize, c.WriteBufferSize, c.MaxPendingRequestsCount, c.ReconnectTimeout, c.requests, connsDone)
	}
	connsDone.Wait()
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

func readItem(r *bufio.Reader, keys []string) (item *Item, ok bool) {
	ok = true
	line, err := r.ReadBytes('\n')
	if err != nil {
		log.Printf("Error when reading 'get' response for keys=[%s]: [%s]", keys, err)
		ok = false
		return
	}
	if bytes.Equal(line, []byte("END\r\n")) {
		return
	}
	var size, casid_unused int
	item = &Item{}
	n, err := fmt.Sscanf(string(line), "VALUE %s %d %d %d\r\n",
		&item.Key, &item.Flags, &size, &casid_unused)
	if err != nil {
		log.Printf("Error when reading VALUE header from 'get' response for keys=[%s]: [%s]", keys, err)
		ok = false
		return
	}
	if n != 4 {
		log.Printf("Unexpected number of arguments in VALUE line of 'get' response for keys=[%s]", keys)
		ok = false
		return
	}
	item.Value, err = ioutil.ReadAll(io.LimitReader(r, int64(size)+2))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", item.Key, err)
		ok = false
		return
	}
	if !bytes.HasSuffix(item.Value, []byte("\r\n")) {
		log.Printf("There is no crlf found in 'get' response value for key=[%s]", item.Key)
		ok = false
		return
	}
	item.Value = item.Value[:size]
	return
}

func (t *taskGetMulti) WriteRequest(w *bufio.Writer) bool {
	_, err := fmt.Fprintf(w, "gets %s\r\n", strings.Join(t.keys, " "))
	if err != nil {
		log.Printf("Cannot issue 'gets' request for keys=[%s]: [%s]", t.keys, err)
		return false
	}
	return true
}

func (t *taskGetMulti) ReadResponse(r *bufio.Reader) bool {
	items := t.items
	for {
		item, ok := readItem(r, t.keys)
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
	_, err := fmt.Fprintf(w, "get %s\r\n", t.key)
	if err != nil {
		log.Printf("Cannot issue 'get' request for key=[%s]: [%s]", t.key, err)
		return false
	}
	return true
}

func (t *taskGet) ReadResponse(r *bufio.Reader) bool {
	line, err := r.ReadBytes('\n')
	if bytes.Equal(line, []byte("END\r\n")) {
		return true
	}
	var size, casid_unused int
	t.item = &Item{}
	n, err := fmt.Sscanf(string(line), "VALUE %s %d %d %d\r\n",
		&t.item.Key, &t.item.Flags, &size, &casid_unused)
	if err != nil {
		log.Printf("Error when reading VALUE header from 'get' response for key=[%s]: [%s]", t.key, err)
		return false
	}
	if n != 4 {
		log.Printf("Unexpected number of arguments in VALUE line of 'get' response for key=[%s]", t.key)
		return false
	}
	if t.item.Key != t.key {
		log.Printf("Unexpected key=[%s] obtained from response. Expected [%s]", t.item.Key, t.key)
		return false
	}
	t.item.Value, err = ioutil.ReadAll(io.LimitReader(r, int64(size)+7))
	if err != nil {
		log.Printf("Error when reading VALUE from 'get' response for key=[%s]: [%s]", t.key, err)
		return false
	}
	if !bytes.HasSuffix(t.item.Value, []byte("\r\nEND\r\n")) {
		log.Printf("There is no crlf found in 'get' response value for key=[%s]", t.key)
		return false
	}
	t.item.Value = t.item.Value[:size]
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

func writeSetRequest(w *bufio.Writer, item *Item, isNoreply bool) bool {
	noreply := ""
	if isNoreply {
		noreply = " noreply"
	}
	_, err := fmt.Fprintf(w, "set %s %d %d %d%s\r\n",
		item.Key, item.Flags, item.Expiration, len(item.Value), noreply)
	if err != nil {
		log.Printf("Cannot issue 'set' request for key=[%s], len(value)=%d: [%s]", item.Key, len(item.Value), err)
		return false
	}
	if _, err = w.Write(item.Value); err != nil {
		log.Printf("Error when sending value in 'set' request for key=[%s]. len(value)=%d: [%s]", item.Key, len(item.Value), err)
		return false
	}
	if _, err = w.Write([]byte("\r\n")); err != nil {
		log.Printf("Error when sending crlf for 'set' request for key=[%s]: [%s]", item.Key, err)
		return false
	}
	return true
}

func (t *taskSet) WriteRequest(w *bufio.Writer) bool {
	return writeSetRequest(w, t.item, false)
}

func (t *taskSet) ReadResponse(r *bufio.Reader) bool {
	_, err := fmt.Fscanf(r, "STORED\r\n")
	if err != nil {
		log.Printf("Unexpected response obtained for 'set' request for key=[%s], len(value)=%d", t.item.Key, len(t.item.Value))
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

func (t *taskSetNowait) ReadResponse(r *bufio.Reader) bool {
	return true
}

func (c *Client) SetNowait(item *Item) {
	t := &taskSetNowait{
		item: item,
	}
	c.do(t)
}
