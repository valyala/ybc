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
	ErrCacheMiss            = errors.New("memcache: cache miss")
	ErrCommunicationFailure = errors.New("memcache: communication failure")
	ErrMalformedKey         = errors.New("memcache: malformed key")
	ErrNotModified          = errors.New("memcache: item not modified")
)

const (
	defaultConnectionsCount        = 4
	defaultMaxPendingRequestsCount = 1024
)

// Fast memcache client.
//
// The client is goroutine-safe. It is designed to work fast when hundreds
// concurrent goroutines are calling simultaneously its' methods.
//
// The client works with a single memcached server.
type Client struct {
	// TCP address of memcached server to connect to.
	// The address should be in the form addr:port.
	ConnectAddr string

	// The number of simultaneous TCP connections to establish
	// to memcached server.
	//
	// The Client is able to squeeze out impossible from a single
	// connection by pipelining a ton of requests on it.
	// Multiple simultaneous connections may be required in the following
	// cases:
	//   * If memcached server delays incoming requests' execution.
	//     Since memcached protocol doesn't allow out-of-order requests'
	//     execution, a single slow request may delay execution of all
	//     the requests pipelined on the connection after it.
	//     Multiple concurrent connections may help in such a situation.
	//   * If memcached server runs on multi-CPU system, but uses a single
	//     CPU (thread) per connection.
	ConnectionsCount int

	// The maximum number of pending requests awaiting to be processed
	// by memcached server.
	MaxPendingRequestsCount int

	// The size in bytes of buffer used by the Client for reading responses
	// received from memcached per connection.
	ReadBufferSize int

	// The size in bytes of buffer used by the Client for writing requests
	// to be sent to memcached per connection.
	WriteBufferSize int

	// The size in bytes of OS-supplied read buffer per TCP connection.
	OSReadBufferSize int

	// The size in bytes of OS-supplied write buffer per TCP connection.
	OSWriteBufferSize int

	requests chan tasker
	done     *sync.WaitGroup
}

// Memcache item.
type Item struct {
	Key   []byte
	Value []byte

	// Expiration time for the item.
	// Zero means the item has no expiration time.
	Expiration time.Duration

	// An opaque value, which is passed to/from memcache
	Flags uint32
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

func handleAddr(c *Client) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", c.ConnectAddr)
	if err != nil {
		log.Printf("Cannot resolve tcp address=[%s]: [%s]", c.ConnectAddr, err)
		return
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Printf("Cannot establish tcp connection to addr=[%s]: [%s]", tcpAddr, err)
		return
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
}

func addrHandler(c *Client, done *sync.WaitGroup) {
	defer done.Done()
	for {
		handleAddr(c)

		// cancel all pending requests
		for t := range c.requests {
			t.Done(false)
		}

		// wait for new incoming requests
		t, ok := <-c.requests
		if !ok {
			// The requests channel is closed.
			return
		}
		c.requests <- t
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
	if c.requests == nil {
		panic("Did you forgot calling Client.Start()?")
	}
	c.requests <- t
	return t.Wait()
}

// Starts the given client.
func (c *Client) Start() {
	if c.requests != nil || c.done != nil {
		panic("Did you forgot calling Client.Stop() before calling Client.Start()?")
	}
	c.init()
	go c.run()
}

// Stops the given client.
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

func readValueResponse(line []byte) (key []byte, flags uint32, size int, ok bool) {
	ok = false

	if !bytes.HasPrefix(line, strValue) {
		log.Printf("Unexpected line read=[%s]. It should start with [%s]", line, strValue)
		return
	}
	line = line[len(strValue):]

	n := -1

	if key = nextToken(line, &n, "key"); key == nil {
		return
	}
	if flags, ok = parseFlagsToken(line, &n); !ok {
		return
	}
	if size, ok = parseSizeToken(line, &n); !ok {
		return
	}
	if n == len(line) {
		return
	}

	if casidUnused := nextToken(line, &n, "casid"); casidUnused == nil {
		ok = false
		return
	}
	ok = expectEof(line, n)
	return
}

func readValue(r *bufio.Reader, size int) (value []byte, ok bool) {
	var err error
	value, err = ioutil.ReadAll(io.LimitReader(r, int64(size)))
	if err != nil {
		log.Printf("Error when reading value with size=%d: [%s]", size, err)
		ok = false
		return
	}
	ok = matchStr(r, strCrLf)
	return
}

func readKeyValue(r *bufio.Reader, line []byte) (key []byte, flags uint32, value []byte, ok bool) {
	var size int
	key, flags, size, ok = readValueResponse(line)
	if !ok {
		return
	}

	value, ok = readValue(r, size)
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

	item.Key, item.Flags, item.Value, ok = readKeyValue(r, line)
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

// Obtains multiple items associated with the given keys.
//
// Sets Item.Key, Item.Value and Item.Flags for each returned item.
//
// The number of returned items may be smaller than the number of keys,
// because certain items may be missing in the memcache server.
func (c *Client) GetMulti(keys [][]byte) (items []Item, err error) {
	for _, key := range keys {
		if !validateKey(key) {
			err = ErrMalformedKey
			return
		}
	}
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
	item  *Item
	found bool
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
	if ok = matchStr(r, strEnd); !ok {
		return
	}
	if ok = matchStr(r, strCrLf); !ok {
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
	t.found = !eof
	return true
}

// Obtains value (item.Value) and flags (item.Flags) for the given key
// (item.Key) from memcache server.
//
// Returns ErrCacheMiss on cache miss.
func (c *Client) Get(item *Item) error {
	if !validateKey(item.Key) {
		return ErrMalformedKey
	}
	t := taskGet{
		item: item,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	if !t.found {
		return ErrCacheMiss
	}
	return nil
}

// The item for 'conditional set/get' requests - Client.Cget(), Client.Cset(),
// Client.CsetNowait().
type Citem struct {
	Key   []byte
	Value []byte

	// Etag should uniquely identify the given item.
	Etag uint64

	// Validation time. After this period of time the item cannot
	// be returned to the caller without re-validation via Client.Cget().
	ValidateTtl time.Duration

	// Expiration time for the item.
	// Zero means the item has no expiration time.
	Expiration time.Duration
}

type taskCget struct {
	item        *Citem
	found       bool
	notModified bool
	taskSync
}

func (t *taskCget) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return (writeStr(w, strCget) && writeStr(w, t.item.Key) && writeStr(w, strWs) &&
		writeUint64(w, t.item.Etag, scratchBuf) && writeCrLf(w))
}

func (t *taskCget) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	if !readLine(r, scratchBuf) {
		return false
	}
	line := *scratchBuf
	if bytes.Equal(line, strNotFound) {
		t.found = false
		t.notModified = false
		return true
	}
	if bytes.Equal(line, strNotModified) {
		t.found = true
		t.notModified = true
		return true
	}
	if !bytes.HasPrefix(line, strValue) {
		log.Printf("Unexpected line read=[%s]. It should start with [%s]", line, strValue)
		return false
	}
	line = line[len(strValue):]

	n := -1

	size, ok := parseSizeToken(line, &n)
	if !ok {
		return false
	}
	if t.item.Expiration, ok = parseExpirationToken(line, &n); !ok {
		return false
	}
	if t.item.Etag, ok = parseEtagToken(line, &n); !ok {
		return false
	}
	if t.item.ValidateTtl, ok = parseMillisecondsToken(line, &n, "validateTtl"); !ok {
		return false
	}
	if !expectEof(line, n) {
		return false
	}
	if t.item.Value, ok = readValue(r, size); !ok {
		return false
	}
	t.found = true
	t.notModified = false
	return true
}

// Performs conditional get request for the given item.Key and item.Etag.
//
// This is an extension to memcache protocol, so it isn't supported
// by the original memcache server.
//
// Conditional get requests must be performed only on items stored in the cache
// via Client.Cset(). The method returns garbage for items stored via other
// mechanisms.
//
// Fills item.Value, item.Expiration, item.Etag and item.ValidateTtl only
// on cache hit and only if the given etag doesn't match the etag on the server,
// i.e. if the server contains new value under the given key.
//
// Returns ErrCacheMiss on cache miss.
// Returns ErrNotModified if the corresponding item on the server has
// the same etag.
//
// Client.Cset() and Client.Cget() are intended for reducing network bandwidth
// consumption in multi-level caches. They are modelled after HTTP cache
// validation approach with entyty tags -
// see http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.11 .
func (c *Client) Cget(item *Citem) error {
	if !validateKey(item.Key) {
		return ErrMalformedKey
	}
	t := taskCget{
		item: item,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	if t.notModified {
		return ErrNotModified
	}
	if !t.found {
		return ErrCacheMiss
	}
	return nil
}

type taskGetDe struct {
	item          *Item
	graceDuration time.Duration
	found         bool
	wouldBlock    bool
	taskSync
}

func (t *taskGetDe) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return (writeStr(w, strGetDe) && writeStr(w, t.item.Key) && writeStr(w, strWs) &&
		writeMilliseconds(w, t.graceDuration, scratchBuf) && writeCrLf(w))
}

func (t *taskGetDe) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	ok, eof, wouldBlock := readSingleItem(r, scratchBuf, t.item)
	if !ok {
		return false
	}
	if wouldBlock {
		t.found = true
		t.wouldBlock = true
		return true
	}
	t.found = !eof
	t.wouldBlock = false
	return true
}

// Performs dogpile effect-aware get for the given item.Key.
//
// This is an extension to memcache protocol, so it isn't supported
// by the original memcache server.
//
// Returns ErrCacheMiss on cache miss. It is expected that the caller
// will create and store in the cache an item on cache miss during the given
// graceDuration interval.
func (c *Client) GetDe(item *Item, graceDuration time.Duration) error {
	if !validateKey(item.Key) {
		return ErrMalformedKey
	}
	for {
		t := taskGetDe{
			item:          item,
			graceDuration: graceDuration,
		}
		t.Init()
		if !c.do(&t) {
			return ErrCommunicationFailure
		}
		if t.wouldBlock {
			time.Sleep(time.Millisecond * time.Duration(100))
			continue
		}
		if !t.found {
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
	if !writeStr(w, strSet) || !writeStr(w, item.Key) || !writeStr(w, strWs) ||
		!writeUint32(w, item.Flags, scratchBuf) || !writeStr(w, strWs) ||
		!writeExpiration(w, item.Expiration, scratchBuf) || !writeStr(w, strWs) ||
		!writeInt(w, size, scratchBuf) {
		return false
	}
	if noreply {
		if !writeNoreply(w) {
			return false
		}
	}
	return writeCrLf(w) && writeStr(w, item.Value) && writeCrLf(w)
}

func readSetResponse(r *bufio.Reader) bool {
	return matchStr(r, strStored) && matchStr(r, strCrLf)
}

func (t *taskSet) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeSetRequest(w, t.item, false, scratchBuf)
}

func (t *taskSet) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	return readSetResponse(r)
}

// Stores the given item in the memcache server.
func (c *Client) Set(item *Item) error {
	if !validateKey(item.Key) {
		return ErrMalformedKey
	}
	t := taskSet{
		item: item,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	return nil
}

type taskCset struct {
	item *Citem
	taskSync
}

func writeCsetRequest(w *bufio.Writer, item *Citem, noreply bool, scratchBuf *[]byte) bool {
	size := len(item.Value)
	if !writeStr(w, strCset) || !writeStr(w, item.Key) || !writeStr(w, strWs) ||
		!writeExpiration(w, item.Expiration, scratchBuf) || !writeStr(w, strWs) ||
		!writeInt(w, size, scratchBuf) || !writeStr(w, strWs) ||
		!writeUint64(w, item.Etag, scratchBuf) || !writeStr(w, strWs) ||
		!writeMilliseconds(w, item.ValidateTtl, scratchBuf) {
		return false
	}
	if noreply {
		if !writeNoreply(w) {
			return false
		}
	}
	return writeCrLf(w) && writeStr(w, item.Value) && writeCrLf(w)
}

func (t *taskCset) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeCsetRequest(w, t.item, false, scratchBuf)
}

func (t *taskCset) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	return readSetResponse(r)
}

// Performs conditional set for the given item.
//
// This is an extension to memcache protocol, so it isn't supported
// by the original memcache server.
//
// Items stored via this method must be obtained only via Client.Cget() call!
// Calls to other methods such as Client.Get() will return garbage
// for item's key.
//
// Client.Cset() and Client.Cget() are intended for reducing network bandwidth
// consumption in multi-level caches. They are modelled after HTTP cache
// validation approach with entyty tags -
// see http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.11 .
func (c *Client) Cset(item *Citem) error {
	if !validateKey(item.Key) {
		return ErrMalformedKey
	}
	t := taskCset{
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
	item Item
	taskNowait
}

func (t *taskSetNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeSetRequest(w, &t.item, true, scratchBuf)
}

// The same as Client.Set(), but doesn't wait for operation completion.
//
// Do not modify slices pointed by item.Key and item.Value after passing
// to this function - it actually becomes an owner of these slices.
func (c *Client) SetNowait(item *Item) {
	if !validateKey(item.Key) {
		return
	}
	t := taskSetNowait{
		item: *item,
	}
	c.do(&t)
}

type taskCsetNowait struct {
	item Citem
	taskNowait
}

func (t *taskCsetNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeCsetRequest(w, &t.item, true, scratchBuf)
}

// The same as Client.Cset(), but doesn't wait for operation completion.
//
// Do not modify slices pointed by item.Key and item.Value after passing
// to this function - it actually becomes an owner of these slices.
func (c *Client) CsetNowait(item *Citem) {
	if !validateKey(item.Key) {
		return
	}
	t := taskCsetNowait{
		item: *item,
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

// Deletes an item with the given key from memcache server.
//
// Returns ErrCacheMiss if there were no item with such key
// on the server.
func (c *Client) Delete(key []byte) error {
	if !validateKey(key) {
		return ErrMalformedKey
	}
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

// The same as Client.Delete(), but doesn't wait for operation completion.
//
// Do not modify slice pointed by key after passing to this function -
// it actually becomes an owner of this slice.
func (c *Client) DeleteNowait(key []byte) {
	if !validateKey(key) {
		return
	}
	t := taskDeleteNowait{
		key: key,
	}
	c.do(&t)
}

type taskFlushAllDelayed struct {
	expiration time.Duration
	taskSync
}

func (t *taskFlushAllDelayed) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeStr(w, strFlushAll) && writeExpiration(w, t.expiration, scratchBuf) && writeStr(w, strCrLf)
}

func (t *taskFlushAllDelayed) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	return matchStr(r, strOkCrLf)
}

// Flushes all the items on the server after the given expiration delay.
func (c *Client) FlushAllDelayed(expiration time.Duration) error {
	t := taskFlushAllDelayed{
		expiration: expiration,
	}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	return nil
}

type taskFlushAll struct {
	taskSync
}

func (t *taskFlushAll) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeStr(w, strFlushAll) && writeStr(w, strCrLf)
}

func (t *taskFlushAll) ReadResponse(r *bufio.Reader, scratchBuf *[]byte) bool {
	return matchStr(r, strOkCrLf)
}

// Flushes all the items on the server.
func (c *Client) FlushAll() error {
	t := taskFlushAll{}
	t.Init()
	if !c.do(&t) {
		return ErrCommunicationFailure
	}
	return nil
}

type taskFlushAllDelayedNowait struct {
	expiration time.Duration
	taskNowait
}

func (t *taskFlushAllDelayedNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeStr(w, strFlushAll) && writeExpiration(w, t.expiration, scratchBuf) && writeStr(w, strWs) &&
		writeStr(w, strNoreply) && writeStr(w, strCrLf)
}

// The same as Client.FlushAllDelayed(), but doesn't wait for operation
// completion.
func (c *Client) FlushAllDelayedNowait(expiration time.Duration) {
	t := taskFlushAllDelayedNowait{
		expiration: expiration,
	}
	c.do(&t)
}

type taskFlushAllNowait struct {
	taskNowait
}

func (t *taskFlushAllNowait) WriteRequest(w *bufio.Writer, scratchBuf *[]byte) bool {
	return writeStr(w, strFlushAll) && writeStr(w, strNoreply) && writeStr(w, strCrLf)
}

// The same as Client.FlushAll(), but doesn't wait for operation completion.
func (c *Client) FlushAllNowait() {
	t := taskFlushAllNowait{}
	c.do(&t)
}
