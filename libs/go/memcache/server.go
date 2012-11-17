package memcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/valyala/ybc/bindings/go/ybc"
	"log"
	"net"
	"sync"
	"time"
)

func writeItem(w *bufio.Writer, item *ybc.Item, size int) bool {
	n, err := item.WriteTo(w)
	if err != nil {
		log.Printf("Error when writing payload with size=[%d] to output stream: [%s]", size, err)
		return false
	}
	if n != int64(size) {
		log.Printf("Invalid length of payload=[%d] written to output stream. Expected [%d]", n, size)
		return false
	}
	return writeCrLf(w)
}

func writeGetResponse(w *bufio.Writer, key []byte, item *ybc.Item, cas bool, scratchBuf *[]byte) bool {
	var flags uint32
	if err := binaryRead(item, &flags, "flags"); err != nil {
		return false
	}

	size := item.Available()
	if !writeStr(w, strValue) || !writeStr(w, key) || !writeStr(w, strWs) ||
		!writeUint32(w, flags, scratchBuf) || !writeStr(w, strWs) ||
		!writeInt(w, size, scratchBuf) {
		return false
	}
	if cas {
		if !writeStr(w, strWs) || !writeStr(w, strZero) {
			return false
		}
	}
	return writeStr(w, strCrLf) && writeItem(w, item, size)
}

func getItemAndWriteResponse(w *bufio.Writer, cache ybc.Cacher, key []byte, cas bool, scratchBuf *[]byte) bool {
	item, err := cache.GetItem(key)
	if err != nil {
		if err == ybc.ErrCacheMiss {
			return true
		}
		log.Fatalf("Unexpected error returned by cache.GetItem(key=[%s]): [%s]", key, err)
	}
	defer item.Close()

	return writeGetResponse(w, key, item, cas, scratchBuf)
}

func writeEndCrLf(w *bufio.Writer) bool {
	return writeStr(w, strEnd) && writeCrLf(w)
}

func processGetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte, cas bool) bool {
	last := -1
	lineSize := len(line)
	for last < lineSize {
		first := last + 1
		last = bytes.IndexByte(line[first:], ' ')
		if last == -1 {
			last = lineSize
		} else {
			last += first
		}
		if first == last {
			continue
		}
		key := line[first:last]
		if !getItemAndWriteResponse(c.Writer, cache, key, cas, scratchBuf) {
			return false
		}
	}

	return writeEndCrLf(c.Writer)
}

func processGetDeCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key := nextToken(line, &n, "key")
	if key == nil {
		return false
	}
	graceDuration, ok := parseMillisecondsToken(line, &n, "graceDuration")
	if !ok {
		return false
	}
	if !expectEof(line, n) {
		return false
	}

	item, err := cache.GetDeAsyncItem(key, graceDuration)
	if err != nil {
		if err == ybc.ErrWouldBlock {
			return writeStr(c.Writer, strWouldBlock) && writeCrLf(c.Writer)
		}
		if err == ybc.ErrCacheMiss {
			return writeEndCrLf(c.Writer)
		}
		log.Fatalf("Unexpected error returned by Cache.GetDeAsyncItem(): [%s]", err)
	}
	defer item.Close()

	return writeGetResponse(c.Writer, key, item, false, scratchBuf) && writeEndCrLf(c.Writer)
}

func writeCgetResponse(w *bufio.Writer, etag uint64, item *ybc.Item, scratchBuf *[]byte) bool {
	var validateTtl, flags uint32
	if err := binaryRead(item, &validateTtl, "validateTtl"); err != nil {
		return false
	}
	if err := binaryRead(item, &flags, "flags"); err != nil {
		return false
	}

	size := item.Available()
	expiration := item.Ttl()
	return writeStr(w, strValue) && writeInt(w, size, scratchBuf) && writeStr(w, strWs) &&
		writeUint32(w, flags, scratchBuf) && writeStr(w, strWs) &&
		writeExpiration(w, expiration, scratchBuf) && writeStr(w, strWs) &&
		writeUint64(w, etag, scratchBuf) && writeStr(w, strWs) &&
		writeUint32(w, validateTtl, scratchBuf) && writeStr(w, strCrLf) &&
		writeItem(w, item, size)
}

func cGetFromCache(cache ybc.Cacher, key []byte, etag *uint64) (item *ybc.Item, err error) {
	item, err = cache.GetItem(key)
	if err == ybc.ErrCacheMiss {
		return
	}
	if err != nil {
		log.Fatalf("Unexpected error returned from Cache.GetItem() for key=[%s]: [%s]", key, err)
	}
	defer func() {
		if err != nil {
			item.Close()
		}
	}()

	etagOld := *etag
	if err = binaryRead(item, etag, "etag"); err != nil {
		return
	}
	if etagOld == *etag {
		item.Close()
		item = nil
		return
	}
	return
}

func processCgetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key := nextToken(line, &n, "key")
	if key == nil {
		return false
	}
	etag, ok := parseEtagToken(line, &n)
	if !ok {
		return false
	}

	item, err := cGetFromCache(cache, key, &etag)
	if err == ybc.ErrCacheMiss {
		return writeStr(c.Writer, strNotFound) && writeStr(c.Writer, strCrLf)
	}
	if err != nil {
		return false
	}
	if item == nil {
		return writeStr(c.Writer, strNotModified) && writeStr(c.Writer, strCrLf)
	}
	defer item.Close()

	return writeCgetResponse(c.Writer, etag, item, scratchBuf)
}

func expectNoreply(line []byte, n *int) bool {
	noreplyStr := nextToken(line, n, "noreply")
	if noreplyStr == nil {
		return false
	}
	if !bytes.Equal(noreplyStr, strNoreply) {
		log.Printf("Unexpected noreply in line=[%s]: [%s]. Expected [%s]", line, noreplyStr, strNoreply)
		return false
	}
	return true
}

func parseSetCmd(line []byte) (key []byte, flags uint32, expiration time.Duration, size int, noreply bool, ok bool) {
	n := -1

	ok = false
	if key = nextToken(line, &n, "key"); key == nil {
		return
	}
	if flags, ok = parseFlagsToken(line, &n); !ok {
		return
	}
	if expiration, ok = parseExpirationToken(line, &n); !ok {
		return
	}
	if size, ok = parseSizeToken(line, &n); !ok {
		return
	}

	noreply = false
	if n == len(line) {
		ok = true
		return
	}

	if ok = expectNoreply(line, &n); !ok {
		return
	}
	if !expectEof(line, n) {
		ok = false
		return
	}
	noreply = true
	ok = true
	return
}

func readValueAndWriteResponse(c *bufio.ReadWriter, txn *ybc.SetTxn, size int, noreply bool) bool {
	n, err := txn.ReadFrom(c.Reader)
	if err != nil {
		log.Printf("Error when reading payload with size=[%d]: [%s]", size, err)
		return false
	}
	if n != int64(size) {
		log.Printf("Unexpected payload size=[%d]. Expected [%d]", n, size)
		return false
	}
	if !matchStr(c.Reader, strCrLf) {
		return false
	}
	if noreply {
		return true
	}
	return writeStr(c.Writer, strStored) && writeCrLf(c.Writer)
}

func setToCache(cache ybc.Cacher, key []byte, flags uint32, expiration time.Duration, size int) *ybc.SetTxn {
	size += binary.Size(&flags)
	txn, err := cache.NewSetTxn(key, size, expiration)
	if err != nil {
		log.Printf("Error in Cache.NewSetTxn() for key=[%s], size=[%d], expiration=[%s]: [%s]", key, size, expiration, err)
		return nil
	}
	binaryWrite(txn, &flags, "flags")
	return txn
}

func processSetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	key, flags, expiration, size, noreply, ok := parseSetCmd(line)
	if !ok {
		return false
	}

	txn := setToCache(cache, key, flags, expiration, size)
	if txn == nil {
		return false
	}
	defer txn.Commit()

	return readValueAndWriteResponse(c, txn, size, noreply)
}

func parseCsetCmd(line []byte) (key []byte, size int, flags uint32, expiration time.Duration, etag uint64, validateTtl uint32, noreply bool, ok bool) {
	n := -1

	ok = false
	if key = nextToken(line, &n, "key"); key == nil {
		return
	}
	if size, ok = parseSizeToken(line, &n); !ok {
		return
	}
	if flags, ok = parseFlagsToken(line, &n); !ok {
		return
	}
	if expiration, ok = parseExpirationToken(line, &n); !ok {
		return
	}
	if etag, ok = parseEtagToken(line, &n); !ok {
		return
	}
	if validateTtl, ok = parseUint32Token(line, &n, "validateTtl"); !ok {
		return
	}

	noreply = false
	if n == len(line) {
		ok = true
		return
	}
	if ok = expectNoreply(line, &n); !ok {
		return
	}
	if !expectEof(line, n) {
		return
	}
	noreply = true
	ok = true
	return
}

func cSetToCache(cache ybc.Cacher, key []byte, size int, flags uint32, expiration time.Duration, etag uint64, validateTtl uint32) *ybc.SetTxn {
	size += binary.Size(&etag) + binary.Size(&validateTtl) + binary.Size(&flags)
	txn, err := cache.NewSetTxn(key, size, expiration)
	if err != nil {
		log.Printf("Error in Cache.NewSetTxn() for key=[%s], size=[%d], expiration=[%s]: [%s]", key, size, expiration, err)
		return nil
	}
	binaryWrite(txn, &etag, "etag")
	binaryWrite(txn, &validateTtl, "validateTtl")
	binaryWrite(txn, &flags, "flags")
	return txn
}

func processCsetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	key, size, flags, expiration, etag, validateTtl, noreply, ok := parseCsetCmd(line)
	if !ok {
		return false
	}

	txn := cSetToCache(cache, key, size, flags, expiration, etag, validateTtl)
	if txn == nil {
		return false
	}
	defer txn.Commit()

	return readValueAndWriteResponse(c, txn, size, noreply)
}

func processDeleteCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key := nextToken(line, &n, "key")
	if key == nil {
		return false
	}

	noreply := false
	if n < len(line) {
		if !expectNoreply(line, &n) {
			return false
		}
		noreply = true
	}
	if !expectEof(line, n) {
		return false
	}

	ok := cache.Delete(key)
	if noreply {
		return true
	}

	response := strDeleted
	if !ok {
		response = strNotFound
	}
	return writeStr(c.Writer, response) && writeCrLf(c.Writer)
}

func parseFlushAllCmd(line []byte) (expiration time.Duration, noreply bool, ok bool) {
	if len(line) == 0 {
		noreply = false
		ok = true
		return
	}

	ok = false
	noreply = false
	n := -1

	s := nextToken(line, &n, "expiration_or_noreply")
	if s == nil {
		return
	}
	if bytes.Equal(s, strNoreply) {
		noreply = true
		ok = expectEof(line, n)
		return
	}

	if expiration, ok = parseExpiration(s); !ok {
		return
	}
	if n == len(line) {
		ok = true
		return
	}

	if ok = expectNoreply(line, &n); !ok {
		return
	}
	noreply = true
	ok = expectEof(line, n)
	return
}

func cacheClearFunc(cache ybc.Cacher) func() {
	return func() { cache.Clear() }
}

func processFlushAllCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, flushAllTimer **time.Timer) bool {
	expiration, noreply, ok := parseFlushAllCmd(line)
	if !ok {
		return false
	}
	(*flushAllTimer).Stop()
	if expiration <= 0 {
		cache.Clear()
	} else {
		*flushAllTimer = time.AfterFunc(expiration, cacheClearFunc(cache))
	}
	if noreply {
		return true
	}
	return writeStr(c.Writer, strOkCrLf)
}

func processRequest(c *bufio.ReadWriter, cache ybc.Cacher, scratchBuf *[]byte, flushAllTimer **time.Timer) bool {
	if !readLine(c.Reader, scratchBuf) {
		return false
	}
	line := *scratchBuf
	if len(line) == 0 {
		return false
	}
	if bytes.HasPrefix(line, strGet) {
		return processGetCmd(c, cache, line[len(strGet):], scratchBuf, false)
	}
	if bytes.HasPrefix(line, strGets) {
		return processGetCmd(c, cache, line[len(strGets):], scratchBuf, true)
	}
	if bytes.HasPrefix(line, strGetDe) {
		return processGetDeCmd(c, cache, line[len(strGetDe):], scratchBuf)
	}
	if bytes.HasPrefix(line, strCget) {
		return processCgetCmd(c, cache, line[len(strCget):], scratchBuf)
	}
	if bytes.HasPrefix(line, strSet) {
		return processSetCmd(c, cache, line[len(strSet):], scratchBuf)
	}
	if bytes.HasPrefix(line, strCset) {
		return processCsetCmd(c, cache, line[len(strCset):], scratchBuf)
	}
	if bytes.HasPrefix(line, strDelete) {
		return processDeleteCmd(c, cache, line[len(strDelete):], scratchBuf)
	}
	if bytes.HasPrefix(line, strFlushAll) {
		return processFlushAllCmd(c, cache, line[len(strFlushAll):], flushAllTimer)
	}
	log.Printf("Unrecognized command=[%s]", line)
	return false
}

func handleConn(conn net.Conn, cache ybc.Cacher, readBufferSize, writeBufferSize int, done *sync.WaitGroup) {
	defer conn.Close()
	defer done.Done()
	r := bufio.NewReaderSize(conn, readBufferSize)
	w := bufio.NewWriterSize(conn, writeBufferSize)
	c := bufio.NewReadWriter(r, w)
	defer w.Flush()

	flushAllTimer := time.NewTimer(0)
	defer flushAllTimer.Stop()

	scratchBuf := make([]byte, 0, 1024)
	for {
		if !processRequest(c, cache, &scratchBuf, &flushAllTimer) {
			break
		}
		if r.Buffered() == 0 {
			w.Flush()
		}
	}
}

// Memcache server.
type Server struct {
	// The underlying cache storage.
	// Required parameter.
	//
	// The cache must be initialized before passing it here.
	//
	// Currently ybc.Cache and ybc.Cluster may be passed here.
	Cache ybc.Cacher

	// TCP address to listen to. Must be in the form addr:port.
	// Required parameter.
	ListenAddr string

	// The size of buffer used for reading requests from clients
	// per each connection.
	// Optional parameter.
	ReadBufferSize int

	// The size of buffer used for writing responses to clients
	// per each connection.
	// Optional parameter.
	WriteBufferSize int

	// The size in bytes of OS-supplied read buffer per TCP connection.
	// Optional parameter.
	OSReadBufferSize int

	// The size in bytes of OS-supplied write buffer per TCP connection.
	// Optional parameter.
	OSWriteBufferSize int

	listenSocket *net.TCPListener
	done         *sync.WaitGroup
	err          error
}

func (s *Server) init() {
	if s.ReadBufferSize == 0 {
		s.ReadBufferSize = defaultReadBufferSize
	}
	if s.WriteBufferSize == 0 {
		s.WriteBufferSize = defaultWriteBufferSize
	}
	if s.OSReadBufferSize == 0 {
		s.OSReadBufferSize = defaultOSReadBufferSize
	}
	if s.OSWriteBufferSize == 0 {
		s.OSWriteBufferSize = defaultOSWriteBufferSize
	}

	listenAddr, err := net.ResolveTCPAddr("tcp", s.ListenAddr)
	if err != nil {
		log.Fatalf("Cannot resolve listenAddr=[%s]: [%s]", s.ListenAddr, err)
	}
	s.listenSocket, err = net.ListenTCP("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Cannot listen for ListenAddr=[%s]: [%s]", listenAddr, err)
	}
	s.done = &sync.WaitGroup{}
	s.done.Add(1)
}

func (s *Server) run() {
	defer s.done.Done()

	connsDone := &sync.WaitGroup{}
	defer connsDone.Wait()
	for {
		conn, err := s.listenSocket.AcceptTCP()
		if err != nil {
			s.err = err
			break
		}
		if err = conn.SetReadBuffer(s.OSReadBufferSize); err != nil {
			log.Fatalf("Cannot set TCP read buffer size to %d: [%s]", s.OSReadBufferSize, err)
		}
		if err = conn.SetWriteBuffer(s.OSWriteBufferSize); err != nil {
			log.Fatalf("Cannot set TCP write buffer size to %d: [%s]", s.OSWriteBufferSize, err)
		}
		connsDone.Add(1)
		go handleConn(conn, s.Cache, s.ReadBufferSize, s.WriteBufferSize, connsDone)
	}
}

// Starts the given server.
//
// No longer needed servers must be stopped via Server.Stop() call.
func (s *Server) Start() {
	if s.listenSocket != nil || s.done != nil {
		panic("Did you forgot calling Server.Stop() before calling Server.Start()?")
	}
	s.init()
	go s.run()
}

// Waits until the server is stopped.
func (s *Server) Wait() error {
	s.done.Wait()
	return s.err
}

// Start the server and waits until it is stopped via Server.Stop() call.
func (s *Server) Serve() error {
	s.Start()
	return s.Wait()
}

// Stops the server, which has been started via either Server.Start()
// or Server.Serve() calls.
func (s *Server) Stop() {
	s.listenSocket.Close()
	s.Wait()
	s.listenSocket = nil
	s.done = nil
}
