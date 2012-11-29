package memcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/valyala/ybc/bindings/go/ybc"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	casidCounter uint64
	casidLock    sync.Mutex
)

func init() {
	casidCounter = uint64(time.Now().UnixNano())
}

func getCasid() uint64 {
	return atomic.AddUint64(&casidCounter, 1)
}

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

func writeGetResponse(w *bufio.Writer, key []byte, item *ybc.Item, shouldWriteCasid bool, scratchBuf *[]byte) bool {
	var casid uint64
	var buf [casidSize + flagsSize]byte
	if _, err := item.Read(buf[:]); err != nil {
		log.Printf("error when reading item metadata: [%s]", err)
		return false
	}
	if shouldWriteCasid {
		casid = binary.LittleEndian.Uint64(buf[:])
	}
	flags := binary.LittleEndian.Uint32(buf[casidSize:])

	size := item.Available()
	if !writeStr(w, strValue) || !writeStr(w, key) || !writeWs(w) ||
		!writeUint32(w, flags, scratchBuf) || !writeWs(w) ||
		!writeInt(w, size, scratchBuf) {
		return false
	}

	if shouldWriteCasid {
		if !writeWs(w) || !writeUint64(w, casid, scratchBuf) {
			return false
		}
	}

	return writeStr(w, strCrLf) && writeItem(w, item, size)
}

func getItemAndWriteResponse(w *bufio.Writer, cache ybc.Cacher, key []byte, shouldWriteCasid bool, scratchBuf *[]byte) bool {
	item, err := cache.GetItem(key)
	if err != nil {
		if err == ybc.ErrCacheMiss {
			return true
		}
		log.Fatalf("Unexpected error returned by cache.GetItem(key=[%s]): [%s]", key, err)
	}
	defer item.Close()

	return writeGetResponse(w, key, item, shouldWriteCasid, scratchBuf)
}

func writeGetResponseWithEof(w *bufio.Writer, key []byte, item *ybc.Item, scratchBuf *[]byte) bool {
	return writeGetResponse(w, key, item, true, scratchBuf) && writeStr(w, strEndCrLf)
}

func writeEndCrLf(w *bufio.Writer) bool {
	return writeStr(w, strEndCrLf)
}

func processGetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte, shouldWriteCasid bool) bool {
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
		if !getItemAndWriteResponse(c.Writer, cache, key, shouldWriteCasid, scratchBuf) {
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
			return writeStr(c.Writer, strWouldBlockCrLf)
		}
		if err == ybc.ErrCacheMiss {
			return writeEndCrLf(c.Writer)
		}
		log.Fatalf("Unexpected error returned by Cache.GetDeAsyncItem(): [%s]", err)
	}
	defer item.Close()

	return writeGetResponseWithEof(c.Writer, key, item, scratchBuf)
}

func checkAndUpdateCasid(item *ybc.Item, casid *uint64) (isModified, ok bool) {
	casidOld := *casid
	var buf [casidSize]byte
	if _, err := item.Read(buf[:]); err != nil {
		log.Printf("Cannod read casid from item: [%s]", err)
		return
	}
	*casid = binary.LittleEndian.Uint64(buf[:])

	if _, err := item.Seek(-casidSize, 1); err != nil {
		log.Fatalf("Unexpected error returned from ybc.Item.Seek(%d, 1): [%s]", -casidSize, err)
	}

	isModified = (casidOld != *casid)
	ok = true
	return
}

func processCgetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key := nextToken(line, &n, "key")
	if key == nil {
		return false
	}
	casid, ok := parseUint64Token(line, &n, "casid")
	if !ok {
		return false
	}
	if !expectEof(line, n) {
		return false
	}

	item, err := cache.GetItem(key)
	if err == ybc.ErrCacheMiss {
		return writeStr(c.Writer, strEndCrLf)
	}
	if err != nil {
		log.Fatalf("Unexpected error returned: [%s]", err)
	}
	defer item.Close()

	isModified, ok := checkAndUpdateCasid(item, &casid)
	if !ok {
		return false
	}
	if !isModified {
		return writeStr(c.Writer, strNotModifiedCrLf)
	}

	return writeGetResponseWithEof(c.Writer, key, item, scratchBuf)
}

func processCgetDeCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key := nextToken(line, &n, "key")
	if key == nil {
		return false
	}
	casid, ok := parseUint64Token(line, &n, "casid")
	if !ok {
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
	if err == ybc.ErrWouldBlock {
		return writeStr(c.Writer, strWouldBlockCrLf)
	}
	if err == ybc.ErrCacheMiss {
		return writeStr(c.Writer, strEndCrLf)
	}
	if err != nil {
		log.Fatalf("Unexpected error returned: [%s]", err)
	}
	defer item.Close()

	isModified, ok := checkAndUpdateCasid(item, &casid)
	if !ok {
		return false
	}
	if !isModified {
		return writeStr(c.Writer, strNotModifiedCrLf)
	}

	return writeGetResponseWithEof(c.Writer, key, item, scratchBuf)
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

func parseSetCmd(line []byte, shouldParseCasid bool) (key []byte, flags uint32, expiration time.Duration, size int, casid uint64, noreply bool, ok bool) {
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
	if shouldParseCasid {
		if casid, ok = parseUint64Token(line, &n, "casid"); !ok {
			return
		}
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

func readValueToTxn(r *bufio.Reader, txn *ybc.SetTxn, size int) bool {
	n, err := txn.ReadFrom(r)
	if err != nil {
		log.Printf("Error when reading payload with size=[%d]: [%s]", size, err)
		return false
	}
	if n != int64(size) {
		log.Printf("Unexpected payload size=[%d]. Expected [%d]", n, size)
		return false
	}
	return matchCrLf(r)
}

func writeSetResponse(w *bufio.Writer, noreply bool) bool {
	if noreply {
		return true
	}
	return writeStr(w, strStoredCrLf)
}

func startSetTxn(cache ybc.Cacher, key []byte, flags uint32, expiration time.Duration, size int) *ybc.SetTxn {
	casid := getCasid()
	size += casidSize + flagsSize
	txn, err := cache.NewSetTxn(key, size, expiration)
	if err != nil {
		log.Printf("Error in Cache.NewSetTxn() for key=[%s], size=[%d], expiration=[%s]: [%s]", key, size, expiration, err)
		return nil
	}

	var buf [casidSize + flagsSize]byte
	binary.LittleEndian.PutUint64(buf[:casidSize], casid)
	binary.LittleEndian.PutUint32(buf[casidSize:], flags)
	n, err := txn.Write(buf[:])
	if err != nil {
		log.Fatalf("Error in SetTxn.Write(): [%s]", err)
	}
	if n != len(buf) {
		log.Fatalf("Unexpected result returned from SetTxn.Write(): %d. Expected %d", n, len(buf))
	}
	return txn
}

func closeSetTxn(txn *ybc.SetTxn, shouldRollback *bool) {
	if *shouldRollback {
		txn.Rollback()
	}
}

func readValueToTxnAndWriteResponse(c *bufio.ReadWriter, txn *ybc.SetTxn, size int, noreply bool) bool {
	if txn == nil {
		return false
	}
	shouldRollback := true
	defer closeSetTxn(txn, &shouldRollback)

	if !readValueToTxn(c.Reader, txn, size) {
		return false
	}

	if err := txn.Commit(); err != nil {
		log.Fatalf("Unexpected error returned from SetTxn.Commit(): [%s]", err)
	}
	shouldRollback = false
	return writeSetResponse(c.Writer, noreply)
}

func processSetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	key, flags, expiration, size, _, noreply, ok := parseSetCmd(line, false)
	if !ok {
		return false
	}

	txn := startSetTxn(cache, key, flags, expiration, size)
	return readValueToTxnAndWriteResponse(c, txn, size, noreply)
}

func getCasidForCachedItem(cache ybc.Cacher, key []byte) (casid uint64, cacheMiss, ok bool) {
	item, err := cache.GetItem(key)
	if err != nil {
		if err == ybc.ErrCacheMiss {
			cacheMiss = true
			return
		}
		log.Fatalf("Unexpected error returned from Cache.GetItem() for key=[%s]: [%s]", key, err)
	}
	defer item.Close()

	var buf [casidSize]byte
	if _, err = item.Read(buf[:]); err != nil {
		log.Printf("Error when reading casid for the item: [%s]", err)
		return
	}
	casid = binary.LittleEndian.Uint64(buf[:])
	ok = true
	return
}

func cachedItemExists(cache ybc.Cacher, key []byte) bool {
	item, err := cache.GetItem(key)
	if err == ybc.ErrCacheMiss {
		return false
	}
	if err != nil {
		log.Fatalf("Unexpected error returned from Cacher.GetItem(): [%s]", err)
	}
	item.Close()
	return true
}

func processAddCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	key, flags, expiration, size, _, noreply, ok := parseSetCmd(line, false)
	if !ok {
		return false
	}

	txn := startSetTxn(cache, key, flags, expiration, size)
	if txn == nil {
		return false
	}
	shouldRollback := true
	defer closeSetTxn(txn, &shouldRollback)

	if !readValueToTxn(c.Reader, txn, size) {
		return false
	}

	casidLock.Lock()
	defer casidLock.Unlock()

	if cachedItemExists(cache, key) {
		if noreply {
			return true
		}
		return writeStr(c.Writer, strNotStoredCrLf)
	}

	if err := txn.Commit(); err != nil {
		log.Fatalf("Unexpected error in SetTxn.Commit(): [%s]", err)
	}
	shouldRollback = false

	return writeSetResponse(c.Writer, noreply)
}

func processCasCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	key, flags, expiration, size, casid, noreply, ok := parseSetCmd(line, true)
	if !ok {
		return false
	}

	txn := startSetTxn(cache, key, flags, expiration, size)
	if txn == nil {
		return false
	}
	shouldRollback := true
	defer closeSetTxn(txn, &shouldRollback)

	if !readValueToTxn(c.Reader, txn, size) {
		return false
	}

	casidLock.Lock()
	defer casidLock.Unlock()

	casidOrig, cacheMiss, ok := getCasidForCachedItem(cache, key)
	if !ok {
		return false
	}
	if cacheMiss {
		if noreply {
			return true
		}
		return writeStr(c.Writer, strNotFoundCrLf)
	}
	if casidOrig != casid {
		if noreply {
			return true
		}
		return writeStr(c.Writer, strExistsCrLf)
	}

	if err := txn.Commit(); err != nil {
		log.Fatalf("Unexpected error in SetTxn.Commit(): [%s]", err)
	}
	shouldRollback = false

	return writeSetResponse(c.Writer, noreply)
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

	response := strDeletedCrLf
	if !ok {
		response = strNotFoundCrLf
	}
	return writeStr(c.Writer, response)
}

func parseFlushAllCmd(line []byte) (expiration time.Duration, noreply bool, ok bool) {
	if len(line) == 0 {
		noreply = false
		ok = true
		return
	}

	ok = false
	noreply = false
	if line[0] != ' ' {
		log.Printf("Unexpected character at the beginning of line=[%s]. Expected whitespace", line)
		return
	}

	n := 0

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
	if bytes.HasPrefix(line, strCgetDe) {
		return processCgetDeCmd(c, cache, line[len(strCgetDe):], scratchBuf)
	}
	if bytes.HasPrefix(line, strSet) {
		return processSetCmd(c, cache, line[len(strSet):], scratchBuf)
	}
	if bytes.HasPrefix(line, strCas) {
		return processCasCmd(c, cache, line[len(strCas):], scratchBuf)
	}
	if bytes.HasPrefix(line, strAdd) {
		return processAddCmd(c, cache, line[len(strAdd):], scratchBuf)
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
//
// Usage:
//
//   cache := openCache()
//   defer cache.Close()
//
//   s := Server{
//       Cache: cache,
//       ListenAddr: ":11211",
//   }
//   if err := s.Serve(); err != nil {
//       handleError(err)
//   }
//
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
//
// Don't forget closing the Server.Cache, since the server doesn't close it
// automatically.
func (s *Server) Stop() {
	s.listenSocket.Close()
	s.Wait()
	s.listenSocket = nil
	s.done = nil
}
