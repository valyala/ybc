package memcache

import (
	"../../../bindings/go/ybc"
	"bufio"
	"bytes"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

func parseExptime(s []byte) (exptime time.Duration, ok bool) {
	t, err := strconv.Atoi(string(s))
	if err != nil {
		log.Printf("Cannot convert exptime=[%s] to integer: [%s]", s, err)
		ok = false
		return
	}
	if t == 0 {
		exptime = ybc.MaxTtl
	} else if t > 30*24*3600 {
		exptime = time.Unix(int64(t), 0).Sub(time.Now())
	} else {
		exptime = time.Second * time.Duration(t)
	}
	ok = true
	return
}

func writeGetResponse(w *bufio.Writer, key []byte, item *ybc.Item, scratchBuf *[]byte) bool {
	if _, err := w.Write(strValue); err != nil {
		log.Printf("Error when writing VALUE response: [%s]", err)
		return false
	}
	if _, err := w.Write(key); err != nil {
		log.Printf("Error when writing key=[%s] to 'get' response: [%s]", key, err)
		return false
	}
	if _, err := w.Write(strZero); err != nil {
		log.Printf("Error when writing ' 0 ' to 'get' response: [%s]", err)
		return false
	}
	size := item.Size()
	if !writeInt(w, size, scratchBuf) {
		return false
	}
	if _, err := w.Write(strZeroCrLf); err != nil {
		log.Printf("Error when writing 0\\r\\n to 'get' response: [%s]", err)
		return false
	}
	n, err := item.WriteTo(w)
	if err != nil {
		log.Printf("Error when writing payload: [%s]", err)
		return false
	}
	if n != int64(size) {
		log.Printf("Invalid length of payload=[%d]. Expected [%d]", n, size)
		return false
	}
	if _, err := w.Write(strCrLf); err != nil {
		log.Printf("Error when writing \\r\\n to response: [%s]", err)
		return false
	}
	return true
}

func getItemAndWriteResponse(w *bufio.Writer, cache ybc.Cacher, key []byte, scratchBuf *[]byte) bool {
	item, err := cache.GetItem(key)
	if err != nil {
		if err == ybc.ErrNotFound {
			return true
		}
		log.Fatalf("Unexpected error returned by cache.GetItem(): [%s]", err)
	}
	defer item.Close()

	return writeGetResponse(w, key, item, scratchBuf)
}

func writeEndCrLf(w *bufio.Writer) bool {
	if _, err := w.Write(strEndCrLf); err != nil {
		log.Printf("Error when writing \\r\\n: [%s]", err)
		return false
	}
	return true
}

func processGetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
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
		if !getItemAndWriteResponse(c.Writer, cache, key, scratchBuf) {
			return false
		}
	}

	return writeEndCrLf(c.Writer)
}

func processGetDeCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, scratchBuf *[]byte) bool {
	n := -1

	key, n := nextToken(line, n, "key")
	if key == nil {
		return false
	}
	graceStr, n := nextToken(line, n, "grace")
	if graceStr == nil {
		return false
	}
	if n != len(line) {
		return false
	}

	graceInt, err := strconv.Atoi(string(graceStr))
	if err != nil {
		log.Printf("Cannot parse grace=[%s] in 'getde' query for key=[%s]", graceStr, key)
		return false
	}
	if graceInt <= 0 {
		log.Printf("grace=[%d] cannot be negative or zero", graceInt)
		return false
	}
	grace := time.Millisecond * time.Duration(graceInt)

	item, err := cache.GetDeAsyncItem(key, grace)
	if err != nil {
		if err == ybc.ErrWouldBlock {
			if _, err = c.Write(strWouldBlockCrLf); err != nil {
				log.Printf("Cannot write 'WOULDBLOCK' to 'getde' response: [%s]", err)
				return false
			}
			return true
		}
		if err == ybc.ErrNotFound {
			return writeEndCrLf(c.Writer)
		}
		log.Fatalf("Unexpected error returned by cache.GetDeAsyncItem(): [%s]", err)
	}
	defer item.Close()

	if !writeGetResponse(c.Writer, key, item, scratchBuf) {
		return false
	}

	return writeEndCrLf(c.Writer)
}

type setCmd struct {
	key     []byte
	exptime []byte
	size    []byte
	noreply []byte
}

func parseSetCmd(line []byte, cmd *setCmd) bool {
	n := -1

	cmd.key, n = nextToken(line, n, "key")
	if cmd.key == nil {
		return false
	}
	flagsUnused, n := nextToken(line, n, "flags")
	if flagsUnused == nil {
		return false
	}
	cmd.exptime, n = nextToken(line, n, "exptime")
	if cmd.exptime == nil {
		return false
	}
	cmd.size, n = nextToken(line, n, "size")
	if cmd.size == nil {
		return false
	}

	if n == len(line) {
		return true
	}

	cmd.noreply, n = nextToken(line, n, "noreply")
	if cmd.noreply == nil {
		return false
	}
	return n == len(line)
}

func processSetCmd(c *bufio.ReadWriter, cache ybc.Cacher, line []byte, cmd *setCmd) bool {
	cmd.noreply = nil
	if !parseSetCmd(line, cmd) {
		return false
	}

	key := cmd.key
	exptime, ok := parseExptime(cmd.exptime)
	if !ok {
		return false
	}
	size, ok := parseInt(cmd.size)
	if !ok {
		return false
	}
	noreply := false
	if cmd.noreply != nil {
		if !bytes.Equal(cmd.noreply, strNoreply) {
			return false
		}
		noreply = true
	}
	txn, err := cache.NewSetTxn(key, size, exptime)
	if err != nil {
		log.Printf("Cannot start 'set' transaction for key=[%s], size=[%d], exptime=[%d]: [%s]", key, size, exptime, err)
		return false
	}
	defer txn.Commit()
	n, err := txn.ReadFrom(c.Reader)
	if err != nil {
		log.Printf("Error when reading payload for key=[%s], size=[%d]: [%s]", key, size, err)
		return false
	}
	if n != int64(size) {
		log.Printf("Unexpected payload size=[%d]. Expected [%d]", n, size)
		return false
	}
	if !matchBytes(c.Reader, strCrLf) {
		return false
	}
	if !noreply {
		if _, err := c.Write(strStoredCrLf); err != nil {
			log.Printf("Error when writing response: [%s]", err)
			return false
		}
	}
	return true
}

func processRequest(c *bufio.ReadWriter, cache ybc.Cacher, scratchBuf *[]byte, cmd *setCmd) bool {
	if !readLine(c.Reader, scratchBuf) {
		return false
	}
	line := *scratchBuf
	if len(line) == 0 {
		return false
	}
	if bytes.HasPrefix(line, strGet) {
		return processGetCmd(c, cache, line[len(strGet):], scratchBuf)
	}
	if bytes.HasPrefix(line, strGets) {
		return processGetCmd(c, cache, line[len(strGets):], scratchBuf)
	}
	if bytes.HasPrefix(line, strGetDe) {
		return processGetDeCmd(c, cache, line[len(strGetDe):], scratchBuf)
	}
	if bytes.HasPrefix(line, strSet) {
		return processSetCmd(c, cache, line[len(strSet):], cmd)
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

	scratchBuf := make([]byte, 0, 1024)
	cmd := setCmd{}
	for {
		if !processRequest(c, cache, &scratchBuf, &cmd) {
			break
		}
		if r.Buffered() == 0 {
			w.Flush()
		}
	}
}

type Server struct {
	Cache             ybc.Cacher
	ListenAddr        string
	ReadBufferSize    int
	WriteBufferSize   int
	OSReadBufferSize  int
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

func (s *Server) Start() {
	if s.listenSocket != nil || s.done != nil {
		panic("Did you forgot calling Server.Stop() before calling Server.Start()?")
	}
	s.init()
	go s.run()
}

func (s *Server) Wait() error {
	s.done.Wait()
	return s.err
}

func (s *Server) Serve() error {
	s.Start()
	return s.Wait()
}

func (s *Server) Stop() {
	s.listenSocket.Close()
	s.Wait()
	s.listenSocket = nil
	s.done = nil
}
