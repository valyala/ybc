package memcache

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/valyala/ybc/bindings/go/ybc"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

func readByte(r *bufio.Reader, ch byte) bool {
	c, err := r.ReadByte()
	if err != nil {
		log.Printf("Unexpected error when reading [%d]: [%s]", ch, err)
		return false
	}
	if c != ch {
		log.Printf("Unexpected byte read=[%d]. Expected [%d]", c, ch)
		return false
	}
	return true
}

func readBytesUntil(r *bufio.Reader, endCh byte, cmdLineBuf *[]byte) bool {
	cmdLine := *cmdLineBuf
	cmdLine = cmdLine[0:0]
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && len(cmdLine) == 0 {
				break
			}
			log.Printf("Error when reading bytes until endCh=[%s]: [%s]", endCh, err)
			return false
		}
		if c == endCh {
			break
		}
		cmdLine = append(cmdLine, c)
	}
	*cmdLineBuf = cmdLine
	return true
}

func readCmdLine(r *bufio.Reader, cmdLineBuf *[]byte) bool {
	if !readBytesUntil(r, '\n', cmdLineBuf) {
		return false
	}
	cmdLine := *cmdLineBuf
	if len(cmdLine) == 0 {
		return true
	}
	lastN := len(cmdLine) - 1
	if cmdLine[lastN] != '\r' {
		log.Printf("Unexpected byte read=[%d]. Expected \\r", cmdLine[lastN])
		return false
	}
	cmdLine = cmdLine[:lastN]
	*cmdLineBuf = cmdLine
	return true
}

func readCrLf(r *bufio.Reader) bool {
	if !readByte(r, '\r') {
		return false
	}
	return readByte(r, '\n')
}

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

func parseSize(s []byte) (size int, ok bool) {
	var err error
	size, err = strconv.Atoi(string(s))
	if err != nil {
		log.Printf("Cannot convert size=[%s] to integer: [%s]", s, err)
		ok = false
		return
	}
	ok = true
	return
}

func clientError(w *bufio.Writer, s string) {
	fmt.Fprintf(w, "CLIENT_ERROR %s\r\n", s)
}

func serverError(w *bufio.Writer, s string) {
	fmt.Fprintf(w, "SERVER_ERROR %s\r\n", s)
}

func protocolError(w *bufio.Writer) {
	w.WriteString("ERROR\r\n")
}

func getItem(c *bufio.ReadWriter, cache ybc.Cacher, key []byte) bool {
	item, err := cache.GetItem(key)
	if err != nil {
		log.Printf("There is no item with key=[%s]", key)
		return true
	}
	defer item.Close()

	flags := "0"
	size := item.Size()
	_, err = fmt.Fprintf(c, "VALUE %s %s %d 0\r\n", key, flags, size)
	if err != nil {
		log.Printf("Error when writing response: [%s]", err)
		return false
	}
	n, err := item.WriteTo(c)
	if err != nil {
		log.Printf("Error when writing payload: [%s]", err)
		return false
	}
	if n != int64(size) {
		log.Printf("Invalid length of payload=[%d]. Expected [%d]", n, size)
		return false
	}
	_, err = c.WriteString("\r\n")
	if err != nil {
		log.Printf("Error when writing \\r\\n to response: [%s]", err)
		return false
	}
	return true
}

func processGetCmd(c *bufio.ReadWriter, cache ybc.Cacher, cmdLine []byte) bool {
	last := -1
	cmdLineSize := len(cmdLine)
	for last < cmdLineSize {
		first := last + 1
		last = bytes.IndexByte(cmdLine[first:], ' ')
		if last == -1 {
			last = cmdLineSize
		} else {
			last += first
		}
		if first == last {
			continue
		}
		key := cmdLine[first:last]
		if !getItem(c, cache, key) {
			return false
		}
	}

	_, err := c.WriteString("END\r\n")
	if err != nil {
		log.Printf("Error when writing END to response: [%s]", err)
		return false
	}
	return true
}

type setCmd struct {
	key     []byte
	flags   []byte
	exptime []byte
	size    []byte
	noreply []byte
}

func nextToken(cmdLine []byte, first int, entity string) (s []byte, last int) {
	first += 1
	if first >= len(cmdLine) {
		log.Printf("No enough space for [%s] in 'set' command=[%s]", entity, cmdLine)
		return
	}
	last = bytes.IndexByte(cmdLine[first:], ' ')
	if last == -1 {
		last = len(cmdLine)
	} else {
		last += first
	}
	if first == last {
		log.Printf("Cannot find [%s] in 'set' command=[%s]", entity, cmdLine)
		return
	}
	s = cmdLine[first:last]
	return
}

func parseSetCmd(cmdLine []byte, cmd *setCmd) bool {
	var s []byte
	n := -1

	s, n = nextToken(cmdLine, n, "key")
	if s == nil {
		return false
	}
	cmd.key = s

	s, n = nextToken(cmdLine, n, "flags")
	if s == nil {
		return false
	}
	cmd.flags = s

	s, n = nextToken(cmdLine, n, "exptime")
	if s == nil {
		return false
	}
	cmd.exptime = s

	s, n = nextToken(cmdLine, n, "size")
	if s == nil {
		return false
	}
	cmd.size = s

	if n == len(cmdLine) {
		return true
	}

	s, n = nextToken(cmdLine, n, "noreply")
	if s == nil {
		return false
	}
	cmd.noreply = s
	return true
}

func processSetCmd(c *bufio.ReadWriter, cache ybc.Cacher, cmdLine []byte, cmd *setCmd) bool {
	cmd.noreply = nil
	if !parseSetCmd(cmdLine, cmd) {
		clientError(c.Writer, "unrecognized 'set' command")
		return false
	}

	key := cmd.key
	exptime, ok := parseExptime(cmd.exptime)
	if !ok {
		clientError(c.Writer, "invalid exptime")
		return false
	}
	size, ok := parseSize(cmd.size)
	if !ok {
		clientError(c.Writer, "invalid size")
		return false
	}
	noreply := false
	if cmd.noreply != nil {
		if !bytes.Equal(cmd.noreply, []byte("noreply")) {
			clientError(c.Writer, "unrecognized noreply")
			return false
		}
		noreply = true
	}
	txn, err := cache.NewSetTxn(key, size, exptime)
	if err != nil {
		log.Printf("Cannot start 'set' transaction for key=[%s], size=[%d], exptime=[%d]: [%s]", key, size, exptime, err)
		serverError(c.Writer, "cannot start 'set' transaction")
		return false
	}
	defer txn.Commit()
	n, err := txn.ReadFrom(c)
	if err != nil {
		log.Printf("Error when reading payload for key=[%s], size=[%d]: [%s]", key, size, err)
		clientError(c.Writer, "cannot read payload")
		return false
	}
	if n != int64(size) {
		log.Printf("Unexpected payload size=[%d]. Expected [%d]", n, size)
		clientError(c.Writer, "unexpected payload size")
		return false
	}
	if !readCrLf(c.Reader) {
		clientError(c.Writer, "cannot read crlf after payload")
		return false
	}
	if !noreply {
		_, err = c.Write([]byte("STORED\r\n"))
		if err != nil {
			log.Printf("Error when writing response: [%s]", err)
			return false
		}
	}
	return true
}

func processRequest(c *bufio.ReadWriter, cache ybc.Cacher, cmdLineBuf *[]byte, cmd *setCmd) bool {
	if !readCmdLine(c.Reader, cmdLineBuf) {
		protocolError(c.Writer)
		return false
	}
	cmdLine := *cmdLineBuf
	if len(cmdLine) == 0 {
		return false
	}
	if bytes.HasPrefix(cmdLine, []byte("get ")) {
		return processGetCmd(c, cache, cmdLine[4:])
	}
	if bytes.HasPrefix(cmdLine, []byte("gets ")) {
		return processGetCmd(c, cache, cmdLine[5:])
	}
	if bytes.HasPrefix(cmdLine, []byte("set ")) {
		return processSetCmd(c, cache, cmdLine[4:], cmd)
	}
	log.Printf("Unrecognized command=[%s]", cmdLine)
	protocolError(c.Writer)
	return false
}

func handleConn(conn net.Conn, cache ybc.Cacher, readBufferSize, writeBufferSize int, done *sync.WaitGroup) {
	defer conn.Close()
	defer done.Done()
	r := bufio.NewReaderSize(conn, readBufferSize)
	w := bufio.NewWriterSize(conn, writeBufferSize)
	c := bufio.NewReadWriter(r, w)
	defer w.Flush()

	cmdLineBuf := make([]byte, 0, 1024)
	cmd := setCmd{}
	for {
		if !processRequest(c, cache, &cmdLineBuf, &cmd) {
			break
		}
		if r.Buffered() == 0 {
			w.Flush()
		}
	}
}

type Server struct {
	Cache           ybc.Cacher
	ListenAddr      string
	ReadBufferSize  int
	WriteBufferSize int

	listenSocket net.Listener
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

	var err error
	s.listenSocket, err = net.Listen("tcp", s.ListenAddr)
	if err != nil {
		log.Fatal("Cannot listen for ListenAddr=[%s]: [%s]", s.ListenAddr, err)
	}
	s.done = &sync.WaitGroup{}
	s.done.Add(1)
}

func (s *Server) run() {
	defer s.done.Done()

	connsDone := &sync.WaitGroup{}
	for {
		conn, err := s.listenSocket.Accept()
		if err != nil {
			s.err = err
			break
		}
		connsDone.Add(1)
		go handleConn(conn, s.Cache, s.ReadBufferSize, s.WriteBufferSize, connsDone)
	}
	connsDone.Wait()
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
