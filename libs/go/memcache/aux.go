package memcache

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strconv"
)

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096

	// see /proc/sys/net/core/rmem_default
	defaultOSReadBufferSize = 224 * 1024

	// see /proc/sys/net/core/wmem_default
	defaultOSWriteBufferSize = 224 * 1024
)

var (
	strCrLf           = []byte("\r\n")
	strEnd            = []byte("END")
	strEndCrLf        = []byte("END\r\n")
	strGet            = []byte("get ")
	strGetDe          = []byte("getde ")
	strGets           = []byte("gets ")
	strNoreply        = []byte("noreply")
	strSet            = []byte("set ")
	strStored         = []byte("STORED")
	strStoredCrLf     = []byte("STORED\r\n")
	strValue          = []byte("VALUE ")
	strWouldBlock     = []byte("WB")
	strWouldBlockCrLf = []byte("WB\r\n")
	strWs             = []byte(" ")
	strWsNoreply      = []byte(" noreply")
	strZero           = []byte(" 0 ")
	strZeroCrLf       = []byte(" 0\r\n")
)

func matchByte(r *bufio.Reader, ch byte) bool {
	c, err := r.ReadByte()
	if err != nil {
		log.Printf("Unexpected error when reading [%d]: [%s]", int(ch), err)
		return false
	}
	if c != ch {
		log.Printf("Unexpected byte read=[%d]. Expected [%d]", int(c), int(ch))
		return false
	}
	return true
}

func matchBytes(r *bufio.Reader, s []byte) bool {
	for _, c := range s {
		if !matchByte(r, c) {
			return false
		}
	}
	return true
}

func readBytesUntil(r *bufio.Reader, endCh byte, lineBuf *[]byte) bool {
	line := *lineBuf
	line = line[0:0]
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF && len(line) == 0 {
				break
			}
			log.Printf("Error when reading bytes until endCh=[%s]: [%s]", endCh, err)
			return false
		}
		if c == endCh {
			break
		}
		line = append(line, c)
	}
	*lineBuf = line
	return true
}

func readLine(r *bufio.Reader, lineBuf *[]byte) bool {
	if !readBytesUntil(r, '\n', lineBuf) {
		return false
	}
	line := *lineBuf
	if len(line) == 0 {
		return true
	}
	lastN := len(line) - 1
	if line[lastN] != '\r' {
		log.Printf("Unexpected byte read=[%d]. Expected \\r", line[lastN])
		return false
	}
	line = line[:lastN]
	*lineBuf = line
	return true
}

func nextToken(line []byte, first int, entity string) (s []byte, last int) {
	first += 1
	if first >= len(line) {
		log.Printf("No enough space for [%s] in 'set' command=[%s]", entity, line)
		return
	}
	last = bytes.IndexByte(line[first:], ' ')
	if last == -1 {
		last = len(line)
	} else {
		last += first
	}
	if first == last {
		log.Printf("Cannot find [%s] in 'set' command=[%s]", entity, line)
		return
	}
	s = line[first:last]
	return
}

func parseInt(s []byte) (size int, ok bool) {
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

func writeInt(w *bufio.Writer, n int, scratchBuf *[]byte) bool {
	buf := *scratchBuf
	buf = buf[0:0]
	buf = strconv.AppendInt(buf, int64(n), 10)
	*scratchBuf = buf
	if _, err := w.Write(buf); err != nil {
		log.Printf("Cannot write integer=[%s] to output stream: [%s]", n, err)
		return false
	}
	return true
}
