package memcache

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"strconv"
)

const (
	defaultReadBufferSize    = 4096
	defaultWriteBufferSize   = 4096
	defaultOSReadBufferSize  = 4096
	defaultOSWriteBufferSize = 4096
)

var (
	strCrLf        = []byte("\r\n")
	strCrLfEndCrLf = []byte("\r\nEND\r\n")
	strEnd         = []byte("END")
	strEndCrLf     = []byte("END\r\n")
	strGet         = []byte("get ")
	strGets        = []byte("gets ")
	strNoreply     = []byte("noreply")
	strSet         = []byte("set ")
	strStored      = []byte("STORED")
	strStoredCrLf  = []byte("STORED\r\n")
	strValue       = []byte("VALUE ")
	strWsNoreply   = []byte(" noreply")
	strZero        = []byte(" 0 ")
	strZeroCrLf    = []byte(" 0\r\n")
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
