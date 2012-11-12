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
	strCrLf        = []byte("\r\n")
	strCGet        = []byte("cget ")
	strCSet        = []byte("cset ")
	strDelete      = []byte("delete ")
	strDeleted     = []byte("DELETED")
	strEnd         = []byte("END")
	strFlushAll    = []byte("flush_all ")
	strGet         = []byte("get ")
	strGetDe       = []byte("getde ")
	strGets        = []byte("gets ")
	strNoreply     = []byte("noreply")
	strNotFound    = []byte("NOT_FOUND")
	strNotModified = []byte("NM")
	strOkCrLf      = []byte("OK\r\n")
	strSet         = []byte("set ")
	strStored      = []byte("STORED")
	strValue       = []byte("VALUE ")
	strWouldBlock  = []byte("WB")
	strWs          = []byte(" ")
	strZero        = []byte("0")
)

func expectEof(line []byte, n int) bool {
	if len(line) != n {
		log.Printf("Unexpected length of line=[%s]: %d. Expected %d", line, len(line), n)
		return false
	}
	return true
}

func matchByte(r *bufio.Reader, ch byte) bool {
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

func matchStr(r *bufio.Reader, s []byte) bool {
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
			log.Printf("Error when reading bytes until endCh=[%d]: [%s]", endCh, err)
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
		log.Printf("Cannot find entity=[%s] in line=[%s]: end of line", entity, line)
		return
	}
	last = bytes.IndexByte(line[first:], ' ')
	if last == -1 {
		last = len(line)
	} else {
		last += first
	}
	if first == last {
		log.Printf("Cannot find entity=[%s] in line=[%s]: unexpected whitespace", entity, line)
		return
	}
	s = line[first:last]
	return
}

func parseInt64(s []byte) (n int64, ok bool) {
	n, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		log.Printf("Cannot convert n=[%s] to integer: [%s]", s, err)
		ok = false
		return
	}
	ok = true
	return
}

func parseInt(s []byte) (n int, ok bool) {
	nn, ok := parseInt64(s)
	n = int(nn)
	return
}

func writeStr(w *bufio.Writer, s []byte) bool {
	if _, err := w.Write(s); err != nil {
		log.Printf("Cannot write [%s] to output stream: [%s]", s, err)
		return false
	}
	return true
}

func writeInt64(w *bufio.Writer, n int64, scratchBuf *[]byte) bool {
	buf := *scratchBuf
	buf = buf[0:0]
	buf = strconv.AppendInt(buf, n, 10)
	*scratchBuf = buf
	return writeStr(w, buf)
}

func writeInt(w *bufio.Writer, n int, scratchBuf *[]byte) bool {
	return writeInt64(w, int64(n), scratchBuf)
}

func writeCrLf(w *bufio.Writer) bool {
	return writeStr(w, strCrLf)
}

func writeNoreply(w *bufio.Writer) bool {
	return writeStr(w, strWs) && writeStr(w, strNoreply)
}
