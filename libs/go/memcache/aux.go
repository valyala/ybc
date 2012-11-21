package memcache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/valyala/ybc/bindings/go/ybc"
	"io"
	"log"
	"strconv"
	"time"
)

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096

	// see /proc/sys/net/core/rmem_default
	defaultOSReadBufferSize = 224 * 1024

	// see /proc/sys/net/core/wmem_default
	defaultOSWriteBufferSize = 224 * 1024
)

const (
	maxExpirationSeconds = 30 * 24 * 3600
	maxExpiration        = time.Hour * 24 * 365
	maxMilliseconds      = 1 << 31
)

var (
	strCas                 = []byte("cas ")
	strCget                = []byte("cget ")
	strCgetDe              = []byte("cgetde ")
	strCrLf                = []byte("\r\n")
	strCset                = []byte("cset ")
	strDelete              = []byte("delete ")
	strDeleted             = []byte("DELETED")
	strDeletedCrLf         = []byte("DELETED\r\n")
	strEnd                 = []byte("END")
	strEndCrLf             = []byte("END\r\n")
	strExistsCrLf          = []byte("EXISTS\r\n")
	strFlushAll            = []byte("flush_all")
	strFlushAllCrLf        = []byte("flush_all\r\n")
	strFlushAllWs          = []byte("flush_all ")
	strFlushAllNoreplyCrLf = []byte("flush_all noreply\r\n")
	strGet                 = []byte("get ")
	strGetDe               = []byte("getde ")
	strGets                = []byte("gets ")
	strNoreply             = []byte("noreply")
	strNotFound            = []byte("NOT_FOUND")
	strNotFoundCrLf        = []byte("NOT_FOUND\r\n")
	strNotModified         = []byte("NM")
	strNotModifiedCrLf     = []byte("NM\r\n")
	strOkCrLf              = []byte("OK\r\n")
	strSet                 = []byte("set ")
	strStored              = []byte("STORED")
	strStoredCrLf          = []byte("STORED\r\n")
	strValue               = []byte("VALUE ")
	strWouldBlock          = []byte("WB")
	strWouldBlockCrLf      = []byte("WB\r\n")
	strWsNoreplyCrLf       = []byte(" noreply\r\n")
)

func validateKey(key []byte) bool {
	// Disallow empty keys.
	if len(key) == 0 {
		return false
	}

	// Do not check for key length - let servers with key length limit
	// validate it instead.
	for _, ch := range key {
		if ch == ' ' || ch == '\n' {
			return false
		}
	}
	return true
}

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

func matchCrLf(r *bufio.Reader) bool {
	return matchByte(r, '\r') && matchByte(r, '\n')
}

func readBytesUntil(r *bufio.Reader, endCh byte, lineBuf *[]byte) bool {
	line := *lineBuf
	line = line[0:0]
	for {
		s, err := r.ReadSlice(endCh)
		if err == nil {
			line = append(line, s...)
			break
		}
		if err == bufio.ErrBufferFull {
			line = append(line, s...)
			c, err := r.ReadByte()
			if err != nil {
				log.Printf("Error when reading next byte from buffer: [%s]", err)
			}
			line = append(line, c)
			continue
		}
		if err == io.EOF && len(line) == 0 {
			*lineBuf = line
			return true
		}
		log.Printf("Error when reading bytes until endCh=[%d]: [%s]", endCh, err)
		return false
	}
	*lineBuf = line[:len(line)-1]
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
	if line[lastN] == '\r' {
		line = line[:lastN]
	}
	*lineBuf = line
	return true
}

func nextToken(line []byte, n *int, entity string) []byte {
	first := *n
	first += 1
	if first >= len(line) {
		log.Printf("Cannot find entity=[%s] in line=[%s]: end of line", entity, line)
		return nil
	}
	last := bytes.IndexByte(line[first:], ' ')
	if last == -1 {
		last = len(line)
	} else {
		last += first
	}
	if first == last {
		log.Printf("Cannot find entity=[%s] in line=[%s]: unexpected whitespace", entity, line)
		return nil
	}
	*n = last
	return line[first:last]
}

func parseUint64(s []byte) (n uint64, ok bool) {
	for _, c := range s {
		if c < '0' || c > '9' {
			log.Printf("Cannot convert s=[%] to integer", s)
			ok = false
			return
		}
		n *= 10
		n += uint64(c - '0')
	}
	ok = true
	return
}

func parseInt(s []byte) (n int, ok bool) {
	negative := false
	var n64 uint64
	switch s[0] {
	case '-':
		negative = true
		n64, ok = parseUint64(s[1:])
	case '+':
		n64, ok = parseUint64(s[1:])
	default:
		n64, ok = parseUint64(s)
	}

	if !ok {
		return
	}
	if n64 >= (uint64(1) << 31) {
		log.Printf("Too big number for int=%d", n64)
		ok = false
		return
	}
	n = int(n64)
	if negative {
		n = -n
	}
	return
}

func parseUint32(s []byte) (n uint32, ok bool) {
	n64, ok := parseUint64(s)
	if !ok {
		return
	}
	if n64 >= (uint64(1) << 32) {
		log.Printf("Too big number for uint32=%d", n64)
		ok = false
		return
	}
	n = uint32(n64)
	return
}

func parseExpiration(s []byte) (expiration time.Duration, ok bool) {
	t, ok := parseInt(s)
	if !ok {
		return
	}
	if t == 0 {
		expiration = maxExpiration
	} else if t > maxExpirationSeconds {
		expiration = time.Unix(int64(t), 0).Sub(time.Now())
	} else {
		expiration = time.Second * time.Duration(t)
	}
	ok = true
	return
}

func parseFlagsToken(line []byte, n *int) (flags uint32, ok bool) {
	flagsStr := nextToken(line, n, "flags")
	if flagsStr == nil {
		ok = false
		return
	}
	flags, ok = parseUint32(flagsStr)
	return
}

func parseSizeToken(line []byte, n *int) (size int, ok bool) {
	sizeStr := nextToken(line, n, "size")
	if sizeStr == nil {
		ok = false
		return
	}
	size, ok = parseInt(sizeStr)
	return
}

func parseExpirationToken(line []byte, n *int) (expiration time.Duration, ok bool) {
	expirationStr := nextToken(line, n, "expiration")
	if expirationStr == nil {
		ok = false
		return
	}
	expiration, ok = parseExpiration(expirationStr)
	return
}

func parseUint64Token(line []byte, n *int, tokenName string) (etag uint64, ok bool) {
	etagStr := nextToken(line, n, tokenName)
	if etagStr == nil {
		ok = false
		return
	}
	etag, ok = parseUint64(etagStr)
	return
}

func parseUint32Token(line []byte, n *int, tokenName string) (n32 uint32, ok bool) {
	s := nextToken(line, n, tokenName)
	if s == nil {
		ok = false
		return
	}
	n32, ok = parseUint32(s)
	return
}

func parseMillisecondsToken(line []byte, n *int, tokenName string) (duration time.Duration, ok bool) {
	s := nextToken(line, n, tokenName)
	if s == nil {
		ok = false
		return
	}
	t, ok := parseUint32(s)
	if !ok {
		return
	}
	duration = time.Millisecond * time.Duration(t)
	return
}

func writeByte(w *bufio.Writer, c byte) bool {
	if err := w.WriteByte(c); err != nil {
		log.Printf("Cannot write byte [%d] to output stream: [%s]", c, err)
		return false
	}
	return true
}

func writeWs(w *bufio.Writer) bool {
	return writeByte(w, ' ')
}

func writeStr(w *bufio.Writer, s []byte) bool {
	if _, err := w.Write(s); err != nil {
		log.Printf("Cannot write [%s] to output stream: [%s]", s, err)
		return false
	}
	return true
}

func writeUint64(w *bufio.Writer, n uint64, scratchBuf *[]byte) bool {
	buf := *scratchBuf
	buf = buf[0:0]
	buf = strconv.AppendUint(buf, n, 10)
	*scratchBuf = buf
	return writeStr(w, buf)
}

func writeUint32(w *bufio.Writer, n uint32, scratchBuf *[]byte) bool {
	return writeUint64(w, uint64(n), scratchBuf)
}

func writeInt(w *bufio.Writer, n int, scratchBuf *[]byte) bool {
	buf := *scratchBuf
	buf = buf[0:0]
	buf = strconv.AppendInt(buf, int64(n), 10)
	*scratchBuf = buf
	return writeStr(w, buf)
}

func writeCrLf(w *bufio.Writer) bool {
	return writeStr(w, strCrLf)
}

func writeExpiration(w *bufio.Writer, expiration time.Duration, scratchBuf *[]byte) bool {
	var t time.Duration
	if expiration != 0 {
		t = expiration / time.Second
		if t <= 0 {
			// Since parseExpiration() considers 0 as 'no expiration',
			// set the expiration to negative value in order to mean
			// 'expiration is over'.
			t = -1
		} else if t > time.Duration(maxExpirationSeconds) {
			// 0 means 'no expiration'
			t = 0
		}
	}
	return writeInt(w, int(t), scratchBuf)
}

func writeMilliseconds(w *bufio.Writer, duration time.Duration, scratchBuf *[]byte) bool {
	t := duration / time.Millisecond
	if t < 0 {
		t = 0
	} else if t >= time.Duration(maxMilliseconds) {
		t = maxMilliseconds
	}
	return writeUint32(w, uint32(t), scratchBuf)
}

func binaryRead(r io.Reader, data interface{}, name string) bool {
	if err := binary.Read(r, binary.LittleEndian, data); err != nil {
		log.Printf("Error in binary.Read() for [%s]: [%s]", name, err)
		return false
	}
	return true
}

func binaryWrite(w io.Writer, data interface{}, name string) {
	if err := binary.Write(w, binary.LittleEndian, data); err != nil {
		log.Fatalf("Error in binary.Write() for [%s]: [%s]", name, err)
	}
}

func cacheClearFunc(cache ybc.Cacher) func() {
	return func() { cache.Clear() }
}
