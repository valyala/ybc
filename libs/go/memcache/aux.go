package memcache

import (
	"bufio"
	"io"
	"log"
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
