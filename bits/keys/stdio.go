package bitskeys

import (
	"bufio"
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//TextLineKeyWriter writes keys in encoded, each on a new line
type TextLineKeyWriter struct {
	w io.Writer
}

//Write implementes key writer
func (kw *TextLineKeyWriter) Write(k bits.K) (err error) {
	_, err = fmt.Fprintf(kw.w, "%s\n", k)
	return err
}

//TextLineKeyReader writes keys in encoded, each on a new line
type TextLineKeyReader struct {
	sc *bufio.Scanner
}

//Reset is not possible for this reader
func (kr *TextLineKeyReader) Reset() {}

//Read implementes key reader
func (kr *TextLineKeyReader) Read() (k bits.K, err error) {
	if !kr.sc.Scan() {
		return k, io.EOF
	}

	if kr.sc.Err() != nil {
		return k, kr.sc.Err()
	}

	return bits.DecodeKey(kr.sc.Bytes())
}
