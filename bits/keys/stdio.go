package bitskeys

import (
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
