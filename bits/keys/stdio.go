package bitskeys

import (
	"fmt"
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//StdioIterator scans newline separated encoded keys from a reader
//amd writes encoded (Stringer) keys to a writer as a key handler
type StdioIterator struct {
	w io.Writer
	r io.Reader
}

//NewStdioIterator creates a new iterator that writes to 'w' and read from 'r'
func NewStdioIterator(r io.Reader, w io.Writer) *StdioIterator {
	return &StdioIterator{w, r}
}

//Reset is a no-op for this iterator
func (iter *StdioIterator) Reset() {}

//Next scans for the next key on the reader
func (iter *StdioIterator) Next() (k bits.K, err error) {
	return k, fmt.Errorf("not yet implemented")
}

//Handle will simply encode and write the key newline separated
func (iter *StdioIterator) Handle(k bits.K) (err error) {
	_, err = fmt.Fprintf(iter.w, "%s\n", k)
	return err
}
