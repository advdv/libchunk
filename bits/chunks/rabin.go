package bitschunks

import (
	"io"

	"github.com/restic/chunker"
)

//RabinChunker is a chunker that uses a rolling rabin checksum
type RabinChunker struct {
	buf     []byte
	chunker *chunker.Chunker
}

//NewRabinChunker creates a chunker that uses a rolling rabin checksum
func NewRabinChunker(r io.Reader, pol chunker.Pol) *RabinChunker {
	return &RabinChunker{
		buf:     make([]byte, chunker.MaxSize),
		chunker: chunker.New(r, pol),
	}
}

//Next will return the next chunk for processing
func (c *RabinChunker) Next() (chunk []byte, err error) {
	rchunk, err := c.chunker.Next(c.buf)
	if err != nil {
		return chunk, err
	}

	chunk = make([]byte, rchunk.Length)
	copy(chunk, rchunk.Data)
	return chunk, nil
}
