package bitsiterator

import (
	"io"

	"github.com/advanderveer/libchunk/bits"
)

//MemIterator stores key in memory for iteration. It also conveniently
//implements the KeyIndex and KeyHandler interface
type MemIterator struct {
	i    int
	Keys []bits.K
}

//NewPopulatedMemIterator creates a preopulated iterator
func NewPopulatedMemIterator(keys []bits.K) *MemIterator {
	return &MemIterator{
		Keys: keys,
	}
}

//NewMemIterator creates an empty iterator
func NewMemIterator() *MemIterator {
	return &MemIterator{}
}

//Reset the iterator to index 0
func (iter *MemIterator) Reset() {
	iter.i = 0
}

//Handle appends a new key to the internal slice
func (iter *MemIterator) Handle(k bits.K) (err error) {
	iter.Keys = append(iter.Keys, k)
	return nil
}

//Next returns a new key or io.EOF if no more keys are available
func (iter *MemIterator) Next() (k bits.K, err error) {
	if iter.i > len(iter.Keys)-1 {
		return k, io.EOF
	}

	k = iter.Keys[iter.i]
	iter.i++
	return k, nil
}
