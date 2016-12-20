package bitsiterator

import (
	"io"

	"github.com/advanderveer/libchunk"
)

//MemIterator stores key in memory for iteration. It also conveniently
//implements the KeyIndex and KeyHandler interface
type MemIterator struct {
	i    int
	Keys []libchunk.K
}

//NewPopulatedMemIterator creates a preopulated iterator
func NewPopulatedMemIterator(keys []libchunk.K) *MemIterator {
	return &MemIterator{
		Keys: keys,
	}
}

//NewMemIterator creates an empty iterator
func NewMemIterator() *MemIterator {
	return &MemIterator{}
}

//Has a certain key be added to the iterator, lookup is takes linear time
//and is inefficient for large sets, outside of testing scenarious please
//use a dedicated index from the bitsindex package
func (iter *MemIterator) Has(k libchunk.K) bool {
	for _, kk := range iter.Keys {
		if kk == k {
			return true
		}
	}

	return false
}

//Reset the iterator to index 0
func (iter *MemIterator) Reset() {
	iter.i = 0
}

//Handle appends a new key to the internal, does not check for uniqueness
func (iter *MemIterator) Handle(k libchunk.K) (err error) {
	iter.Keys = append(iter.Keys, k)
	return nil
}

//Next returns a new key or io.EOF if no more keys are available
func (iter *MemIterator) Next() (k libchunk.K, err error) {
	if iter.i > len(iter.Keys)-1 {
		return k, io.EOF
	}

	k = iter.Keys[iter.i]
	iter.i++
	return k, nil
}
