package bitsindex

import "github.com/advanderveer/libchunk/bits"

//MemIndex uses an in-memory hash table to store key information
type MemIndex struct {
	Keys map[bits.K]struct{}
}

//NewMemIndex creates an empty index
func NewMemIndex() *MemIndex {
	return &MemIndex{Keys: map[bits.K]struct{}{}}
}

//Has returns whether a key is contained in this index
func (idx *MemIndex) Has(k bits.K) bool {
	if _, ok := idx.Keys[k]; ok {
		return true
	}

	return false
}

//Handle allows this index to be used as a KeyHandler
//in this case each key 'k' is added uniquely to the index
func (idx *MemIndex) Write(k bits.K) error {
	idx.Keys[k] = struct{}{}
	return nil
}
