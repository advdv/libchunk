package bitsstore

import (
	"fmt"
	"os"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
)

var (
	//BoltChunkBucket is the name of the bucket that holds all chunks
	BoltChunkBucket = []byte("chunks")
)

//BoltStore stores chunks into a mmap file using a B+tree
type BoltStore struct {
	DB *bolt.DB
}

//NewBoltStore creates a new store by memory-mapping the file at path 'p', the
//database file is created if non exists at the destination
func NewBoltStore(p string) (s *BoltStore, err error) {
	s = &BoltStore{}
	s.DB, err = bolt.Open(p, 0600, &bolt.Options{Timeout: time.Second * 5})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %v", err)
	}

	err = s.DB.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists(BoltChunkBucket)
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create buckets: %v", err)
	}

	return s, nil
}

//Put a new chunk 'chunk' with key 'k' into the store
func (s *BoltStore) Put(k libchunk.K, chunk []byte) (err error) {
	return s.DB.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(BoltChunkBucket)
		if b == nil {
			return fmt.Errorf("chunk bucket '%s' must first be created", string(BoltChunkBucket))
		}

		existing := b.Get(k[:])
		if existing != nil {
			return nil
		}

		return b.Put(k[:], chunk)
	})
}

//Get an existhing with 'k' from the store, returns an os.ErrNotExist if
//no chunk with the given key exists in this store.
func (s *BoltStore) Get(k libchunk.K) (chunk []byte, err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(BoltChunkBucket)
		if b == nil {
			return fmt.Errorf("chunk bucket '%s' must first be created", string(BoltChunkBucket))
		}

		v := b.Get(k[:])
		if v == nil || len(v) < 1 {
			return os.ErrNotExist
		}

		chunk = make([]byte, len(v))
		copy(chunk, v)
		return nil
	})

	return chunk, err
}
