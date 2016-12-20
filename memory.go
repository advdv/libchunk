package libchunk

import (
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

//MemStore stores Chunks in a map that only exists for the
//duration of the process, it conveniently also implements the
//http.Handler interface to allow it to be used as a testing remote
//for the S3 remote.
type MemStore struct {
	*sync.Mutex
	Chunks map[K][]byte
}

//NewMemStore sets up an empty memory store
func NewMemStore() *MemStore {
	return &MemStore{
		Mutex:  &sync.Mutex{},
		Chunks: map[K][]byte{},
	}
}

//Put a chunk into the Chunks map under the given 'k'
func (s *MemStore) Put(k K, chunk []byte) (err error) {
	s.Lock()
	defer s.Unlock()
	s.Chunks[k] = chunk
	return nil
}

//Get a chunk from the in-memory map
func (s *MemStore) Get(k K) (chunk []byte, err error) {
	s.Lock()
	defer s.Unlock()
	var ok bool
	chunk, ok = s.Chunks[k]
	if !ok {
		return chunk, os.ErrNotExist
	}

	return chunk, nil
}

func (s *MemStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Lock()
	defer s.Unlock()

	if r.Method == "PUT" {
		io.Copy(ioutil.Discard, r.Body)
	} else {
		k, err := DecodeKey(bytes.TrimLeft([]byte(r.URL.String()), "/"))
		if err != nil {
			log.Println("failed to decode", err, r.URL.String())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		chunk, ok := s.Chunks[k]
		if !ok {
			w.WriteHeader(404)
			return
		}

		_, err = io.Copy(w, bytes.NewReader(chunk))
		if err != nil {
			log.Println("failed to copy chunk", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
