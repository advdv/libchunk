package bitsstore

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/advanderveer/libchunk"
)

//MemStore stores Chunks in a map that only exists for the
//duration of the process, it conveniently also implements the
//http.Handler interface to allow it to be used as a testing remote
//for the S3 remote.
type MemStore struct {
	*sync.Mutex
	Chunks map[libchunk.K][]byte
}

//NewMemStore sets up an empty memory store
func NewMemStore() *MemStore {
	return &MemStore{
		Mutex:  &sync.Mutex{},
		Chunks: map[libchunk.K][]byte{},
	}
}

//Put a chunk into the Chunks map under the given 'k'
func (s *MemStore) Put(k libchunk.K, chunk []byte) (err error) {
	s.Lock()
	defer s.Unlock()
	s.Chunks[k] = chunk
	return nil
}

//Get a chunk from the in-memory map
func (s *MemStore) Get(k libchunk.K) (chunk []byte, err error) {
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
	if r.Method == "PUT" {
		k, err := libchunk.DecodeKey(bytes.TrimLeft([]byte(r.URL.String()), "/"))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		chunk, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = s.Put(k, chunk)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

	} else if r.URL.Query().Get("list-type") != "" {

		fmt.Fprintf(w, `
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)

		s.Lock()
		defer s.Unlock()
		for k, chunk := range s.Chunks {
			fmt.Fprintf(w, `
	<Contents>
		<Key>%s</Key>
		<Size>%d</Size>
	</Contents>
			`, k, len(chunk))
		}

		fmt.Fprintf(w, `
</ListBucketResult>`)
	} else {
		k, err := libchunk.DecodeKey(bytes.TrimLeft([]byte(r.URL.String()), "/"))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		chunk, err := s.Get(k)
		if err != nil {
			if os.IsNotExist(err) {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
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
