package libchunk_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
)

//
// Test types
//

func BenchmarkConfigurations(b *testing.B) {
	go func() {
		log.Fatal(http.ListenAndServe("localhost:9000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(ioutil.Discard, r.Body)
		})))
	}()

	dbdir, err := ioutil.TempDir("", "libchunk_db_")
	if err != nil {
		b.Fatal(err)
	}

	dbpath := filepath.Join(dbdir, "db.bolt")
	db, err := bolt.Open(dbpath, 0777, nil)
	if err != nil {
		b.Fatal(err)
	}

	defer db.Close()
	defer os.RemoveAll(dbdir)

	//Default libchunk.Configuration is cryptograpically the most secure
	b.Run("default-conf", func(b *testing.B) {
		conf := defaultConfigWithRemote(b, nil)
		sizes := []int64{
			1024,
			1024 * 1024,
			12 * 1024 * 1024,
			1024 * 1024 * 1024,
		}

		for _, s := range sizes {
			b.Run(fmt.Sprintf("%dMiB", s/1024/1024), func(b *testing.B) {
				data := randb(s)
				keys := []libchunk.K{}
				b.Run("split", func(b *testing.B) {
					keys = benchmarkBoltRandomWritesChunkHashEncrypt(b, data, conf)
				})

				b.Run("join", func(b *testing.B) {
					benchmarkBoltRandomReadsMergeToFile(b, keys, data, conf)
				})

				b.Run("push", func(b *testing.B) {
					benchmarkBoltRandomReadsPushToLocalHTTP(b, keys, data, conf)
				})

				//@TODO benchmark this
				// b.Run("join-from-remote", func(b *testing.B) {
				// 	benchmarkBoltRandomReadsMergeToFile(b, keys, data, conf)
				// })
			})
		}
	})
}

func benchmarkBoltRandomWritesChunkHashEncrypt(b *testing.B, data []byte, conf libchunk.Config) (keys []libchunk.K) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		input := &randomBytesInput{bytes.NewBuffer(data)}
		h := &sliceKeyIterator{}
		err := libchunk.Split(input, h, conf)

		keys = h.Keys
		if err != nil {
			b.Fatal(err)
		}

		if len(keys) < 1 {
			b.Fatal("expected split to output at least one key")
		}
	}

	return keys
}

type sliceKeyIterator struct {
	i    int
	Keys []libchunk.K
}

func (iter *sliceKeyIterator) Reset() {
	iter.i = 0
}

func (iter *sliceKeyIterator) Put(k libchunk.K) (err error) {
	iter.Keys = append(iter.Keys, k)
	return nil
}

func (iter *sliceKeyIterator) Next() (k libchunk.K, err error) {
	if iter.i > len(iter.Keys)-1 {
		return k, io.EOF
	}

	k = iter.Keys[iter.i]
	iter.i++
	return k, nil
}

type failingSlicePutter struct{}

func (iter *failingSlicePutter) Put(k libchunk.K) (err error) {
	return fmt.Errorf("handler_failed")
}

func benchmarkBoltRandomReadsMergeToFile(b *testing.B, keys []libchunk.K, data []byte, conf libchunk.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		outf, err := ioutil.TempFile("", "libchunk_")
		if err != nil {
			b.Fatal(err)
		}

		defer os.Remove(outf.Name())
		err = libchunk.Join(&sliceKeyIterator{0, keys}, outf, conf)
		outf.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkBoltRandomReadsPushToLocalHTTP(b *testing.B, keys []libchunk.K, data []byte, conf libchunk.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		iter := &sliceKeyIterator{Keys: keys}
		err := libchunk.Push(iter, conf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
