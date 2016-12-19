package libchunk_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/kr/s3"
)

//
// Test types
//

type httpRemote struct {
	scheme string
	host   string
	client *http.Client
}

func (r *httpRemote) Put(k libchunk.K, chunk []byte) (err error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s://%s/%x", r.scheme, r.host, k), bytes.NewReader(chunk))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request for '%x': %v", k, err)
	}

	s3.Sign(req, s3.Keys{AccessKey: "access-key-id", SecretKey: "secret-key-id"})
	resp, err := r.client.Do(req)
	if err != nil || resp == nil || resp.StatusCode != 200 {
		return fmt.Errorf("failed to perform HTTP request for '%x': %v", k, err)
	}

	return nil
}

type boltStore struct {
	db         *bolt.DB
	bucketName []byte
}

func (s *boltStore) Put(k libchunk.K, chunk []byte) (err error) {
	return s.db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(s.bucketName)
		if err != nil {
			return fmt.Errorf("failed to create-if-not-exist: %v", err)
		}

		existing := b.Get(k[:])
		if existing != nil {
			return nil
		}

		return b.Put(k[:], chunk)
	})
}

func (s *boltStore) Get(k libchunk.K) (chunk []byte, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		if b == nil {
			return fmt.Errorf("bucket '%s' must first be created", string(s.bucketName))
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

type quiter interface {
	Fatalf(format string, args ...interface{})
}

var secret = libchunk.Secret{
	0x3D, 0xA3, 0x35, 0x8B, 0x4D, 0xC1, 0x73, 0x00, //polynomial
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //random bytes
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

func randr(size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), size)
}

func randb(size int64) []byte {
	b, err := ioutil.ReadAll(randr(size))
	if err != nil {
		panic(err)
	}

	return b
}

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
		conf := defaultConfig(b)
		conf.Store = &boltStore{db, []byte("chunks")}
		conf.Remote = &httpRemote{"http", "localhost:9000", &http.Client{}}

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
