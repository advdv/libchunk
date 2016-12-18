package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
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
	"github.com/restic/chunker"
)

//
// Test types
//

type fsStore string

func (s fsStore) Put(k [libchunk.KeySize]byte, chunk []byte) error {
	f, err := os.OpenFile(filepath.Join(string(s), fmt.Sprintf("%x", k)), os.O_CREATE|os.O_RDWR|os.O_EXCL, 0777)
	if err != nil {
		if os.IsExist(err) {
			return nil
		}

		return err
	}

	defer f.Close()
	_, err = f.Write(chunk)
	if err != nil {
		return err
	}

	return nil
}

func (s fsStore) Get(k [libchunk.KeySize]byte) ([]byte, error) {
	kpath := filepath.Join(string(s), fmt.Sprintf("%x", k))
	f, err := os.OpenFile(kpath, os.O_RDONLY, 0777)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	chunk, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}

type boltStore struct {
	db         *bolt.DB
	bucketName []byte
}

func (s *boltStore) Put(k [libchunk.KeySize]byte, chunk []byte) (err error) {
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

func (s *boltStore) Get(k [libchunk.KeySize]byte) (chunk []byte, err error) {
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

	block, err := aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	defer db.Close()
	defer os.RemoveAll(dbdir)

	//Default libchunk.Configuration is cryptograpically the most secure
	b.Run("default-conf", func(b *testing.B) {
		conf := libchunk.Config{
			SplitBufSize:     chunker.MaxSize,
			SplitConcurrency: 64,
			AEAD:             aead,
			KeyFunc: func(b []byte) libchunk.K {
				return sha256.Sum256(b)
			},
			Store: &boltStore{db, []byte("chunks")},
		}

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
		keys = []libchunk.K{}
		r := bytes.NewReader(data)
		conf.Chunker = chunker.New(r, secret.Pol())

		err := libchunk.Split(r, func(k libchunk.K) error {
			keys = append(keys, k)
			return nil
		}, conf)

		if err != nil {
			b.Fatal(err)
		}

		if len(keys) < 1 {
			b.Fatal("expected split to output at least one key")
		}
	}

	return keys
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

		for _, k := range keys {
			chunk, err := conf.Store.Get(k)
			if err != nil {
				b.Fatalf("failed to find key '%s': %v", k, err)
			}

			plaintext, err := conf.AEAD.Open(nil, k[:], chunk, nil)
			if err != nil {
				b.Fatal(err)
			}

			_, err = outf.Write(plaintext)
			if err != nil {
				b.Fatal(err)
			}
		}

		outf.Close()
		output, err := ioutil.ReadFile(outf.Name())
		if err != nil {
			b.Fatal(err)
		}

		if !bytes.Equal(data, output) {
			b.Errorf("written output data should be equal to input data, len input: %d (%x ...), len output: %d (%x ...)", len(data), data[:64], len(output), output[:64])
		}
	}
}

func benchmarkBoltRandomReadsPushToLocalHTTP(b *testing.B, keys []libchunk.K, data []byte, conf libchunk.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		client := http.Client{}
		concurrency := 64
		sem := make(chan bool, concurrency)
		for _, key := range keys {
			chunk, err := conf.Store.Get(key)
			if err != nil {
				b.Fatal(err)
			}

			sem <- true
			go func(k libchunk.K, c []byte) {
				defer func() { <-sem }()
				req, err := http.NewRequest("POST", fmt.Sprintf("http://localhost:9000/%x", k), bytes.NewReader(c))
				if err != nil {
					b.Fatal(err)
				}

				s3.Sign(req, s3.Keys{AccessKey: "access-key-id", SecretKey: "secret-key-id"})
				resp, err := client.Do(req)
				if err != nil || resp == nil || resp.StatusCode != 200 {
					b.Fatal(err)
				}

			}(key, chunk)
		}

		for i := 0; i < cap(sem); i++ {
			sem <- true
		}
	}
}
