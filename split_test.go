package libchunk_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/restic/chunker"
)

const KeySize = 32

type KeyFunc func([]byte) [KeySize]byte

type Chunker interface {
	Next(data []byte) (chunker.Chunk, error)
}

//Store holds chunks
type Store interface {

	//will do nothing if exists, must be atomic
	Put(k [KeySize]byte, chunk []byte) error

	//returns os.NotExist if the chunk doesnt exist
	Get(k [KeySize]byte) (chunk []byte, err error)
}

//RemoteStore holds chunks at a remote location such
//that it becomes efficient to keep an up-to-date local
//index to prevent unnesseary interactions
type RemoteStore interface {
	Store

	//update the local index
	Index(ctx context.Context) error
}

//
// Test types
//

type fsStore string

func (s fsStore) Put(k [KeySize]byte, chunk []byte) error {
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

func (s fsStore) Get(k [KeySize]byte) ([]byte, error) {
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

func (s *boltStore) Put(k [KeySize]byte, chunk []byte) (err error) {
	return s.db.Batch(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists(s.bucketName)
		existing := b.Get(k[:])
		if existing != nil {
			return nil
		}

		return b.Put(k[:], chunk)
	})
}

func (s *boltStore) Get(k [KeySize]byte) (chunk []byte, err error) {
	return chunk, s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.bucketName)
		v := b.Get(k[:])
		if v == nil || len(v) < 1 {
			return os.ErrNotExist
		}

		chunk = make([]byte, len(v))
		copy(chunk, v)
		return nil
	})
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

var boltSize int64
var boltPath string
var boltKeys [][32]byte
var boltInput []byte

func BenchmarkConcurrentChunkingSha2Bolt(b *testing.B) {
	var (
		err   error
		block cipher.Block
		aead  cipher.AEAD
		hash  KeyFunc
		chnkr Chunker
		store Store
	)

	boltSize = int64(1024 * 1024 * 1024)
	boltInput = randb(boltSize)
	r := bytes.NewReader(boltInput)
	chnkr = chunker.New(r, secret.Pol())
	buf := make([]byte, chunker.MaxSize)
	dir, _ := ioutil.TempDir("", "libchunk")
	boltPath = filepath.Join(dir, "db.bolt")

	hash = func(b []byte) [KeySize]byte {
		return sha256.Sum256(b)
	}

	db, _ := bolt.Open(boltPath, 0777, nil)
	store = &boltStore{db, []byte("a")}

	block, err = aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	defer db.Close()

	b.ResetTimer()
	b.SetBytes(int64(boltSize))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		chnkr = chunker.New(r, secret.Pol())
		wg := &sync.WaitGroup{}
		for {
			chunk, err := chnkr.Next(buf)
			if err == io.EOF {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				k := hash(chunk.Data)

				wg.Add(1)
				go func() {
					defer wg.Done()
					encrypted := aead.Seal(nil, k[:], chunk.Data, nil)
					if bytes.Equal(encrypted, chunk.Data) {
						b.Fatal("huh")
					}

					wg.Add(1)
					go func() {
						defer wg.Done()

						err = store.Put(k, encrypted)
						if err != nil {
							b.Fatal(err)
						}

						boltKeys = append(boltKeys, k)
					}()
				}()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkConcurrentChunkingSha2FromBolt(b *testing.B) {
	var (
		err   error
		block cipher.Block
		aead  cipher.AEAD
		store Store
	)

	db, _ := bolt.Open(boltPath, 0777, nil)

	store = &boltStore{db, []byte("a")}

	block, err = aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	defer db.Close()
	defer os.Remove(boltPath)

	b.ResetTimer()
	b.SetBytes(int64(boltSize))
	for i := 0; i < b.N; i++ {
		outf, _ := ioutil.TempFile("", "libchunk_")
		defer outf.Close()
		defer os.Remove(outf.Name())

		for _, k := range boltKeys {
			chunk, err := store.Get(k)
			if err != nil {
				b.Fatal(err)
			}

			plaintext, err := aead.Open(nil, k[:], chunk, nil)
			if err != nil {
				b.Fatal(err)
			}

			//@TODO benchmark output to file: benchmarked at 700Mb/s
			//@TODO benchmark push to s3, goal: 1GB/s
			_, err = outf.Write(plaintext)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

var diskDir string
var diskKeys [][32]byte
var diskSize int64

func BenchmarkConcurrentChunkingSha2Disk(b *testing.B) {
	var (
		err   error
		block cipher.Block
		aead  cipher.AEAD
		hash  KeyFunc
		chnkr Chunker
		store Store
	)

	diskSize = int64(1024 * 1024 * 1024)
	r := bytes.NewReader(randb(diskSize))
	chnkr = chunker.New(r, secret.Pol())
	buf := make([]byte, chunker.MaxSize)
	diskDir, _ = ioutil.TempDir("", "libchunk")

	store = fsStore(diskDir)

	hash = func(b []byte) [KeySize]byte { return sha256.Sum256(b) }

	block, err = aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(diskSize))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		chnkr = chunker.New(r, secret.Pol())
		wg := &sync.WaitGroup{}
		for {
			chunk, err := chnkr.Next(buf)
			if err == io.EOF {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				k := hash(chunk.Data)

				wg.Add(1)
				go func() {
					defer wg.Done()
					encrypted := aead.Seal(nil, k[:], chunk.Data, nil)

					wg.Add(1)
					go func() {
						defer wg.Done()

						err = store.Put(k, encrypted)
						if err != nil {
							b.Fatal(err)
						}

						diskKeys = append(diskKeys, k)
					}()
				}()
			}()
		}

		wg.Wait()
	}
}

func BenchmarkConcurrentChunkingSha2FromFS(b *testing.B) {
	var (
		err   error
		block cipher.Block
		aead  cipher.AEAD
		store Store
	)

	block, err = aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	store = fsStore(diskDir)
	defer os.RemoveAll(diskDir)

	b.ResetTimer()
	b.SetBytes(int64(boltSize))
	for i := 0; i < b.N; i++ {
		outf, _ := ioutil.TempFile("", "libchunk_")
		defer outf.Close()
		defer os.Remove(outf.Name())

		for _, k := range diskKeys {

			func() {
				chunk, err := store.Get(k)
				if err != nil {
					b.Fatal(err)
				}

				plaintext, err := aead.Open(nil, k[:], chunk, nil)
				if err != nil {
					b.Fatal(err)
				}

				_, err = outf.Write(plaintext)
				if err != nil {
					b.Fatal(err)
				}
			}()
		}
	}
}

func tempDB(t quiter) (p string, db *bolt.DB) {
	dbdir, err := ioutil.TempDir("", "libchunk_")
	if err != nil {
		t.Fatalf("failed to create temp dir for db: %v", err)
	}

	dbpath := filepath.Join(dbdir, "db.bolt")
	db, err = bolt.Open(dbpath, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Fatalf("failed to open chunks database '%s': %v", dbpath, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(libchunk.BucketNameChunks)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %s", err)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("failed to create buckets: %v", err)
	}

	return dbpath, db
}

func TestSplitJoinSmallNonRandom(t *testing.T) {
	_, db := tempDB(t)
	input := []byte("foo bar") //@TODO find somall content that generates multiple chunks
	output := bytes.NewBuffer(nil)

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err := libchunk.Split(db, secret, bytes.NewBuffer(input), pw)
		if err != nil {
			t.Fatalf("failed to split: %v", err)
		}
	}()

	err := libchunk.Join(db, secret, pr, output)
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if !bytes.Equal(input, output.Bytes()) {
		t.Errorf("expected joined output (len %d) to be the same as input (len %d)", output.Len(), len(input))
	}

	//assert encryption at rest
	//assert throughput

	//assert:
	//  - assert writer output
	//  - different polynomials generate different
	//    - key
	//    - encrypted content
	//  - different secret parts generate random noise
	//  -assert multiple chunks being outputed

	//assert db file size
	//assert buffer content
	//assert chunks in db
	//assert deduplication
	//assert encryption

}
