package libchunk_test

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/kr/s3"
	"github.com/restic/chunker"
)

const KeySize = 32

type K [KeySize]byte

func (k K) String() string {
	return base64.StdEncoding.EncodeToString(k[:])
}

func (k K) ByteLine() []byte {
	return []byte(fmt.Sprintf("%s\n", k))
}

func DecodeKey(b []byte) (k K, err error) {
	data := make([]byte, base64.StdEncoding.DecodedLen(len(b)))
	_, err = base64.StdEncoding.Decode(data, b)
	if err != nil {
		return k, fmt.Errorf("failed to decode '%x' as base64: %v", b, err)
	}

	//check key length
	k = K{}
	if len(data) > len(k)+1 {
		return k, fmt.Errorf("decoded chunk key '%x' has an invalid length %d, expected %d (or %d+1)", data, len(data), len(k), len(k))
	}

	//fill K and hand it over
	copy(k[:], data[:KeySize])
	return k, nil
}

func KeyScan(r io.Reader, fn func(k K) error) (err error) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		k, err := DecodeKey(s.Bytes())
		if err != nil {
			return fmt.Errorf("failed to decode chunk key: %v", err)
		}

		err = fn(k)
		if err != nil {
			return fmt.Errorf("failed to handle key '%s': %v", k, err)
		}
	}

	if err := s.Err(); err != nil {
		return fmt.Errorf("failed to scan chunk keys: %v", err)
	}

	return nil
}

type KeyFunc func([]byte) K

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

func (s *boltStore) Get(k [KeySize]byte) (chunk []byte, err error) {
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

//ErrCollector allows for reaping errors concurrently
type ErrCollector []error

func (errs ErrCollector) Collect(errCh <-chan error) {
	for err := range errCh {
		errs = append(errs, err)
	}
}

//returns nil if no errors are collecd or else lists
//the errors with whitespace
func (errs ErrCollector) ErrorOrNil() error {
	if len(errs) < 1 {
		return nil
	}
	var msgs []string
	for _, err := range errs {
		msgs = append(msgs, err.Error())
	}

	return errors.New(strings.Join(msgs, "\n"))
}

type Config struct {
	Chunker          Chunker
	AEAD             cipher.AEAD
	Secret           libchunk.Secret
	SplitBufSize     int64
	SplitConcurrency int
	KeyFunc          KeyFunc
	Store            Store
}

//Split reads a stream of bytes on 'r' and creates variable-sized chunks that
//are stored and encrypted under a content-based key 'k' in the configured store.
//Compute intensive operations are run concurrently but keys are guaranteed to
//be handed over to 'keyH' in order: key of the first chunk will be pushed first
func Split(r io.Reader, keyH func(k K) error, conf Config) (err error) {

	//work item
	type item struct {
		pos   int64
		chunk []byte
		keyCh chan K
	}

	//concurrent work
	work := func(it *item, errCh chan<- error) {
		k := conf.KeyFunc(it.chunk)                           //Hash
		encrypted := conf.AEAD.Seal(nil, k[:], it.chunk, nil) //Encrypt
		err = conf.Store.Put(k, encrypted)                    //Store
		if err != nil {
			errCh <- err
		}

		it.keyCh <- k //Output
	}

	//handle concurrent errors
	errs := ErrCollector{}
	errCh := make(chan error)
	go errs.Collect(errCh)

	//fan in, doneCh is closed whenever all key handlers have been called
	doneCh := make(chan struct{})
	itemCh := make(chan *item, conf.SplitConcurrency)
	go func() {
		var lastpos int64
		defer close(doneCh)
		for it := range itemCh {
			if lastpos > it.pos {
				//the language spec is unclear about guaranteed FIFO behaviour of
				//buffered channels, in rare conditions this behaviour might not
				//be guaranteed, for this project such a case be catestropic as it WILL
				//corrupt large files. This is a buildin safeguard that asks the user to
				//submit a real world example if this happens
				panic(fmt.Sprintf("Unexpected race condition during splitting, chunk '%d' arrived before chunk '%d', please report this to the author with the file that is being split", lastpos, it.pos))
			}

			err = keyH(<-it.keyCh)
			if err != nil {
				errCh <- err
			}

			lastpos = it.pos
		}
	}()

	buf := make([]byte, conf.SplitBufSize)
	pos := int64(0)
	for {
		chunk, err := conf.Chunker.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("failed to chunk input: %v", err)
		}

		it := &item{
			pos:   pos,
			chunk: make([]byte, chunk.Length),
			keyCh: make(chan K),
		}

		//the chunker reuses the buffer that underpins the chunk.Data
		//causing concurrent work to access unexpected data
		copy(it.chunk, chunk.Data)

		go work(it, errCh)
		itemCh <- it
		pos++
	}

	close(itemCh)
	<-doneCh
	return errs.ErrorOrNil()
}

func BenchmarkBolt(b *testing.B) {
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

	//Default Configuration is cryptograpically the most secure
	b.Run("default-conf", func(b *testing.B) {
		conf := Config{
			SplitBufSize:     chunker.MaxSize,
			SplitConcurrency: 64,
			AEAD:             aead,
			KeyFunc: func(b []byte) K {
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
				keys := []K{}
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

func benchmarkBoltRandomWritesChunkHashEncrypt(b *testing.B, data []byte, conf Config) (keys []K) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		keys = []K{}
		r := bytes.NewReader(data)
		conf.Chunker = chunker.New(r, secret.Pol())

		err := Split(r, func(k K) error {
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

func benchmarkBoltRandomReadsMergeToFile(b *testing.B, keys []K, data []byte, conf Config) {
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

func init() {
	go func() {
		log.Fatal(http.ListenAndServe("localhost:9000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(ioutil.Discard, r.Body)
		})))
	}()
}

func benchmarkBoltRandomReadsPushToLocalHTTP(b *testing.B, keys []K, data []byte, conf Config) {
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
			go func(k K, c []byte) {
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

// var diskDir string
// var diskKeys [][32]byte
// var diskSize int64

// func BenchmarkConcurrentChunkingSha2Disk(b *testing.B) {
// 	var (
// 		err   error
// 		block cipher.Block
// 		aead  cipher.AEAD
// 		hash  KeyFunc
// 		chnkr Chunker
// 		store Store
// 	)
//
// 	diskSize = int64(1024 * 1024 * 1024)
// 	r := bytes.NewReader(randb(diskSize))
// 	chnkr = chunker.New(r, secret.Pol())
// 	buf := make([]byte, chunker.MaxSize)
// 	diskDir, _ = ioutil.TempDir("", "libchunk")
//
// 	store = fsStore(diskDir)
//
// 	hash = func(b []byte) [KeySize]byte { return sha256.Sum256(b) }
//
// 	block, err = aes.NewCipher(secret[:])
// 	if err != nil {
// 		b.Fatalf("failed to create AES block cipher: %v", err)
// 	}
//
// 	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
// 	if err != nil {
// 		b.Fatalf("failed to setup GCM cipher mode: %v", err)
// 	}
//
// 	b.ResetTimer()
// 	b.SetBytes(int64(diskSize))
// 	for i := 0; i < b.N; i++ {
// 		r.Seek(0, 0)
// 		chnkr = chunker.New(r, secret.Pol())
// 		wg := &sync.WaitGroup{}
// 		for {
// 			chunk, err := chnkr.Next(buf)
// 			if err == io.EOF {
// 				break
// 			}
//
// 			wg.Add(1)
// 			go func() {
// 				defer wg.Done()
// 				k := hash(chunk.Data)
//
// 				wg.Add(1)
// 				go func() {
// 					defer wg.Done()
// 					encrypted := aead.Seal(nil, k[:], chunk.Data, nil)
//
// 					wg.Add(1)
// 					go func() {
// 						defer wg.Done()
//
// 						err = store.Put(k, encrypted)
// 						if err != nil {
// 							b.Fatal(err)
// 						}
//
// 						diskKeys = append(diskKeys, k)
// 					}()
// 				}()
// 			}()
// 		}
//
// 		wg.Wait()
// 	}
// }
//
// func BenchmarkConcurrentChunkingSha2FromFS(b *testing.B) {
// 	var (
// 		err   error
// 		block cipher.Block
// 		aead  cipher.AEAD
// 		store Store
// 	)
//
// 	block, err = aes.NewCipher(secret[:])
// 	if err != nil {
// 		b.Fatalf("failed to create AES block cipher: %v", err)
// 	}
//
// 	aead, err = cipher.NewGCMWithNonceSize(block, sha256.Size)
// 	if err != nil {
// 		b.Fatalf("failed to setup GCM cipher mode: %v", err)
// 	}
//
// 	store = fsStore(diskDir)
// 	defer os.RemoveAll(diskDir)
//
// 	b.ResetTimer()
// 	b.SetBytes(int64(boltSize))
// 	for i := 0; i < b.N; i++ {
// 		outf, _ := ioutil.TempFile("", "libchunk_")
// 		defer outf.Close()
// 		defer os.Remove(outf.Name())
//
// 		for _, k := range diskKeys {
//
// 			func() {
// 				chunk, err := store.Get(k)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
//
// 				plaintext, err := aead.Open(nil, k[:], chunk, nil)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
//
// 				_, err = outf.Write(plaintext)
// 				if err != nil {
// 					b.Fatal(err)
// 				}
// 			}()
// 		}
// 	}
// }
//
// func tempDB(t quiter) (p string, db *bolt.DB) {
// 	dbdir, err := ioutil.TempDir("", "libchunk_")
// 	if err != nil {
// 		t.Fatalf("failed to create temp dir for db: %v", err)
// 	}
//
// 	dbpath := filepath.Join(dbdir, "db.bolt")
// 	db, err = bolt.Open(dbpath, 0666, &bolt.Options{Timeout: 1 * time.Second})
// 	if err != nil {
// 		t.Fatalf("failed to open chunks database '%s': %v", dbpath, err)
// 	}
//
// 	err = db.Update(func(tx *bolt.Tx) error {
// 		_, err := tx.CreateBucketIfNotExists(libchunk.BucketNameChunks)
// 		if err != nil {
// 			return fmt.Errorf("failed to create bucket: %s", err)
// 		}
// 		return nil
// 	})
//
// 	if err != nil {
// 		t.Fatalf("failed to create buckets: %v", err)
// 	}
//
// 	return dbpath, db
// }
//
// func TestSplitJoinSmallNonRandom(t *testing.T) {
// 	_, db := tempDB(t)
// 	input := []byte("foo bar") //@TODO find somall content that generates multiple chunks
// 	output := bytes.NewBuffer(nil)
//
// 	pr, pw := io.Pipe()
// 	go func() {
// 		defer pw.Close()
// 		err := libchunk.Split(db, secret, bytes.NewBuffer(input), pw)
// 		if err != nil {
// 			t.Fatalf("failed to split: %v", err)
// 		}
// 	}()
//
// 	err := libchunk.Join(db, secret, pr, output)
// 	if err != nil {
// 		t.Fatalf("failed to join: %v", err)
// 	}
//
// 	if !bytes.Equal(input, output.Bytes()) {
// 		t.Errorf("expected joined output (len %d) to be the same as input (len %d)", output.Len(), len(input))
// 	}
//
// 	//assert encryption at rest
// 	//assert throughput
//
// 	//assert:
// 	//  - assert writer output
// 	//  - different polynomials generate different
// 	//    - key
// 	//    - encrypted content
// 	//  - different secret parts generate random noise
// 	//  -assert multiple chunks being outputed
//
// 	//assert db file size
// 	//assert buffer content
// 	//assert chunks in db
// 	//assert deduplication
// 	//assert encryption
//
// }
