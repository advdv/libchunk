package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
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

func BenchmarkRawChunking(b *testing.B) {
	size := int64(32 * 1024 * 1024)
	r := bytes.NewReader(randb(size))
	chnkr := chunker.New(r, secret.Pol())
	buf := make([]byte, chunker.MaxSize)

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		chnkr.Reset(r, secret.Pol())
		for {
			_, err := chnkr.Next(buf)
			if err == io.EOF {
				break
			}
		}
	}
}

func BenchmarkConcurrentChunkingSha2Bolt(b *testing.B) {
	size := int64(1024 * 1024 * 1024)
	r := bytes.NewReader(randb(size))
	chnkr := chunker.New(r, secret.Pol())
	buf := make([]byte, chunker.MaxSize)
	dir, _ := ioutil.TempDir("", "libchunk")
	path := filepath.Join(dir, "db.bolt")
	db, _ := bolt.Open(path, 0777, nil)
	block, err := aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	gcm, err := cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	defer db.Close()
	defer os.Remove(path)

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		chnkr.Reset(r, secret.Pol())
		wg := &sync.WaitGroup{}
		for {
			chunk, err := chnkr.Next(buf)
			if err == io.EOF {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				k := sha256.Sum256(chunk.Data)

				wg.Add(1)
				go func() {
					defer wg.Done()
					encrypted := gcm.Seal(chunk.Data[:0], k[:], chunk.Data, nil)

					wg.Add(1)
					go func() {
						defer wg.Done()
						err = db.Batch(func(tx *bolt.Tx) error {
							b, _ := tx.CreateBucketIfNotExists([]byte("a"))
							return b.Put(k[:], encrypted)
						})

						if err != nil {
							b.Fatal(err)
						}
					}()

				}()

			}()
		}

		wg.Wait()
	}
}

func BenchmarkRawSha1(b *testing.B) {
	size := int64(32 * 1024 * 1024)
	r := bytes.NewReader(randb(size))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		h := sha1.New()
		io.Copy(h, r)
	}
}

func BenchmarkRawSha2Chunk(b *testing.B) {
	size := int64(32 * 1024 * 1024)
	r := bytes.NewReader(randb(size))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		h := sha256.New()
		io.Copy(h, r)
	}
}

func BenchmarkRawSha2Concurrent4MBChunks(b *testing.B) {
	size := int64(1024 * 1024 * 1024)
	r := bytes.NewReader(randb(size))

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		wg := &sync.WaitGroup{}
		for {
			v := make([]byte, 4*1024*1024)
			_, err := r.Read(v)
			if err == io.EOF {
				break
			}

			wg.Add(1)
			go func() {
				wg.Done()
				k := sha256.Sum256(v)
				ioutil.Discard.Write(k[:])
			}()
		}
	}
}

func BenchmarkRawGCM(b *testing.B) {
	size := int64(64 * 1024 * 1024)
	data := randb(size)

	block, err := aes.NewCipher(secret[:])
	if err != nil {
		b.Fatalf("failed to create AES block cipher: %v", err)
	}

	gcm, err := cipher.NewGCMWithNonceSize(block, sha1.Size)
	if err != nil {
		b.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		buf := make([]byte, gcm.NonceSize())
		binary.PutVarint(buf, int64(i))
		encrypted := gcm.Seal(data[:0], buf, data, nil)
		ioutil.Discard.Write([]byte(encrypted))
	}
}

//slowness of random writes on boltdb?
//other approach: http://nyeggen.com/post/2014-06-07-b-trees-are-overrated-try-hashing-instead/
func BenchmarkRawBoltDBRandomWrites(b *testing.B) {
	size := int64(1024 * 1024 * 1024)
	dir, _ := ioutil.TempDir("", "libchunk")
	path := filepath.Join(dir, "db.bolt")
	db, _ := bolt.Open(path, 0777, nil)
	defer db.Close()
	defer os.Remove(path)

	r := bytes.NewReader(randb(size))
	b.ResetTimer()
	b.SetBytes(size)
	for i := 0; i < b.N; i++ {
		r.Seek(0, 0)
		wg := &sync.WaitGroup{}
		for {
			k := make([]byte, 1024)
			v := make([]byte, 4*1024*1024)
			_, err := r.Read(k)
			if err == io.EOF {
				break
			}

			_, err = r.Read(v)
			if err == io.EOF {
				break
			}

			wg.Add(1)
			go func() {
				defer wg.Done()
				db.Batch(func(tx *bolt.Tx) error {
					b, err := tx.CreateBucketIfNotExists([]byte("a"))
					if err != nil {
						return err
					}

					return b.Put(k, v)
				})
			}()
		}

		wg.Wait()
	}

	fi, err := os.Stat(path)
	if err != nil {
		b.Error(err)
	}

	fmt.Println("input size:", size/1024/1024, "db file size:", fi.Size()/1024/1024)
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

// func BenchmarkRandData(b *testing.B) {
// 	table := map[string]struct {
// 		sizes []int64 //sizes in MiB
// 		fn    func(*testing.B, io.Reader) error
// 	}{
// 		//max speed at which we can read random bytes from math package
// 		"writing-dev-null": {
// 			sizes: []int64{5},
// 			fn: func(b *testing.B, r io.Reader) error {
// 				for n := 0; n < b.N; n++ {
// 					_, err := io.Copy(ioutil.Discard, r)
// 					if err != nil {
// 						return err
// 					}
// 				}
//
// 				return nil
// 			},
// 		},
//
// 		// //just using CBC
// 		// "plain chunking": {
// 		// 	sizes: []int64{5, 10},
// 		// 	fn: func(b *testing.B, r io.Reader) {
// 		// 		// data, _ := ioutil.ReadAll(r)
// 		// 		// rd := bytes.NewReader(data)
// 		// 		// buf := make([]byte, chunker.MaxSize)
// 		// 		// chnkr := chunker.New(rd, secret.Pol())
// 		// 		// b.ResetTimer()
// 		// 		// b.SetBytes(int64(len(data)))
// 		// 		// for {
// 		// 		// 	_, err := chnkr.Next(buf)
// 		// 		// 	if err == io.EOF {
// 		// 		// 		break
// 		// 		// 	}
// 		// 		// }
// 		// 		//
// 		// 		// return nil
// 		//
// 		// 	},
// 		// },
// 	}
//
// 	for name, c := range table {
// 		for _, size := range c.sizes {
// 			b.Run(fmt.Sprintf("%s(%dMiB)", name, size), func(b *testing.B) {
// 				b.SetBytes(size * 1024 * 1024)
// 				err := c.fn(b, io.LimitReader(mathr, size*1024*1024))
// 				if err != nil {
// 					b.Error(err)
// 				}
//
// 				// bsize := size * 1024 * 1024
// 				// b.SetBytes(bsize)
// 				// for n := 0; n < b.N; n++ {
// 				// 	c.fn
// 				// }
// 			})
// 		}
// 	}
// }

// func BenchmarkSplit(b *testing.B) {
// 	basesize := int64(40 * 1024 * 1024)
// 	b.Run("baseline", func(b *testing.B) {
// 		b.SetBytes(basesize)
// 		for n := 0; n < b.N; n++ {
// 			io.Copy(ioutil.Discard, io.LimitReader(mathr, basesize))
// 		}
// 	})
//
// 	b.Run("sha256", func(b *testing.B) {
// 		b.SetBytes(basesize)
// 		for n := 0; n < b.N; n++ {
// 			h := sha256.New()
// 			io.Copy(h, io.LimitReader(mathr, basesize))
// 			h.Sum(nil)
// 		}
// 	})
//
// 	b.Run("chunking + sha256", func(b *testing.B) {
// 		dbp, db := tempDB(b)
// 		defer db.Close()
//
// 		block, err := aes.NewCipher(secret[:])
// 		if err != nil {
// 			b.Fatalf("failed to create AES block cipher: %v", err)
// 		}
//
// 		gcm, err := cipher.NewGCMWithNonceSize(block, sha1.Size)
// 		if err != nil {
// 			b.Fatalf("failed to setup GCM cipher mode: %v", err)
// 		}
//
// 		//if we elimate boltdb because it isnt very good at random
// 		//writes the chunking logic become the bottleneck. library implementation
// 		//reports a bencharm with upt 400mb/s but we dont see that here
// 		b.SetBytes(basesize)
// 		b.ResetTimer()
// 		for n := 0; n < b.N; n++ {
// 			chunker := chunker.New(io.LimitReader(mathr, basesize), secret.Pol())
// 			buf := make([]byte, chunker.MaxSize)
// 			wg := &sync.WaitGroup{}
// 			for {
// 				chunk, err := chunker.Next(buf)
// 				if err != nil {
// 					if err == io.EOF {
// 						break
// 					}
//
// 					b.Fatalf("chunking failed: %v", err)
// 				}
//
// 				_ = chunk
// 				_ = dbp
// 				_ = gcm
//
// 				// go func() {
// 				// 	wg.Add(1)
// 				// 	defer wg.Done()
// 				// 	k := sha1.Sum(chunk.Data)
// 				//
// 				// 	_ = dbp
// 				// 	ioutil.Discard.Write(k[:])
// 				// 	_ = gcm
// 				// 	// go func() {
// 				// 	// 	wg.Add(1)
// 				// 	// 	defer wg.Done()
// 				// 	// 	encrypted := gcm.Seal(chunk.Data[:0], k[:], chunk.Data, nil)
// 				// 	// 	if err != nil {
// 				// 	// 		b.Fatalf("failed to encrypt chunk '%s': %v", k, err)
// 				// 	// 	}
// 				// 	//
// 				// 	// 	_ = dbp
// 				// 	// 	ioutil.Discard.Write(encrypted)
// 				//
// 				// 	// go func() {
// 				// 	// 	wg.Add(1)
// 				// 	// 	defer wg.Done()
// 				// 	// 	chunkp := filepath.Join(filepath.Dir(dbp), fmt.Sprintf("%x", k))
// 				// 	// 	f, err := os.OpenFile(chunkp, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0777)
// 				// 	// 	if err != nil {
// 				// 	// 		if os.IsExist(err) {
// 				// 	// 			return
// 				// 	// 		}
// 				// 	//
// 				// 	// 		b.Fatal(err)
// 				// 	// 	}
// 				// 	//
// 				// 	// 	defer f.Close()
// 				// 	// 	_, err = f.Write(encrypted)
// 				// 	// 	if err != nil {
// 				// 	// 		b.Fatal(err)
// 				// 	// 	}
// 				// 	// }()
// 				//
// 				// 	// limits throughput to 150Mbs: possibly random writes
// 				// 	// go func() {
// 				// 	// 	wg.Add(1)
// 				// 	// 	defer wg.Done()
// 				// 	//
// 				// 	// 	err = db.Batch(func(tx *bolt.Tx) error {
// 				// 	// 		b := tx.Bucket(libchunk.BucketNameChunks)
// 				// 	// 		c := b.Get(k[:8])
// 				// 	// 		if c != nil {
// 				// 	// 			return nil
// 				// 	// 		}
// 				// 	//
// 				// 	// 		return b.Put(k[:], encrypted)
// 				// 	// 	})
// 				// 	//
// 				// 	// 	if err != nil {
// 				// 	// 		b.Fatalf("failed to batch: %v", err)
// 				// 	// 	}
// 				// 	// }()
// 				// 	// }()
// 				// }()
// 			}
//
// 			wg.Wait()
// 		}
// 	})
//

// func BenchmarkSplit(b *testing.B) {
// 	cases := []int64{5, 10, 20, 40, 80, 160}
// 	for _, c := range cases {
// 		b.Run(fmt.Sprintf("size=rand(%dMiB)", c), func(b *testing.B) {
// 			benchRandSplit(b, c*1024*1024)
// 		})
// 	}
// }
//
// func benchRandSplit(b *testing.B, size int64) {
// 	dbp, db := tempDB(b)
// 	b.SetBytes(size)
// 	b.ResetTimer()
// 	for n := 0; n < b.N; n++ {
// 		err := libchunk.Split(db, secret, randr(size), ioutil.Discard)
// 		if err != nil {
// 			b.Fatalf("failed to join: %v", err)
// 		}
// 	}
//
// 	err := db.Close()
// 	if err != nil {
// 		b.Fatalf("failed to close db: %v", err)
// 	}
//
// 	fi, err := os.Stat(dbp)
// 	if err != nil {
// 		b.Fatalf("failed to stat db file: %v", err)
// 	}
//
// 	_ = fi
// 	// if fi.Size() < size {
// 	// 	b.Fatalf("on-disk db file was smaller then input data: %v", err)
// 	// }
//
// 	err = os.Remove(dbp)
// 	if err != nil {
// 		b.Fatalf("failed to remove db after benchmark: %v", err)
// 	}
// }

//
// // func benchRandSplit(b *testing.B, size int64) {
// // 	buf := bytes.NewBuffer(nil)
// // 	_, err := io.Copy(buf, io.LimitReader(rand.Reader, size))
// // 	if err != nil {
// // 		b.Fatalf("failed to buffer random bytes: %v", err)
// // 	}
// //
// // 	benchBufSplit(b, buf)
// // }
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
