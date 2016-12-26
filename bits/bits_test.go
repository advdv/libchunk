package bits_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/store"
)

func BenchmarkConfigurations(b *testing.B) {
	go func() {
		log.Fatal(http.ListenAndServe("localhost:9000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(ioutil.Discard, r.Body)
		})))
	}()

	//Default bits.Configuration is cryptograpically the most secure
	b.Run("default-conf", func(b *testing.B) {
		sizes := []int64{
			1024,
			1024 * 1024,
			12 * 1024 * 1024,
			1024 * 1024 * 1024,
		}

		for _, s := range sizes {
			conf := withS3Remote(b, withTmpBoltStore(b, defaultConf(b, secret)), nil)

			b.Run(fmt.Sprintf("%dMiB", s/1024/1024), func(b *testing.B) {
				data := randb(s)
				keys := []bits.K{}
				b.Run("put", func(b *testing.B) {
					keys = benchmarkBoltRandomWritesChunkHashEncrypt(b, data, conf)
				})

				b.Run("get", func(b *testing.B) {
					benchmarkBoltRandomReadsGetToFile(b, keys, data, conf)
				})

				b.Run("mv", func(b *testing.B) {
					benchmarkBoltRandomReadsMoveToLocalHTTP(b, keys, data, conf)
				})

				b.Run("get-from-remote", func(b *testing.B) {
					benchmarkRemoteGetToFile(b, data, conf)
				})
			})
		}
	})
}

func benchmarkRemoteGetToFile(b *testing.B, data []byte, conf bits.Config) {
	keys := &bitskeys.MemIterator{}
	store := bitsstore.NewMemStore()
	input := randBytesInput(bytes.NewReader(data), secret)
	err := bits.Put(input, keys, withStore(b, defaultConf(b, secret), store))
	if err != nil {
		b.Fatalf("couldnt split for test prep: %v", err)
	}

	conf = withStore(b, conf, nil)             //disable local store
	conf = withS3Remote(b, conf, store.Chunks) //use chunks stored in memory

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outf, err := ioutil.TempFile("", "bits_")
		if err != nil {
			b.Fatal(err)
		}

		defer os.Remove(outf.Name())
		err = bits.Get(keys, outf, conf)
		outf.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkBoltRandomWritesChunkHashEncrypt(b *testing.B, data []byte, conf bits.Config) (keys []bits.K) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		input := randBytesInput(bytes.NewReader(data), secret)
		h := &bitskeys.MemIterator{}
		err := bits.Put(input, h, conf)

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

func benchmarkBoltRandomReadsGetToFile(b *testing.B, keys []bits.K, data []byte, conf bits.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		outf, err := ioutil.TempFile("", "bits_")
		if err != nil {
			b.Fatal(err)
		}

		defer os.Remove(outf.Name())
		iter := bitskeys.NewMemIterator()
		iter.Keys = keys
		err = bits.Get(iter, outf, conf)
		outf.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkBoltRandomReadsMoveToLocalHTTP(b *testing.B, keys []bits.K, data []byte, conf bits.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		iter := &bitskeys.MemIterator{Keys: keys}
		h := &bitskeys.MemIterator{}
		err := bits.Move(iter, h, conf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
