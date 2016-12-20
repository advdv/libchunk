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
	"github.com/advanderveer/libchunk/bits/iterator"
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
				b.Run("split", func(b *testing.B) {
					keys = benchmarkBoltRandomWritesChunkHashEncrypt(b, data, conf)
				})

				b.Run("join", func(b *testing.B) {
					benchmarkBoltRandomReadsJoinToFile(b, keys, data, conf)
				})

				b.Run("push", func(b *testing.B) {
					benchmarkBoltRandomReadsPushToLocalHTTP(b, keys, data, conf)
				})

				b.Run("join-from-remote", func(b *testing.B) {
					benchmarkRemoteJoinToFile(b, data, conf)
				})
			})
		}
	})
}

func benchmarkRemoteJoinToFile(b *testing.B, data []byte, conf bits.Config) {
	keys := &bitsiterator.MemIterator{}
	store := bitsstore.NewMemStore()
	input := &randomBytesInput{bytes.NewReader(data)}
	err := bits.Split(input, keys, withStore(b, defaultConf(b, secret), store))
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
		err = bits.Join(keys, outf, conf)
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
		input := &randomBytesInput{bytes.NewBuffer(data)}
		h := &bitsiterator.MemIterator{}
		err := bits.Split(input, h, conf)

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

func benchmarkBoltRandomReadsJoinToFile(b *testing.B, keys []bits.K, data []byte, conf bits.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		outf, err := ioutil.TempFile("", "bits_")
		if err != nil {
			b.Fatal(err)
		}

		defer os.Remove(outf.Name())
		iter := bitsiterator.NewMemIterator()
		iter.Keys = keys
		err = bits.Join(iter, outf, conf)
		outf.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkBoltRandomReadsPushToLocalHTTP(b *testing.B, keys []bits.K, data []byte, conf bits.Config) {
	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		iter := &bitsiterator.MemIterator{Keys: keys}
		err := bits.Push(iter, conf)
		if err != nil {
			b.Fatal(err)
		}
	}
}
