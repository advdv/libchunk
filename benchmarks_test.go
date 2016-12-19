package libchunk_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"

	"github.com/advanderveer/libchunk"
)

func BenchmarkConfigurations(b *testing.B) {
	go func() {
		log.Fatal(http.ListenAndServe("localhost:9000", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(ioutil.Discard, r.Body)
		})))
	}()

	//Default libchunk.Configuration is cryptograpically the most secure
	b.Run("default-conf", func(b *testing.B) {
		sizes := []int64{
			1024,
			1024 * 1024,
			12 * 1024 * 1024,
			1024 * 1024 * 1024,
		}

		for _, s := range sizes {
			conf := withHTTPRemote(b, withTmpBoltStore(b, defaultConf(b, secret)), nil)

			b.Run(fmt.Sprintf("%dMiB", s/1024/1024), func(b *testing.B) {
				data := randb(s)
				keys := []libchunk.K{}
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

func benchmarkRemoteJoinToFile(b *testing.B, data []byte, conf libchunk.Config) {
	keys := &sliceKeyIterator{}
	store := NewMemoryStore()
	input := &randomBytesInput{bytes.NewReader(data)}
	err := libchunk.Split(input, keys, withStore(b, defaultConf(b, secret), store))
	if err != nil {
		b.Fatalf("couldnt split for test prep: %v", err)
	}

	conf = withStore(b, conf, nil)               //disable local store
	conf = withHTTPRemote(b, conf, store.chunks) //use chunks stored in memory

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outf, err := ioutil.TempFile("", "libchunk_")
		if err != nil {
			b.Fatal(err)
		}

		defer os.Remove(outf.Name())
		err = libchunk.Join(keys, outf, conf)
		outf.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
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

func benchmarkBoltRandomReadsJoinToFile(b *testing.B, keys []libchunk.K, data []byte, conf libchunk.Config) {
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
