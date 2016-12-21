package bits_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/chunker"
	"github.com/advanderveer/libchunk/bits/iterator"
	"github.com/advanderveer/libchunk/bits/remote"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/smartystreets/go-aws-auth"
)

type quiter interface {
	Fatalf(format string, args ...interface{})
}

var secret = bits.Secret{
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

type failingWriter struct {
	*bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("writer_failure")
}

type failingKeyIterator struct {
	*bitsiterator.MemIterator
}

func (iter *failingKeyIterator) Next() (k bits.K, err error) {
	return k, fmt.Errorf("iterator_failure")
}

type failingStore struct{}

func (store *failingStore) Put(k bits.K, c []byte) error {
	return fmt.Errorf("storage_failed")
}

func (store *failingStore) Get(k bits.K) (c []byte, err error) {
	return c, fmt.Errorf("storage_failed")
}

type emptyStore struct{}

func (store *emptyStore) Put(k bits.K, c []byte) error {
	return nil
}

func (store *emptyStore) Get(k bits.K) (c []byte, err error) {
	return c, os.ErrNotExist
}

func defaultConf(t quiter, secret bits.Secret) bits.Config {
	conf, err := bits.DefaultConf(secret)
	if err != nil {
		t.Fatalf("failed to get default conf: %v", err)
	}

	return conf
}

func withStore(t quiter, conf bits.Config, store bits.Store) bits.Config {
	conf.Store = store
	return conf
}

func withTmpBoltStore(t quiter, conf bits.Config) bits.Config {
	dbdir, err := ioutil.TempDir("", "bits_db_")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbpath := filepath.Join(dbdir, "db.bolt")
	store, err := bitsstore.NewBoltStore(dbpath)
	if err != nil {
		t.Fatalf("failed to setup bolt store: %v", err)
	}

	return withStore(t, conf, store)
}

func withIndex(t quiter, conf bits.Config, index bits.KeyIndex) bits.Config {
	conf.Index = index
	return conf
}

func withRemote(t quiter, conf bits.Config, remote bits.Remote) bits.Config {
	conf.Remote = remote
	return conf
}

func withS3Remote(t quiter, conf bits.Config, chunks map[bits.K][]byte) bits.Config {
	l, err := net.Listen("tcp", ":")
	if err != nil {
		t.Fatalf("failed to setup test server: %v", err)
	}

	go func() {
		store := bitsstore.NewMemStore()
		if chunks != nil {
			store.Chunks = chunks
		}

		t.Fatalf("failed to serve: %v", http.Serve(l, store))
	}()

	return withRemote(t, conf, bitsremote.NewS3Remote("http", l.Addr().String(), "", awsauth.Credentials{}))
}

type randomBytesInput struct {
	bits.InputChunker
}

func randBytesInput(r io.Reader, secret bits.Secret) *randomBytesInput {
	return &randomBytesInput{
		InputChunker: bitschunker.NewRabinChunker(r, secret.Pol()),
	}
}

// func (input *randomBytesInput) Chunker(conf bits.Config) (bits.Chunker, error) {
// 	return chunker.New(input, conf.Secret.Pol()), nil
// }

type failingChunker struct{}

func (input *failingChunker) Next() (c []byte, err error) {
	return c, fmt.Errorf("chunking_failed")
}

// type failingInput struct {
// 	*bytes.Buffer
// }
//
// func (input *failingInput) Next([]byte) (c chunker.Chunk, err error) {
// 	return c, fmt.Errorf("input_failed")
// }

type failingKeyHandler struct{}

func (iter *failingKeyHandler) Handle(k bits.K) (err error) {
	return fmt.Errorf("handler_failed")
}
