package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/advanderveer/libchunk/iterator"
	"github.com/advanderveer/libchunk/remote"
	"github.com/advanderveer/libchunk/store"

	"github.com/restic/chunker"
	"github.com/smartystreets/go-aws-auth"
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

type failingWriter struct {
	*bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("writer_failure")
}

type failingKeyIterator struct {
	*bitsiterator.MemIterator
}

func (iter *failingKeyIterator) Next() (k libchunk.K, err error) {
	return k, fmt.Errorf("iterator_failure")
}

type failingStore struct{}

func (store *failingStore) Put(k libchunk.K, c []byte) error {
	return fmt.Errorf("storage_failed")
}

func (store *failingStore) Get(k libchunk.K) (c []byte, err error) {
	return c, fmt.Errorf("storage_failed")
}

type emptyStore struct{}

func (store *emptyStore) Put(k libchunk.K, c []byte) error {
	return nil
}

func (store *emptyStore) Get(k libchunk.K) (c []byte, err error) {
	return c, os.ErrNotExist
}

func defaultConf(t quiter, secret libchunk.Secret) libchunk.Config {
	block, err := aes.NewCipher(secret[:])
	if err != nil {
		t.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		t.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	return libchunk.Config{
		Secret:           secret,
		SplitBufSize:     chunker.MaxSize,
		SplitConcurrency: 64,
		PushConcurrency:  64,
		JoinConcurrency:  10,
		AEAD:             aead,
		KeyHash: func(b []byte) libchunk.K {
			return sha256.Sum256(b)
		},
	}
}

func withStore(t quiter, conf libchunk.Config, store libchunk.Store) libchunk.Config {
	conf.Store = store
	return conf
}

func withTmpBoltStore(t quiter, conf libchunk.Config) libchunk.Config {
	dbdir, err := ioutil.TempDir("", "libchunk_db_")
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

func withIndex(t quiter, conf libchunk.Config, index libchunk.KeyIndex) libchunk.Config {
	conf.Index = index
	return conf
}

func withRemote(t quiter, conf libchunk.Config, remote libchunk.Remote) libchunk.Config {
	conf.Remote = remote
	return conf
}

func withS3Remote(t quiter, conf libchunk.Config, chunks map[libchunk.K][]byte) libchunk.Config {
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
	io.Reader
}

func (input *randomBytesInput) Chunker(conf libchunk.Config) (libchunk.Chunker, error) {
	return chunker.New(input, conf.Secret.Pol()), nil
}

type failingChunkerInput struct {
	*bytes.Buffer
}

func (input *failingChunkerInput) Next([]byte) (c chunker.Chunk, err error) {
	return c, fmt.Errorf("chunking_failed")
}

func (input *failingChunkerInput) Chunker(conf libchunk.Config) (libchunk.Chunker, error) {
	return input, nil
}

type failingInput struct {
	*bytes.Buffer
}

func (input *failingInput) Next([]byte) (c chunker.Chunk, err error) {
	return c, nil
}

func (input *failingInput) Chunker(conf libchunk.Config) (libchunk.Chunker, error) {
	return input, fmt.Errorf("input_failed")
}

type failingKeyHandler struct{}

func (iter *failingKeyHandler) Handle(k libchunk.K) (err error) {
	return fmt.Errorf("handler_failed")
}
