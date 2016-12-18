package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/restic/chunker"
)

type failingStore struct{}

func (store *failingStore) Put(k libchunk.K, c []byte) error {
	return fmt.Errorf("storage_failed")
}

func (store *failingStore) Get(k libchunk.K) (c []byte, err error) {
	return c, fmt.Errorf("storage_failed")
}

func defaultConfig(t *testing.T) libchunk.Config {
	dbdir, err := ioutil.TempDir("", "libchunk_db_")
	if err != nil {
		t.Fatal(err)
	}

	dbpath := filepath.Join(dbdir, "db.bolt")
	db, err := bolt.Open(dbpath, 0777, nil)
	if err != nil {
		t.Fatal(err)
	}

	block, err := aes.NewCipher(secret[:])
	if err != nil {
		t.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		t.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	conf := libchunk.Config{
		Secret:           secret,
		SplitBufSize:     chunker.MaxSize,
		SplitConcurrency: 64,
		AEAD:             aead,
		KeyFunc: func(b []byte) libchunk.K {
			return sha256.Sum256(b)
		},
		Store: &boltStore{db, []byte("chunks")},
	}

	return conf
}

func failingStorageConfig(t *testing.T) libchunk.Config {
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
		AEAD:             aead,
		KeyFunc: func(b []byte) libchunk.K {
			return sha256.Sum256(b)
		},
		Store: &failingStore{},
	}
}

type randomBytesInput struct {
	*bytes.Buffer
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

//
// Actual tests
//

//TestSplit tests splitting of data streams
func TestSplit(t *testing.T) {
	cases := []struct {
		name        string
		input       libchunk.Input
		conf        libchunk.Config
		minKeys     int
		expectedErr string
		keyHandler  libchunk.KeyHandler
	}{{
		"9MiB_random_default_conf", //chunker max size is 8Mib, so expect at least 2 chunks
		&randomBytesInput{bytes.NewBuffer(randb(9 * 1024 * 1024))},
		defaultConfig(t),
		2,
		"",
		nil,
	}, {
		"1MiB_random_storage_failed",
		&randomBytesInput{bytes.NewBuffer(randb(1024 * 1024))},
		failingStorageConfig(t),
		0,
		"storage_failed",
		nil,
	}, {
		"1MiB_random_chunker_failed",
		&failingChunkerInput{},
		defaultConfig(t),
		0,
		"chunking_failed",
		nil,
	}, {
		"1MiB_input_fails",
		&failingInput{},
		defaultConfig(t),
		0,
		"input_failed",
		nil,
	}, {
		"1MiB_handler_failed",
		&randomBytesInput{bytes.NewBuffer(randb(1024 * 1024))},
		defaultConfig(t),
		0,
		"handler_failed",
		func(k libchunk.K) error { return fmt.Errorf("handler_failed") },
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			keys := []libchunk.K{}
			var err error
			if c.keyHandler == nil {
				err = libchunk.Split(c.input, func(k libchunk.K) error {
					keys = append(keys, k)
					return nil
				}, c.conf)

				if len(keys) < c.minKeys {
					t.Errorf("expected at least '%d' keys, got: '%d'", c.minKeys, len(keys))
				}
			} else {
				err = libchunk.Split(c.input, c.keyHandler, c.conf)
			}

			if err != nil {
				if c.expectedErr == "" {
					t.Errorf("splitting shouldnt fail but got: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErr) {
					t.Errorf("expected an error that contains message '%s', got: %v", c.expectedErr, err)
				}
			} else if c.expectedErr != "" {
				t.Errorf("expected an error, got success")
			}
		})
	}
}
