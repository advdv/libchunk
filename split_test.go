package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"errors"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/restic/chunker"
)

var (
	expectedTestFailure = errors.New("expected")
)

type failingStore struct{}

func (store *failingStore) Put(k libchunk.K, c []byte) error {
	return expectedTestFailure
}

func (store *failingStore) Get(k libchunk.K) (c []byte, err error) {
	return c, expectedTestFailure
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

//TestSplit tests splitting of data streams
func TestSplit(t *testing.T) {
	cases := []struct {
		name        string
		input       *bytes.Buffer
		conf        libchunk.Config
		minKeys     int
		expectedErr string
	}{{
		"9MiB_random_default_conf", //chunker max size is 8Mib, so expect at least 2 chunks
		bytes.NewBuffer(randb(9 * 1024 * 1024)),
		defaultConfig(t),
		2,
		"",
	}, {
		"1MiB_random_storage_failed", //chunker max size is 8Mib, so expect at least 2 chunks
		bytes.NewBuffer(randb(1024 * 1024)),
		failingStorageConfig(t),
		0,
		"expected",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			//chunker is chosen based on input
			c.conf.Chunker = chunker.New(c.input, c.conf.Secret.Pol())

			keys := []libchunk.K{}
			err := libchunk.Split(c.input, func(k libchunk.K) error {
				keys = append(keys, k)
				return nil
			}, c.conf)

			if len(keys) < c.minKeys {
				t.Errorf("expected at least '%d' keys, got: '%d'", c.minKeys, len(keys))
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
