package libchunk_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
	"github.com/restic/chunker"
)

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

func TestSplit(t *testing.T) {
	cases := []struct {
		name    string
		input   *bytes.Buffer
		conf    libchunk.Config
		minKeys int
	}{{
		"9MiB_random_default_conf", //chunker max size is 8Mib, so expect at least 2 chunks
		bytes.NewBuffer(randb(9 * 1024 * 1024)),
		defaultConfig(t),
		2,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			c.conf.Chunker = chunker.New(c.input, c.conf.Secret.Pol())

			keys := []libchunk.K{}
			err := libchunk.Split(c.input, func(k libchunk.K) error {
				keys = append(keys, k)
				return nil
			}, c.conf)

			if len(keys) <= c.minKeys {
				t.Errorf("expected more then '%d' keys, got: '%d'", c.minKeys, len(keys))
			}

			if err != nil {
				t.Errorf("splitting shouldnt fail but got: %v", err)
			}
		})
	}

	// input := bytes.NewReader(randb(4 * 1024 * 1024)) //4MiB random bytes
	// conf := defaultConfig(t)
	// conf.Chunker = chunker.New(input, secret.Pol())
	//
	//
	// for
	//
	// keys := []libchunk.K{}
	// err := libchunk.Split(input, func(k libchunk.K) error {
	// 	keys = append(keys, k)
	// 	return nil
	// }, conf)
	//
	// if err != nil {
	// 	t.Errorf("splitting shouldnt fail but got: %v", err)
	// }

}

//
// func TestSplitHappyPath(t *testing.T) {
//
// }
//
// func TestStorageFailure(t *testing.T) {
//
// }
