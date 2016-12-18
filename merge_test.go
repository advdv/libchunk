package libchunk_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
)

type failingWriter struct {
	*bytes.Buffer
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("writer_failure")
}

type failingKeyIterator struct {
	*sliceKeyIterator
}

func (iter *failingKeyIterator) Next() (k libchunk.K, err error) {
	return k, fmt.Errorf("iterator_failure")
}

//
// Actual tests
//

//TestMerge tests splitting of data streams
func TestMerge(t *testing.T) {
	cases := []struct {
		name   string
		input  []byte
		output io.ReadWriter
		iter   interface {
			libchunk.KeyPutter
			libchunk.KeyIterator
		}
		corrupt     func(libchunk.K, libchunk.Config)
		conf        libchunk.Config
		expectedErr string
	}{{
		"no_keys_provided",
		nil,
		nil,
		&sliceKeyIterator{0, []libchunk.K{}},
		nil,
		defaultConfig(t),
		"",
	}, {
		"key_not_in_db",
		nil,
		nil,
		&sliceKeyIterator{0, []libchunk.K{libchunk.K([32]byte{})}},
		nil,
		defaultConfig(t),
		"no such key",
	}, {
		"storage_failure",
		nil,
		nil,
		&sliceKeyIterator{0, []libchunk.K{libchunk.K([32]byte{})}},
		nil,
		failingStorageConfig(t),
		"storage_failed",
	}, {
		"iterator_fails",
		nil,
		nil,
		&failingKeyIterator{},
		nil,
		defaultConfig(t),
		"iterator_failure",
	}, {
		"9MiB_random_defaultconf",
		randb(9 * 1024 * 1024),
		nil,
		&sliceKeyIterator{0, []libchunk.K{}},
		nil,
		defaultConfig(t),
		"",
	}, {
		"9MiB_fail_to_write_output",
		randb(9 * 1024 * 1024),
		&failingWriter{bytes.NewBuffer(nil)},
		&sliceKeyIterator{0, []libchunk.K{}},
		nil,
		defaultConfig(t),
		"writer_failure",
	}, {
		"chunk_corrupted",
		randb(9 * 1024 * 1024),
		nil,
		&sliceKeyIterator{0, []libchunk.K{}},
		func(k libchunk.K, conf libchunk.Config) {
			switch store := conf.Store.(type) {
			case *boltStore:
				err := store.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(store.bucketName).Put(k[:], []byte{0x00})
				})

				if err != nil {
					t.Error("failed to corrupt store: %v", err)
				}
			default:
				t.Fatal("cant corrupt '%T'", conf.Store)
			}
		},
		defaultConfig(t),
		"authentication failed",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			iter := c.iter
			if c.input != nil {
				err := libchunk.Split(&randomBytesInput{bytes.NewBuffer(c.input)}, iter, c.conf)
				if err != nil {
					t.Fatal("failed to spit first: %v", err)
				}
			}

			if c.corrupt != nil {
				k, err := iter.Next()
				if err != nil {
					t.Fatal("instructed to corrupt a key, but no keys available")
				}

				iter.Reset()
				c.corrupt(k, c.conf)
			}

			output := c.output
			if output == nil {
				output = bytes.NewBuffer(nil)
			}

			err := libchunk.Merge(iter, output, c.conf)
			if err != nil {
				if c.expectedErr == "" {
					t.Errorf("splitting shouldnt fail but got: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErr) {
					t.Errorf("expected an error that contains message '%s', got: %v", c.expectedErr, err)
				}

				return
			} else if c.expectedErr != "" {
				t.Errorf("expected an error, got success")
			}

			if c.input != nil && c.corrupt == nil && output != nil {
				outdata, err := ioutil.ReadAll(output)
				if err != nil {
					t.Fatal("failed to read to compare output: %v", err)
				}

				if !bytes.Equal(c.input, outdata) {
					t.Errorf("expected merge output to equal split input, input len: %d, output len: %d", len(c.input), len(outdata))
				}
			}
		})
	}

}
