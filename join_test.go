package libchunk_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/advanderveer/libchunk/iterator"
	"github.com/advanderveer/libchunk/store"

	"github.com/boltdb/bolt"
)

func TestJoinFromRemote(t *testing.T) {
	data := randb(9 * 1024 * 1024)
	keys := bitsiterator.NewMemIterator()
	store := bitsstore.NewMemStore()
	input := &randomBytesInput{bytes.NewReader(data)}
	err := libchunk.Split(input, keys, withStore(t, defaultConf(t, secret), store))
	if err != nil {
		t.Fatalf("couldnt split for test prep: %v", err)
	}

	conf := withS3Remote(t, defaultConf(t, secret), store.Chunks)
	cases := []struct {
		name string
		conf libchunk.Config
		iter libchunk.KeyIterator
	}{{
		"9MiB_from_remote",
		conf,
		keys,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			err := libchunk.Join(c.iter, buf, c.conf)
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(buf.Bytes(), data) {
				t.Fatalf("expected joined output from remote to be equal to input, input len %d output len %d", len(data), buf.Len())
			}
		})
	}
}

//TestJoinFromLocal tests splitting of data streams
func TestJoinFromLocal(t *testing.T) {
	conf := withTmpBoltStore(t, defaultConf(t, secret))

	cases := []struct {
		name   string
		input  []byte
		output io.ReadWriter
		iter   interface {
			libchunk.KeyHandler
			libchunk.KeyIterator
		}
		corrupt     func(libchunk.K, libchunk.Config)
		conf        libchunk.Config
		expectedErr string
	}{{
		"no_keys_provided",
		nil,
		nil,
		bitsiterator.NewMemIterator(),
		nil,
		conf,
		"",
	}, {
		"key_not_in_db",
		nil,
		nil,
		bitsiterator.NewPopulatedMemIterator([]libchunk.K{libchunk.K([32]byte{})}),
		nil,
		conf,
		"no such key",
	}, {
		"storage_failure",
		nil,
		nil,
		bitsiterator.NewPopulatedMemIterator([]libchunk.K{libchunk.K([32]byte{})}),
		nil,
		withStore(t, defaultConf(t, secret), &failingStore{}),
		"storage_failed",
	}, {
		"iterator_fails",
		nil,
		nil,
		&failingKeyIterator{},
		nil,
		conf,
		"iterator_failure",
	}, {
		"9MiB_random_defaultconf",
		randb(9 * 1024 * 1024),
		nil,
		bitsiterator.NewMemIterator(),
		nil,
		conf,
		"",
	}, {
		"9MiB_fail_to_write_output",
		randb(9 * 1024 * 1024),
		&failingWriter{bytes.NewBuffer(nil)},
		bitsiterator.NewMemIterator(),
		nil,
		conf,
		"writer_failure",
	}, {
		"chunk_corrupted",
		randb(9 * 1024 * 1024),
		nil,
		bitsiterator.NewMemIterator(),
		func(k libchunk.K, conf libchunk.Config) {
			switch store := conf.Store.(type) {
			case *bitsstore.BoltStore:
				err := store.DB.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(bitsstore.BoltChunkBucket).Put(k[:], []byte{0x00})
				})

				if err != nil {
					t.Errorf("failed to corrupt store: %v", err)
				}
			default:
				t.Fatalf("cant corrupt '%T'", conf.Store)
			}
		},
		conf,
		"authentication failed",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			iter := c.iter
			if c.input != nil {
				err := libchunk.Split(&randomBytesInput{bytes.NewBuffer(c.input)}, iter, c.conf)
				if err != nil {
					t.Fatalf("failed to spit first: %v", err)
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

			err := libchunk.Join(iter, output, c.conf)
			if err != nil {
				if c.expectedErr == "" {
					t.Errorf("joining shouldnt fail but got: %v", err)
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
					t.Fatalf("failed to read to compare output: %v", err)
				}

				if !bytes.Equal(c.input, outdata) {
					t.Errorf("expected merge output to equal split input, input len: %d, output len: %d", len(c.input), len(outdata))
				}
			}
		})
	}

}
