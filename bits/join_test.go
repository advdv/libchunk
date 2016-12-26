package bits_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/store"

	"github.com/boltdb/bolt"
)

func TestJoinFromRemote(t *testing.T) {
	data := randb(9 * 1024 * 1024)
	keys := bitskeys.NewMemIterator()
	store := bitsstore.NewMemStore()
	input := randBytesInput(bytes.NewReader(data), secret)
	err := bits.Split(input, keys, withStore(t, defaultConf(t, secret), store))
	if err != nil {
		t.Fatalf("couldnt split for test prep: %v", err)
	}

	conf := withS3Remote(t, defaultConf(t, secret), store.Chunks)
	cases := []struct {
		name string
		conf bits.Config
		kr   bits.KeyReader
	}{{
		"9MiB_from_remote",
		conf,
		keys,
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			err := bits.Join(c.kr, buf, c.conf)
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
		keyrw  interface {
			bits.KeyWriter
			bits.KeyReader
		}
		corrupt     func(bits.K, bits.Config)
		conf        bits.Config
		expectedErr string
	}{{
		"no_keys_provided",
		nil,
		nil,
		bitskeys.NewMemIterator(),
		nil,
		conf,
		"",
	}, {
		"key_not_in_db",
		nil,
		nil,
		bitskeys.NewPopulatedMemIterator([]bits.K{bits.K([32]byte{})}),
		nil,
		conf,
		"no such key",
	}, {
		"storage_failure",
		nil,
		nil,
		bitskeys.NewPopulatedMemIterator([]bits.K{bits.K([32]byte{})}),
		nil,
		withStore(t, defaultConf(t, secret), &failingStore{}),
		"storage_failed",
	}, {
		"keys_fails",
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
		bitskeys.NewMemIterator(),
		nil,
		conf,
		"",
	}, {
		"9MiB_fail_to_write_output",
		randb(9 * 1024 * 1024),
		&failingWriter{bytes.NewBuffer(nil)},
		bitskeys.NewMemIterator(),
		nil,
		conf,
		"writer_failure",
	}, {
		"chunk_corrupted",
		randb(9 * 1024 * 1024),
		nil,
		bitskeys.NewMemIterator(),
		func(k bits.K, conf bits.Config) {
			dst, err := conf.Stores.PutDst() //get the store used for putting chunks
			if err != nil {
				t.Fatal(err)
			}

			switch store := dst.(type) {
			case *bitsstore.BoltStore:
				err := store.DB.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(bitsstore.BoltChunkBucket).Put(k[:], []byte{0x00})
				})

				if err != nil {
					t.Errorf("failed to corrupt store: %v", err)
				}
			default:
				t.Fatalf("cant corrupt '%T'", dst)
			}
		},
		conf,
		"authentication failed",
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			krw := c.keyrw
			if c.input != nil {
				err := bits.Split(randBytesInput(bytes.NewBuffer(c.input), secret), krw, c.conf)
				if err != nil {
					t.Fatalf("failed to spit first: %v", err)
				}
			}

			if c.corrupt != nil {
				k, err := krw.Read()
				if err != nil {
					t.Fatal("instructed to corrupt a key, but no keys available")
				}

				krw.Reset()
				c.corrupt(k, c.conf)
			}

			output := c.output
			if output == nil {
				output = bytes.NewBuffer(nil)
			}

			err := bits.Join(krw, output, c.conf)
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
