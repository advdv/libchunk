package libchunk_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
)

//
// Actual tests
//

//TestMerge tests splitting of data streams
func TestMerge(t *testing.T) {
	cases := []struct {
		name  string
		input []byte
		iter  interface {
			libchunk.KeyPutter
			libchunk.KeyIterator
		}
		conf        libchunk.Config
		expectedErr string
	}{{
		"no_keys_provided",
		nil,
		&sliceKeyIterator{0, []libchunk.K{}},
		defaultConfig(t),
		"",
	}, {
		"key_not_in_db",
		nil,
		&sliceKeyIterator{0, []libchunk.K{libchunk.K([32]byte{})}},
		defaultConfig(t),
		"no such key",
	}, {
		"storage_failure",
		nil,
		&sliceKeyIterator{0, []libchunk.K{libchunk.K([32]byte{})}},
		failingStorageConfig(t),
		"storage_failed",
	}, {
		"9MiB_random_defaultconf",
		randb(9 * 1024 * 1024),
		&sliceKeyIterator{0, []libchunk.K{}},
		defaultConfig(t),
		"",
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

			output := bytes.NewBuffer(nil)
			err := libchunk.Merge(iter, output, c.conf)
			if err != nil {
				if c.expectedErr == "" {
					t.Errorf("splitting shouldnt fail but got: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErr) {
					t.Errorf("expected an error that contains message '%s', got: %v", c.expectedErr, err)
				}
			} else if c.expectedErr != "" {
				t.Errorf("expected an error, got success")
			}

			if c.input != nil {
				if !bytes.Equal(c.input, output.Bytes()) {
					t.Errorf("expected merge output to equal split input, input len: %d, output len: %d", c.input, output.Len())
				}
			}
		})
	}
}
