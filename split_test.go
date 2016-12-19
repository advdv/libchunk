package libchunk_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
)

//TestSplit tests splitting of data streams
func TestSplit(t *testing.T) {
	conf := withTmpBoltStore(t, defaultConf(t, secret))
	cases := []struct {
		name        string
		input       libchunk.Input
		conf        libchunk.Config
		minKeys     int
		expectedErr string
		keyPutter   libchunk.KeyHandler
	}{{
		"9MiB_random_default_conf", //chunker max size is 8Mib, so expect at least 2 chunks
		&randomBytesInput{bytes.NewBuffer(randb(9 * 1024 * 1024))},
		conf,
		2,
		"",
		nil,
	}, {
		"1MiB_random_storage_failed",
		&randomBytesInput{bytes.NewBuffer(randb(1024 * 1024))},
		withStore(t, defaultConf(t, secret), &failingStore{}),
		0,
		"storage_failed",
		nil,
	}, {
		"1MiB_random_chunker_failed",
		&failingChunkerInput{},
		conf,
		0,
		"chunking_failed",
		nil,
	}, {
		"1MiB_input_fails",
		&failingInput{},
		conf,
		0,
		"input_failed",
		nil,
	}, {
		"1MiB_handler_failed",
		&randomBytesInput{bytes.NewBuffer(randb(1024 * 1024))},
		conf,
		0,
		"handler_failed",
		&failingKeyHandler{},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {

			keys := []libchunk.K{}
			var err error
			if c.keyPutter == nil {
				h := &sliceKeyIterator{}
				err = libchunk.Split(c.input, h, c.conf)
				keys = h.Keys

				if len(keys) < c.minKeys {
					t.Errorf("expected at least '%d' keys, got: '%d'", c.minKeys, len(keys))
				}
			} else {
				err = libchunk.Split(c.input, c.keyPutter, c.conf)
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
