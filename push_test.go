package libchunk_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
)

func TestPush(t *testing.T) {
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
		"9MiB_random_defaultconf",
		randb(9 * 1024 * 1024),
		&sliceKeyIterator{0, []libchunk.K{}},
		defaultConfigWithRemote(t, nil),
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

			err := libchunk.Push(iter, c.conf)
			if err != nil {
				if c.expectedErr == "" {
					t.Errorf("pushing shouldnt fail but got: %v", err)
				} else if !strings.Contains(err.Error(), c.expectedErr) {
					t.Errorf("expected an error that contains message '%s', got: %v", c.expectedErr, err)
				}

				return
			} else if c.expectedErr != "" {
				t.Errorf("expected an error, got success")
			}
		})
	}
}
