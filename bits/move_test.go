package bits_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/index"
	"github.com/advanderveer/libchunk/bits/keys"
)

func TestMove(t *testing.T) {
	conf := withTmpBoltStore(t, withS3Remote(t, defaultConf(t, secret), nil))
	cases := []struct {
		name  string
		input []byte
		keyrw interface {
			bits.KeyWriter
			bits.KeyReader
		}
		conf        bits.Config
		expectedErr string
	}{
		{
			"9MiB_random_defaultconf",
			randb(9 * 1024 * 1024),
			bitskeys.NewMemIterator(),
			conf,
			"",
		}, {
			"9MiB_random_defaultconf_index",
			randb(9 * 1024 * 1024),
			bitskeys.NewMemIterator(),
			withIndex(t, conf, bitsindex.NewMemIndex()),
			"",
		}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			krw := c.keyrw
			if c.input != nil {

				err := bits.Put(randBytesInput(bytes.NewBuffer(c.input), secret), krw, c.conf)
				if err != nil {
					t.Fatalf("failed to spit first: %v", err)
				}
			}

			firstH := &bitskeys.MemIterator{}
			err := bits.Move(krw, firstH, c.conf)
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

			if len(firstH.Keys) < 1 {
				t.Error("expected at least some keys to be pushed")
			}

			if c.conf.Index != nil {
				secondH := &bitskeys.MemIterator{}
				krw.Reset()
				err = bits.Move(krw, secondH, c.conf)
				if err != nil {
					t.Errorf("second (index test) push failed: %v", err)
				}

				if len(secondH.Keys) >= len(firstH.Keys) {
					t.Error("expected some keys to be skipped on second push")
				}
			}
		})
	}
}
