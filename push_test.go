package libchunk_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/advanderveer/libchunk/iterator"
)

func TestPush(t *testing.T) {
	conf := withTmpBoltStore(t, withS3Remote(t, defaultConf(t, secret), nil))
	cases := []struct {
		name  string
		input []byte
		iter  interface {
			libchunk.KeyHandler
			libchunk.KeyIterator
		}
		conf        libchunk.Config
		expectedErr string
	}{
		{
			"9MiB_random_defaultconf",
			randb(9 * 1024 * 1024),
			bitsiterator.NewMemIterator(),
			conf,
			"",
		}, {
			"9MiB_random_defaultconf_index",
			randb(9 * 1024 * 1024),
			bitsiterator.NewMemIterator(),
			withIndex(t, conf, bitsiterator.NewMemIterator()),
			"",
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

			if c.conf.Index != nil {
				iter.Reset()
				err = libchunk.Push(iter, c.conf)
				if err != nil {
					t.Errorf("second (index test) push failed: %v", err)
				}

				//@TODO measure&test how much was actually put?
			}
		})
	}
}
