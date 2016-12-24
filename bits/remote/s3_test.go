package bitsremote_test

import (
	"bytes"
	"crypto/sha256"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/keys"
	"github.com/advanderveer/libchunk/bits/remote"

	"github.com/smartystreets/go-aws-auth"
)

func randr(size int64) io.Reader {
	return io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), size)
}

func randb(size int64) []byte {
	b, err := ioutil.ReadAll(randr(size))
	if err != nil {
		panic(err)
	}

	return b
}

func envRemote(t *testing.T) *bitsremote.S3Remote {
	creds := awsauth.Credentials{
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}

	host := os.Getenv("AWS_S3_HOST")
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" || host == "" {
		t.Skip("skip s3 testing, 'AWS_ACCESS_KEY_ID' or 'AWS_SECRET_ACCESS_KEY' or 'AWS_S3_HOST' not set in environment")
		return nil
	}

	return bitsremote.NewS3Remote("https", host, "tests", creds.AccessKeyID, creds.SecretAccessKey)
}

func TestActualS3PutGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	var remote bits.Remote
	remote = envRemote(t)

	input := randb(4 * 1024 * 1024)
	k := bits.K(sha256.Sum256(input))

	err := remote.Put(k, input)
	if err != nil {
		t.Fatalf("failed to put chunk '%s': %v", k, err)
	}

	output, err := remote.Get(k)
	if err != nil {
		t.Fatalf("failed to get chunk '%s': %v", k, err)
	}

	if !bytes.Equal(output, input) {
		t.Fatal("expected input and output to be the same")
	}

	iter := bitskeys.NewMemIterator()
	err = remote.Index(iter)
	if err != nil {
		t.Fatalf("key indexing of remote shouldnt fail: %v", err)
	}

	if len(iter.Keys) < 1 {
		t.Fatal("should return al least one key from indexing")
	}
}
