package libchunk_test

import (
	"bytes"
	"crypto/sha256"
	"os"
	"testing"

	"github.com/advanderveer/libchunk"
	"github.com/smartystreets/go-aws-auth"
)

func envRemote(t *testing.T) *libchunk.S3Remote {
	creds := awsauth.Credentials{
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}

	host := os.Getenv("AWS_S3_HOST")
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" || host == "" {
		t.Skip("skip s3 testing, 'AWS_ACCESS_KEY_ID' or 'AWS_SECRET_ACCESS_KEY' or 'AWS_S3_HOST' not set in environment")
		return nil
	}

	return libchunk.NewS3Remote("https", host, "tests", creds)
}

func TestActualS3PutGet(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped in short mode")
	}

	var remote libchunk.Remote
	remote = envRemote(t)

	input := randb(4 * 1024 * 1024)
	k := libchunk.K(sha256.Sum256(input))

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
}
