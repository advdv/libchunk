package libchunk_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"

	"github.com/advanderveer/libchunk"
	"github.com/boltdb/bolt"
)

var secret = libchunk.Secret{
	0x3D, 0xA3, 0x35, 0x8B, 0x4D, 0xC1, 0x73, 0x00, //polynomial
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, //random bytes
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

func tempDB(t *testing.T) (db *bolt.DB) {
	dbdir, err := ioutil.TempDir("", "libchunk_")
	if err != nil {
		t.Fatalf("failed to create temp dir for db: %v", err)
	}

	dbpath := filepath.Join(dbdir, "db.bolt")
	db, err = bolt.Open(dbpath, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		t.Errorf("failed to open chunks database '%s': %v", dbpath, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(libchunk.BucketNameChunks)
		if err != nil {
			return fmt.Errorf("failed to create bucket: %s", err)
		}
		return nil
	})

	if err != nil {
		t.Fatalf("failed to create buckets: %v", err)
	}

	return db
}

func TestSplitJoinSmallNonRandom(t *testing.T) {
	db := tempDB(t)
	input := []byte("foo bar") //@TODO find somall content that generates multiple chunks
	output := bytes.NewBuffer(nil)

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		err := libchunk.Split(db, secret, bytes.NewBuffer(input), pw)
		if err != nil {
			t.Fatalf("failed to split: %v", err)
		}
	}()

	err := libchunk.Join(db, secret, pr, output)
	if err != nil {
		t.Fatalf("failed to join: %v", err)
	}

	if !bytes.Equal(input, output.Bytes()) {
		t.Errorf("expected joined output (len %d) to be the same as input (len %d)", output.Len(), len(input))
	}

	//assert encryption at rest
	//assert throughput

	//assert:
	//  - assert writer output
	//  - different polynomials generate different
	//    - key
	//    - encrypted content
	//  - different secret parts generate random noise
	//  -assert multiple chunks being outputed

	//assert db file size
	//assert buffer content
	//assert chunks in db
	//assert deduplication
	//assert encryption

}
