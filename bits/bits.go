package bits

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
)

const (
	//KeySize describes the byte size of each chunk key
	KeySize = 32
)

var (
	//ErrNoSuchKey is returned when a given key could not be found
	ErrNoSuchKey = errors.New("no such key")
)

//KeyWriter is called when a key is outputted
type KeyWriter interface {
	Write(k K) error
}

//KeyReader will return keys while calling
type KeyReader interface {
	Reset()
	Read() (K, error)
}

//ChunkReader allows reading one piece of input at a time
type ChunkReader interface {
	Read() ([]byte, error)
}

//ChunkWriter accepts chunks for writing
type ChunkWriter interface {
	io.Writer
}

//KeyIndex holds information about just the chunk keys
type KeyIndex interface {
	KeyWriter
	Has(k K) bool
}

//Remote stores chunks remotely but should provide an indexing mechanism
//that allow clients to skip PUT calls altogether
type Remote interface {
	Index(KeyWriter) error
	Store //but it is still a store
}

//Store holds chunks locally and throught is expected to be fast enough
//to not require a mechanism for skipping put calls
type Store interface {

	//will do nothing if exists, must be atomic
	Put(k K, chunk []byte) error

	//returns os.NotExist if the chunk doesnt exist
	Get(k K) (chunk []byte, err error)
}

//KeyHash turns a arbitrary sized chunk into content-based key
type KeyHash func([]byte) K

//K is the key of a single chunk it is both used to store each
//piece as well as to encrypt it
type K [32]byte

//Returns the key encoded such that it is human readable, with only
//ASCII characters but is yet space efficient.
func (k K) String() string {
	return base64.URLEncoding.EncodeToString(k[:])
}

//DecodeKey attempts to read a key from a byteslice
func DecodeKey(b []byte) (k K, err error) {
	buf := make([]byte, base64.URLEncoding.DecodedLen(len(b)))
	n, err := base64.URLEncoding.Decode(buf, b)
	if err != nil {
		return k, fmt.Errorf("failed to decode '%s' as key: %v", b, err)
	}

	if n != KeySize {
		return k, fmt.Errorf("decoded incorrect length, expected %d got %d", KeySize, n)
	}

	copy(k[:], buf)
	return k, nil
}

//Config describes how the library's Split, Join and Push behaves
type Config struct {
	Secret  Secret
	AEAD    cipher.AEAD
	KeyHash KeyHash

	SplitConcurrency int
	PushConcurrency  int
	JoinConcurrency  int

	Store  Store
	Remote Remote
	Index  KeyIndex
}

//DefaultConf sets up sensible configs
func DefaultConf(secret Secret) (conf Config, err error) {
	block, err := aes.NewCipher(secret[:])
	if err != nil {
		return conf, fmt.Errorf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCMWithNonceSize(block, sha256.Size)
	if err != nil {
		return conf, fmt.Errorf("failed to setup GCM cipher mode: %v", err)
	}

	return Config{
		Secret:           secret,
		SplitConcurrency: 64,
		PushConcurrency:  64,
		JoinConcurrency:  10,
		AEAD:             aead,
		KeyHash: func(b []byte) K {
			return sha256.Sum256(b)
		},
	}, nil
}
