package bits

import (
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/restic/chunker"
)

//KeySize describes the size of each chunk ley
const KeySize = 32

var (
	//ErrNoSuchKey is returned when a given key could not be found
	ErrNoSuchKey = errors.New("no such key")
)

//KeyHandler is called when a key is outputted
type KeyHandler interface {
	Handle(k K) error
}

//KeyIterator will return keys while calling
type KeyIterator interface {
	Reset()
	Next() (K, error)
}

//Chunker allows users to read a chunks of data at a time
type Chunker interface {
	Next(data []byte) (chunker.Chunk, error)
}

//KeyIndex holds information about just the chunk keys
type KeyIndex interface {
	KeyHandler
	Has(k K) bool
}

//Remote stores chunks remotely but should provide an indexing mechanism
//that allow clients to skip PUT calls altogether
type Remote interface {
	Index(KeyHandler) error

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

//Secret is the 32 byte key that scopes the deduplication
//and facilitates end-to-end encryption
type Secret [32]byte

//Pol returns the first 8 bytes of the secret as a polynomial that
//can be used for CBC
func (s Secret) Pol() (p chunker.Pol) {
	i, _ := binary.Uvarint(s[:8])
	return chunker.Pol(i)
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

//Input is a reader that can determine how it will be chunked
type Input interface {
	io.Reader
	Chunker(conf Config) (Chunker, error)
}

//Config describes how the library's Split, Join and Push behaves
type Config struct {
	AEAD             cipher.AEAD
	Secret           Secret
	SplitBufSize     int64
	SplitConcurrency int
	PushConcurrency  int
	JoinConcurrency  int
	KeyHash          KeyHash

	Store  Store
	Remote Remote
	Index  KeyIndex
}
