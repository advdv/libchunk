package libchunk

import (
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"

	"github.com/restic/chunker"
)

//KeySize describes the size of each chunk ley
const KeySize = 32

var (
	//ErrNoSuchKey is returned when a given key could not be found
	ErrNoSuchKey = errors.New("no such key")
)

//KeyPutter is used when a key itself needs to be received
type KeyPutter interface {
	Put(k K) error
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

//Store holds chunks
type Store interface {

	//will do nothing if exists, must be atomic
	Put(k K, chunk []byte) error

	//returns os.NotExist if the chunk doesnt exist
	Get(k K) (chunk []byte, err error)
}

//Secret is the 32 byte key that scopes the deduplication
//and facilitates end-to-end encryption
type Secret [32]byte

//return the first 8 bytes of the secret as a polynomial that
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
	return base64.StdEncoding.EncodeToString(k[:])
}

//Input is a reader that can determine how it will be chunked
type Input interface {
	io.Reader
	Chunker(conf Config) (Chunker, error)
}

//Configure Split, Join, Push and Fetch behaviour
type Config struct {
	AEAD             cipher.AEAD
	Secret           Secret
	SplitBufSize     int64
	SplitConcurrency int
	KeyHash          KeyHash
	Store            Store
	RemoteHost       string
	RemoteScheme     string
}
