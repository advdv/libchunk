package libchunk

import (
	"crypto/cipher"
	"encoding/base64"
	"encoding/binary"

	"github.com/restic/chunker"
)

//KeySize describes the size of each chunk ley
const KeySize = 32

//KeyFunc turns a arbitrary sized chunk into content-based key
type KeyFunc func([]byte) K

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

//K is the key of a single chunk it is both used to store each
//piece as well as to encrypt it
type K [32]byte

//Returns the key encoded such that it is human readable, with only
//ASCII characters but is yet space efficient.
func (k K) String() string {
	return base64.StdEncoding.EncodeToString(k[:])
}

//Configure Split, Join, Push and Fetch behaviour
type Config struct {
	Chunker          Chunker
	AEAD             cipher.AEAD
	Secret           Secret
	SplitBufSize     int64
	SplitConcurrency int
	KeyFunc          KeyFunc
	Store            Store
}
