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

//Store holds chunks and is expected to have such a low latency that
//checking existence before put call is economic per key
type Store interface {

	//will do nothing if exists, must be atomic
	Put(k K, chunk []byte) error

	//returns os.NotExist if the chunk doesnt exist
	Get(k K) (chunk []byte, err error)
}

//RemoteStore stores chunks at a distant location such that an indexing
//mechanism is economic to prevent movement of chunks that are already present
type RemoteStore interface {
	Index(KeyWriter) error
	Store
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

//StoreMap hols our store configurations and allows retrieval
//of stores for various purposes thoughout our code
type StoreMap map[string]Store

//GetSrcs returns an ordered list of stores for getting chunks
//for the current store configuration. this can be a default or
//explictely overwitten by the user
//@TODO store by 'localness' or 'likelyhood of having chunks'
func (sm StoreMap) GetSrcs() (stores []Store) {
	for _, s := range sm {
		stores = append(stores, s)
	}
	return stores
}

//PutDst returns a store that can be used for putting chunks
//in the current store configuration, this can either be the default
//or overwitten by user configuration
func (sm StoreMap) PutDst() (store Store, err error) {
	if s, ok := sm["local"]; ok {
		return s, nil
	}

	return nil, fmt.Errorf("couldnt get store")
}

//MoveSrc returns a store from which chunks will be moved from
//in the current store configuration. this can either be the default
//or the store overwritten by user configuration
func (sm StoreMap) MoveSrc() (store Store, err error) {
	return sm.PutDst() //@TODO is this always the dst of the put
}

//MoveDst returns a store to which chunks will be moved for the
//current store configration. this can be the default or overwitten
//by user preference
func (sm StoreMap) MoveDst() (store Store, err error) {
	if s, ok := sm["remote"]; ok {
		return s, nil
	}

	return nil, fmt.Errorf("couldnt get store")
}

//Config describes how the library's Split, Join and Push behaves
type Config struct {
	Secret  Secret
	AEAD    cipher.AEAD
	KeyHash KeyHash

	PutConcurrency  int
	MoveConcurrency int
	GetConcurrency  int

	Stores StoreMap

	Index KeyIndex
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
		Secret:          secret,
		PutConcurrency:  64,
		MoveConcurrency: 64,
		GetConcurrency:  10,
		AEAD:            aead,
		Stores:          StoreMap{},
		KeyHash: func(b []byte) K {
			return sha256.Sum256(b)
		},
	}, nil
}
