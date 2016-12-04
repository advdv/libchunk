package libchunk

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/boltdb/bolt"
	"github.com/restic/chunker"
)

//KeySize describes the size of each chunk ley
const KeySize = 32

var (
	//BucketNameChunks is the name of the bolt bucket that holds chunks
	BucketNameChunks = []byte("chunks")
)

var (
	//ErrNotLocallyStored indicates the requested chunk could not be found locally
	ErrNotLocallyStored = fmt.Errorf("chunk is not stored locally")
)

//Secret is the 32 byte key that scopes the deduplication
//and facilitates end-to-end encryption
type Secret [32]byte

//K is the key of a single chunk it is both used to store each
//piece as well as to encryp it
type K [32]byte

//Returns the key encoded such that it is human readable, with only
//ASCII characters but is yet space efficient.
func (k K) String() string {
	return base64.StdEncoding.EncodeToString(k[:])
}

//DecodeKey attempts to decodes chunk key by reading byte slice 'b'
func DecodeKey(b []byte) (k K, err error) {
	data := make([]byte, base64.StdEncoding.DecodedLen(len(b)))
	_, err = base64.StdEncoding.Decode(data, b)
	if err != nil {
		return k, fmt.Errorf("failed to decode '%x' as base64: %v", b, err)
	}

	//check key length
	k = K{}
	if len(data) > len(k)+1 {
		return k, fmt.Errorf("decoded chunk key '%x' has an invalid length %d, expected %d (or %d+1)", data, len(data), len(k), len(k))
	}

	//fill K and hand it over
	copy(k[:], data[:KeySize])
	return k, nil
}

//return the first 8 bytes of the secret as a polynomial that
//can be used for CBC
func (s Secret) Pol() (p chunker.Pol) {
	i, _ := binary.Uvarint(s[:8])
	return chunker.Pol(i)
}

//ForEach is a convenient method for running logic for each chunk
//key in stream 'r', it will skip the chunk header and footer
func ForEach(r io.Reader, fn func(K) error) error {
	s := bufio.NewScanner(r)
	for s.Scan() {
		k, err := DecodeKey(s.Bytes())
		if err != nil {
			return fmt.Errorf("failed to decode chunk key: %v", err)
		}

		err = fn(k)
		if err != nil {
			return fmt.Errorf("failed to handle key '%s': %v", k, err)
		}
	}

	if err := s.Err(); err != nil {
		return fmt.Errorf("failed to scan chunk keys: %v", err)
	}

	return nil
}

//Join reads a stream of keys from reader 'r' and reads each chunk from
//the database. Each chunk is decrypted and written to writer 'w' in
//in order to create the original file
func Join(db *bolt.DB, s Secret, r io.Reader, w io.Writer) (err error) {
	return ForEach(r, func(k K) error {
		var chunk []byte
		err = db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket(BucketNameChunks)
			val := b.Get(k[:])
			if val == nil {
				return ErrNotLocallyStored
			}

			chunk = make([]byte, len(val))
			copy(chunk, val)
			return nil
		})

		if err != nil {
			if err == ErrNotLocallyStored {
				return fmt.Errorf("%v, fetching from remotes is not yet implemented", err)
			}

			return fmt.Errorf("failed to read chunk '%s': %v", k, err)
		}

		block, err := aes.NewCipher(s[:])
		if err != nil {
			return fmt.Errorf("failed to create AES block cipher: %v", err)
		}

		gcm, err := cipher.NewGCMWithNonceSize(block, KeySize)
		if err != nil {
			return fmt.Errorf("failed to setup GCM cipher mode: %v", err)
		}

		plaintext, err := gcm.Open(chunk[:0], k[:], chunk, nil)
		if err != nil {
			return fmt.Errorf("failed to decrypt chunk '%x': %v", k, err)
		}

		_, err = w.Write(plaintext)
		if err != nil {
			return fmt.Errorf("failed to write plain text chunk '%x' to output: %v", k, err)
		}

		return nil
	})
}

//Split reads a stream of bytes on reader 'r' and create chunks of
//variable size that are stored in the database with the sha2 hash
//as its key
func Split(db *bolt.DB, s Secret, r io.Reader, w io.Writer) (err error) {
	chunker := chunker.New(r, s.Pol())
	buf := make([]byte, chunker.MaxSize)
	for {
		chunk, err := chunker.Next(buf)
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("chunking failed: %v", err)
		}

		//Usage is based on minimal research
		//a: https://www.youtube.com/watch?v=2r_KMzXB74w
		//b: https://www.youtube.com/watch?v=H8nA_ZZxaMU
		//c: https://leanpub.com/gocrypto/read#leanpub-auto-aes-gcm

		//our block cipher uses the shared key, the nonce
		block, err := aes.NewCipher(s[:])
		if err != nil {
			return fmt.Errorf("failed to create AES block cipher: %v", err)
		}

		//we use a non-standard nonce size that is bigger, it has more
		//random bytes and is supported but doesnt couldnft find a source
		//to confirm its. Some speculation over her:
		//https://www.reddit.com/r/crypto/comments/4h6fxr/aesgcm_nonce_size_in_crypto/
		gcm, err := cipher.NewGCMWithNonceSize(block, KeySize)
		if err != nil {
			return fmt.Errorf("failed to setup GCM cipher mode: %v", err)
		}

		//we use a modern sha to hash the chunk's content. this key
		//is used for content addressability and for nonces
		k := K(sha256.Sum256(chunk.Data))

		//we use the hashed content as a nonce, this should be safe in our system
		//because its goal its inherent goal is deduplication: we are never encrypting a different
		//'message' (chunk) with the same nonce-key (hash-secret in our case) combination.
		//@TODO this needs to be looked at by someone with more experience in working with GCM
		encrypted := gcm.Seal(chunk.Data[:0], k[:], chunk.Data, nil)
		if err != nil {
			return fmt.Errorf("failed to encrypt chunk '%s': %v", k, err)
		}

		//after this transaction we know the chunk is peristed to disk as far as
		//userland is concerned.
		err = db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(BucketNameChunks)
			existing := b.Get(k[:])
			if existing == nil {
				return b.Put(k[:], encrypted)
			}

			return nil
		})

		if err != nil {
			return fmt.Errorf("failed to persist chunk '%s' locally: %v", k, err)
		}

		//we managed to persist the chunk, output key to writer
		fmt.Fprintf(w, "%s\n", k)
	}

	return nil
}
