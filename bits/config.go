package bits

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
)

//Config describes how the library's Split, Join and Push behaves
type Config struct {
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

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return conf, fmt.Errorf("failed to setup GCM cipher mode: %v", err)
	}

	return Config{
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
