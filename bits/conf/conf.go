package conf

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"

	"github.com/advanderveer/libchunk/bits/store"
)

//StoreConfig holds the configuration for a named store
type StoreConfig struct {
	Kind string `json:"kind"`
	bitsstore.S3StoreConfig
}

//Config is the structure that is (de)serialized in order
//to configure bits from files
type Config struct {
	//setting the aead before encoding/decoding allows the
	//the secrets map belowed to be encrypted at rest
	aead cipher.AEAD

	Stores  map[string]*StoreConfig `json:"stores"`
	Secrets map[string]string       `json:"secrets"`
}

//UnmarshalJSON decode the config structure and decrypt
//the secrets field with the configured secret
func (conf *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	econf := &struct {
		*Alias
		Secrets []byte `json:"secrets"`
	}{
		Alias: (*Alias)(conf),
	}

	if err := json.Unmarshal(data, &econf); err != nil {
		return err
	}

	if len(econf.Secrets) > 0 {
		if conf.aead == nil {
			return fmt.Errorf("to decrypt the secrets, an aead must be configured")
		}

		if len(econf.Secrets) < conf.aead.NonceSize() {
			return fmt.Errorf("encrypted secrets must be at least '%d', got '%d' byte", conf.aead.NonceSize(), len(econf.Secrets))
		}

		secretsb, err := conf.aead.Open(
			nil,
			econf.Secrets[:conf.aead.NonceSize()],
			econf.Secrets[conf.aead.NonceSize():],
			nil)
		if err != nil {
			return fmt.Errorf("failed to decrypt secrets: %v, please check your key", err)
		}

		err = json.Unmarshal(secretsb, &conf.Secrets)
		if err != nil {
			return fmt.Errorf("failed to decode decrypted secrets: %v", err)
		}
	}

	return nil
}

//MarshalJSON encodes the config structure but encrypt
//the secrets field using AES-256 GCM with the provided secret
func (conf *Config) MarshalJSON() (b []byte, err error) {
	encrypted := []byte{}
	if conf.Secrets != nil {
		if conf.aead == nil {
			return nil, fmt.Errorf("to encrypt the secrets, a aead must be configured")
		}

		secretb, err := json.Marshal(conf.Secrets)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal secrets: %v", err)
		}

		nonce := make([]byte, conf.aead.NonceSize())
		if _, err = rand.Read(nonce); err != nil {
			return nil, fmt.Errorf("failed to generate nonce: %v", err)
		}

		encrypted = conf.aead.Seal(nil, nonce, secretb, nil)
		encrypted = append(nonce, encrypted...)
	}

	type Alias Config
	return json.Marshal(struct {
		*Alias
		Secrets []byte `json:"secrets"`
	}{
		Alias:   (*Alias)(conf),
		Secrets: encrypted,
	})
}
