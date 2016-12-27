package conf

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"testing"

	"github.com/advanderveer/libchunk/bits"
	"github.com/advanderveer/libchunk/bits/store"
)

func TestMarshalUnmarshal(t *testing.T) {
	secret, err := bits.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}

	block, err := aes.NewCipher(secret[:])
	if err != nil {
		t.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	conf1 := &Config{
		aead: aead,

		Stores: map[string]*StoreConfig{},
		Secrets: map[string]string{
			"foo": "bar",
		},
	}

	data1, err := json.Marshal(conf1)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	data2, err := json.Marshal(conf1)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	if bytes.Equal(data1, data2) {
		t.Error("nonce should prevent second marshal from being equal")
	}

	if bytes.Contains(data1, []byte(`"aead"`)) {
		t.Error("data should not contain a aead for the secret used for encryption")
	}

	if bytes.Contains(data1, []byte(`s3`)) {
		t.Error("data should not contain s3 keys as they are omit empty")
	}

	decoded2 := &Config{aead: aead}
	err = json.Unmarshal(data1, decoded2)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if val, ok := decoded2.Secrets["foo"]; !ok || val != "bar" {
		t.Errorf("decoded value was unexpected, got: %+v", decoded2)
	}

	decoded3 := &Config{aead: aead}
	err = json.Unmarshal(data2, decoded3)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if val, ok := decoded3.Secrets["foo"]; !ok || val != "bar" {
		t.Errorf("decoded value was unexpected, got: %+v", decoded3)
	}
}

func TestS3StoreCreation(t *testing.T) {
	secret, err := bits.GenerateSecret()
	if err != nil {
		t.Fatal(err)
	}

	block, err := aes.NewCipher(secret[:])
	if err != nil {
		t.Fatalf("failed to create AES block cipher: %v", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("failed to setup GCM cipher mode: %v", err)
	}

	conf1 := &Config{
		aead: aead,

		Stores: map[string]*StoreConfig{
			"my-remote": {
				S3StoreConfig: bitsstore.S3StoreConfig{
					Scheme:    "http",
					Host:      "demo-git-bits.s3-eu-west-1.amazonaws.com",
					AccessKey: "my-access-key",
				},
			},
		},
		Secrets: map[string]string{
			"s3_secret_key": "my-secret",
		},
	}

	data1, err := json.Marshal(conf1)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	if !bytes.Contains(data1, []byte(`s3`)) {
		t.Error("data should contain s3 keys as they are omit empty")
	}

}
