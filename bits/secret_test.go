package bits_test

import (
	"testing"

	"github.com/advanderveer/libchunk/bits"
)

func TestSecretGeneration(t *testing.T) {
	secret, err := bits.GenerateSecret()
	if err != nil {
		t.Errorf("generating the secret should fail, got: %v", err)
	}

	if secret == bits.ZeroSecret {
		t.Error("generated secret should not be empty")
	}
}

func TestEncodeDecodeSecret(t *testing.T) {
	encoded := secret.Encode()
	_, err := bits.DecodeSecret([]byte("3d"))
	if err == nil {
		t.Error("should fail because byte slice length is invalid")
	}

	str := secret.String()
	if str != string(encoded) {
		t.Error("expected stringer version to equal encoded casted to string")
	}

	nsecret, err := bits.DecodeSecret(encoded)
	if err != nil {
		t.Fatal("decode shouldnt fail")
	}

	if nsecret != secret {
		t.Error("expected encoded, decoded secret to be equal to input secret")
	}
}
