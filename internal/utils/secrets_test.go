package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadSecret(t *testing.T) {
	path := "./testdata/secret.txt"
	data, err := ReadSecret(path)
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, data, "skibidi")
}

func TestReadSecretError(t *testing.T) {
	path := "./path/does/not/exist"
	_, err := ReadSecret(path)
	if err == nil {
		t.Fatal("Expecting error, got none")
	}
}
