package consumer

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestDiscoverSuccessfulRun(t *testing.T) {
	root := "./testdata/"
	sut := NewFileWalker(root)
	idx, err := sut.Discover(".zst")
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, len(idx), 2, "Expecting 2 .zst files, got %d", len(idx))
}
