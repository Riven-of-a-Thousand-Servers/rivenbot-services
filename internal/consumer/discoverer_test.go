package consumer

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestDiscoverSuccessfulRun(t *testing.T) {
	root := "./testdata/"
	sut := NewDiscoverer(root)
	idx, err := sut.Discover(context.Background(), ".zst")
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, len(idx), 2, "Expecting 2 .zst files, got %d", len(idx))
}
