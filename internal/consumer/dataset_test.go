package consumer

import (
	"context"
	"testing"

	"pgcr-processing-service/internal/types/dataset"

	"gotest.tools/v3/assert"
)

func TestStartShouldRunSuccesfully(t *testing.T) {
	var fileIdx FileIndex = map[string]*FileEntry{
		"./testdata/example.jsonl.zst": {
			Name:    "test.zst",
			Started: false,
			Done:    false,
		},
	}
	sut := NewDatasetConsumer(fileIdx, 12, ConsumerOpts{})
	consumeCh := make(chan Delivery[dataset.RawContent], 3)
	sut.ch = consumeCh
	subCh, unsub := sut.Subscribe()
	defer unsub()

	err := sut.Start(context.Background())
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, len(consumeCh), 3, "Expecting 3 pgcrs from FileIndex, got %d", len(consumeCh))
	assert.Equal(t, len(subCh), 5, "Expecting 5 events, got %d", len(subCh))
}

func TestLineLimitsShouldBeRespected(t *testing.T) {
	var fileIdx FileIndex = map[string]*FileEntry{
		"./testdata/example.jsonl.zst": {
			Name:    "example.jsonl.zst",
			Started: false,
			Done:    false,
		},
	}
	sut := NewDatasetConsumer(fileIdx, 12, ConsumerOpts{
		NumLines: 1,
	})
	consumeCh := make(chan Delivery[dataset.RawContent], 3)
	sut.ch = consumeCh
	subCh, unsub := sut.Subscribe()
	defer unsub()

	err := sut.Start(context.Background())
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, len(consumeCh), 1, "Expecting 1 pgcrs from FileIndex, got %d", len(consumeCh))
	assert.Equal(t, len(subCh), 3, "Expecting 3 events, got %d", len(subCh))
}

func TestFileLimitsShouldBeRespected(t *testing.T) {
	var fileIdx FileIndex = map[string]*FileEntry{
		"./testdata/example.jsonl.zst": {
			Name:    "example.jsonl.zst",
			Started: false,
			Done:    false,
		},
		"./testdata/example2.jsonl.zst": {
			Name:    "example2.jsonl.zst",
			Started: false,
			Done:    false,
		},
	}
	sut := NewDatasetConsumer(fileIdx, 12, ConsumerOpts{
		NumFiles: 1,
	})
	consumeCh := make(chan Delivery[dataset.RawContent], 3)
	sut.ch = consumeCh
	subCh, unsub := sut.Subscribe()
	defer unsub()

	err := sut.Start(context.Background())
	if err != nil {
		t.Fatalf("Not expecting error, got %v", err)
	}

	assert.Equal(t, len(consumeCh), 3, "Expecting 3 pgcrs from FileIndex, got %d", len(consumeCh))
	assert.Equal(t, len(subCh), 5, "Expecting 5 events, got %d", len(subCh))
}
