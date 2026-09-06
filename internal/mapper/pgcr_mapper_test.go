package mapper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"pgcr-processing-service/internal/types/manifest"

	"github.com/stretchr/testify/mock"
)

func openPgcr[T any](t *testing.T, filename string) T {
	t.Helper()

	var zero T
	bytes, err := os.ReadFile(filepath.Join("./testdata/", filename))
	if err != nil {
		t.Fatalf("Error reading file %s: %v", filename, err)
		return zero
	}

	var pgcr T
	if err = json.Unmarshal(bytes, &pgcr); err != nil {
		t.Fatalf("Error marshaling pgcr for file %s: %v", filename, err)
	}
	return pgcr
}

type mockCacheService[T any] struct {
	mock.Mock
}

func (m *mockCacheService[T]) Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (T, error) {
	args := m.Called(ctx, hash)
	return args.Get(0).(T), args.Error(1)
}
