package mapper

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/types/pgcr"

	"github.com/stretchr/testify/mock"
)

func TestExtractInfo_ShouldWorkForAPIPgcr(t *testing.T) {
	mockCache := new(mockCacheService[manifest.Entry])
	mockCache.On("Get", mock.Anything, mock.Anything, mock.Anything).
		Return(manifest.Entry{DisplayProperties: manifest.DisplayProperties{Name: "Last Wish"}}, nil)

	ctx := context.Background()
	pgcr := openPgcr(t, "beyond_light_pgcr.json")
	sut := New(mockCache)

	res, err := sut.ExtractInfo(ctx, &pgcr.Response)
	if err != nil {
		t.Fatal("Unable to extract info from API-originated pgcr")
	}

	if res == nil {
		t.Fatal("Response is nil")
	}
}

func TestExtractInfo_ShouldWorkForDatasetPgcr(t *testing.T) {
	mockCache := new(mockCacheService[manifest.Entry])
	mockCache.On("Get", mock.Anything, mock.Anything).
		Return(manifest.Entry{DisplayProperties: manifest.DisplayProperties{Name: "Last Wish"}}, nil)

	ctx := context.Background()
	pgcr := openPgcr(t, "beyond_light_pgcr.json")
	sut := New(mockCache)

	res, err := sut.ExtractInfo(ctx, &pgcr.Response)
	if err != nil {
		t.Fatal("Unable to extract info from dataset-originated pgcr")
	}

	if res == nil {
		t.Fatalf("Response is nil")
	}
}

func openPgcr(t *testing.T, filename string) *pgcr.Response {
	t.Helper()
	bytes, err := os.ReadFile(filepath.Join("./testdata/", filename))
	if err != nil {
		t.Fatalf("Error reading file %s: %v", filename, err)
		return nil
	}

	var pgcr pgcr.Response
	if err = json.Unmarshal(bytes, &pgcr); err != nil {
		t.Fatalf("Error marshaling pgcr for file %s: %v", filename, err)
	}
	return &pgcr
}

type mockCacheService[T any] struct {
	mock.Mock
}

func (m *mockCacheService[T]) Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (T, error) {
	args := m.Called(ctx, hash)
	return args.Get(0).(T), args.Error(1)
}
