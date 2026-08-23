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
	pgcr := openPgcr[pgcr.Response](t, "beyond_light_pgcr.json")
	sut := New(mockCache)

	res, err := sut.PgcrToPgcrInfo(ctx, &pgcr.Response)
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
	pgcr := openPgcr[pgcr.PostGameCarnageReport](t, "dataset_pgcr.json")
	sut := New(mockCache)

	res, err := sut.PgcrToPgcrInfo(ctx, &pgcr)
	if err != nil {
		t.Fatal("Unable to extract info from dataset-originated pgcr")
	}

	if res == nil {
		t.Fatalf("Response is nil")
	}

	if res.InstanceId != 15780000000 {
		t.Fatalf("Marshalling did not work")
	}

	if len(res.PlayerInfo) != 2 {
		t.Fatalf("Error, was expecting two entries but found %d", len(res.PlayerInfo))
	}

	if res.PlayerInfo[0].CharacterInfo[0].TimePlayedSeconds != 23 {
		t.Fatalf("Expecting 23 seconds of played time, got %v", res.PlayerInfo[0].CharacterInfo[0].TimePlayedSeconds)
	}
}

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
