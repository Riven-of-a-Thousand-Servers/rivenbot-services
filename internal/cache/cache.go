package cache

import (
	"context"

	"pgcr-processing-service/internal/types/manifest"
)

// Manifest cache represents any implementation that is able to fetch a key
// from the Bungie.net manifest SQLlite database exposed by their API
// It should fetch based on a given hash and addtionally an entity defintion const
type ManifestCache[T any] interface {
	Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (T, error)
}
