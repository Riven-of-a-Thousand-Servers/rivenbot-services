package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"

	"pgcr-processing-service/internal/types/manifest"
)

const (
	baseUrl      = "https://www.bungie.net"
	manifestPath = "/Destiny2/Manifest"
)

// BungieFetcher represents a function that takes care of fetching
// a bungie resource and hiding implementation details from the cache
// It takes in a context, ideally a request will be created with a context
// and a URL string to fetch
type BungieFetcher[T any] func(context.Context, string) (T, error)

type InMemoryCache[T any] struct {
	manifestFetcher BungieFetcher[manifest.Response[manifest.CompleteManifest]]
	entryFetcher    BungieFetcher[manifest.RawComponent[T]]
	entries         map[string]T
}

func NewInMemoryCache[T any](
	manifestFetcher BungieFetcher[manifest.Response[manifest.CompleteManifest]],
	entryFetcher BungieFetcher[manifest.RawComponent[T]],
) *InMemoryCache[T] {
	entries := make(map[string]T)
	return &InMemoryCache[T]{
		entries:         entries,
		manifestFetcher: manifestFetcher,
		entryFetcher:    entryFetcher,
	}
}

func (c *InMemoryCache[T]) Get(ctx context.Context, hash string) (T, error) {
	var zero T
	return zero, nil
}

// This method will populate the in-memory cache with the passed in definitions
// from the /Destiny2/Manifest/ endpoint from Bungie.net
func (c *InMemoryCache[T]) Prepopulate(ctx context.Context, defs ...manifest.EntityDefinition) error {
	manifest, err := c.manifestFetcher(ctx, baseUrl+manifestPath)
	if err != nil {
		return err
	}

	// List of manifest paths to fetch according to what the cache needs
	var paths []string
	for _, def := range defs {
		paths = append(paths, manifest.Response.WorldComponentContentPaths.English[def.String()])
	}

	for _, path := range paths {
		entry, err := c.entryFetcher(ctx, baseUrl+path)
		if err != nil {
			return err
		}

		maps.Copy(c.entries, entry)
	}

	return nil
}

func HttpFetcher[T any](client *http.Client) BungieFetcher[T] {
	return func(ctx context.Context, s string) (T, error) {
		var zero T

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s, nil)
		if err != nil {
			return zero, err
		}

		res, err := client.Do(req)
		if err != nil {
			return zero, err
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			return zero, fmt.Errorf("Response status %d", res.StatusCode)
		}

		var m T
		if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
			return zero, err
		}

		return m, err
	}
}
