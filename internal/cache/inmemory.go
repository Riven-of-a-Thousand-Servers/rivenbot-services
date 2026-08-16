package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"

	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/types/ui"
)

const (
	baseUrl      = "https://www.bungie.net"
	manifestPath = "/Platform/Destiny2/Manifest"
	apiKeyHeader = "x-api-key"
)

// BungieFetcher represents a function that takes care of fetching
// a bungie resource and hiding implementation details from the cache
// It takes in a context, ideally a request will be created with a context
// and a URL string to fetch
type BungieFetcher[T any] func(context.Context, string) (T, error)

type InMemoryCache[T any] struct {
	entries map[string]T
	*pubsub.Broker[ui.CacheEvent]
}

// Creates an In-memory cache with an internal broker with buffer size specified
func NewInMemoryCache[T any](size int) *InMemoryCache[T] {
	entries := make(map[string]T)
	return &InMemoryCache[T]{
		entries: entries,
		Broker:  pubsub.NewBroker[ui.CacheEvent](size),
	}
}

func (c *InMemoryCache[T]) Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (T, error) {
	var zero T
	return zero, nil
}

// This method will populate the in-memory cache with the passed in definitions
// from the /Destiny2/Manifest/ endpoint from Bungie.net
func (c *InMemoryCache[T]) Prepopulate(ctx context.Context, apiKey string, defs ...manifest.EntityDefinition) error {
	c.Publish(ui.CacheEvent{
		Type: ui.CacheStarted,
	})

	manifestFetcher := HttpFetcher[manifest.Response[manifest.CompleteManifest]](http.DefaultClient, apiKey)
	manifestComponentFetcher := HttpFetcher[manifest.RawComponent[T]](http.DefaultClient, apiKey)
	manifest, err := manifestFetcher(ctx, baseUrl+manifestPath)
	if err != nil {
		return err
	}

	// List of manifest paths to fetch according to what the cache needs
	for _, def := range defs {
		path := manifest.Response.WorldComponentContentPaths.English[def.String()]

		c.Publish(ui.CacheEvent{
			Type:              ui.CacheLoading,
			CurrentDefinition: def,
		})

		entry, err := manifestComponentFetcher(ctx, baseUrl+path)
		if err != nil {
			return err
		}

		maps.Copy(c.entries, entry)
	}

	c.Publish(ui.CacheEvent{
		Type: ui.CacheFinished,
		Size: len(c.entries),
	})

	return nil
}

func HttpFetcher[T any](client *http.Client, apiKey string) BungieFetcher[T] {
	return func(ctx context.Context, s string) (T, error) {
		var zero T

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s, nil)
		if err != nil {
			return zero, err
		}

		req.Header.Add(apiKeyHeader, apiKey)
		res, err := client.Do(req)
		if err != nil {
			slog.Error("Error while doing request", "error", err)
			return zero, err
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			bytes, _ := io.ReadAll(res.Body)

			slog.Error("Response status is not 200", "url", s, "statusCode", res.StatusCode, "response", string(bytes))
			return zero, fmt.Errorf("Response status %d", res.StatusCode)
		}

		var m T
		if err := json.NewDecoder(res.Body).Decode(&m); err != nil {
			return zero, err
		}

		return m, err
	}
}
