package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/types/manifest"
)

const (
	baseUrl      = "https://www.bungie.net"
	manifestPath = "/Platform/Destiny2/Manifest"
	apiKeyHeader = "x-api-key"
)

type CacheEvent struct {
	CurrentDefinition manifest.EntityDefinition
	Size              int
}

type InMemoryCache struct {
	entries map[string]manifest.Entry
	queries *db.Queries

	*pubsub.Broker[CacheEvent]
}

// Creates an In-memory cache with an internal broker with buffer size specified
func NewInMemoryCache(brokerSize int) *InMemoryCache {
	return &InMemoryCache{
		entries: make(map[string]manifest.Entry),
		Broker:  pubsub.NewBroker[CacheEvent](brokerSize),
	}
}

func (c *InMemoryCache) Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (manifest.Entry, error) {
	var zero manifest.Entry
	res, ok := c.entries[hash]
	if !ok {
		return zero, fmt.Errorf("No cache entry found for hash %s", hash)
	}
	return res, nil
}

func (c *InMemoryCache) Init(ctx context.Context, apiKey string, definitions ...manifest.EntityDefinition) error {
	c.Publish(pubsub.CacheStarted, CacheEvent{})

	mFetch := HttpFetcher[manifest.Response[manifest.CompleteManifest]](http.DefaultClient, apiKey)
	cFetch := HttpFetcher[manifest.RawComponent[manifest.Entry]](http.DefaultClient, apiKey)
	response, err := mFetch(ctx, baseUrl+manifestPath)
	if err != nil {
		return err
	}

	// List of manifest paths to fetch according to what the cache needs
	for _, manifestDefinition := range definitions {
		path := response.Response.WorldComponentContentPaths.English[manifestDefinition.String()]
		c.Publish(pubsub.CacheLoading, CacheEvent{
			CurrentDefinition: manifestDefinition,
		})

		rawEntries, err := cFetch(ctx, baseUrl+path)
		if err != nil {
			return err
		}

		if manifestDefinition == manifest.InventoryItemDefinition {
			// TODO: Insert inventory definitions to the database here!
		}

		maps.Copy(c.entries, rawEntries)
	}

	c.Publish(pubsub.CacheFinished, CacheEvent{
		Size: len(c.entries),
	})

	slog.Info("In-memory cache prepopulated with manifest entries", "length", len(c.entries))
	return nil
}

// BungieFetcher represents a function that takes care of fetching
// a bungie resource and hiding implementation details from the cache
// It takes in a context, ideally a request will be created with a context
// and a URL string to fetch
type BungieFetcher[T any] func(context.Context, string) (T, error)

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
