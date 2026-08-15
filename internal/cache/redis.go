package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"pgcr-processing-service/internal/types/manifest"

	"github.com/redis/go-redis/v9"
)

type Fetcher[T any] func(context.Context, string, manifest.EntityDefinition) (manifest.Response[T], error)

type ManifestCache[T any] interface {
	Get(ctx context.Context, hash string, entity manifest.EntityDefinition) (T, error)
}

type RedisCache[T any] struct {
	// The Redis client to use for fetching the cached manifest object
	redis *redis.Client

	// Time-to-live duration for the cached manifest object
	ttl time.Duration

	// The Fetcher function ideally is a function that takes the passed in key
	// to the Get() method and fetches the specific manifest definition using
	// the passed in hash through its string parameter
	fetch Fetcher[T]
}

func New[T any](redis *redis.Client, ttl time.Duration, fetch Fetcher[T]) *RedisCache[T] {
	return &RedisCache[T]{redis: redis, fetch: fetch, ttl: ttl}
}

// Returns a given manifest entity based on a hash
func (c *RedisCache[T]) Get(ctx context.Context, key string, entity manifest.EntityDefinition) (T, error) {
	var zero T

	raw, err := c.redis.Get(ctx, key).Bytes()
	switch {
	case err == nil:
		var val T
		if unmarshalErr := json.Unmarshal(raw, &val); unmarshalErr != nil {
			return zero, unmarshalErr
		}
	case errors.Is(err, redis.Nil):
	default:
		val, fetchErr := c.fetch(ctx, key, entity)
		if fetchErr != nil {
			return zero, fetchErr
		}
		return val.Response, nil
	}

	val, err := c.fetch(ctx, key, entity)
	if err != nil {
		return zero, err
	}

	if data, marshalErr := json.Marshal(val); marshalErr == nil {
		if setErr := c.redis.Set(ctx, key, data, c.ttl).Err(); setErr != nil {
			slog.Warn("Failed to populate cache", "key", key, "error", setErr)
		}
	}

	return val.Response, nil
}
