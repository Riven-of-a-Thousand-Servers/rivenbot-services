package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Fetcher[T any] func(ctx context.Context, key, entity string) (T, error)

type Service[T any] interface {
	Get(ctx context.Context, hash, entity string) (T, error)
}

type CacheService[T any] struct {
	redis *redis.Client
	ttl   time.Duration
	fetch Fetcher[T]
}

func New[T any](redis *redis.Client, ttl time.Duration, fetch Fetcher[T]) *CacheService[T] {
	return &CacheService[T]{redis: redis, fetch: fetch, ttl: ttl}
}

// Returns a given manifest entity based on a hash
func (c *CacheService[T]) Get(ctx context.Context, key, entity string) (T, error) {
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
		return val, nil
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

	return val, nil
}
