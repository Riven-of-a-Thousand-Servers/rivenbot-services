package writer

import (
	"context"
	"log/slog"
)

type NoopProcessor[T any] struct{}

func NoOpProcessor[T any]() *NoopProcessor[T] {
	return &NoopProcessor[T]{}
}

func (p *NoopProcessor[T]) Write(ctx context.Context, b T) error {
	slog.Debug("Processed Pgcr! (Noop)")
	return nil
}
