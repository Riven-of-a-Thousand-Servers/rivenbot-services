package process

import (
	"context"
	"log/slog"

	types "pgcr-processing-service/internal/types/processor"
)

type NoopProcessor[T any] struct{}

func NoOpProcessor[T any]() *NoopProcessor[T] {
	return &NoopProcessor[T]{}
}

func (p *NoopProcessor[T]) ProcessPgcr(ctx context.Context, b T, source types.Source) error {
	slog.Info("Processed Pgcr! (Noop)")
	return nil
}
