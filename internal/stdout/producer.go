package stdout

import (
	"context"

	"pgcr-processing-service/internal/producer"
)

type Factory[T any] struct{}

func NewFactory[T any]() *Factory[T] {
	return &Factory[T]{}
}

func (f *Factory[T]) NewProducer(ctx context.Context) (producer.ProducerCloser[T], error) {
	return &stdoutProducer[T]{}, nil
}

type stdoutProducer[T any] struct{}

func (p *stdoutProducer[T]) Produce(ctx context.Context, item T) error {
	// quite literally no-op lol
	return nil
}

func (p *stdoutProducer[T]) Close() error {
	return nil // Do nothing
}
