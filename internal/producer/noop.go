package producer

import "context"

type NoopFactory[T any] struct{}

func NewNoopFactory[T any]() *NoopFactory[T] {
	return &NoopFactory[T]{}
}

func (f *NoopFactory[T]) NewProducer(ctx context.Context) (ProducerCloser[T], error) {
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
