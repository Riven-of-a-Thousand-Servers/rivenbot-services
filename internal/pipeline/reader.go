package pipeline

import (
	"context"

	"github.com/deahtstroke/chainmorph"
)

type channelReader[T any] struct {
	ch <-chan T
}

func NewChannelReader[T any](ch <-chan T) *channelReader[T] {
	return &channelReader[T]{
		ch: ch,
	}
}

func (r *channelReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case el, ok := <-r.ch:
		if !ok {
			return zero, chainmorph.ErrEndOfStream
		}

		return el, nil
	}
}
