package writer

import (
	"context"
	"time"

	"github.com/deahtstroke/chainmorph"
)

type RetryWriter[T any] struct {
	inner   chainmorph.ItemWriter[T]
	backoff time.Duration
	retries int
}

func NewRetryWriter[T any](
	inner chainmorph.ItemWriter[T],
	backoff time.Duration,
	retries int,
) *RetryWriter[T] {
	return &RetryWriter[T]{
		inner:   inner,
		retries: retries,
		backoff: backoff,
	}
}

func (w *RetryWriter[T]) Write(ctx context.Context, item T) error {
	var err error
	for i := range w.retries {
		if i > 0 {
			timer := time.NewTimer(w.backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				// Fallthrough
			}
		}

		// Success then just return the result
		if err = w.inner.Write(ctx, item); err == nil {
			return nil
		}

		// TODO: Check if the error returned is retry-able?
	}
	return err
}
