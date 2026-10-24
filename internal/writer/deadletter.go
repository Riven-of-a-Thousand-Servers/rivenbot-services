package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"

	"github.com/deahtstroke/chainmorph"
)

// This function takes the item that was to be written
// and process it to an entry in the DeadLetterSink
type SinkableFunc[T any] func(context.Context, T) []byte

// DeadLetterWriter will handle writing to a dead letter queue
// in the case that the underlying
//
// It needs access to the metadata for retrying/where it failed
// like Line Offset, File name, etc
type DeadLetterWriter[T any] struct {
	sink     io.Writer
	inner    chainmorph.ItemWriter[T]
	sinkable SinkableFunc[T]
}

func (w *DeadLetterWriter[T]) shouldQuarantine(err error) bool {
	switch {
	case errors.Is(err, sql.ErrTxDone):
	}

	return false
}

func (w *DeadLetterWriter[T]) Write(ctx context.Context, item T) error {
	err := w.inner.Write(ctx, item)
	if err == nil || !w.shouldQuarantine(err) {
		return err
	}
	if _, sinkErr := w.sink.Write(w.sinkable(ctx, item)); sinkErr != nil {
		return fmt.Errorf("dead-letter write failed: %w (original error %v)", sinkErr, err)
	}
	return nil
}
