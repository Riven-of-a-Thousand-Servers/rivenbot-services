package writer

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/deahtstroke/chainmorph"
	errs "pgcr-processing-service/internal/errors"
)

// This function takes the item that was to be written
// and process it to an entry in the DeadLetterSink
type SinkableFunc[T any] func(T) []byte

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

func NewDeadLetterWriter[T any](sink io.Writer,
	inner chainmorph.ItemWriter[T],
	sinkFunc SinkableFunc[T],
) *DeadLetterWriter[T] {
	return &DeadLetterWriter[T]{
		sink:     sink,
		inner:    inner,
		sinkable: sinkFunc,
	}
}

func (w *DeadLetterWriter[T]) shouldQuarantine(err error) bool {
	switch {
	case errors.Is(err, errs.ErrPermanent), errors.Is(err, errs.ErrAlreadyProcessed):
		return false
	case errors.Is(err, errs.ErrTransient):
		return true
	}

	return false
}

func (w *DeadLetterWriter[T]) Write(ctx context.Context, item T) error {
	err := w.inner.Write(ctx, item)
	if err == nil || !w.shouldQuarantine(err) {
		return err
	}
	if _, sinkErr := w.sink.Write(w.sinkable(item)); sinkErr != nil {
		return fmt.Errorf("dead-letter write failed: %w (original error %v)", sinkErr, err)
	}
	return nil
}
