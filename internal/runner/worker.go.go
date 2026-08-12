package runner

import (
	"context"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/process"
)

// The Worker relies on the dependencies of the
// Processor to insert PGCRs into the db
type Worker[T any] struct {
	Processor process.Processor[T]
	Consumer  consumer.Consumer[T]
}

func NewWorker[T any](processor process.Processor[T], consumer consumer.Consumer[T]) *Worker[T] {
	return &Worker[T]{
		Processor: processor,
		Consumer:  consumer,
	}
}

func (w *Worker[T]) Begin(ctx context.Context) error {
	deliveries, err := w.Consumer.Consume(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("Consumer shutting down")
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.Process(ctx, delivery)
		}
	}
}

func (w *Worker[T]) Process(ctx context.Context, delivery consumer.Delivery[T]) {
	source, err := delivery.GetSource()
	if err != nil {
		slog.Warn("Unable to extract PGCR source from headers", "error", err)
		delivery.Nack(false)
		return
	}

	if err := w.Processor.ProcessPgcr(ctx, delivery.Payload, source); err != nil {
		slog.Warn("Error processing pgcr", "error", err)
		delivery.Nack(false)
		return
	}

	delivery.Ack()
}
