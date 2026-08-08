package pgcrdataset

import (
	"context"
	"log/slog"

	"pgcr-processing-service/internal/process"
)

type Worker struct {
	Processor *process.PgcrProcessor
}

func NewWorker(processor *process.PgcrProcessor) *Worker {
	return &Worker{
		Processor: processor,
	}
}

func (w *Worker) Start(ctx context.Context, pipeline <-chan DatasetEntry) error {
	for {
		select {
		case <-ctx.Done():
			slog.Error("Context cancelled. Returning")
			return ctx.Err()
		case item, ok := <-pipeline:
			if !ok {
				slog.Warn("Channel closed while processing", "item", item.Number, "filename", item.Filename)
				return nil
			}

			err := w.Processor.ProcessPgcr(ctx, item.Bytes, process.Dataset)
			if err != nil {
				slog.Error("Failed to process pgcr", "item", item.Number, "filename", item.Filename, "error", err)
				return err
			}
		}
	}
}
