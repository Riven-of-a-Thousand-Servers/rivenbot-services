package process

import (
	"context"
	"encoding/json"

	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/types/dataset"
	types "pgcr-processing-service/internal/types/processor"
	uiEvents "pgcr-processing-service/internal/types/ui"
)

type DatasetProcessor struct {
	inner  Processor[json.RawMessage]
	broker *pubsub.Broker[uiEvents.FileEvent]
}

func NewDatasetProcessor(inner Processor[json.RawMessage], broker *pubsub.Broker[uiEvents.FileEvent]) *DatasetProcessor {
	return &DatasetProcessor{inner: inner, broker: broker}
}

func (p *DatasetProcessor) ProcessPgcr(ctx context.Context, entry dataset.Entry, source types.Source) error {
	if err := p.inner.ProcessPgcr(ctx, entry.Bytes, source); err != nil {
		p.broker.Publish(uiEvents.FileEvent{
			Type:     uiEvents.FileError,
			Filename: entry.Filename,
			RowsDone: entry.RowsDone,
			Err:      err,
		})
		return err
	}
	return nil
}
