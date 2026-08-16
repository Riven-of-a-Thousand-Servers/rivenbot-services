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
	*pubsub.Broker[uiEvents.FileEvent]
	inner Processor[json.RawMessage]
}

func NewDatasetProcessor(inner Processor[json.RawMessage]) *DatasetProcessor {
	return &DatasetProcessor{
		inner:  inner,
		Broker: pubsub.NewBroker[uiEvents.FileEvent](2048),
	}
}

func (p *DatasetProcessor) ProcessPgcr(ctx context.Context, entry dataset.Entry, source types.Source) error {
	if err := p.inner.ProcessPgcr(ctx, entry.Bytes, source); err != nil {
		p.Publish(uiEvents.FileEvent{
			Type:     uiEvents.FileError,
			Filename: entry.Filename,
			RowsDone: entry.RowsDone,
			Err:      err,
		})
		return err
	}
	return nil
}
