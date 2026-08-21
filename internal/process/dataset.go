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
	processor Processor[json.RawMessage]
}

func NewDatasetProcessor(processor Processor[json.RawMessage]) *DatasetProcessor {
	return &DatasetProcessor{
		processor: processor,
		Broker:    pubsub.NewBroker[uiEvents.FileEvent](30000),
	}
}

func (p *DatasetProcessor) ProcessPgcr(ctx context.Context, entry dataset.Entry, source types.Source) error {
	if err := p.processor.ProcessPgcr(ctx, entry.Bytes, source); err != nil {
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
