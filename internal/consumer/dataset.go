package consumer

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"sync"

	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/types/dataset"
	uiEvents "pgcr-processing-service/internal/types/ui"

	"github.com/klauspost/compress/zstd"
)

const maxSize = 1024 * 1024 * 20 // 20 MBs

type DatasetConsumer struct {
	Broker    pubsub.Broker[uiEvents.Event]
	FileIndex *FileIndex
	once      sync.Once
	ch        chan Delivery[dataset.Entry]
}

func NewDatasetConsumer(idx *FileIndex) *DatasetConsumer {
	return &DatasetConsumer{FileIndex: idx}
}

func (c *DatasetConsumer) Consume(ctx context.Context) (<-chan Delivery[dataset.Entry], error) {
	c.once.Do(func() {
		c.ch = make(chan Delivery[dataset.Entry])
		go c.Start(ctx)
	})

	return c.ch, nil
}

func (c *DatasetConsumer) Start(ctx context.Context) error {
	defer close(c.ch)

	for filepath, entry := range *c.FileIndex {
		file, err := os.Open(filepath)
		if err != nil {
			slog.Error("Error while opening file", "file", file, "error", err)
			return err
		}
		defer file.Close()

		bufReader := bufio.NewReader(file)
		decoder, err := zstd.NewReader(bufReader)
		if err != nil {
			slog.Error("Error creating zstd reader", "file", filepath, "error", err)
			return err
		}
		defer decoder.Close()

		buf := make([]byte, maxSize)
		scanner := bufio.NewScanner(decoder)
		scanner.Buffer(buf, maxSize)

		entry.Started = true

		count := 0
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				payload := dataset.Entry{
					Bytes:    scanner.Bytes(),
					Filename: entry.Name,
					RowsDone: count + 1,
				}

				c.ch <- Delivery[dataset.Entry]{
					Payload: payload,
					// Empty Ack/Nack functions
					Ack: func() error {
						return nil
					},
					Nack: func(requeue bool) error {
						return nil
					},
					Headers: map[string]any{
						"source": "dataset",
					},
				}
				count++
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}

	return nil
}
