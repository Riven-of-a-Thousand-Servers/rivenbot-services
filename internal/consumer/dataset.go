package consumer

import (
	"bufio"
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/types/dataset"
	events "pgcr-processing-service/internal/types/ui"

	"github.com/klauspost/compress/zstd"
)

const maxSize = 1024 * 1024 * 20 // 20 MBs

type DatasetConsumer struct {
	*pubsub.Broker[events.FileEvent]
	FileIndex FileIndex
	once      sync.Once
	ch        chan Delivery[dataset.Entry]
}

func NewDatasetConsumer(idx FileIndex, brokerSize int) *DatasetConsumer {
	return &DatasetConsumer{
		FileIndex: idx,
		Broker:    pubsub.NewBroker[events.FileEvent](brokerSize),
	}
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

	for filepath, entry := range c.FileIndex {
		start := time.Now()
		slog.Info("Processing zstd file", "filename", entry.Name)
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

		c.Publish(events.FileEvent{
			Type:     events.FileStarted,
			RowsDone: 0,
			Filename: file.Name(),
			Elapsed:  time.Since(start),
		})

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

				c.Publish(events.FileEvent{
					Type:     events.FileProgress,
					RowsDone: count,
					Filename: file.Name(),
					Elapsed:  time.Since(start),
				})
				count++
			}
		}

		c.Publish(events.FileEvent{
			Type:     events.FileCompleted,
			RowsDone: 10_000_000,
			Filename: file.Name(),
			Elapsed:  time.Since(start),
		})

		if err := scanner.Err(); err != nil {
			return err
		}
	}

	return nil
}
