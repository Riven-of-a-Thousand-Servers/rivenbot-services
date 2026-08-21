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

type ConsumerOpts struct {
	NumFiles int
	NumLines int
}

type DatasetConsumer struct {
	*pubsub.Broker[events.FileEvent]
	FileIndex FileIndex
	once      sync.Once
	ch        chan Delivery[dataset.Entry]
	numFiles  int
	numLines  int
}

func NewDatasetConsumer(idx FileIndex, brokerSize int, opts ConsumerOpts) *DatasetConsumer {
	return &DatasetConsumer{
		FileIndex: idx,
		Broker:    pubsub.NewBroker[events.FileEvent](brokerSize),
		numFiles:  opts.NumFiles,
		numLines:  opts.NumLines,
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

	fileCount := 0
	for filepath, entry := range c.FileIndex {
		if c.numFiles != 0 && fileCount > c.numFiles {
			return nil
		}

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

		lineCount := 0
		count := 0
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				if c.numFiles > 0 && lineCount > c.numFiles {
					break
				}

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

				lineCount++
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

		fileCount++
	}

	return nil
}
