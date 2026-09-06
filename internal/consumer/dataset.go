package consumer

import (
	"bufio"
	"context"
	"errors"
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
	ch        chan Delivery[dataset.RawContent]
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

func (c *DatasetConsumer) Consume(ctx context.Context) (<-chan Delivery[dataset.RawContent], error) {
	c.once.Do(func() {
		c.ch = make(chan Delivery[dataset.RawContent])
		go c.Start(ctx)
	})

	return c.ch, nil
}

func (c *DatasetConsumer) Start(ctx context.Context) error {
	defer close(c.ch)

	fileCount := 0
	for filepath, entry := range c.FileIndex {
		if c.numFiles > 0 && fileCount >= c.numFiles {
			break
		}

		if err := c.setupFile(ctx, filepath, entry); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Consumer stopped: context cancelled", "path", filepath)
				return err
			}

			slog.Error("Error scanning file", "path", filepath, "error", err)
			return err
		}
		fileCount++
	}

	return nil
}

func (c *DatasetConsumer) setupFile(ctx context.Context, path string, entry *FileEntry) error {
	start := time.Now()
	slog.Info("Starting to setup file for consumption", "file", entry.Name)
	file, err := os.Open(path)
	if err != nil {
		slog.Error("Failed to open file", "path", path, "error", err)
		return err
	}

	defer file.Close()

	bufReader := bufio.NewReader(file)
	decoder, err := zstd.NewReader(bufReader)
	if err != nil {
		slog.Error("Error creating zstd reader", "file", entry.Name, "error", err)
		return err
	}
	defer decoder.Close()

	buf := make([]byte, maxSize)
	scanner := bufio.NewScanner(decoder)
	scanner.Buffer(buf, maxSize)

	entry.SetStarted()
	c.Publish(events.FileEvent{
		Type:     events.FileStarted,
		RowsDone: 0,
		Filename: entry.Name,
		Elapsed:  time.Since(start),
	})

	if err := c.scanLines(ctx, scanner, entry); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	entry.SetDone()
	c.Publish(events.FileEvent{
		Type:     events.FileCompleted,
		RowsDone: 10_000_000,
		Filename: file.Name(),
		Elapsed:  time.Since(start),
	})

	return nil
}

func (c *DatasetConsumer) scanLines(ctx context.Context, scanner *bufio.Scanner, entry *FileEntry) error {
	lineCount := 0

ScanLoop:
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if c.numLines > 0 && lineCount >= c.numLines {
				break ScanLoop
			}

			payload := dataset.RawContent(scanner.Bytes())

			select {
			case c.ch <- Delivery[dataset.RawContent]{
				Payload: payload,
				Ack: func() error {
					return nil
				},
				Nack: func(requeue bool) error {
					return nil
				},
				Headers: map[string]any{
					"source": "dataset",
				},
			}:
			case <-ctx.Done():
				return ctx.Err()
			}

			c.Publish(events.FileEvent{
				Type:     events.FileProgress,
				RowsDone: lineCount,
				Filename: entry.Name,
			})
		}
		lineCount++
	}
	return nil
}
