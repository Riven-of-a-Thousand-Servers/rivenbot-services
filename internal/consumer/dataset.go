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

	"github.com/klauspost/compress/zstd"
)

const maxSize = 1024 * 1024 * 20 // 20 MBs

type ConsumerOpts struct {
	NumFiles int
	NumLines int
}

type File struct {
	Filename string
	RowsDone int
	Elapsed  time.Duration
	Err      error
}

type FileConsumer struct {
	*pubsub.Broker[File]
	FileIndex FileIndex
	once      sync.Once
	ch        chan Delivery[dataset.RawContent]
	numFiles  int
	numLines  int
}

func NewFileConsumer(idx FileIndex, brokerSize int, opts ConsumerOpts) *FileConsumer {
	return &FileConsumer{
		FileIndex: idx,
		Broker:    pubsub.NewBroker[File](brokerSize),
		numFiles:  opts.NumFiles,
		numLines:  opts.NumLines,
	}
}

func (c *FileConsumer) Consume(ctx context.Context) (<-chan Delivery[dataset.RawContent], error) {
	c.once.Do(func() {
		c.ch = make(chan Delivery[dataset.RawContent])
		go c.Start(ctx)
	})

	return c.ch, nil
}

func (c *FileConsumer) Start(ctx context.Context) error {
	defer close(c.ch)

	fileCount := 0
	for _, entry := range c.FileIndex {
		if c.numFiles > 0 && fileCount >= c.numFiles {
			break
		}

		if err := c.setupFile(ctx, entry); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Consumer stopped: context cancelled")
				return err
			}

			slog.Error("Error scanning file", "path", entry.Path, "error", err)
			return err
		}
		fileCount++
	}

	return nil
}

func (c *FileConsumer) setupFile(ctx context.Context, entry FileEntry) error {
	start := time.Now()
	slog.Info("Starting to setup file for consumption", "file", entry.Filename)
	file, err := os.Open(entry.Path)
	if err != nil {
		slog.Error("Failed to open file", "path", entry.Path, "error", err)
		return err
	}

	defer file.Close()

	bufReader := bufio.NewReader(file)
	decoder, err := zstd.NewReader(bufReader)
	if err != nil {
		slog.Error("Error creating zstd reader", "file", entry.Filename, "error", err)
		return err
	}
	defer decoder.Close()

	buf := make([]byte, maxSize)
	scanner := bufio.NewScanner(decoder)
	scanner.Buffer(buf, maxSize)

	c.Publish(pubsub.FileStarted, File{
		RowsDone: 0,
		Filename: entry.Filename,
		Elapsed:  time.Since(start),
	})

	if err := c.scanLines(ctx, scanner, entry); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	c.Publish(pubsub.FileCompleted, File{
		RowsDone: 10_000_000,
		Filename: file.Name(),
		Elapsed:  time.Since(start),
	})

	return nil
}

func (c *FileConsumer) scanLines(ctx context.Context, scanner *bufio.Scanner, entry FileEntry) error {
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

			c.Publish(pubsub.FileProgress, File{
				RowsDone: lineCount,
				Filename: entry.Filename,
			})
		}
		lineCount++
	}
	return nil
}
