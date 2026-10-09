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
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/types/dataset"
	"pgcr-processing-service/internal/walker"

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
	fileIndex walker.FileIndex
	tracker   *telemetry.Tracker

	once     sync.Once
	ch       chan telemetry.Job[Delivery[dataset.RawContent]]
	numFiles int
	numLines int
}

func NewFileConsumer(idx walker.FileIndex,
	tracker *telemetry.Tracker,
	brokerSize int,
	opts ConsumerOpts,
) *FileConsumer {
	return &FileConsumer{
		fileIndex: idx,
		tracker:   tracker,
		Broker:    pubsub.NewBroker[File](brokerSize),
		numFiles:  opts.NumFiles,
		numLines:  opts.NumLines,
	}
}

func (c *FileConsumer) Consume(ctx context.Context) (<-chan telemetry.Job[Delivery[dataset.RawContent]], error) {
	c.once.Do(func() {
		c.ch = make(chan telemetry.Job[Delivery[dataset.RawContent]])
		go c.Start(ctx)
	})

	return c.ch, nil
}

func (c *FileConsumer) Start(ctx context.Context) error {
	defer close(c.ch)

	fileCount := 0
	for _, entry := range c.fileIndex {
		if c.numFiles > 0 && fileCount >= c.numFiles {
			break
		}

		task := c.tracker.AddTask(entry.Filename)
		task.StartedAt = time.Now()
		if err := c.setupFile(ctx, entry, task); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Consumer stopped: context cancelled")
				return err
			}

			slog.Error("Error scanning file", "path", entry.Path, "error", err)
			return err
		}
		task.FinishedAt = time.Now()
	}

	return nil
}

func (c *FileConsumer) setupFile(ctx context.Context, entry walker.FileEntry, task *telemetry.Task) error {
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

	if err := c.scanLines(ctx, scanner, task); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (c *FileConsumer) scanLines(ctx context.Context, scanner *bufio.Scanner, task *telemetry.Task) error {
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
			delivery := emptyDeliveryDS(payload)
			Job := telemetry.Job[Delivery[dataset.RawContent]]{
				Task: task,
				ToDo: delivery,
			}

			select {
			case c.ch <- Job:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		lineCount++
	}
	return nil
}
