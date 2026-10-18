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

const (
	maxSize      = 1024 * 1024 * 20 // 20 MBs
	linesPerFile = 10_000_000
	queueName    = "File Consumer"
	queueUnit    = "Lines per File"
)

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
		c.ch = make(chan telemetry.Job[Delivery[dataset.RawContent]], 512)
		unregister := c.tracker.RegisterGauge(queueName, queueUnit, func() (queueLength int, queueCap int) {
			return len(c.ch), cap(c.ch)
		})
		go c.Start(ctx, unregister)
	})

	return c.ch, nil
}

func (c *FileConsumer) Start(ctx context.Context, unregisterFunc func()) error {
	defer close(c.ch)
	defer unregisterFunc()

	fileCount := 0
	for _, entry := range c.fileIndex {
		if c.numFiles > 0 && fileCount >= c.numFiles {
			break
		}

		task := c.tracker.AddTask(entry.Filename, linesPerFile)
		task.SetStarted()
		if err := c.prepareFile(ctx, entry, task); err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Consumer stopped: context cancelled")
				task.SetFinished()
				return err
			}

			slog.Error("Error scanning file", "path", entry.Path, "error", err)
			task.SetErrored()
			return err
		}
		task.SetFinished()
	}

	return nil
}

func (c *FileConsumer) prepareFile(ctx context.Context, entry walker.FileEntry, task *telemetry.Task) error {
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

	if err := c.consumeLines(ctx, scanner, task); err != nil {
		return err
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}

func (c *FileConsumer) consumeLines(ctx context.Context, scanner *bufio.Scanner, task *telemetry.Task) error {
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

			job := CreateJob(scanner, lineCount, task)

			select {
			case c.ch <- job:
			case <-ctx.Done():
				return ctx.Err()
			}

			task.IncrementBytesRead(job.ToDo.Payload.Len())
			task.IncrementLinesRead()
		}
		lineCount++
	}
	return nil
}

func CreateJob(scanner *bufio.Scanner, lineCount int, task *telemetry.Task) telemetry.Job[Delivery[dataset.RawContent]] {
	payload := dataset.RawContent(scanner.Bytes())
	delivery := NewDelivery(payload)
	delivery.Add(OffsetHeader, lineCount)

	Job := telemetry.Job[Delivery[dataset.RawContent]]{
		Task: task,
		ToDo: delivery,
	}
	return Job
}
