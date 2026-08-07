package pgcrdataset

import (
	"bufio"
	"context"
	"log/slog"
	"os"

	"github.com/klauspost/compress/zstd"
)

const maxSize = 1024 * 1024 * 20 // 20 MBs

type Ingester struct {
	FileIndex FileIndex
	Pipeline  chan<- DatasetEntry
}

func NewIngester(idx FileIndex, pipeline chan<- DatasetEntry) *Ingester {
	return &Ingester{FileIndex: idx, Pipeline: pipeline}
}

func (i *Ingester) Start(ctx context.Context) error {
	for filename, entry := range i.FileIndex {
		file, err := os.Open(entry.Path)
		if err != nil {
			slog.Error("Error while opening file", "file", file, "error", err)
			return err
		}

		bufReader := bufio.NewReader(file)
		decoder, err := zstd.NewReader(bufReader)

		buf := make([]byte, maxSize)
		scanner := bufio.NewScanner(decoder)
		scanner.Buffer(buf, maxSize)

		entry.Started = true
		entry.Progress = make(chan int64)

		count := 0
		for scanner.Scan() {
			select {
			case <-ctx.Done():
				close(i.Pipeline)
				return ctx.Err()
			default:
				i.Pipeline <- DatasetEntry{
					Bytes:    scanner.Bytes(),
					Filename: filename,
					Number:   int64(count + 1),
				}

				entry.Progress <- int64(1)
				count++
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}

	close(i.Pipeline)
	return nil
}
