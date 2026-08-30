package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/pipeline"
	"pgcr-processing-service/internal/types/dataset"
	"pgcr-processing-service/internal/types/pgcr"
)

type ChannelReader[T any] struct {
	Input <-chan T
}

func (c ChannelReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	select {
	case <-ctx.Done():
		return zero, nil
	case item, ok := <-c.Input:
		if !ok {
			return zero, fmt.Errorf("Channel is closed")
		}

		return item, nil
	}
}

type PayloadMapper struct{}

func (p PayloadMapper) Map(ctx context.Context, item consumer.Delivery[dataset.Entry]) (pgcr.PostGameCarnageReport, error) {
	var i pgcr.PostGameCarnageReport
	if err := json.Unmarshal(item.Payload.Bytes, &i); err != nil {
		return i, err
	}

	return i, nil
}

// POC: Process a file into the database
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	discoverer := consumer.NewDiscoverer("/Volumes/T7 Shield/")
	fileIdx, err := discoverer.Discover(ctx, ".zst")
	if err != nil {
		slog.Error("Unable to discover files of extension '.zst'", "error", err)
		os.Exit(1)
	}
	c := consumer.NewDatasetConsumer(fileIdx, 10, consumer.ConsumerOpts{NumFiles: 1, NumLines: 100_000})
	ch, err := c.Consume(ctx)

	r := ChannelReader[consumer.Delivery[dataset.Entry]]{Input: ch}
	m1 := PayloadMapper{}
	w := &pipeline.StdoutWriter[pgcr.PostGameCarnageReport]{}

	err = pipeline.From(r).
		MapTo(m1).
		If(func(item pgcr.PostGameCarnageReport) bool {
			return item.ActivityDetails.Mode == 4
		}).
		WriteTo(ctx, w)
}
