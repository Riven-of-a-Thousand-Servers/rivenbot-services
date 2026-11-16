package app

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"slices"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/walker"
)

var (
	samplerInterval    = 200 * time.Millisecond
	logFileName        = "dataset.log"
	deadLetterFileName = "deadletter.log"
)

type BackfillAppConfig struct {
	WalkerRootDir string
	noTui         bool
}

type BackfillApp struct {
	tracker *telemetry.Tracker
	sampler *telemetry.MetricsSampler
	walker  *walker.FileWalker
	files   *consumer.FileConsumer
	cache   *cache.InMemoryCache
	mapper  *mapper.PgcrMapper

	cleanup []func() error

	noTui      bool
	deadLetter io.Writer
	logs       io.Writer
}

func NewBackfill(ctx context.Context, db *sql.DB, tracker *telemetry.Tracker, cfg BackfillAppConfig) (*BackfillApp, error) {
	fileWalker := walker.NewFileWalker(cfg.WalkerRootDir)
	metricsSampler := telemetry.NewSampler(telemetry.Samples, samplerInterval)
	cache := cache.NewInMemoryCache()
	mapper := mapper.New(cache)

	app := &BackfillApp{
		walker:  fileWalker,
		tracker: tracker,
		sampler: metricsSampler,
		cache:   cache,
		mapper:  mapper,
	}

	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	deadLetterFile, err := os.OpenFile(deadLetterFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	app.cleanup = append(app.cleanup,
		deadLetterFile.Close,
		logFile.Close,
		metricsSampler.Shutdown,
		tracker.Close,
	)

	return app, nil
}

// This will run the backfill job on just one file
func (a *BackfillApp) RunOnce(ctx context.Context, file string) error {
	panic("Not implemented")
}

func (a *BackfillApp) Clean() {
	for _, c := range slices.Backward(a.cleanup) {
		if err := c(); err != nil {
			slog.Error("Error while cleaning up", "error", err)
		}
	}
}
