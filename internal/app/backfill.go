package app

import (
	"context"
	"database/sql"
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
	ApiKey        string
	noTui         bool
}

type BackfillApp struct {
	tracker *telemetry.Tracker
	walker  *walker.FileWalker
	files   *consumer.FileConsumer
	cache   *cache.InMemoryCache
	mapper  *mapper.PgcrMapper

	cleanup []func() error

	noTui          bool
	quarantineFile *os.File
	logFile        *os.File
}

func NewBackfill(db *sql.DB, tracker *telemetry.Tracker, cfg BackfillAppConfig) (*BackfillApp, error) {
	var err error

	fileWalker := walker.NewFileWalker(cfg.WalkerRootDir)
	cache := cache.NewInMemoryCache()
	mapper := mapper.New(cache)

	app := &BackfillApp{
		walker:  fileWalker,
		tracker: tracker,
		cache:   cache,
		mapper:  mapper,
	}

	app.quarantineFile, err = os.Open(deadLetterFileName)
	if err != nil {
		return nil, err
	}

	app.cleanup = append(app.cleanup,
		app.quarantineFile.Close,
		app.tracker.Close,
	)

	return app, nil
}

// This will run the backfill job on just one file
func (a *BackfillApp) RunOnce(ctx context.Context, file string, apiKey string) error {
	// TODO: Implement the chainmorph run here
	panic("Not implemented")
}

func (a *BackfillApp) Clean() {
	for _, c := range slices.Backward(a.cleanup) {
		if err := c(); err != nil {
			slog.Error("Error while cleaning up", "error", err)
		}
	}
}

func (a *BackfillApp) DeadLetter() *os.File {
	return a.quarantineFile
}
