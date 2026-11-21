package app

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/pipeline"
	"pgcr-processing-service/internal/publisher"
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/walker"
	"pgcr-processing-service/internal/writer"

	"github.com/deahtstroke/chainmorph"
	"golang.org/x/sync/errgroup"
)

var (
	samplerInterval    = 200 * time.Millisecond
	logFileName        = "dataset.log"
	deadLetterFileName = "deadletter.log"
)

type BackfillApp struct {
	tracker *telemetry.Tracker
	walker  *walker.FileWalker
	cache   *cache.InMemoryCache
	mapper  *mapper.PgcrMapper
	config  *config.Config

	cleanup []func() error

	noTui          bool
	quarantineFile *os.File
	logFile        *os.File
}

func NewBackfill(db *sql.DB, tracker *telemetry.Tracker) (*BackfillApp, error) {
	var err error

	app := &BackfillApp{}
	cfg, err := app.GetConfig()

	cache := cache.NewInMemoryCache()
	mapper := mapper.New(cache)

	fileWalker := walker.NewFileWalker(cfg.Backfill.WalkerOpts.RootDir)

	if err != nil {
		slog.Error("unable to fetch config for backfill application", "error", err)
		return nil, err
	}

	app.tracker = tracker
	app.cache = cache
	app.mapper = mapper
	app.walker = fileWalker

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

func (a *BackfillApp) RunOnce(ctx context.Context, file string) error {
	dbUrl := a.config.Database.URL
	conn, err := db.Connect(ctx, dbUrl)
	if err != nil {
		return err
	}

	queries, err := db.Prepare(ctx, conn)
	if err != nil {
		return err
	}

	var eg *errgroup.Group
	eg, ctx = errgroup.WithContext(ctx)

	files, err := a.walker.WalkAndAccumulate()
	if err != nil {
		return err
	}

	apiKey := a.config.Backfill.Processor.ApiKey
	if apiKey == "" {
		return fmt.Errorf("API key not set for the current config")
	}

	if err := a.cache.Init(ctx, apiKey, manifest.InventoryItemDefinition,
		manifest.ActivityDefinition,
		manifest.DestinationDefinition,
		manifest.EquipmentSlotDefinition,
		manifest.DamageTypeDefinition); err != nil {
		return err
	}

	mapper := mapper.New(a.cache)

	scanPublisher := publisher.NewScanningPublisher(a.tracker, publisher.WithFileAmount(1))
	eg.Go(func() error {
		return scanPublisher.Start(ctx, files...)
	})

	pgcrWriter := writer.NewPgcrWriter(conn, queries, mapper)

	// Don't need a file index, just read one file
	eg.Go(func() error {
		return chainmorph.From(pipeline.NewChannelReader(scanPublisher.Out())).
			MapFunc(pipeline.MapRawPgcr).
			If(pipeline.FilterRaids).
			WriteTo(ctx, pgcrWriter)
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("Error while running command for file %s: %v", file, err)
	}

	return nil
}

func (a *BackfillApp) GetConfig() (*config.Config, error) {
	var err error
	if a.config == nil {
		a.config, err = config.Init()
		if err != nil {
			return nil, err
		}
	}

	return a.config, nil
}

func (a *BackfillApp) GetTracker() (*telemetry.Tracker, error) {
	if a.tracker == nil {
		return nil, fmt.Errorf("tracker instance not created yet for this backfill app")
	}

	return a.tracker, nil
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
