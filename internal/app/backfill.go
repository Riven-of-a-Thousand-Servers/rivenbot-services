package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"

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

var deadLetterFileName = "deadletter.log"

type BackfillApp struct {
	config  *config.BackfillConfig
	tracker *telemetry.Tracker
	cache   *cache.InMemoryCache
	mapper  *mapper.PgcrMapper

	cleanup []func() error

	noTui          bool
	quarantineFile *os.File
	logFile        *os.File
}

var _ Backfill = (*BackfillApp)(nil)

func NewBackfill(cfg *config.BackfillConfig) (*BackfillApp, error) {
	var err error

	app := &BackfillApp{}

	cache := cache.NewInMemoryCache()
	mapper := mapper.New(cache)

	app.cache = cache
	app.mapper = mapper
	app.config = cfg

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
	dbUrl := ""
	conn, err := db.Connect(ctx, dbUrl)
	if err != nil {
		return err
	}
	a.cleanup = append(a.cleanup, conn.Close)

	queries, err := db.Prepare(ctx, conn)
	if err != nil {
		return err
	}
	a.cleanup = append(a.cleanup, queries.Close)

	var eg *errgroup.Group
	eg, ctx = errgroup.WithContext(ctx)

	dirFilters := []walker.FilterFunc{
		walker.ExcludeHidden,
		walker.ExcludeReserved,
	}

	files, err := walker.WalkAndAccumulate(
		a.config.WalkerOpts.RootDir,
		a.config.WalkerOpts.Filepattern,
		dirFilters...,
	)
	if err != nil {
		slog.Error("failed to walk and accumulate files", "error", err)
		return err
	}

	tracker, err := telemetry.NewTracker(len(files))
	if err != nil {
		slog.Error("failed to create telemetry tracker", "error", err)
		return err
	}

	a.tracker = tracker

	apiKey := a.config.Processor.ApiKey
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

func (a *BackfillApp) Tracker() *telemetry.Tracker {
	return a.tracker
}

func (a *BackfillApp) Out() io.Writer {
	return os.Stdout
}

func (a *BackfillApp) In() io.Reader {
	return os.Stdin
}

func (a *BackfillApp) Close() error {
	var errs []error
	for _, c := range slices.Backward(a.cleanup) {
		if err := c(); err != nil {
			slog.Error("Error while cleaning up", "error", err)
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (a *BackfillApp) DeadLetter() *os.File {
	return a.quarantineFile
}
