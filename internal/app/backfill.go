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

type BackfillAppConfig struct {
	WalkerRootDir string
	ApiKey        string
	noTui         bool
}

type BackfillApp struct {
	tracker *telemetry.Tracker
	walker  *walker.FileWalker
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

func (a *BackfillApp) RunOnce(ctx context.Context, file string, apiKey string, dbUrl string) error {
	if dbUrl == "" {
		return fmt.Errorf("Databse url is needed")
	}

	if apiKey == "" {
		return fmt.Errorf("Api Key is needed as well")
	}

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
