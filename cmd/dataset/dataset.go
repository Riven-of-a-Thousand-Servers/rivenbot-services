/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package dataset

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"pgcr-processing-service/internal/app"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	EventChanSize = 20_000
)

func NewCommand(app *app.BackfillApp) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "dataset",
		Short:        "Process a Destiny 2 PGCR dataset",
		SilenceUsage: true,
		// RunE: func(cmd *cobra.Command, args []string) error {
		// globalCtx, globalCancel := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
		// defer globalCancel()
		//
		// // open a custom json-log file
		//
		// handlerOptions := &slog.HandlerOptions{
		// 	Level: slog.LevelInfo.Level(),
		// }
		//
		// textHandler := slog.NewTextHandler(logFile, handlerOptions)
		// ringBuf := telemetry.NewRing[telemetry.LogEntry](30)
		// tuiHandler := logging.NewTuiHandler(ringBuf, slog.LevelInfo.Level())
		// metricsSampler := telemetry.NewSampler(
		// 	telemetry.Samples,
		// 	200*time.Millisecond,
		// )
		// defer metricsSampler.Shutdown()
		//
		// slog.SetDefault(slog.New(slog.NewMultiHandler(textHandler, tuiHandler)))
		//
		// // Main recovery statement in case anything panics in the main goroutine
		// defer func() {
		// 	if r := recover(); r != nil {
		// 		slog.Error("Fatal panc:", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
		// 	}
		// }()
		//
		// eg, groupCtx := errgroup.WithContext(globalCtx)
		//
		// // Discover all .zst files before anything
		// // This cannot fail, otherwise everything goes to shit
		// fileWalker := walker.NewFileWalker(opts.RootDir,
		// 	walker.WithEventsEnabled,
		// 	walker.WithDirFilter(walker.ExcludeHidden),
		// 	walker.WithDirFilter(walker.ExcludeReserved),
		// 	walker.WithFileFilter(walker.WithExtension("zst")),
		// 	walker.WithFileFilter(walker.WithPattern(opts.FilePattern)))
		// files, err := fileWalker.WalkAndAccumulate()
		// if err != nil {
		// 	return err
		// }
		//
		// samples, _ := metricsSampler.Subscribe()
		//
		// buckets := []time.Duration{
		// 	200 * time.Millisecond,
		// 	1 * time.Microsecond,
		// 	10 * time.Microsecond,
		// 	100 * time.Microsecond,
		// 	1 * time.Millisecond,
		// 	10 * time.Millisecond,
		// }
		//
		// hist := telemetry.NewHistogram(
		// 	globalCtx,
		// 	samples,
		// 	telemetry.GarbageCollectionCycles,
		// 	buckets...,
		// )
		//
		// tracker := telemetry.NewTracker(ringBuf, hist, len(files))
		//
		// // Setup Tea.Program before cache start prepopulating so we can capture events
		// program := tea.NewProgram(ui.NewModel(tracker, globalCancel), tea.WithContext(groupCtx))
		// goSafe(eg, func() error {
		// 	_, err := program.Run()
		// 	return err
		// })
		//
		// // setup events
		// var eventsWg sync.WaitGroup
		//
		// eventsCh := make(chan tea.Msg, EventChanSize)
		// go publishEventsToTea(groupCtx, program, eventsCh)
		//
		// datasetConsumer := consumer.NewFileConsumer(files, tracker,
		// 	consumer.ConsumerOpts{
		// 		NumFiles: opts.NumFiles,
		// 		NumLines: opts.NumLines,
		// 	})
		//
		// cache := cache.NewInMemoryCache()
		// setupEvents(globalCtx, &eventsWg, eventsCh, cache)
		//
		// cleanup = append(cleanup, func() error {
		// 	globalCancel()
		// 	eventsWg.Wait()
		// 	return nil
		// })
		//
		// // Prepopulate immediately before instantiating mapper
		// if err = cache.Init(groupCtx,
		// 	opts.ApiKey,
		// 	manifest.InventoryItemDefinition,
		// 	manifest.ActivityDefinition,
		// 	manifest.DestinationDefinition,
		// 	manifest.EquipmentSlotDefinition,
		// 	manifest.DamageTypeDefinition); err != nil {
		// 	return err
		// }
		//
		// mapper := mapper.New(cache)
		// conn, err := db.Connect(groupCtx, opts.DbUrl)
		// if err != nil {
		// 	return err
		// }
		// cleanup = append(cleanup, conn.Close)
		//
		// // So we reuse as many connections as possible
		// // the extra 4 is for resource contention headroom
		// conn.SetMaxIdleConns(opts.Goroutines + 4)
		// conn.SetMaxOpenConns(opts.Goroutines + 4)
		// conn.SetConnMaxLifetime(30 * time.Minute)
		//
		// queries, err := db.Prepare(groupCtx, conn)
		// if err != nil {
		// 	return err
		// }
		// cleanup = append(cleanup, queries.Close)
		//
		// pgcrWriter := writer.NewPgcrWriter(conn, queries, mapper)
		// tracker.RegisterGauge("Writing", "workers", func() (queueLength int, queueCap int) {
		// 	return pgcrWriter.Writing(), opts.Goroutines
		// })
		//
		// var itemWriter chainmorph.ItemWriter[telemetry.Job[bungie.PostGameCarnageReport]]
		// itemWriter = writer.NewRetryWriter(pgcrWriter, 3*time.Millisecond, 3)
		// itemWriter = writer.NewDeadLetterWriter(deadLetterFile, itemWriter,
		// 	func(item telemetry.Job[bungie.PostGameCarnageReport]) []byte {
		// 		// TODO: This function should map the type T to a byte array that will
		// 		// be saved in the DeadLetterFile, in this case, it will be a
		// 		// telemetry Job with a PGCR
		// 		return []byte("Hello world")
		// 	})
		//
		// tracker.RegisterGauge("Mapping", "workers", func() (gaugeLen int, gaugeCap int) {
		// 	return int(pipeline.MapCount.Load()), opts.Goroutines
		// })
		// goSafe(eg, func() error {
		// 	return datasetConsumer.Start(globalCtx)
		// })
		//
		// consumerCh := datasetConsumer.Out()
		//
		// for range opts.Goroutines {
		// 	goSafe(eg, func() error {
		// 		return chainmorph.From(pipeline.NewChannelReader(consumerCh)).
		// 			MapFunc(pipeline.MapRawPgcr).
		// 			If(pipeline.FilterRaids).
		// 			WriteTo(globalCtx, itemWriter)
		// 	})
		// }
		//
		// err = eg.Wait()
		// if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, tea.ErrProgramKilled) {
		// 	slog.Error("Error during execution of dataset", "error", err)
		// 	return err
		// }
		//
		// 	return nil
		// },
	}

	// cmd.AddCommand(
	// 	newDatasetRunCommand(),
	// 	newDatasetOnceCommand(),
	// )

	return cmd
}

// Wrapper function around the errgroup's Go() function
// Adds a defer statement that recovers the Goroutine in case
// of a panic, logs it using slog and then overrides the return
// value with an error instead
func goSafe(g *errgroup.Group, f func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Panic in worker", slog.Any("panic", r), slog.String("Stack", string(debug.Stack())))
				err = fmt.Errorf("worker panic: %v", err)
			}
		}()
		return f()
	})
}
