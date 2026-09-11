/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/pubsub"
	ui "pgcr-processing-service/internal/tui"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/writer"

	"pgcr-processing-service/internal/pipeline"

	tea "charm.land/bubbletea/v2"
	"github.com/deahtstroke/chainmorph"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

const (
	DatasetBrokerSize     = 48
	FileBrokerSize        = 1000
	EventThroughput       = 20_000
	CacheEventsBrokerSize = 10
)

type datasetOpts struct {
	RootDir    string
	DbUrl      string
	ApiKey     string
	Goroutines int
	Noop       bool
	NumFiles   int
	NumLines   int
}

func newRootCommand() *cobra.Command {
	var opts datasetOpts

	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Runs a one-off job to process PGCRs from the Asun ZSTD dataset to backfill database",
		Long: `This command spins up several workers to backfill the Rivenbot database from the Asun
dataset`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			type cleanupFunc func() error
			var cleanup []cleanupFunc
			defer func() {
				for _, c := range slices.Backward(cleanup) {
					if err := c(); err != nil {
						slog.Error("Error while cleaning up", "error", err)
					}
				}
			}()

			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
			defer cancel()

			// open a custom json-log file
			logFile, err := os.OpenFile("dataset-import.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}

			cleanup = append(cleanup, logFile.Close)

			slog.SetDefault(slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{
				Level: slog.LevelInfo.Level(),
			})))

			g, groupCtx := errgroup.WithContext(ctx)

			// Discover all .zst files before anything
			// This cannot fail, otherwise everything goes to shit
			walker := consumer.NewFileWalker(opts.RootDir)
			files, err := walker.DiscoverFunc(
				consumer.HasExtension(".zst"),
				consumer.NotHiddenFile,
				consumer.NotReserved)
			if err != nil {
				return err
			}

			// Setup Tea.Program before cache start prepopulating so we can capture events
			program := tea.NewProgram(ui.NewModel(cancel), tea.WithContext(groupCtx))
			g.Go(func() error {
				_, err := program.Run()
				return err
			})

			// setup events
			var eventsWg sync.WaitGroup

			eventsCh := make(chan tea.Msg, EventThroughput)
			go publishEventsToTea(groupCtx, program, eventsCh)

			datasetConsumer := consumer.NewFileConsumer(files,
				DatasetBrokerSize,
				consumer.ConsumerOpts{
					NumFiles: opts.NumFiles,
					NumLines: opts.NumLines,
				})
			cache := cache.NewInMemoryCache[manifest.Entry](CacheEventsBrokerSize)
			setupEvents(ctx, &eventsWg, eventsCh, cache)

			// Prepopulate immediately before instantiating mapper
			if err = cache.Prepopulate(groupCtx,
				opts.ApiKey,
				manifest.InventoryItemDefinition,
				manifest.ActivityDefinition,
				manifest.DestinationDefinition,
				manifest.EquipmentSlotDefinition,
				manifest.DamageTypeDefinition); err != nil {
				return err
			}

			mapper := mapper.New(cache)
			var itemWriter chainmorph.ItemWriter[bungie.PostGameCarnageReport]
			switch {
			case opts.Noop:
				itemWriter = writer.NoOpProcessor[bungie.PostGameCarnageReport]()
			default:
				conn, err := db.Connect(groupCtx, opts.DbUrl)
				if err != nil {
					return err
				}
				cleanup = append(cleanup, conn.Close)

				queries, err := db.Prepare(groupCtx, conn)
				if err != nil {
					return err
				}
				cleanup = append(cleanup, queries.Close)

				itemWriter = writer.NewPgcrWriter(conn, queries, mapper, FileBrokerSize)
			}

			ch, err := datasetConsumer.Consume(ctx)
			if err != nil {
				return err
			}

			setupEvents(ctx, &eventsWg, eventsCh, datasetConsumer)

			for range opts.Goroutines {
				g.Go(func() error {
					return chainmorph.From(pipeline.NewChannelReader(ch)).
						MapFunc(pipeline.MapRawPgcr).
						If(pipeline.FilterRaids).
						WriteTo(ctx, itemWriter)
				})
			}

			err = g.Wait()
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, tea.ErrProgramKilled) {
				slog.Error("Error during execution of dataset", "error", err)
				return err
			}

			return nil
		},
	}

	flags := cmd.Flags()
	flags.IntVar(&opts.NumLines, "lines", 0, "Number of rows-per-file to process")
	flags.IntVar(&opts.NumFiles, "files", 0, "Number of files to process")
	flags.StringVarP(&opts.RootDir, "root-dir", "r", "", "Root directory to scan files from")
	flags.StringVarP(&opts.DbUrl, "db-url", "d", "", "URL to the Postgres DB")
	flags.StringVarP(&opts.ApiKey, "api-key", "a", "", "Bungie.net API key")
	flags.BoolVar(&opts.Noop, "noop", false, "If the processor to be used is a Noop processor")
	flags.IntVarP(&opts.Goroutines, "goroutines", "g", 1, "Number of workers to spin up")

	return cmd
}

func publishEventsToTea(ctx context.Context, program *tea.Program, out <-chan tea.Msg) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("Context cancelled, TUI message handler is shutting down")
			return
		case msg, ok := <-out:
			if !ok {
				slog.Debug("TUI message channel closed")
				return
			}

			slog.Info("Publishing event", "msg", msg)
			program.Send(msg)
		}
	}
}

// Setup events takes a subscriber, subscribes to it and asynchronously
// emits the events from its chanel down to the output channel that will
// comunicate with the tea.Program
func setupEvents[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	outCh chan<- tea.Msg,
	subscriber pubsub.Subscriber[T],
) {
	wg.Go(func() {
		subCh, _ := subscriber.Subscribe()
		for {
			select {
			case e, ok := <-subCh:
				if !ok {
					slog.Debug("Subscription channel closed")
					return
				}

				var msg tea.Msg = e
				select {
				case outCh <- msg:
				case <-ctx.Done():
					slog.Debug("Global context was cancelled. Returning")
					return
				}

			case <-ctx.Done():
				slog.Debug("Global context was cancelled. Returning")
				return
			}
		}
	})
}

// rootCmd represents the base command when called without any subcommands

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newRootCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}
