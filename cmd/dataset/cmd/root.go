/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/process"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/runner"
	ui "pgcr-processing-service/internal/tui"
	"pgcr-processing-service/internal/types/manifest"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type datasetOpts struct {
	RootDir    string
	DbUrl      string
	ApiKey     string
	Goroutines int
	Noop       bool
}

func newRootCommand() *cobra.Command {
	var opts datasetOpts

	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Runs a one-off job to process PGCRs from the Asun ZSTD dataset to backfill database",
		Long: `This command spins up several workers to backfill the Rivenbot database from the Asun
dataset`,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			type cleanupFunc func() error
			var cleanup []cleanupFunc
			defer func(clean []cleanupFunc) {
				for _, c := range clean {
					c()
				}
			}(cleanup)

			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
			defer cancel()

			// open a custom json-log file
			logFile, err := os.OpenFile("dataset-import.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			defer logFile.Close()

			// Will only log errors
			slog.SetDefault(slog.New(slog.NewJSONHandler(logFile, &slog.HandlerOptions{
				Level: slog.LevelInfo.Level(),
			})))

			g, groupCtx := errgroup.WithContext(ctx)

			program := tea.NewProgram(ui.NewModel(cancel), tea.WithContext(groupCtx))
			g.Go(func() error {
				_, err := program.Run()
				return err
			})
			// Discover all .zst files before anything
			// This cannot fail, otherwise everything goes to shit
			discoverer := consumer.NewDiscoverer(opts.RootDir)
			files, err := discoverer.Discover(groupCtx, ".zst")
			if err != nil {
				return err
			}

			consumer := consumer.NewDatasetConsumer(files, 2048)
			cache := cache.NewInMemoryCache[manifest.Entry](5)
			mapper := mapper.New(cache)

			var processor *process.DatasetProcessor
			switch {
			case opts.Noop:
				processor = process.NewDatasetProcessor(process.NoOpProcessor[json.RawMessage]())
			default:
				conn, err := db.Connect(groupCtx, opts.DbUrl)
				if err != nil {
					return err
				}
				defer conn.Close()

				queries, err := db.Prepare(groupCtx, conn)
				if err != nil {
					return err
				}

				inner := process.NewPgcrProcessor(conn, queries, mapper)
				processor = process.NewDatasetProcessor(inner)
			}

			if err = cache.Prepopulate(groupCtx,
				opts.ApiKey,
				manifest.InventoryItemDefinition,
				manifest.ActivityDefinition,
				manifest.DestinationDefinition,
				manifest.EquipmentSlotDefinition,
				manifest.DamageTypeDefinition); err != nil {
				return err
			}

			worker := runner.NewWorker(processor, consumer)
			for range opts.Goroutines {
				g.Go(func() error {
					return worker.Begin(groupCtx)
				})
			}

			// setup events
			var eventsWg sync.WaitGroup
			eventsCh := make(chan tea.Msg, 1000)
			setupEvents(ctx, &eventsWg, eventsCh, processor)
			setupEvents(ctx, &eventsWg, eventsCh, cache)
			setupEvents(ctx, &eventsWg, eventsCh, consumer)

			cleanup = append(cleanup, func() error {
				eventsWg.Wait()
				return nil
			})

			go publishEventsToTea(ctx, program, eventsCh)

			err = g.Wait()
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("Error during execution of dataset", "error", err)
				return err
			}

			return nil
		},
	}

	flags := cmd.Flags()
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
