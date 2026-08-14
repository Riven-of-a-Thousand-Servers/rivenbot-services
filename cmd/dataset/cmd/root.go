/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/process"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/runner"
	ui "pgcr-processing-service/internal/tui"
	"pgcr-processing-service/internal/types/dataset"
	"pgcr-processing-service/internal/types/manifest"
	uiEvents "pgcr-processing-service/internal/types/ui"

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
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGTERM, syscall.SIGINT)
			defer cancel()

			// open a custom json-log file
			logFile, err := os.OpenFile("dataset-import.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			defer logFile.Close()

			slog.SetDefault(slog.New(slog.NewJSONHandler(logFile, nil)))

			// Discover all .zst files before anything
			// This cannot fail, otherwise everything goes to shit
			discoverer := consumer.NewDiscoverer(opts.RootDir)
			files, err := discoverer.Discover(ctx, ".zst")
			if err != nil {
				return err
			}

			g, ctx := errgroup.WithContext(ctx)
			broker := pubsub.NewBroker[uiEvents.Event](2048)
			defer broker.Shutdown()

			consumer := consumer.NewDatasetConsumer(files, broker)

			events, unsub := broker.Subscribe()
			defer unsub()

			var processor process.Processor[dataset.Entry]
			switch {
			case opts.Noop:
				processor = process.NewDatasetProcessor(process.NoOpProcessor[json.RawMessage](), broker)
			default:
				conn, err := db.Connect(ctx, opts.DbUrl)
				if err != nil {
					return err
				}
				defer conn.Close()

				queries, err := db.Prepare(ctx, conn)
				if err != nil {
					return err
				}

				manifestFetcher := cache.HttpFetcher[manifest.Response[manifest.TopLevel]](http.DefaultClient)
				componentFetcher := cache.HttpFetcher[manifest.RawComponent[manifest.Entry]](http.DefaultClient)

				inMemCache := cache.NewInMemoryCache[manifest.Entry](manifestFetcher, componentFetcher)
				inMemCache.Prepopulate(ctx,
					manifest.ActivityDefinition,
					manifest.InventoryItemDefinition,
					manifest.DestinationDefinition,
				)

				mapper := mapper.New(inMemCache)
				inner := process.NewPgcrProcessor(conn, queries, mapper, inMemCache)

				processor = process.NewDatasetProcessor(inner, broker)
			}

			worker := runner.NewWorker(processor, consumer)
			for range opts.Goroutines {
				g.Go(func() error {
					return worker.Begin(ctx)
				})
			}

			uiLog, _ := os.OpenFile("ui.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)

			logger := slog.New(slog.NewJSONHandler(uiLog, nil))
			program := tea.NewProgram(ui.NewModel(events, len(files), logger, cancel))

			g.Go(func() error {
				_, err := program.Run()
				// Should cancel context on UI exiting
				return err
			})

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

// rootCmd represents the base command when called without any subcommands

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newRootCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}
