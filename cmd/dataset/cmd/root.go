/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"pgcr-processing-service/internal/bungie"
	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/db"
	pgcrdataset "pgcr-processing-service/internal/jobs/pgcr-dataset"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/process"
	"pgcr-processing-service/internal/types/manifest"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type datasetOpts struct {
	RootDir    string
	DbUrl      string
	ApiKey     string
	Goroutines int
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
			ctx := cmd.Context()
			discoverer := pgcrdataset.NewDiscoverer(opts.RootDir)
			if err := discoverer.Discover(".zst"); err != nil {
				return err
			}
			input := make(chan pgcrdataset.DatasetEntry, 1500)
			ingester := pgcrdataset.NewIngester(&discoverer.Files, input)

			g := new(errgroup.Group)
			g.Go(func() error {
				return ingester.Start(cmd.Context())
			})

			conn, err := db.Connect(ctx, opts.DbUrl)
			if err != nil {
				return err
			}
			defer conn.Close()

			queries, err := db.Prepare(ctx, conn)
			if err != nil {
				return err
			}

			redis := redis.NewClient(&redis.Options{
				Addr:     "redis:6379",
				Password: "",
				DB:       0,
				Protocol: 2,
			})
			defer redis.Close()

			fetcher := bungie.BungieManifestFetcher[manifest.Response](http.DefaultClient, opts.ApiKey)
			cacheService := cache.NewService(redis, 12*time.Hour, fetcher)
			mapper := mapper.New(cacheService)
			processor := process.NewDatasetProcessor(conn, queries, mapper, cacheService)

			worker := pgcrdataset.NewWorker(processor)
			for range opts.Goroutines {
				g.Go(func() error {
					return worker.Start(ctx, input)
				})
			}

			if err := g.Wait(); err == nil {
				slog.Info("Successfully processed all dataset!", "num of files", len(discoverer.Files))
				return nil
			} else {
				slog.Info("Error during execution of dataset", "error", err)
				return err
			}
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.RootDir, "root-dir", "r", "", "Root directory to scan files from")
	flags.StringVarP(&opts.DbUrl, "db-url", "d", "", "URL to the Postgres DB")
	flags.StringVarP(&opts.ApiKey, "api-key", "a", "", "Bungie.net API key")
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
