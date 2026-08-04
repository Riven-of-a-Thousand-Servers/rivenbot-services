/*
Copyright © 2026 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"pgcr-processing-service/internal/bungie"
	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/process"
	"pgcr-processing-service/internal/rabbitmq"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/utils"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
func newProcessCommand() *cobra.Command {
	var config process.ProcessorConfig
	rootCmd := &cobra.Command{
		Use:   "processor",
		Short: "PGCR processor service for Rivenbot",
		Long: `Processor service fetches PGCRs published by the Crawler from whichever
	Publisher is configured and saves it to a database`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			var processor *process.PgcrProcessor
			rabbitmq, err := rabbitmq.New[json.RawMessage](config.RabbitMQQueue, config.RabbitMQUrl)
			if err != nil {
				slog.Error("Error happened while connecting to RabbitMQ", "error", err)
				os.Exit(1)
			}
			defer rabbitmq.Conn.Close()

			// Switch if Noop is passed in
			switch {
			case config.Noop:
				processor = process.NoOpProcessor(rabbitmq, config.Goroutines)
			default:
				// Check for docker secret notation, e.g., /run/secret/${my_secret}
				if strings.HasPrefix(config.DatasourceUrl, "/") {
					config.DatasourceUrl, err = utils.ReadSecret(config.DatasourceUrl)
					if err != nil {
						slog.Error("Error while reading data source URL from within docker secret", "error", err)
						os.Exit(1)
					}
				}

				conn, err := db.Connect(ctx, config.DatasourceUrl)
				if err != nil {
					slog.Error("Error happened while connecting to DB", "error", err)
					os.Exit(1)
				}
				defer conn.Close()

				queries, err := db.Prepare(ctx, conn)
				if err != nil {
					slog.Error("Error creating and preparing queries", "error", err)
					os.Exit(1)
				}

				redis := redis.NewClient(&redis.Options{
					Addr:     config.RedisUrl,
					Password: "",
					DB:       0,
					Protocol: 2,
				})
				defer redis.Close()

				cacheService := cache.NewService(redis, 12*time.Hour, bungie.BungieManifestFetcher[manifest.Response](http.DefaultClient, ""))
				mapper := mapper.New(cacheService)

				processor = process.NewProcessor(conn, queries, rabbitmq, mapper, cacheService, config.Goroutines)
			}
			return runProcessor(cmd.Context(), processor, config.Noop)
		},
	}

	flags := rootCmd.Flags()
	flags.StringVar(&config.DatasourceUrl, "database-url", "", "Database URL to connect to")
	flags.StringVar(&config.RedisUrl, "redis-url", "", "URL to reach Redis")
	flags.IntVar(&config.Goroutines, "goroutines", 1, "Number of goroutines to spin up")
	flags.StringVar(&config.RabbitMQUrl, "rabbitmq-url", "", "URL to reach RabbitMQ")
	flags.StringVar(&config.RabbitMQQueue, "rabbitmq-queue", "rivenbot", "RabbitMQ queue name")
	flags.BoolVar(&config.Noop, "noop", false, "Whether this processor will do something when consuming")

	return rootCmd
}

func runProcessor(ctx context.Context, processor *process.PgcrProcessor, noop bool) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	var wg sync.WaitGroup
	for i := range processor.Concurrency {
		wg.Go(func() {
			slog.Info("Starting worker", "Id", i)
			err := processor.StartWork(ctx, i, noop)
			if err != nil {
				slog.Error("Something bad happened", "error", err)
			}
			slog.Info("Shutting down worker", "Id", i)
		})
	}

	wg.Wait()
	slog.Info("All workers stopped, cleaning up resources")
	return nil
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newProcessCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}
