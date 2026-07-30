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
	"sync"
	"syscall"
	"time"

	"pgcr-processing-service/internal/bungie"
	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/processing"
	"pgcr-processing-service/internal/rabbitmq"
	"pgcr-processing-service/internal/types/manifest"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

type processorConfig struct {
	RedisUrl      string
	RabbitMQUrl   string
	RabbitMQQueue string
	DatasourceUrl string
	Goroutines    int
}

// rootCmd represents the base command when called without any subcommands
func newProcessCommand() *cobra.Command {
	var config processorConfig
	rootCmd := &cobra.Command{
		Use:   "processor",
		Short: "PGCR processor service for Rivenbot",
		Long: `Processor service fetches PGCRs published by the Crawler from whichever
	Publisher is configured and saves it to a database`,
		RunE: func(cmd *cobra.Command, args []string) error {
			processor, err := buildProcessor(cmd.Context(), config)
			if err != nil {
				return err
			}
			return runProcessor(cmd.Context(), processor)
		},
	}

	flags := rootCmd.Flags()
	flags.StringVar(&config.DatasourceUrl, "database-url", "", "Database URL to connect to")
	flags.StringVar(&config.RedisUrl, "redis-url", "", "URL to reach Redis")
	flags.IntVar(&config.Goroutines, "goroutines", 1, "Number of goroutines to spin up")
	flags.StringVar(&config.RabbitMQUrl, "rabbitmq-url", "", "URL to reach RabbitMQ")
	flags.StringVar(&config.RabbitMQQueue, "rabbitmq-queue", "rivenbot", "RabbitMQ queue name")

	return rootCmd
}

func buildProcessor(ctx context.Context, config processorConfig) (*processing.PgcrProcessor, error) {
	rabbitmq, err := rabbitmq.New[json.RawMessage](config.RabbitMQQueue, config.RabbitMQUrl)
	if err != nil {
		slog.Error("Error happened while connecting to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitmq.Conn.Close()

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
		Addr:     "redis:6379",
		Password: "",
		DB:       0,
		Protocol: 2,
	})
	defer redis.Close()

	cacheService := cache.NewService(redis, 12*time.Hour, bungie.BungieManifestFetcher[manifest.ManifestEntry](http.DefaultClient, ""))
	mapper := mapper.New(cacheService)
	return processing.NewProcessor(conn, queries, rabbitmq, mapper, cacheService, config.Goroutines), nil
}

func runProcessor(ctx context.Context, processor *processing.PgcrProcessor) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	var wg sync.WaitGroup
	for i := range processor.Concurrency {
		wg.Go(func() {
			slog.Info("Starting worker", "Id", i)
			_ = processor.StartWork(ctx, i)
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
