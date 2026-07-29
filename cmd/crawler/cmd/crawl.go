/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
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

	"pgcr-processing-service/internal/crawling"
	"pgcr-processing-service/internal/producer"
	"pgcr-processing-service/internal/rabbitmq"
	"pgcr-processing-service/internal/stdout"
	"pgcr-processing-service/internal/transport"
	"pgcr-processing-service/internal/types/net"

	"github.com/spf13/cobra"
)

type crawlerConfig struct {
	Goroutines    int
	ApiKey        string
	Interval      int
	Offset        int64
	BaseUrl       string
	RabbitMQUrl   string
	RabbitMQQueue string
	QueueName     string
	Noop          bool
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newCrawlerCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}

func newCrawlerCommand() *cobra.Command {
	var config crawlerConfig
	cmd := &cobra.Command{
		Use:   "crawler",
		Short: "Crawler service for Rivenbot",
		Long: `Crawler service sequentially fetches PGCRs from Bungie's API
to have up-to-date information regarding player's raids statistics`,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		RunE: func(cmd *cobra.Command, args []string) error {
			return runCrawler(cmd.Context(), config)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&config.BaseUrl, "base-url", "b", "", "Base URL for fetching PGCRs")
	flags.IntVarP(&config.Goroutines, "goroutines", "g", 1, "Number of goroutines to spin up")
	flags.StringVarP(&config.ApiKey, "api-key", "a", "", "Bungie API key")
	flags.IntVarP(&config.Interval, "interval", "i", 10, "Duration in-between requests to Bungie")
	flags.Int64VarP(&config.Offset, "offset", "o", 0, "PGCR scraping offset (Initial point to start fetching PGCRs)")
	flags.StringVar(&config.RabbitMQUrl, "rabbitmq-url", "", "RabbitMQ URL for publishing PGCRs")
	flags.StringVar(&config.RabbitMQQueue, "rabbitmq-queue", "", "RabbitMQ queue name")
	flags.BoolVar(&config.Noop, "noop", false, "Swap out producer implementation for a no-op producer that prints to Stdout")

	return cmd
}

func runCrawler(ctx context.Context, config crawlerConfig) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	var factory producer.Factory[json.RawMessage]
	switch {
	case config.Noop:
		noOpFactory := stdout.NewFactory[json.RawMessage]()
		factory = noOpFactory
	default:
		if config.RabbitMQQueue == "" {
			slog.Error("No RabbitMQ queue declared")
			os.Exit(1)
		}

		if config.RabbitMQUrl == "" {
			slog.Error("No valid RabbitMQ URL")
			os.Exit(1)
		}

		rmqFactory, err := rabbitmq.New[json.RawMessage](config.RabbitMQQueue, config.RabbitMQUrl)
		if err != nil {
			slog.Error("Error happened while connecting to RabbitMQ", "error", err)
			return err
		}
		defer rmqFactory.Conn.Close()
		factory = rmqFactory
	}

	client := http.Client{
		Transport: &transport.MaxSizeTransport{
			Base:    http.DefaultTransport,
			MaxSize: net.MaxRequestSizeKB,
		},
		Timeout: 10 * time.Second,
	}

	var wg sync.WaitGroup
	tick := time.NewTicker(time.Duration(config.Interval) * time.Second)
	defer tick.Stop()

	wg.Add(1)
	in := make(chan int64, 100)
	go func(ctx context.Context, throttle *time.Ticker, start int64, in chan<- int64) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				slog.Info("Context cancelled. Exiting.")
				close(in)
				return
			case <-throttle.C:
				in <- int64(start)
				start++
			}
		}
	}(ctx, tick, config.Offset, in)

	opts := []crawling.PgcrCrawlerOpt{
		crawling.WithMaxSize(net.MaxRequestSizeKB),
		crawling.WithBaseUrl(config.BaseUrl),
		crawling.WithOffset(config.Offset),
	}

	crawler := crawling.NewPgcrCrawler(factory, &client, in, opts...)
	for id := range config.Goroutines {
		wg.Go(func() {
			crawler.Crawl(ctx, id, config.ApiKey)
		})
	}

	wg.Wait()
	slog.Info("All workers stopped, cleaning up resources")
	return nil
}
