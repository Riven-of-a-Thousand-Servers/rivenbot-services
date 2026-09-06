/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/pipeline"
	"pgcr-processing-service/internal/rabbitmq"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/utils"
	"pgcr-processing-service/internal/writer"

	"github.com/deahtstroke/chainmorph"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
)

type processorOpts struct {
	RedisUrl      string
	RabbitMQUrl   string
	RabbitMQQueue string
	DatasourceUrl string
	ApiKey        string
	Concurrency   int
	Noop          bool
}

// rootCmd represents the base command when called without any subcommands
func newProcessCommand() *cobra.Command {
	var opts processorOpts
	rootCmd := &cobra.Command{
		Use:   "processor",
		Short: "PGCR processor service for Rivenbot",
		Long: `Processor service fetches PGCRs published by the Crawler from whichever
	Publisher is configured and saves it to a database`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			rabbitmq, err := rabbitmq.New[json.RawMessage](opts.RabbitMQQueue, opts.RabbitMQUrl)
			if err != nil {
				slog.Error("Error happened while connecting to RabbitMQ", "error", err)
				os.Exit(1)
			}
			defer rabbitmq.Conn.Close()

			// Switch if Noop is passed in
			var itemWriter chainmorph.ItemWriter[bungie.PostGameCarnageReport]
			switch {
			case opts.Noop:
				itemWriter = writer.NoOpProcessor[bungie.PostGameCarnageReport]()
			default:
				// Check for docker secret notation, e.g., /run/secret/${my_secret}
				if strings.HasPrefix(opts.DatasourceUrl, "/") {
					opts.DatasourceUrl, err = utils.ReadSecret(opts.DatasourceUrl)
					if err != nil {
						slog.Error("Error while reading data source URL from within docker secret", "error", err)
						os.Exit(1)
					}
				}

				conn, err := db.Connect(ctx, opts.DatasourceUrl)
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
					Addr:     opts.RedisUrl,
					Password: "",
					DB:       0,
					Protocol: 2,
				})
				defer redis.Close()
				fetcher := cache.BungieManifestFetcher[manifest.Entry](http.DefaultClient, opts.ApiKey)
				redisCache := cache.New(redis, 12*time.Hour, fetcher)

				mapper := mapper.New(redisCache)

				itemWriter = writer.NewPgcrWriter(conn, queries, mapper)
			}

			ch, err := rabbitmq.Consume(ctx)
			if err != nil {
				slog.Error("Error while starting consumer process", "error", err)
				os.Exit(1)
			}
			reader := pipeline.NewChannelReader(ch)
			err = chainmorph.From(reader).
				MapFunc(pipeline.MapRawPgcr).
				WriteTo(ctx, itemWriter)

			return err
		},
	}

	flags := rootCmd.Flags()
	flags.StringVar(&opts.DatasourceUrl, "database-url", "", "Database URL to connect to")
	flags.StringVar(&opts.RedisUrl, "redis-url", "", "URL to reach Redis")
	flags.IntVar(&opts.Concurrency, "goroutines", 1, "Number of goroutines to spin up")
	flags.StringVar(&opts.RabbitMQUrl, "rabbitmq-url", "", "URL to reach RabbitMQ")
	flags.StringVar(&opts.RabbitMQQueue, "rabbitmq-queue", "rivenbot", "RabbitMQ queue name")
	flags.StringVar(&opts.ApiKey, "api-key", "", "Bungie API key for manifest requests")
	flags.BoolVar(&opts.Noop, "noop", false, "Whether this processor will do something when consuming")

	return rootCmd
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newProcessCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}
