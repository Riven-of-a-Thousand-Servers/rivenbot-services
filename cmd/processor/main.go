package main

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
	rabbitmq1 "pgcr-processing-service/internal/types/rabbitmq"

	"github.com/redis/go-redis/v9"
)

var (
	postgresUrl = "postgres://%s:%s@postgres:5432/postgres?sslmode=disable"
	redisUrl    = "redis:6379"
	goroutines  = 5
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGQUIT)
	defer cancel()

	rabbitmq, err := rabbitmq.New[json.RawMessage](rabbitmq1.RabbitQueueName, rabbitmq1.RabbitMQUrl)
	if err != nil {
		slog.Error("Error happened while connecting to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitmq.Conn.Close()

	conn, err := db.Connect(ctx, postgresUrl)
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
	processor := processing.NewProcessor(conn, queries, rabbitmq, mapper, cacheService)

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Go(func() {
			slog.Info("Starting worker", "Id", i)
			_ = processor.StartWork(ctx, i)
			slog.Info("Shutting down worker", "Id", i)
		})
	}

	wg.Wait()
	slog.Info("All workers stopped, cleaning up resources")
}
