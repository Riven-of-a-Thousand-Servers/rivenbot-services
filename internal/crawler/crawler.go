package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"pgcr-processing-service/internal/producer"
	"pgcr-processing-service/internal/types/net"
)

const (
	apiKeyHeader = "x-api-key"
)

type PgcrCrawler struct {
	Id       int
	Offset   int64
	MaxSize  int64
	In       <-chan int64
	BaseUrl  string
	Client   *http.Client
	Producer producer.Factory[json.RawMessage]
}

type PgcrCrawlerOpt func(*PgcrCrawler)

func WithBaseUrl(url string) PgcrCrawlerOpt {
	return func(pc *PgcrCrawler) {
		pc.BaseUrl = url
	}
}

func WithMaxSize(size int64) PgcrCrawlerOpt {
	return func(pc *PgcrCrawler) {
		pc.MaxSize = size
	}
}

func WithOffset(off int64) PgcrCrawlerOpt {
	return func(pc *PgcrCrawler) {
		pc.Offset = off
	}
}

func NewPgcrCrawler(producer producer.Factory[json.RawMessage], client *http.Client, gen <-chan int64, opts ...PgcrCrawlerOpt) *PgcrCrawler {
	crawler := &PgcrCrawler{
		Producer: producer,
		Client:   client,
		In:       gen,
	}

	for _, opt := range opts {
		opt(crawler)
	}

	if crawler.MaxSize == 0 {
		crawler.MaxSize = net.MaxRequestSizeKB
	}

	if crawler.BaseUrl == "" {
		crawler.BaseUrl = "https://stats.bungie.net"
	}

	return crawler
}

func (c *PgcrCrawler) Get(ctx context.Context, workerId int, instanceId int64, apiKey string) ([]byte, error) {
	path := "/Platform/Destiny2/Stats/PostGameCarnageReport/%d/"
	url := fmt.Sprintf(c.BaseUrl+path, instanceId)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Error("Worker unable to create requests. Exiting.", "workerId", workerId, "error", err)
		return nil, err
	}

	req.Header.Add(apiKeyHeader, apiKey)
	res, err := c.Client.Do(req)
	if err != nil {
		slog.Error("Unable to get a response from Bungie. Exiting.", "workerId", workerId, "error", err)
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(res.Body)
		msg := fmt.Sprintf("unexpected status code %d", res.StatusCode)
		slog.Error(msg, "workerId", workerId, "instanceId", instanceId, "body", string(data))
	}

	// Stop if there's errors reading HTTP bodies from requests
	// Should this panic?
	data, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		slog.Error("Fatal: Error reading response body", "error", err)
		return nil, err
	}

	if int64(len(data)) > c.MaxSize {
		msg := fmt.Sprintf("Response exceeded limit of %d bytes, refusing to process", c.MaxSize)
		slog.Error(msg, "workerId", workerId)
		return nil, errors.New(msg)
	}

	return data, nil
}

func (c *PgcrCrawler) Crawl(ctx context.Context, workerId int, apiKey string) {
	prod, err := c.Producer.NewProducer(ctx)
	// Error opening a channel should immediately return
	if err != nil {
		slog.Error("Failed to open rabbitmq channel", "workerId", workerId, "error", err)
		return
	}
	defer prod.Close()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Crawler instance shutting down", "workerId", workerId)
			return
		case next, ok := <-c.In:
			if !ok {
				slog.Info("Input channel closed. Exiting", "workerId", workerId)
				return
			}

			slog.Info("Worker processing pgcr", "workerId", workerId, "pgcr", next)
			data, err := c.Get(ctx, workerId, next, apiKey)
			if err != nil {
				slog.Error("Error fetching pgcr", "err", err)
				continue
			}

			if err = prod.Produce(ctx, json.RawMessage(data)); err != nil {
				slog.Error("Unable to publish message", "pgcr", next, "workerId", workerId, "error", err)
				continue
			}

			slog.Info("Successfully published pgcr", "pgcr", next)
		}
	}
}
