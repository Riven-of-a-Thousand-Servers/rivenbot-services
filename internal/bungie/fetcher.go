package bungie

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/types/manifest"
)

var (
	manifestUrl  = "http://proxy:8081/Platform/Destiny2/Manifest/%s/%s/"
	apiKeyHeader = "x-api-key"
)

func ManifestFetcher[T any](client *http.Client, apiKey string, def manifest.EntityDefinition) cache.Fetcher[T] {
	return func(ctx context.Context, key string) (T, error) {
		var zero T

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf(manifestUrl, def.String(), key), nil)
		if err != nil {
			return zero, err
		}

		req.Header.Add(apiKeyHeader, apiKey)
		req.Header.Add("content-type", "application/json")

		res, err := client.Do(req)
		if err != nil {
			return zero, err
		}
		defer res.Body.Close()

		if res.StatusCode != http.StatusOK {
			slog.Error("Received an error from the manifest API", "error", err)
			return zero, err
		}

		data, err := io.ReadAll(res.Body)
		if err != nil {
			return zero, err
		}

		var manifestResponse T
		if err := json.Unmarshal(data, &manifestResponse); err != nil {
			return zero, err
		}

		return manifestResponse, nil
	}
}
