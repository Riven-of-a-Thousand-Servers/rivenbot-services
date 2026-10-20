package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/types/bungie"
)

var MapCount atomic.Int32

func MapRawPgcr[T ~[]byte](ctx context.Context, item telemetry.Job[consumer.Delivery[T]]) (telemetry.Job[bungie.PostGameCarnageReport], error) {
	MapCount.Add(1)
	defer MapCount.Add(-1)

	var job telemetry.Job[bungie.PostGameCarnageReport]
	var out bungie.PostGameCarnageReport

	if err := json.Unmarshal([]byte(item.Todo.Payload), &out); err != nil {
		slog.Error("Error unmarshalling body from message", "error", err)
		item.Task.IncrementErrors()
		return job, err
	}

	job = telemetry.Job[bungie.PostGameCarnageReport]{
		Todo: out,
		Task: item.Task,
	}
	return job, nil
}
