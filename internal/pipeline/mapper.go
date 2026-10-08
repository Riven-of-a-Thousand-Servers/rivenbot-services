package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/types/bungie"
)

func MapRawPgcr[T ~[]byte](ctx context.Context, item telemetry.Job[consumer.Delivery[T]]) (telemetry.Job[bungie.PostGameCarnageReport], error) {
	var job telemetry.Job[bungie.PostGameCarnageReport]
	var out bungie.PostGameCarnageReport

	if err := json.Unmarshal([]byte(item.Skibidi.Payload), &out); err != nil {
		slog.Error("Error unmarshalling body from message", "error", err)
		return job, err
	}

	job = telemetry.Job[bungie.PostGameCarnageReport]{
		Skibidi: out,
		Task:    item.Task,
	}
	return job, nil
}
