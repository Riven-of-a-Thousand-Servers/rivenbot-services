package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/constraints"
)

func MapRawPgcr[T constraints.Bytes](ctx context.Context, item consumer.Delivery[T]) (bungie.PostGameCarnageReport, error) {
	var out bungie.PostGameCarnageReport
	if err := json.Unmarshal([]byte(item.Payload), &out); err != nil {
		slog.Error("Error unmarshalling body from message", "error", err)
		return out, err
	}

	return out, nil
}
