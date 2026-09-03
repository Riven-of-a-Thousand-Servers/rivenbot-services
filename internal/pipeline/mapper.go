package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/dataset"
)

func MapRawPgcr(ctx context.Context, item consumer.Delivery[dataset.Entry]) (bungie.PostGameCarnageReport, error) {
	var out bungie.PostGameCarnageReport
	if err := json.Unmarshal(item.Payload.Bytes, &out); err != nil {
		slog.Error("Error unmarshalling body from message", "error", err)
		return out, err
	}

	return out, nil
}
