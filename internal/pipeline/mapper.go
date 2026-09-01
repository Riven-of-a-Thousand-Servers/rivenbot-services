package pipeline

import (
	"context"
	"encoding/json"
	"log/slog"

	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/types/dataset"
	"pgcr-processing-service/internal/types/pgcr"
)

func MapRawPgcr(ctx context.Context, item consumer.Delivery[dataset.Entry]) (pgcr.PostGameCarnageReport, error) {
	var out pgcr.PostGameCarnageReport
	if err := json.Unmarshal(item.Payload.Bytes, &out); err != nil {
		slog.Error("Error unmarshalling body from message", "error", err)
		return out, err
	}

	return out, nil
}

type PgcrInfoMapper struct {
	mapper.Mapper
}

func NewPgcrInfoMapper(m mapper.Mapper) *PgcrInfoMapper {
	return &PgcrInfoMapper{Mapper: m}
}

func (m *PgcrInfoMapper) Map(ctx context.Context, item pgcr.PostGameCarnageReport) (pgcr.PgcrInfo, error) {
	var zero pgcr.PgcrInfo
	out, err := m.PgcrToPgcrInfo(ctx, &item)
	if err != nil {
		return zero, err
	}
	return *out, nil
}
