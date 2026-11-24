package log

import (
	"context"
	"fmt"
	"log/slog"

	"pgcr-processing-service/internal/telemetry"
)

type RingHandler struct {
	level slog.Level
	buf   *telemetry.Ring[telemetry.LogEntry]
}

func NewRingHandler(level slog.Level) *RingHandler {
	return &RingHandler{
		buf:   DefaultRing(),
		level: level,
	}
}

func NewRingHandlerWith(buf *telemetry.Ring[telemetry.LogEntry], level slog.Level) *RingHandler {
	return &RingHandler{
		level: level,
		buf:   buf,
	}
}

func (h *RingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *RingHandler) Handle(ctx context.Context, record slog.Record) error {
	var attrs []string
	record.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, fmt.Sprintf("%s=%v", a.Key, a.Value))
		return true
	})
	entry := telemetry.LogEntry{
		Timestamp:  record.Time,
		Message:    record.Message,
		Level:      record.Level.String(),
		Attributes: attrs,
	}
	h.buf.Push(entry)
	return nil
}

func (h *RingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *RingHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h
}
