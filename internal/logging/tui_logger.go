package logging

import (
	"context"
	"log/slog"

	"pgcr-processing-service/internal/telemetry"
)

type RingBufferHandler struct {
	level slog.Level
	buf   *telemetry.RingBuffer
}

func NewTuiHandler(buf *telemetry.RingBuffer, level slog.Level) *RingBufferHandler {
	return &RingBufferHandler{
		level: level,
		buf:   buf,
	}
}

func (h *RingBufferHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *RingBufferHandler) Handle(ctx context.Context, record slog.Record) error {
	entry := telemetry.LogEntry{
		Timestamp: record.Time,
		Message:   record.Message,
		Level:     record.Level.String(),
	}
	h.buf.Push(entry)
	return nil
}

func (h *RingBufferHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *RingBufferHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return h
}
