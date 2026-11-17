package log

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"pgcr-processing-service/internal/telemetry"
)

var (
	initOnce    sync.Once
	initialized atomic.Bool
)

func Setup(logFile io.Writer, level slog.Level, ringBuf *telemetry.Ring[telemetry.LogEntry]) {
	initOnce.Do(func() {
		handlerOpts := &slog.HandlerOptions{
			Level: level,
		}

		mainHandler := slog.NewTextHandler(logFile, handlerOpts)
		tuiHandler := NewTuiHandler(ringBuf, level)

		slog.SetDefault(slog.New(slog.NewMultiHandler(mainHandler, tuiHandler)))
		initialized.Store(true)
	})
}

func Initialized() bool {
	return initialized.Load()
}
