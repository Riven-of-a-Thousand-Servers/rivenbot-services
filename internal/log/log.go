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

func Setup(mainLogger io.Writer, level slog.Level, ws ...io.Writer) {
	initOnce.Do(func() {
		handlerOpts := &slog.HandlerOptions{
			Level: level,
		}

		mainHandler := slog.NewTextHandler(mainLogger, handlerOpts)
		ringBuf := telemetry.NewRing[telemetry.LogEntry](30)
		tuiHandler := NewTuiHandler(ringBuf, level)

		slog.SetDefault(slog.New(slog.NewMultiHandler(mainHandler, tuiHandler)))
		initialized.Store(true)
	})
}

func Initialized() bool {
	return initialized.Load()
}
