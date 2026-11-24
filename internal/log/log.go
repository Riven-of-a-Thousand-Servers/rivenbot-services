package log

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"pgcr-processing-service/internal/telemetry"
)

func init() {
	defaultRing.Store(telemetry.NewRing[telemetry.LogEntry](50))
}

var (
	defaultRing atomic.Pointer[telemetry.Ring[telemetry.LogEntry]]
	initOnce    sync.Once
	initialized atomic.Bool
)

func Setup(logFile io.Writer) {
	initOnce.Do(func() {
		mainHandler := slog.NewTextHandler(logFile, nil)
		tuiHandler := NewRingHandler(slog.LevelInfo)

		slog.SetDefault(slog.New(slog.NewMultiHandler(mainHandler, tuiHandler)))
		initialized.Store(true)
	})
}

func DefaultRing() *telemetry.Ring[telemetry.LogEntry] {
	return defaultRing.Load()
}

func SetDefaultRing(ring *telemetry.Ring[telemetry.LogEntry]) {
	defaultRing.Swap(ring)
}

func Initialized() bool {
	return initialized.Load()
}
