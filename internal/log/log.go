package log

import (
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
)

func init() {
	defaultRing.Store(NewRing[LogEntry](50))
}

var (
	defaultRing atomic.Pointer[Ring[LogEntry]]
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

func DefaultRing() *Ring[LogEntry] {
	return defaultRing.Load()
}

func SetDefaultRing(ring *Ring[LogEntry]) {
	defaultRing.Swap(ring)
}

func Initialized() bool {
	return initialized.Load()
}
