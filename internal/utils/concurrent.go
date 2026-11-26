package utils

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"golang.org/x/sync/errgroup"
)

// Wrapper function around the errgroup's Go() function
// Adds a defer statement that recovers the Goroutine in case
// of a panic, logs it using slog and then overrides the return
// value with an error instead
func goSafe(g *errgroup.Group, f func() error) {
	g.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("Panic in worker", slog.Any("panic", r), slog.String("Stack", string(debug.Stack())))
				err = fmt.Errorf("worker panic: %v", err)
			}
		}()
		return f()
	})
}
