/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package backfill

import (
	"fmt"
	"log/slog"
	"runtime/debug"

	"pgcr-processing-service/internal/app"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

// TODO: How do we create a Backfill from a Cli?
// Need to figure out how to do that because Backfill
// has heavy dependencies like telemetry.Tracker
// and a DB connection
// Perhaps even, the DB connection could be offloaded?
func NewCommand(a app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "dataset",
		Short:        "Process a Destiny 2 PGCR dataset",
		SilenceUsage: true,
	}

	// cmd.AddCommand(
	// 	newRunCommand(),
	// 	newOnceCommand())
	//
	return cmd
}

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
