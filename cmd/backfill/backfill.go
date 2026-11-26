/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package backfill

import (
	"log/slog"
	"os"

	"pgcr-processing-service/internal/app"

	"github.com/spf13/cobra"
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

	bApp, err := app.NewBackfill(a.GetConfig().Backfill)
	if err != nil {
		slog.Error("Error creating backfill")
		os.Exit(1)
	}
	cmd.AddCommand(
		newRunCommand(bApp),
		newOnceCommand(bApp))

	return cmd
}
