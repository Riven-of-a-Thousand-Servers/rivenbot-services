package cmd

import (
	"log/slog"
	"os"

	"pgcr-processing-service/cmd/dataset"
	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/log"
	"pgcr-processing-service/internal/telemetry"

	"github.com/spf13/cobra"
)

func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "rivenbot [OPTIONS] COMMAND [ARG....]",
		Short:            "CLI tool to commandeer Rivenbot",
		SilenceUsage:     true,
		SilenceErrors:    true,
		TraverseChildren: true,
		Args:             cobra.ExactArgs(0),
	}

	cfg, err := config.Init()
	if err != nil {
		slog.Error("failed to initialize configuration", "error", err)
		os.Exit(1)
	}

	ringBuf := telemetry.NewRing[telemetry.LogEntry](120)
	logFile, err := os.Open(cfg.Logging.FileName)
	if err != nil {
		slog.Error("failed setting up logging configuraiton", "error", err)
		os.Exit(1)
	}

	if !log.Initialized() {
		log.Setup(logFile, slog.LevelInfo, ringBuf)
	}

	globalCtx := cmd.Context()
	db, err := db.Connect(globalCtx, cfg.Database.URL)
	if err != nil {
		slog.Error("failed to initialize database", "error", err)
		os.Exit(1)
	}

	tr, err := telemetry.NewTracker(globalCtx, ringBuf, 30)
	if err != nil {
		slog.Error("failed to initialize tracker", "error", err)
		os.Exit(1)
	}

	bApp, err := app.NewBackfill(db, tr)
	if err != nil {
		slog.Error("failed to initialize backfill app", "error", err)
		os.Exit(1)
	}
	cmd.AddCommand(dataset.NewCommand(bApp))
	return cmd
}
