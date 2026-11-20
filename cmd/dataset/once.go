package dataset

import (
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/log"
	"pgcr-processing-service/internal/telemetry"

	"github.com/spf13/cobra"
)

var logFileName = "rivenbot.log"

type DatasetOnceOpts struct {
	DBUrl    string
	ApiKey   string
	Headless bool
}

// TODO: Big change that needs to happen here.
// Most of the big structs that do the heavy lifting
// like telemetry.Tracker and app.Backfill
// are directly created in the command itself which
// means I'm not able to test this command properly
func newDatasetOnceCommand(a app.App) *cobra.Command {
	var opts DatasetOnceOpts
	cmd := &cobra.Command{
		Use:               "once [OPTIONS] [FILE]",
		Short:             "Process a single dataset file",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeDatasetFiles,
		RunE: func(cmd *cobra.Command, args []string) error {
			todo := args[0]

			ringBuf := telemetry.NewRing[telemetry.LogEntry](120)
			logFile, err := os.Open(logFileName)
			if err != nil {
				return err
			}

			if !log.Initialized() {
				log.Setup(logFile, slog.LevelInfo, ringBuf)
			}

			tracker, err := telemetry.NewTracker(cmd.Context(), ringBuf, 1)
			if err != nil {
				slog.Error("failed to setup telemetry tracker", "error", err)
				return err
			}

			conn, err := db.Connect(cmd.Context(), opts.DBUrl)
			if err != nil {
				slog.Error("failed to connect to database", "url", opts.DBUrl, "error", err)
				return err
			}

			bApp, err := app.NewBackfill(conn, tracker, app.BackfillAppConfig{})
			if err != nil {
				slog.Error("failed to initialize backfill app", "error", err)
				return err
			}

			// TODO: Implement headless vs non-headless logic
			return bApp.RunOnce(cmd.Context(), todo, opts.ApiKey, opts.DBUrl)
		},
	}

	flags := cmd.Flags()
	flags.BoolVar(&opts.Headless, "headless", false, "whether the app will bootstrap its TUI or not")
	flags.StringVar(&opts.DBUrl, "url", "", "database URL to connect to")
	flags.StringVar(&opts.ApiKey, "api-key", "", "bungie API key to communicate with the Bnet API")

	return cmd
}

func completeDatasetFiles(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	rootDir, err := rootDir()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	if rootDir == "" {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	var matches []string
	filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".jsonl.zst") {
			return nil
		}

		rel, _ := filepath.Rel(rootDir, path)
		if strings.HasPrefix(rel, toComplete) {
			matches = append(matches, rel)
		}

		return nil
	})

	return matches, cobra.ShellCompDirectiveNoFileComp
}
