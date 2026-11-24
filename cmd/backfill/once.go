package backfill

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/log"

	"github.com/spf13/cobra"
)

var logFileName = "rivenbot.log"

type DatasetOnceOpts struct {
	DBUrl    string
	ApiKey   string
	Headless bool
}

func newOnceCommand(b app.Backfill) *cobra.Command {
	var opts DatasetOnceOpts
	cmd := &cobra.Command{
		Use:               "once [OPTIONS] [FILE]",
		Short:             "Process a single dataset file",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeDatasetFiles,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := args[0]

			logFile, err := os.Open(logFileName)
			if err != nil {
				return err
			}

			if !log.Initialized() {
				log.Setup(logFile)
			}
			return b.RunOnce(cmd.Context(), file)
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
