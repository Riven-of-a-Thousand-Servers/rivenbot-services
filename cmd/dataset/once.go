package dataset

import (
	"io/fs"
	"path/filepath"
	"strings"

	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/db"

	"github.com/spf13/cobra"
)

func newDatasetOnceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "once [OPTIONS] [FILE]",
		Short:             "Process a single dataset file",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeDatasetFiles,
		RunE: func(cmd *cobra.Command, args []string) error {
			file := args[0]
			conn, err := db.Connect(cmd.Context())
			app := app.NewBackfill(cmd.Context())
		},
	}

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
