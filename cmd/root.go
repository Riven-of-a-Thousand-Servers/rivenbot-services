package cmd

import (
	"pgcr-processing-service/cmd/dataset"
	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/db"

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

	db, err := db.Connect(cmd.Context())
	bApp := app.NewBackfill()
	cmd.AddCommand(dataset.NewCommand())
	return cmd
}
