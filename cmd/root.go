package cmd

import (
	// "pgcr-processing-service/cmd/backfill"
	"pgcr-processing-service/cmd/backfill"
	"pgcr-processing-service/cmd/config"
	"pgcr-processing-service/internal/app"

	"github.com/spf13/cobra"
)

func NewRootCommand(cli *app.Cli) *cobra.Command {
	cmd := &cobra.Command{
		Use:              "rivenbot [OPTIONS] COMMAND [ARG....]",
		Short:            "CLI tool to commandeer Rivenbot",
		SilenceUsage:     true,
		SilenceErrors:    true,
		TraverseChildren: true,
		Args:             cobra.ExactArgs(0),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return cli.Initialize()
		},
	}

	// Backfill command needs a new backfill app
	// cmd.AddCommand(backfill.NewCommand())
	cmd.AddCommand(config.NewCommand(cli))
	cmd.AddCommand(backfill.NewCommand(cli))

	return cmd
}
