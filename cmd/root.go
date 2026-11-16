package cmd

import (
	"pgcr-processing-service/cmd/dataset"

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

	cmd.AddCommand(dataset.NewCommand())
	return cmd
}
