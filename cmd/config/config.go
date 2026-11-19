package config

import "github.com/spf13/cobra"

func NewConfigCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Configure RivenBot",
	}

	return cmd
}
