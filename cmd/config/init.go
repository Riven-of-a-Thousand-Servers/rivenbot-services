package config

import (
	"fmt"

	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/config"

	"github.com/spf13/cobra"
)

func newConfigInitCommand(a app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize an RivenBot configuration",
		Long: `Initializes a RivenBot configuration
in the current working directory`,
		Args: cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, err := config.Init(); err != nil {
				return fmt.Errorf("Unable to initialize config: %v", err)
			}

			path, _ := config.Find()
			fmt.Fprintf(a.Out(), "Config initialized at %s\n", path)
			return nil
		},
	}

	return cmd
}
