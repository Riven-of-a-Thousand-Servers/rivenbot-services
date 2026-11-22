package config

import (
	"pgcr-processing-service/internal/app"

	"github.com/spf13/cobra"
)

func NewConfigCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage configuration for RivenBot",
		Long: `Manage the configuration that RivenBot will use.
By default the priority of configuration files is:
1. Current working directory: ${CWD}/.rivenbot/config.json
2. User's home directory: ${HOME_DIR}/.rivenbot/config.json
3. XDG config directory: ${XDG_CONFIG_DIR}/rivenbot/config.json

If no configuration is found then a new one with defaults will be
initialized in the current working directory.`,
	}

	cmd.AddCommand()

	return cmd
}
