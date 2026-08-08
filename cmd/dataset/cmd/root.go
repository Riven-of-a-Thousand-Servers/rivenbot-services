/*
Copyright © 2026 Daniel Villavicencio <dvm3099@pm.me>
*/
package cmd

import (
	"os"
	"sync"

	pgcrdataset "pgcr-processing-service/internal/jobs/pgcr-dataset"
	"pgcr-processing-service/internal/process"

	"github.com/spf13/cobra"
)

type datasetOpts struct {
	RootDir    string
	Goroutines int
}

func newRootCommand() *cobra.Command {
	var opts datasetOpts

	cmd := &cobra.Command{
		Use:   "dataset",
		Short: "Runs a one-off job to process PGCRs from the Asun ZSTD dataset to backfill database",
		Long: `This command spins up several workers to backfill the Rivenbot database from the Asun
dataset`,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		RunE: func(cmd *cobra.Command, args []string) error {
			discoverer := pgcrdataset.NewDiscoverer(opts.RootDir)
			if err := discoverer.Discover(".zst"); err != nil {
				return err
			}
			input := make(chan pgcrdataset.DatasetEntry, 1500)
			ingester := pgcrdataset.NewIngester(&discoverer.Files, input)

			var wg sync.WaitGroup
			wg.Go(func() {
				if err := ingester.Start(cmd.Context()); err != nil {
					return
				}
			})

			processor := process.NewDatasetProcessor()

			for i := range opts.Goroutines {
				worker := pgcrdataset.NewWorker()
			}

			return
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.RootDir, "root-dir", "r", "", "Root directory to scan files from")
	flags.IntVarP(&opts.Goroutines, "goroutines", "g", 1, "Number of workers to spin up")
	return cmd
}

// rootCmd represents the base command when called without any subcommands

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := newRootCommand().Execute()
	if err != nil {
		os.Exit(1)
	}
}
