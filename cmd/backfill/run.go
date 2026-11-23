package backfill

import (
	"context"
	"log/slog"
	"sync"

	"pgcr-processing-service/internal/app"
	"pgcr-processing-service/internal/pubsub"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"
)

type datasetRunOpts struct {
	RootDir     string
	DbUrl       string
	ApiKey      string
	FilePattern string
	Goroutines  int // Dont use
	Noop        bool
	NumFiles    int
	NumLines    int
}

func newRunCommand(b app.Backfill) *cobra.Command {
	var opts datasetRunOpts

	cmd := &cobra.Command{
		Use:   "run [OPTIONS] [ARG...]",
		Short: "Run a specific dataset command",
		Long:  "Run a specific operation on the Asun dataset for PGCRs",
	}

	flags := cmd.Flags()
	flags.IntVar(&opts.NumLines, "lines", 0, "Number of rows-per-file to process")
	flags.IntVar(&opts.NumFiles, "files", 0, "Number of files to process")
	flags.StringVarP(&opts.RootDir, "root-dir", "r", "", "Root directory to scan files from")
	flags.StringVarP(&opts.DbUrl, "db-url", "d", "", "URL to the Postgres DB")
	flags.StringVarP(&opts.ApiKey, "api-key", "a", "", "Bungie.net API key")
	flags.BoolVar(&opts.Noop, "noop", false, "If the processor to be used is a Noop processor")
	flags.IntVarP(&opts.Goroutines, "goroutines", "g", 1, "Number of workers to spin up")
	flags.StringVarP(&opts.FilePattern, "pattern", "p", "", "File pattern to match when discovering files")

	return cmd
}

func publishEventsToTea(ctx context.Context, program *tea.Program, out <-chan tea.Msg) {
	for {
		select {
		case <-ctx.Done():
			slog.Debug("Context cancelled, TUI message handler is shutting down")
			return
		case msg, ok := <-out:
			if !ok {
				slog.Debug("TUI message channel closed")
				return
			}

			program.Send(msg)
		}
	}
}

// Setup events takes a subscriber, subscribes to it and asynchronously
// emits the events from its chanel down to the output channel that will
// comunicate with the tea.Program
func setupEvents[T any](
	ctx context.Context,
	wg *sync.WaitGroup,
	outCh chan<- tea.Msg,
	subscriber pubsub.Subscriber[T],
) {
	wg.Go(func() {
		subCh, _ := subscriber.Subscribe()
		for {
			select {
			case e, ok := <-subCh:
				if !ok {
					slog.Debug("Subscription channel closed")
					return
				}

				var msg tea.Msg = e
				select {
				case outCh <- msg:
				case <-ctx.Done():
					slog.Debug("Global context was cancelled. Returning")
					return
				}

			case <-ctx.Done():
				slog.Debug("Global context was cancelled. Returning")
				return
			}
		}
	})
}
