package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"pgcr-processing-service/cmd"
	"pgcr-processing-service/internal/app"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	cli := app.NewCli()
	defer cli.Close()

	if err := cmd.NewRootCommand(cli).ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
}
