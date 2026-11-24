package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"

	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/log"
)

type Cli struct {
	out     io.Writer
	err     io.Writer
	in      io.Reader
	cfg     *config.Config
	closers []func() error
}

var _ App = (*Cli)(nil)

func NewCli() *Cli { return &Cli{out: os.Stdout, in: os.Stdin, err: os.Stderr} }

func (c *Cli) Initialize() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	c.cfg = cfg

	logFile, err := os.OpenFile(cfg.Logging.FileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	c.closers = append(c.closers, logFile.Close)
	if !log.Initialized() {
		log.Setup(logFile)
	}

	return nil
}

func (c *Cli) GetConfig() *config.Config {
	return c.cfg
}

func (c *Cli) In() io.Reader {
	return c.in
}

func (c *Cli) Out() io.Writer {
	return c.out
}

func (c *Cli) Err() io.Writer {
	return c.err
}

func (c *Cli) DB(ctx context.Context) (*sql.DB, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("failed to read runtime config while opening database pool: %v", err)
	}

	return db.Connect(ctx, cfg.Database.URL)
}

func (c *Cli) Close() error {
	var errs []error
	for _, c := range slices.Backward(c.closers) {
		if err := c(); err != nil {
			slog.Error("failed to close resources on cli App", "error", err)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
