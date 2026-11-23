package app

import (
	"context"
	"database/sql"
	"io"

	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/telemetry"
)

type App interface {
	In() io.Reader
	Out() io.Writer
	Err() io.Writer
	Close() error
	DB(context.Context) (*sql.DB, error)
	config.Provider
}

type Backfill interface {
	RunOnce(ctx context.Context, file string) error
	Tracker() *telemetry.Tracker
	Close() error
}
