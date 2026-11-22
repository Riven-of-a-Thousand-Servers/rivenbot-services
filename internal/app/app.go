package app

import (
	"context"
	"io"

	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/telemetry"
)

type App interface {
	RunOnce(context.Context, string) error
	GetTracker() *telemetry.Tracker
	In() io.Reader
	Out() io.Writer
	config.Provider
}
