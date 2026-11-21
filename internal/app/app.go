package app

import (
	"context"

	"pgcr-processing-service/internal/config"
	"pgcr-processing-service/internal/telemetry"
)

type App interface {
	RunOnce(context.Context, string) error
	GetTracker() *telemetry.Tracker
	config.Provider
}
