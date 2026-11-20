package app

import (
	"context"

	"pgcr-processing-service/internal/telemetry"
)

type App interface {
	RunOnce(context.Context, string, string, string) error
	Tracker() *telemetry.Tracker
}
