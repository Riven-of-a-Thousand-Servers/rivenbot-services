package writer

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/ledger"

	"github.com/deahtstroke/chainmorph"
)

type LedgerWriter struct {
	queries *db.Queries
	inner   chainmorph.ItemWriter[bungie.PostGameCarnageReport]
}

func (w *LedgerWriter) Write(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	instanceId := pgcr.ActivityDetails.InstanceId
	entry, err := w.markStarted(ctx, instanceId.Int64())
	if err != nil {
		slog.Error("Failed to insert to ingestion log", "instanceId", instanceId, "error", err)
		return err
	}

	status, ok := ledger.ParseStatus(entry.Status)
	if !ok {
		slog.Error("Unable to determine status for current ledger entry", "instanceId", instanceId)
	}

	switch status {
	case ledger.Success:
		slog.Debug("Instance already processed successfully, skipping", "instanceId", pgcr.ActivityDetails.InstanceId, "processedAt", entry.FirstSeenAt.String())
		return nil
	case ledger.Errored:
		slog.Warn("Retrying previously failed instance", "instanceId", pgcr.ActivityDetails.InstanceId)
	case ledger.Processing:
		if time.Since(entry.LastAttemptAt) > ledger.StaleThreshold {
			slog.Debug("Reclaiming stale processing entry", "instanceId", pgcr.ActivityDetails.InstanceId)
		} else {
			slog.Debug("Instance actively being processed elsewhere, skipping", "instanceId", pgcr.ActivityDetails.InstanceId)
			return nil
		}
	case ledger.Started:
	}

	claimed, err := w.queries.ClaimLogEntryForProcessing(ctx, db.ClaimLogEntryForProcessingParams{
		InstanceID: pgcr.ActivityDetails.InstanceId.Int64(),
		Status:     entry.Status,
	})

	if errors.Is(err, sql.ErrNoRows) {
		slog.Debug("Lost the claim race, skipping", "instanceId", pgcr.ActivityDetails.InstanceId)
		return nil
	}

	if err != nil {
		slog.Error("Failed to claim ingestion entry", "instanceId", pgcr.ActivityDetails.InstanceId)
		return err
	}

	_ = claimed

	err = w.inner.Write(ctx, pgcr)
	if err != nil {
		if markErr := w.markError(ctx, pgcr.ActivityDetails.InstanceId.Int64(), err); markErr != nil {
			slog.Error("Failed to mark ledger entry as failed", "instanceId", pgcr.ActivityDetails.InstanceId, "error", err)
			return markErr
		}
		slog.Error("Error processing pgcr into db", "instanceId", pgcr.ActivityDetails.InstanceId, "error", err)
		return err
	}

	return w.markSuccess(ctx, instanceId.Int64())
}

func (w *LedgerWriter) markStarted(ctx context.Context, instanceId int64) (db.IngestionLog, error) {
	return w.queries.CreateLogEntry(ctx, db.CreateLogEntryParams{
		InstanceID: instanceId,
		Status:     ledger.Started.String(),
	})
}

func (w *LedgerWriter) markSuccess(ctx context.Context, instanceId int64) error {
	return w.queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     ledger.Success.String(),
		Error:      sql.NullString{Valid: false},
	})
}

func (w *LedgerWriter) markError(ctx context.Context, instanceId int64, cause error) error {
	return w.queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     ledger.Errored.String(),
		Error:      sql.NullString{String: cause.Error(), Valid: cause.Error() != ""},
	})
}
