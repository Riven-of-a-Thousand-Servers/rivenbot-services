package writer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/types/pgcr"
	pgcrs "pgcr-processing-service/internal/types/pgcr"
	types "pgcr-processing-service/internal/types/processor"
)

type PgcrProcessor struct {
	db      *sql.DB
	queries *db.Queries
	mapper  *mapper.DbMapper
}

// Full Processor with RabbitMQ as an extra dependency
func NewPgcrProcessor(db *sql.DB,
	queries *db.Queries,
	mapper *mapper.DbMapper,
) *PgcrProcessor {
	return &PgcrProcessor{
		db:      db,
		queries: queries,
		mapper:  mapper,
	}
}

// Write order goes as follows:
// 1. Compressed Blob
// 2. Destiny Player data
// 3. Weapon Information
// 4. Instance Information
func (p *PgcrProcessor) Write(ctx context.Context, pgcr pgcr.PostGameCarnageReport) error {
	slog.Info("Processing pgcr", "pgcr", pgcr.ActivityDetails.InstanceId)

	blob, err := p.mapper.MapToBlobObject(pgcr)
	if err != nil {
		return err
	}

	if p.queries.CreatePgcr(ctx, blob); err != nil {
		return err
	}

	if err := p.saveWeapons(ctx, pgcr); err != nil {
		return err
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return err
	}
	defer tx.Rollback()

	// TODO: Fix all this shit
	err = p.Save(ctx, qtx, &pgcr)
	if err != nil {
		if markErr := p.LedgerMarkError(ctx, p.queries, pgcr.InstanceId, err); markErr != nil {
			slog.Error("Failed to mark ledger entry as failed", "instanceId", pgcr.InstanceId, "error", err)
			return markErr
		}
		slog.Error("Error processing pgcr into db", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	if err := tx.Commit(); err != nil {
		if markErr := p.LedgerMarkError(ctx, p.queries, pgcr.InstanceId, err); markErr != nil {
			slog.Error("Failed to mark ledger entry as failed", "instanceId", pgcr.InstanceId, "error", err)
			return markErr
		}
		slog.Error("Failed to commit transaction", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	slog.Info("Finished processing pgcr", "InstanceId", pgcr.InstanceId)
	if err := p.LedgerMarkSuccess(ctx, p.queries, pgcr.InstanceId); err != nil {
		slog.Error("Failed to mark ledger entry as processed", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	return nil
}

func (p *PgcrProcessor) saveWeapons(ctx context.Context, pgcr pgcrs.PostGameCarnageReport) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	queries := p.queries.WithTx(tx)
	weapons, err := p.mapper.MapToDBWeapons(ctx, pgcr)
	if err != nil {
		return err
	}

	for _, weapon := range weapons {
		if err := queries.CreateWeapon(ctx, weapon); err != nil {
			slog.Error("Unable to save weapon", "weaponId", weapon.WeaponHash, "error", err)
			continue
		}
	}

	return tx.Commit()
}

func (p *PgcrProcessor) LedgerMarkSuccess(ctx context.Context, queries *db.Queries, instanceId int64) error {
	return queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     types.Success.String(),
		Error:      sql.NullString{Valid: false},
	})
}

func (p *PgcrProcessor) LedgerMarkError(ctx context.Context, queries *db.Queries, instanceId int64, cause error) error {
	return queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     types.Errored.String(),
		Error:      sql.NullString{String: cause.Error(), Valid: cause.Error() != ""},
	})
}

// Saves a processed pgcr to the Postgres DB
func (p *PgcrProcessor) Save(ctx context.Context, qtx *db.Queries, pgcr *pgcrs.PgcrInfo) error {
	// If inserting to the ledger fails, skip inserting to the DB
	entry, err := p.queries.CreateLogEntry(ctx, db.CreateLogEntryParams{
		InstanceID: pgcr.InstanceId,
		Status:     types.Started.String(),
	})
	if err != nil {
		slog.Error("Failed to insert to ingestion log", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	status, ok := types.ParseStatus(entry.Status)
	if !ok {
		slog.Error("Unable to parse status", "value", entry.Status)
		return fmt.Errorf("Unknown status: %s", entry.Status)
	}

	switch status {
	case types.Success:
		slog.Info("Instance already processed successfully, skipping", "instanceId", pgcr.InstanceId, "processedAt", entry.FirstSeenAt.String())
		return nil
	case types.Errored:
		slog.Warn("Retrying previously failed instance", "instanceId", pgcr.InstanceId)
	case types.Processing:
		if time.Since(entry.LastAttemptAt) > types.StaleThreshold {
			slog.Warn("Reclaiming stale processing entry", "instanceId", pgcr.InstanceId)
		} else {
			slog.Info("Instance actively being processed elsewhere, skipping", "instanceId", pgcr.InstanceId)
			return nil
		}
	case types.Started:
	}

	claimed, err := p.queries.ClaimLogEntryForProcessing(ctx, db.ClaimLogEntryForProcessingParams{
		InstanceID: pgcr.InstanceId,
		Status:     entry.Status,
	})

	if errors.Is(err, sql.ErrNoRows) {
		slog.Debug("Lost the claim race, skipping", "instanceId", pgcr.InstanceId)
		return nil
	}

	if err != nil {
		slog.Error("Failed to claim ingestion entry", "instanceId", pgcr.InstanceId)
		return err
	}
	_ = claimed

	if err := qtx.CreateInstance(ctx, db.CreateInstanceParams{
		ID:              pgcr.InstanceId,
		ActivityHash:    pgcr.ActivityHash,
		IsFresh:         pgcr.FromBeginning,
		Flawless:        pgcr.Flawless,
		PlayerCount:     int32(len(pgcr.PlayerInfo)),
		StartTime:       pgcr.StartTime,
		EndTime:         pgcr.EndTime,
		DurationSeconds: int32(pgcr.EndTime.Sub(pgcr.StartTime).Seconds()),
	}); err != nil {
		slog.Error("Failed to save instance to db", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	// TODO: this should be a separate step outside of the main transaction
	//
	// if err := qtx.CreatePgcr(ctx, db.CreatePgcrParams{
	// 	InstanceID: pgcr.InstanceId,
	// 	Blob:       b,
	// }); err != nil {
	// 	slog.Error("Failed to save raw pgcr instance", "instanceId", pgcr.InstanceId, "error", err)
	// 	return err
	// }

	// Player
	for _, pi := range pgcr.PlayerInfo {
		player := db.CreateDestinyPlayerParams{
			MembershipID:   pi.MembershipId,
			MembershipType: int32(pi.MembershipType),
			IsPublic:       sql.NullBool{Bool: pi.IsPublic, Valid: true},
			IconPath:       sql.NullString{String: pi.IconPath, Valid: pi.IconPath != ""},
		}

		if pi.GlobalDisplayName != "" {
			player.DisplayName = sql.NullString{String: pi.GlobalDisplayName, Valid: pi.GlobalDisplayName != ""}
		} else {
			player.DisplayName = sql.NullString{String: pi.DisplayName, Valid: pi.DisplayName != ""}
		}

		if pi.GlobalDisplayNameCode != 0 {
			player.GlobalDisplayNameCode = sql.NullInt32{
				Int32: int32(pi.GlobalDisplayNameCode),
				Valid: pi.GlobalDisplayNameCode != 0,
			}
		}

		_, err := qtx.CreateDestinyPlayer(ctx, player)
		if err != nil {
			slog.Error("Failed to save destiny player", "instanceId", pgcr.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType)
			return err
		}

		// InstancePlayer
		err = qtx.CreateInstancePlayer(ctx, db.CreateInstancePlayerParams{
			InstanceID:        pgcr.InstanceId,
			MembershipID:      pi.MembershipId,
			Completed:         sql.NullBool{Bool: pi.Completed},
			TimePlayedSeconds: pi.TimePlayedSeconds,
		})

		switch {
		case err == nil:
			isFullClear := pgcr.FromBeginning && pi.Completed
			if err := qtx.IncrementPlayerCounts(ctx, db.IncrementPlayerCountsParams{
				MembershipID: pi.MembershipId,
				Column2:      pi.Completed,
				Column3:      isFullClear,
			}); err != nil {
				slog.Error("Failed to increment clear counts", "membershipId", pi.MembershipId, "error", err)
				return err
			}
		case errors.Is(err, sql.ErrNoRows):
			slog.Info("destiny_player already recorded, skipping player entirely", "instanceId", pgcr.InstanceId, "membershipId", pi.MembershipId)
			continue
		default:
			slog.Error("Failed to save destiny_player", "instanceId", pgcr.InstanceId, "membershipId", pi.MembershipId)
			return err
		}

		// InstanceCharacter
		for _, ci := range pi.CharacterInfo {
			if err := qtx.CreateInstanceCharacter(ctx, db.CreateInstanceCharacterParams{
				InstanceID:   pgcr.InstanceId,
				MembershipID: pi.MembershipId,
				CharacterID:  ci.CharacterId,
				EmblemHash:   ci.CharacterEmblem,
				Completed:    ci.ActivityCompleted,
				Kills:        int32(ci.Kills),
				Deaths:       int32(ci.Deaths),
				Assists:      int32(ci.Assists),
				Kda:          strconv.FormatFloat(float64(ci.Kda), 'f', -1, 64),
				Kdr:          strconv.FormatFloat(float64(ci.Kdr), 'f', -1, 64),
				Efficiency:   int32(ci.Efficiency),
				SuperKills:   int32(ci.AbilityInfo.SuperKills),
				GrenadeKills: int32(ci.AbilityInfo.GrenadeKills),
				MeleeKills:   int32(ci.AbilityInfo.MeleeKills),
			}); err != nil {
				slog.Error("Failed to save instance character", "instanceId", pgcr.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType, "characterId", ci.CharacterId)
				return err
			}

			for _, ciw := range ci.WeaponInfo {
				// Weapons
				strHash := strconv.FormatInt(ciw.WeaponHash, 10)
				params, err := p.mapper.WeaponInfoToDBEntity(ctx, &ciw)
				if err != nil {
					slog.Error("Failed to map weapon to db entity", "hash", strHash, "error", err)
					return err
				}

				// Weapons should not be made as part of the whole transaction due to many
				// raids having similar weapon setups, this makes deadlocks be a regular ocurrance
				if err := p.queries.CreateWeapon(ctx, params); err != nil {
					slog.Error("Failed to save weapon", "weaponId", strHash, "error", err)
					return err
				}

				// InstanceCharacterWeapons
				if err := qtx.CreateInstanceCharacterWeapon(ctx, db.CreateInstanceCharacterWeaponParams{
					InstanceID:         pgcr.InstanceId,
					PlayerMembershipID: pi.MembershipId,
					PlayerCharacterID:  ci.CharacterId,
					WeaponID:           ciw.WeaponHash,
					Kills:              int32(ciw.Kills),
					PrecisionKills:     int32(ciw.PrecisionKills),
					PrecisionRatio:     strconv.FormatFloat(float64(ciw.PrecisionRatio), 'f', -1, 64),
				}); err != nil {

					slog.Error("Failed to save instance character", "instanceId", pgcr.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType, "characterId", ci.CharacterId, "weaponId", strHash)
					return err
				}
			}
		}
	}
	return nil
}
