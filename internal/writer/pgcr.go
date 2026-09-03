package writer

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strconv"

	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/types/bungie"
	pgcrs "pgcr-processing-service/internal/types/bungie"
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
func (w *PgcrProcessor) Write(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	instanceId := pgcr.ActivityDetails.InstanceId
	slog.Info("Processing pgcr", "pgcr", pgcr.ActivityDetails.InstanceId)

	if err := w.saveBlob(ctx, pgcr); err != nil {
		slog.Error("Failed to save blob", "pgcr", instanceId, "error", err)
		return err
	}

	if err := w.savePlayers(ctx, pgcr); err != nil {
		slog.Error("Failed to save destiny 2 players", "pgcr", instanceId, "error", err)
	}

	if err := w.saveWeapons(ctx, pgcr); err != nil {
		slog.Error("Failed to save weapons", "pgcr", instanceId, "error", err)
		return err
	}

	if err := w.saveInstance(ctx, pgcr); err != nil {
		slog.Error("Failed to save instance", "pgcr", instanceId, "error", err)
		return err
	}

	slog.Info("Finished processing pgcr", "pgcr", pgcr.ActivityDetails.InstanceId)
	return nil
}

func (w *PgcrProcessor) savePlayers(ctx context.Context, pgcr pgcrs.PostGameCarnageReport) error {
	players, err := w.mapper.MapToDestinyPlayers(pgcr)
	if err != nil {
		return err
	}

	for _, player := range players {
		if _, err := w.queries.CreateDestinyPlayer(ctx, player); err != nil {
			return err
		}
	}

	return nil
}

func (w *PgcrProcessor) saveBlob(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	blob, err := w.mapper.MapToBlobObject(pgcr)
	if err != nil {
		return err
	}

	return w.queries.CreatePgcr(ctx, blob)
}

func (w *PgcrProcessor) saveWeapons(ctx context.Context, pgcr pgcrs.PostGameCarnageReport) error {
	weapons, err := w.mapper.MapToDBWeapons(ctx, pgcr)
	if err != nil {
		return err
	}

	for _, weapon := range weapons {
		if err := w.queries.CreateWeapon(ctx, weapon); err != nil {
			slog.Error("Unable to save weapon", "weaponId", weapon.WeaponHash, "error", err)
			continue
		}
	}

	return nil
}

// Saves a processed pgcr to the Postgres DB
func (w *PgcrProcessor) saveInstance(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	instanceId := pgcr.ActivityDetails.InstanceId
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("Unable to create transaction for instance", "pgcr", instanceId, "error", err)
		return err
	}
	defer tx.Rollback()

	qtx := w.queries.WithTx(tx)
	if err := qtx.CreateInstance(ctx, db.CreateInstanceParams{
		ID:              pgcr.ActivityDetails.InstanceId.Int64(),
		ActivityHash:    pgcr.ActivityHash,
		IsFresh:         pgcr.FromBeginning,
		Flawless:        pgcr.Flawless,
		PlayerCount:     int32(len(pgcr.PlayerInfo)),
		StartTime:       pgcr.StartTime,
		EndTime:         pgcr.EndTime,
		DurationSeconds: int32(pgcr.EndTime.Sub(pgcr.StartTime).Seconds()),
	}); err != nil {
		slog.Error("Failed to save instance to db", "instanceId", pgcr.ActivityDetails.InstanceId, "error", err)
		return err
	}

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
			slog.Error("Failed to save destiny player", "instanceId", pgcr.ActivityDetails.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType)
			return err
		}

		// InstancePlayer
		err = qtx.CreateInstancePlayer(ctx, db.CreateInstancePlayerParams{
			InstanceID:        pgcr.ActivityDetails.InstanceId,
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
			slog.Info("destiny_player already recorded, skipping player entirely", "instanceId", pgcr.ActivityDetails.InstanceId, "membershipId", pi.MembershipId)
			continue
		default:
			slog.Error("Failed to save destiny_player", "instanceId", pgcr.ActivityDetails.InstanceId, "membershipId", pi.MembershipId)
			return err
		}

		// InstanceCharacter
		for _, ci := range pi.CharacterInfo {
			if err := qtx.CreateInstanceCharacter(ctx, db.CreateInstanceCharacterParams{
				InstanceID:   pgcr.ActivityDetails.InstanceId,
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
				slog.Error("Failed to save instance character", "instanceId", pgcr.ActivityDetails.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType, "characterId", ci.CharacterId)
				return err
			}

			for _, ciw := range ci.WeaponInfo {
				// Weapons
				strHash := strconv.FormatInt(ciw.WeaponHash, 10)
				params, err := w.mapper.WeaponInfoToDBEntity(ctx, &ciw)
				if err != nil {
					slog.Error("Failed to map weapon to db entity", "hash", strHash, "error", err)
					return err
				}

				// Weapons should not be made as part of the whole transaction due to many
				// raids having similar weapon setups, this makes deadlocks be a regular ocurrance
				if err := w.queries.CreateWeapon(ctx, params); err != nil {
					slog.Error("Failed to save weapon", "weaponId", strHash, "error", err)
					return err
				}

				// InstanceCharacterWeapons
				if err := qtx.CreateInstanceCharacterWeapon(ctx, db.CreateInstanceCharacterWeaponParams{
					InstanceID:         pgcr.ActivityDetails.InstanceId,
					PlayerMembershipID: pi.MembershipId,
					PlayerCharacterID:  ci.CharacterId,
					WeaponID:           ciw.WeaponHash,
					Kills:              int32(ciw.Kills),
					PrecisionKills:     int32(ciw.PrecisionKills),
					PrecisionRatio:     strconv.FormatFloat(float64(ciw.PrecisionRatio), 'f', -1, 64),
				}); err != nil {

					slog.Error("Failed to save instance character", "instanceId", pgcr.ActivityDetails.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType, "characterId", ci.CharacterId, "weaponId", strHash)
					return err
				}
			}
		}
	}
	return nil
}
