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

type PgcrWriter struct {
	db      *sql.DB
	queries *db.Queries
	mapper  *mapper.DbMapper
}

// Full Processor with RabbitMQ as an extra dependency
func NewPgcrWriter(db *sql.DB,
	queries *db.Queries,
	mapper *mapper.DbMapper,
) *PgcrWriter {
	return &PgcrWriter{
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
func (w *PgcrWriter) Write(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
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

func (w *PgcrWriter) savePlayers(ctx context.Context, pgcr pgcrs.PostGameCarnageReport) error {
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

func (w *PgcrWriter) saveBlob(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	blob, err := w.mapper.MapToBlobObject(pgcr)
	if err != nil {
		return err
	}

	return w.queries.CreatePgcr(ctx, blob)
}

func (w *PgcrWriter) saveWeapons(ctx context.Context, pgcr pgcrs.PostGameCarnageReport) error {
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
func (w *PgcrWriter) saveInstance(ctx context.Context, pgcr bungie.PostGameCarnageReport) error {
	instanceId := pgcr.ActivityDetails.InstanceId
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("Unable to create transaction for instance", "pgcr", instanceId, "error", err)
		return err
	}
	defer tx.Rollback()

	qtx := w.queries.WithTx(tx)

	instance, err := w.mapper.MapToDBInstance(pgcr)
	if err != nil {
		slog.Error("Failed to map to DB instance", "pgcr", instanceId, "error", err)
		return err
	}

	if err := qtx.CreateInstance(ctx, instance); err != nil {
		slog.Error("Failed to save instance to db", "instanceId", pgcr.ActivityDetails.InstanceId, "error", err)
		return err
	}

	// TODO: Map player information to their DB equivalent
	// hopefully get rid of the intermediate PgcrInfo struct soon
	for _, pi := range pgcr.PlayerInfo {
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
