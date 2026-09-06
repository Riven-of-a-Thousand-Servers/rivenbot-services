package writer

import (
	"context"
	"database/sql"
	"log/slog"

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

	instancePlayers, err := w.mapper.MapToInstancePlayers(pgcr)
	if err != nil {
		return err
	}

	for _, player := range instancePlayers {
		if err := qtx.CreateInstancePlayer(ctx, player); err != nil {
			slog.Error("Failed to create instance player", "pgcr", instanceId, "memId", player.MembershipID, "error", err)
			return err
		}
		if err := qtx.IncrementPlayerCounts(ctx, db.IncrementPlayerCountsParams{
			MembershipID: player.MembershipID,
			Column2:      instance.IsFresh,
			Column3:      player.Completed.Bool,
		}); err != nil {
			slog.Error("Failed to increment player raid counts", "pgcr", instanceId, "memId", player.MembershipID, "error", err)
			return err
		}
	}

	instanceToons, err := w.mapper.MapToInstanceToons(pgcr)
	if err != nil {
		slog.Error("Failed to map instance toons", "pgcr", instanceId)
		return err
	}

	for _, toon := range instanceToons {
		if err := qtx.CreateInstanceCharacter(ctx, toon); err != nil {
			slog.Error("Failed to create instance character", "pgcr", instanceId, "memId", toon.MembershipID, "characterId", toon.CharacterID, "error", err)
			return err
		}
	}

	instanceToonWeapons, err := w.mapper.MapToInstanceToonWeapons(pgcr)
	if err != nil {
		slog.Error("Failed to map instance weapons", "pgcr", instanceId, "error", err)
		return err
	}

	for _, toonWeapon := range instanceToonWeapons {
		if err := qtx.CreateInstanceCharacterWeapon(ctx, toonWeapon); err != nil {
			slog.Error("Failed to create instance weapons", "pgcr", instanceId, "memId", toonWeapon.PlayerMembershipID, "characterId", toonWeapon.PlayerCharacterID, "weaponId", toonWeapon.WeaponID, "error", err)
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		slog.Error("Failed to commit transaction", "pgcr", instanceId)
		return err
	}

	return nil
}
