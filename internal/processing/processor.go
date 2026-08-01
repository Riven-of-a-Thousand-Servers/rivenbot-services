package processing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/compress"
	"pgcr-processing-service/internal/consumer"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/mapper"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/types/pgcr"
	"pgcr-processing-service/internal/utils"

	"github.com/rabbitmq/amqp091-go"
)

type Source int

const (
	crawler Source = iota + 1
	dataset
)

var sources map[Source]string = map[Source]string{
	crawler: "crawler",
	dataset: "dataset",
}

var reverseSources map[string]Source = map[string]Source{
	"crawler": crawler,
	"dataset": dataset,
}

type Status int

const (
	processing Status = iota + 1
	errored
	success
)

var statuses map[Status]string = map[Status]string{
	processing: "processing",
	errored:    "error",
	success:    "success",
}

const staleThreshold = 5 * time.Minute

type PgcrProcessor struct {
	db          *sql.DB
	Concurrency int
	queries     *db.Queries
	consumer    consumer.Consumer[json.RawMessage]
	mapper      *mapper.PgcrMapper
	cache       cache.Service[manifest.ManifestEntry]
	noop        bool
}

func NewProcessor(db *sql.DB, queries *db.Queries,
	consumer consumer.Consumer[json.RawMessage],
	mapper *mapper.PgcrMapper,
	redis cache.Service[manifest.ManifestEntry],
	concurrency int,
	noop bool,
) *PgcrProcessor {
	return &PgcrProcessor{
		db:          db,
		queries:     queries,
		consumer:    consumer,
		mapper:      mapper,
		cache:       redis,
		Concurrency: concurrency,
		noop:        noop,
	}
}

// StartWork handles messages received from RabbitMQ to process PGCRs into the postgres db
func (p *PgcrProcessor) StartWork(ctx context.Context, id int) error {
	deliveries, err := p.consumer.Consume(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("Consumer shutting down", "Id", id)
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				slog.Info("Delivery channel closed by the broker", "Id", id)
				return nil
			}
			p.handleDelivery(ctx, delivery)
		}
	}
}

func (p *PgcrProcessor) handleDelivery(ctx context.Context, delivery consumer.Delivery[json.RawMessage]) {
	var pgcr pgcr.PostGameCarnageReportResponse
	err := json.Unmarshal(delivery.Item, &pgcr)
	if err != nil {
		slog.Error("Error unmarshalling body from message", "Error", err)
		delivery.Nack(false)
	}

	instanceId := pgcr.Response.ActivityDetails.InstanceId
	instanceId64, _ := strconv.ParseInt(instanceId, 10, 64)
	mode := pgcr.Response.ActivityDetails.Mode

	if p.noop {
		slog.Info("Processed Pgcr!", "instaceId", instanceId)
		return
	}

	// Only process raid activity
	if pgcr.Response.ActivityDetails.Mode != 4 {
		slog.Info("Pgcr is not a raid", "pgcr", instanceId, "mode", mode)
		delivery.Ack()
		return
	}

	slog.Info("Processing pgcr", "InstanceId", instanceId)
	processed, err := p.mapper.ExtractInfo(&pgcr.Response)
	if err != nil {
		slog.Error("Error mapping pgcr to a processed pgcr", "instanceId", instanceId, "error", err)
		delivery.Nack(false)
		return
	}

	compressed, err := compress.Gzip(&pgcr)
	if err != nil {
		slog.Error("Unable to compress pgcr", "instanceId", instanceId, "error", err)
		return
	}

	source, err := extractSource(delivery.Headers)
	if err != nil {
		slog.Warn("Unable to extract PGCR source from amqp headers", "error", err)
		delivery.Nack(false)
		return
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		delivery.Nack(false)
		return
	}
	defer tx.Rollback()

	qtx := p.queries.WithTx(tx)
	err = p.Save(ctx, qtx, processed, source, compressed)
	if err != nil {
		if markErr := p.LedgerMarkError(ctx, instanceId64, err); markErr != nil {
			slog.Error("Failed to mark ledger entry as failed", "instanceId", instanceId, "error", err)
		}
		slog.Error("Error processing pgcr into db", "instanceId", instanceId, "error", err)
		delivery.Nack(false)
		return
	}

	if err := tx.Commit(); err != nil {
		if markErr := p.LedgerMarkError(ctx, instanceId64, err); markErr != nil {
			slog.Error("Failed to mark ledger entry as failed", "instanceId", instanceId, "error", err)
		}
		slog.Error("Failed to commit transaction", "instanceId", instanceId, "error", err)
		delivery.Nack(false)
		return
	}

	slog.Info("Finished processing pgcr", "InstanceId", instanceId)
	if err := p.LedgerMarkSuccess(ctx, instanceId64); err != nil {
		slog.Error("Failed to mark ledger entry as processed", "instanceId", instanceId)
	}

	delivery.Ack()
}

func (p *PgcrProcessor) LedgerMarkSuccess(ctx context.Context, instanceId int64) error {
	return p.queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     statuses[success],
		Error:      sql.NullString{Valid: false},
	})
}

func (p *PgcrProcessor) LedgerMarkError(ctx context.Context, instanceId int64, cause error) error {
	return p.queries.UpdateLogEntryStatus(ctx, db.UpdateLogEntryStatusParams{
		InstanceID: instanceId,
		Status:     statuses[errored],
		Error:      sql.NullString{String: cause.Error(), Valid: cause.Error() != ""},
	})
}

// Saves a processed pgcr to the Postgres DB
func (p *PgcrProcessor) Save(ctx context.Context, qtx *db.Queries, pgcr *pgcr.PgcrInfo, source Source, b []byte) error {
	// If inserting to the ledger fails, skip inserting to the DB
	entry, err := p.queries.UpsertLogEntry(ctx, db.UpsertLogEntryParams{
		InstanceID: pgcr.InstanceId,
		Source:     sources[source],
		Status:     statuses[processing],
	})
	if err != nil {
		slog.Error("Failed to insert to ingestion log", "instanceId", pgcr.InstanceId, "error", err)
		return err
	}

	switch entry.Status {
	case statuses[success]:
		slog.Info("Instanced already processed successfully, skipping", "instanceId", pgcr.InstanceId)
		return nil
	case statuses[errored]:
		slog.Warn("Retrying previously failed instance", "instanceId", pgcr.InstanceId)
	case statuses[processing]:
		if time.Since(entry.LastAttemptAt) > staleThreshold {
			slog.Warn("Reclaiming stale processing entry", "instanceId", pgcr.InstanceId)
		} else {
			slog.Info("Instance actively being processed elsewhere, skipping", "instanceId", pgcr.InstanceId)
			return nil
		}
	}

	if err := p.queries.CreateInstance(ctx, db.CreateInstanceParams{
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

	if err := p.queries.CreatePgcr(ctx, db.CreatePgcrParams{
		InstanceID: pgcr.InstanceId,
		Blob:       b,
	}); err != nil {
		slog.Error("Failed to save raw pgcr instance", "instanceId", pgcr.InstanceId, "error", err)
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

		_, err := p.queries.CreateDestinyPlayer(ctx, player)
		if err != nil {
			slog.Error("Failed to save destiny player", "instanceId", pgcr.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType)
			return err
		}

		// InstancePlayer
		err = p.queries.CreateInstancePlayer(ctx, db.CreateInstancePlayerParams{
			InstanceID:        pgcr.InstanceId,
			MembershipID:      pi.MembershipId,
			Completed:         sql.NullBool{Bool: pi.Completed},
			TimePlayedSeconds: pi.TimePlayedSeconds,
		})

		switch {
		case err == nil:
			isFullClear := pgcr.FromBeginning && pi.Completed
			if err := p.queries.IncrementPlayerCounts(ctx, db.IncrementPlayerCountsParams{
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
			if err := p.queries.CreateInstanceCharacter(ctx, db.CreateInstanceCharacterParams{
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
				SuperKills:   int32(ci.AbilityInformation.SuperKills),
				GrenadeKills: int32(ci.AbilityInformation.GrenadeKills),
				MeleeKills:   int32(ci.AbilityInformation.MeleeKills),
			}); err != nil {
				slog.Error("Failed to save instance character", "instanceId", pgcr.InstanceId, "membershipId", player.MembershipID, "membershipType", player.MembershipType, "characterId", ci.CharacterId)
				return err
			}

			for _, ciw := range ci.WeaponInformation {
				// Weapons
				strHash := strconv.FormatInt(ciw.WeaponHash, 10)
				manifestEntity, err := p.cache.Get(ctx, "DestinyInventoryItemDefinition", strHash)
				if err != nil {
					slog.Error("Unable to fetch manifest entity", "Hash", ciw.WeaponHash, "Error", err)
					continue
				}

				if err := p.queries.CreateWeapon(ctx, db.CreateWeaponParams{
					WeaponHash:    ciw.WeaponHash,
					IconUrl:       manifestEntity.DisplayProperties.Icon,
					WeaponName:    manifestEntity.DisplayProperties.Name,
					DamageType:    string(utils.GetDamageType(manifestEntity.EquippingBlock.AmmoType)),
					EquipmentSlot: string(utils.GetEquippingSlot(manifestEntity.EquippingBlock.EquipmentSlotTypeHash)),
				}); err != nil {
					slog.Error("Failed to save weapon", "weaponId", strHash)
					return err
				}

				// InstanceCharacterWeapons
				if err := p.queries.CreateInstanceCharacterWeapon(ctx, db.CreateInstanceCharacterWeaponParams{
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

func extractSource(headers amqp091.Table) (Source, error) {
	raw, ok := headers["source"]
	if !ok {
		return 0, fmt.Errorf("missing source header")
	}

	str, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("source header is not a string, got %T", raw)
	}

	src, ok := reverseSources[str]
	if !ok {
		return 0, fmt.Errorf("unrecognized source value: %q", str)
	}

	return src, nil
}
