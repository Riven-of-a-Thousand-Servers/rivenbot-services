package mapper

import (
	"cmp"
	"context"
	"database/sql"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/compress"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/types/bungie"
	"pgcr-processing-service/internal/types/manifest"
)

// Pgcr maps fields from the Raw PGCR json fields to equivalent database entities
type DbMapper struct {
	manifestCache cache.ManifestCache[manifest.Entry]
}

func New(cache cache.ManifestCache[manifest.Entry]) *DbMapper {
	return &DbMapper{
		manifestCache: cache,
	}
}

const (
	pstTimezone string = "America/Los_Angeles"
)

func (m *DbMapper) MapToDestinyPlayers(pgcr bungie.PostGameCarnageReport) []db.CreateDestinyPlayerParams {
	players := getUniquePlayers(pgcr)
	entities := make([]db.CreateDestinyPlayerParams, 0, len(players))
	for _, player := range players {
		entity := db.CreateDestinyPlayerParams{
			MembershipID:      player.MembershipId.Int64(),
			MembershipType:    int32(player.MembershipType),
			IsPublic:          sql.NullBool{Bool: player.IsPublic, Valid: true},
			IconPath:          sql.NullString{String: player.IconPath, Valid: player.IconPath != ""},
			DisplayName:       sql.NullString{String: player.DisplayName, Valid: player.DisplayName != ""},
			GlobalDisplayName: sql.NullString{String: player.BungieGlobalDisplayName, Valid: player.BungieGlobalDisplayName != ""},
			GlobalDisplayNameCode: sql.NullInt32{
				Int32: int32(player.BungieGlobalDisplayNameCode), Valid: player.BungieGlobalDisplayNameCode != 0,
			},
		}
		entities = append(entities, entity)
	}

	// Sort them so every worker locks in the same order when inserting to DB
	slices.SortFunc(entities, func(a, b db.CreateDestinyPlayerParams) int {
		return cmp.Compare(a.MembershipID, b.MembershipID)
	})

	return entities
}

func getUniquePlayers(pgcr bungie.PostGameCarnageReport) map[string]bungie.DestinyUserEntry {
	players := make(map[string]bungie.DestinyUserEntry)
	for _, entry := range pgcr.Entries {
		memId := entry.Player.DestinyUserInfo.MembershipId.String()
		if _, ok := players[memId]; !ok {
			players[memId] = entry.Player.DestinyUserInfo
		}
	}

	return players
}

// Compresses the PostGameCarnageReport and returns the associated DB entity
func (m *DbMapper) MapToBlobObject(pgcr bungie.PostGameCarnageReport) (db.CreatePgcrParams, error) {
	var params db.CreatePgcrParams
	raw, err := compress.Gzip(pgcr)
	if err != nil {
		return params, err
	}

	params.Blob = raw
	params.InstanceID = pgcr.ActivityDetails.InstanceId.Int64()
	return params, nil
}

// Fetches all weapon definitions from a PGCR from all players into the its corresponding
// database entities
func (m *DbMapper) MapToDBWeapons(ctx context.Context, pgcr bungie.PostGameCarnageReport) ([]db.CreateWeaponParams, error) {
	var weps []db.CreateWeaponParams
	for _, entry := range pgcr.Entries {
		if entry.Extended == nil {
			continue
		}
		for _, weapon := range entry.Extended.Weapons {
			params := db.CreateWeaponParams{
				WeaponHash: weapon.ReferenceId,
			}

			itemDef, err := m.manifestCache.Get(ctx, strconv.FormatInt(weapon.ReferenceId, 10), manifest.InventoryItemDefinition)
			if err != nil {
				slog.Warn("Unable to fetch inventory item definiton", "hash", weapon.ReferenceId, "error", err)
				params.IconUrl = ""
				params.WeaponName = ""
			} else {
				params.IconUrl = itemDef.DisplayProperties.Icon
				params.WeaponName = itemDef.DisplayProperties.Name
			}

			if damageDef, err := m.manifestCache.Get(ctx, strconv.FormatInt(itemDef.DefaultDamageTypeHash, 10), manifest.DamageTypeDefinition); err != nil {
				slog.Warn("Unable to fetch default damage type definition", "hash", itemDef.DefaultDamageTypeHash, "error", err)
				params.DamageType = ""
			} else {
				params.DamageType = damageDef.DisplayProperties.Name
			}

			if equipmentSlotDef, err := m.manifestCache.Get(ctx, strconv.FormatInt(itemDef.EquippingBlock.EquipmentSlotTypeHash, 10), manifest.EquipmentSlotDefinition); err != nil {
				slog.Warn("Unable to fetch default equipment slot definition", "hash", itemDef.EquippingBlock.EquipmentSlotTypeHash, "error", err)
				params.EquipmentSlot = ""
			} else {
				params.EquipmentSlot = equipmentSlotDef.DisplayProperties.Name
			}

			weps = append(weps, params)
		}
	}
	return weps, nil
}

func (m *DbMapper) MapToDBInstance(pgcr bungie.PostGameCarnageReport) (db.CreateInstanceParams, error) {
	startTime, err := time.Parse(time.RFC3339, pgcr.Period)
	isFlawless := isFlawless(pgcr)
	isFresh, err := isFresh(&pgcr, isFlawless)
	if err != nil {
		return db.CreateInstanceParams{}, err
	}

	var maxDuration float64 = 0
	for _, e := range pgcr.Entries {
		maxDuration = math.Max(float64(maxDuration), float64(e.Values.ActivityDurationSeconds))
	}

	endTime := startTime.Add(time.Second * time.Duration(maxDuration))

	groups, err := groupByMembershipId(pgcr)
	if err != nil {
		return db.CreateInstanceParams{}, err
	}

	return db.CreateInstanceParams{
		ID:              pgcr.ActivityDetails.InstanceId.Int64(),
		ActivityHash:    pgcr.ActivityDetails.ActivityHash,
		IsFresh:         *isFresh,
		Flawless:        isFlawless,
		PlayerCount:     int32(len(groups)),
		StartTime:       startTime,
		EndTime:         endTime,
		DurationSeconds: int32(endTime.Sub(startTime).Seconds()),
	}, nil
}

func (m *DbMapper) MapToInstancePlayers(pgcr bungie.PostGameCarnageReport) ([]db.CreateInstancePlayerParams, error) {
	var players []db.CreateInstancePlayerParams
	groupedPlayers, err := groupByMembershipId(pgcr)
	if err != nil {
		return nil, err
	}

	for memId, toons := range groupedPlayers {
		players = append(players, db.CreateInstancePlayerParams{
			InstanceID:        pgcr.ActivityDetails.InstanceId.Int64(),
			MembershipID:      memId,
			TimePlayedSeconds: playerTimePlayed(toons),
			Completed:         sql.NullBool{Bool: playerCompleted(toons), Valid: true},
		})
	}

	return players, nil
}

func (m *DbMapper) MapToInstanceToons(pgcr bungie.PostGameCarnageReport) ([]db.CreateInstanceCharacterParams, error) {
	var params []db.CreateInstanceCharacterParams
	grouped, err := groupByMembershipId(pgcr)
	if err != nil {
		return nil, err
	}

	instanceId := pgcr.ActivityDetails.InstanceId.Int64()
	for memId, toons := range grouped {
		for _, toon := range toons {
			var superKills, grenadeKills, meleeKills int32
			if toon.Extended != nil {
				superKills = int32(toon.Extended.Abilities.SuperKills)
				grenadeKills = int32(toon.Extended.Abilities.GrenadeKills)
				meleeKills = int32(toon.Extended.Abilities.MeleeKills)
			}
			params = append(params, db.CreateInstanceCharacterParams{
				InstanceID:   instanceId,
				MembershipID: memId,
				CharacterID:  toon.CharacterId.Int64(),
				Completed:    toon.Values.Completed == 1.0,
				ClassHash:    toon.Player.ClassHash,
				EmblemHash:   toon.Player.EmblemHash,
				Kills:        int32(toon.Values.Kills),
				Deaths:       int32(toon.Values.Deaths),
				Assists:      int32(toon.Values.Assists),
				Kda:          toon.Values.Kda.String(),
				Kdr:          toon.Values.Kdr.String(),
				Efficiency:   int32(toon.Values.Efficiency),
				SuperKills:   superKills,
				GrenadeKills: grenadeKills,
				MeleeKills:   meleeKills,
			})
		}
	}
	return params, nil
}

func (m *DbMapper) MapToInstanceToonWeapons(pgcr bungie.PostGameCarnageReport) ([]db.CreateInstanceCharacterWeaponParams, error) {
	var weapons []db.CreateInstanceCharacterWeaponParams
	grouped, err := groupByMembershipId(pgcr)
	if err != nil {
		return nil, err
	}

	instanceId := pgcr.ActivityDetails.InstanceId
	for memId, toons := range grouped {
		for _, toon := range toons {
			if toon.Extended == nil {
				continue
			}
			charWeapons := toon.Extended.Weapons
			for _, weapon := range charWeapons {
				wep := db.CreateInstanceCharacterWeaponParams{
					InstanceID:         instanceId.Int64(),
					PlayerMembershipID: memId,
					PlayerCharacterID:  toon.CharacterId.Int64(),
					WeaponID:           weapon.ReferenceId,
					Kills:              int32(weapon.Values.WeaponKills),
					PrecisionKills:     int32(weapon.Values.PrecisionKills),
					PrecisionRatio:     weapon.Values.PrecisionRatio.String(),
				}
				weapons = append(weapons, wep)
			}
		}
	}

	return weapons, nil
}

func playerTimePlayed(toons []bungie.StatsEntry) int32 {
	var timePlayed int32
	for _, toon := range toons {
		timePlayed += int32(toon.Values.TimePlayedSeconds)
	}
	return timePlayed
}

func playerCompleted(toons []bungie.StatsEntry) bool {
	for _, toon := range toons {
		if toon.Values.Completed == 1.0 {
			return true
		}
	}
	return false
}

func groupByMembershipId(report bungie.PostGameCarnageReport) (map[int64][]bungie.StatsEntry, error) {
	groupedPlayers := make(map[int64][]bungie.StatsEntry)
	for _, entry := range report.Entries {
		memId := entry.Player.DestinyUserInfo.MembershipId.Int64()
		if val, ok := groupedPlayers[memId]; ok {
			groupedPlayers[memId] = append(val, entry)
		} else {
			groupedPlayers[memId] = []bungie.StatsEntry{entry}
		}
	}
	return groupedPlayers, nil
}

// Create an individual player character info struct based on a stats entry
// This utilizes Redis to fetch several pre-indexed manifest objects
// If querying Redis fails then this method return an error

func isFlawless(pgcr bungie.PostGameCarnageReport) bool {
	for _, entry := range pgcr.Entries {
		if entry.Values.Deaths > 0 {
			return false
		}
	}
	return true
}

// Resolves if a raid was fresh or not, courtesy of @Newo
func isFresh(pgcr *bungie.PostGameCarnageReport, flawless bool) (*bool, error) {
	var result *bool = new(bool)

	startTime, err := time.Parse(time.RFC3339, pgcr.Period)
	if err != nil {
		slog.Error("Failed to parse timestamp to determine isFresh", "pgcr", pgcr.ActivityDetails.InstanceId, "error", err)
		return nil, err
	}

	if startTime.After(hauntedStart) || startTime.Equal(hauntedStart) {
		return &pgcr.ActivityWasStartedFromBeginning, nil
	} else if startTime.Before(beyondLightStart) {
		isScourge := pgcr.ActivityDetails.ActivityHash == sotpHash1 || pgcr.ActivityDetails.ActivityHash == sotpHash2
		isLeviathan := leviHashes[pgcr.ActivityDetails.ActivityHash]

		if isScourge {
			*result = pgcr.StartingPhaseIndex <= 1
			return result, nil
		} else if isLeviathan {
			*result = pgcr.StartingPhaseIndex == 0 || pgcr.StartingPhaseIndex == 2
			return result, nil
		} else {
			*result = pgcr.StartingPhaseIndex == 0
			return result, nil
		}
	} else if startTime.After(witchQueenStart) && (pgcr.ActivityWasStartedFromBeginning || flawless) {
		return &pgcr.ActivityWasStartedFromBeginning, nil
	}

	return result, nil
}
