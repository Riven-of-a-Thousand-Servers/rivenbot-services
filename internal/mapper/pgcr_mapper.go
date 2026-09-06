package mapper

import (
	"context"
	"database/sql"
	"log/slog"
	"math"
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

func (m *DbMapper) MapToDestinyPlayers(pgcr bungie.PostGameCarnageReport) ([]db.CreateDestinyPlayerParams, error) {
	players := getUniquePlayers(pgcr)
	entities := make([]db.CreateDestinyPlayerParams, len(players))
	for _, player := range players {
		membershipId, err := strconv.ParseInt(player.MembershipId, 10, 64)
		if err != nil {
			slog.Error("Failed to parse membershipId to int64", "membershipId", player.MembershipId, "pgcr", pgcr.ActivityDetails.InstanceId, "error", err)
			return nil, err
		}

		entity := db.CreateDestinyPlayerParams{
			MembershipID:   membershipId,
			MembershipType: int32(player.MembershipType),
			IsPublic:       sql.NullBool{Bool: player.IsPublic, Valid: true},
			IconPath:       sql.NullString{String: player.IconPath, Valid: player.IconPath != ""},
			DisplayName:    sql.NullString{String: player.DisplayName, Valid: player.DisplayName != ""},
		}
		if player.BungieGlobalDisplayName != "" {
			entity.DisplayName = sql.NullString{String: player.BungieGlobalDisplayName, Valid: player.BungieGlobalDisplayName != ""}
		} else {
			entity.DisplayName = sql.NullString{String: player.DisplayName, Valid: player.DisplayName != ""}
		}

		if player.BungieGlobalDisplayNameCode != 0 {
			entity.GlobalDisplayNameCode = sql.NullInt32{
				Int32: int32(player.BungieGlobalDisplayNameCode),
				Valid: player.BungieGlobalDisplayNameCode != 0,
			}
		}

		entities = append(entities, entity)
	}

	return entities, nil
}

func getUniquePlayers(pgcr bungie.PostGameCarnageReport) map[string]bungie.DestinyUserEntry {
	players := make(map[string]bungie.DestinyUserEntry)
	for _, entry := range pgcr.Entries {
		if _, ok := players[entry.Player.DestinyUserInfo.MembershipId]; !ok {
			players[entry.Player.DestinyUserInfo.MembershipId] = entry.Player.DestinyUserInfo
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
		dbToon := db.CreateInstanceCharacterParams{
			InstanceID:   instanceId,
			MembershipID: memId,
		}

		for _, toon := range toons {
			dbToon.CharacterID = toon.CharacterId.Int64()
			dbToon.Completed = toon.Values.Completed == 1.0
			dbToon.ClassHash = toon.Player.ClassHash
			dbToon.EmblemHash = toon.Player.EmblemHash
			dbToon.Kills = int32(toon.Values.Kills)
			dbToon.Deaths = int32(toon.Values.Kills)
			dbToon.Assists = int32(toon.Values.Assists)
			dbToon.Kda = toon.Values.Kda.String()
			dbToon.Kdr = toon.Values.Kdr.String()
			dbToon.Efficiency = int32(toon.Values.Efficiency)
			dbToon.SuperKills = int32(toon.Extended.Abilities.SuperKills)
			dbToon.GrenadeKills = int32(toon.Extended.Abilities.GrenadeKills)
			dbToon.MeleeKills = int32(toon.Extended.Abilities.MeleeKills)
		}

		params = append(params, dbToon)
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
		membershipId, err := strconv.ParseInt(entry.Player.DestinyUserInfo.MembershipId, 10, 64)
		if err != nil {
			slog.Error("Something went wrong when parsing membership ID to Int64", "MembershipId", entry.Player.DestinyUserInfo.MembershipId)
			return nil, err
		}
		if val, ok := groupedPlayers[membershipId]; ok {
			groupedPlayers[membershipId] = append(val, entry)
		} else {
			groupedPlayers[membershipId] = []bungie.StatsEntry{entry}
		}
	}
	return groupedPlayers, nil
}

// Takes in a map of grouped up PGCR entries by players' membershipIds and returns an array of PlayerInformation structs
// Ensures that each player will have all their characters respectively
func processPlayers(groups map[int64][]bungie.StatsEntry) ([]bungie.PlayerInfo, error) {
	result := []bungie.PlayerInfo{}
	for membershipId, entries := range groups {
		if len(entries) == 0 {
			slog.Info("Player with membershipId has no entries, skipping", "MembershipId", membershipId)
			continue
		}

		playerInfo := bungie.PlayerInfo{
			MembershipId:          membershipId,
			MembershipType:        entries[0].Player.DestinyUserInfo.MembershipType,
			DisplayName:           entries[0].Player.DestinyUserInfo.DisplayName,
			IsPublic:              entries[0].Player.DestinyUserInfo.IsPublic,
			IconPath:              entries[0].Player.DestinyUserInfo.IconPath,
			GlobalDisplayName:     entries[0].Player.DestinyUserInfo.BungieGlobalDisplayName,
			GlobalDisplayNameCode: entries[0].Player.DestinyUserInfo.BungieGlobalDisplayNameCode,
		}

		for _, e := range entries {
			characterInfo, err := createPlayerCharacter(&e)
			if err != nil {
				slog.Error("There was an error create character information for player with Id", "MembershipId", membershipId, "Error", err)
				return nil, err
			}
			playerInfo.CharacterInfo = append(playerInfo.CharacterInfo, *characterInfo)
		}

		for _, c := range playerInfo.CharacterInfo {
			if c.ActivityCompleted {
				playerInfo.Completed = true
				break
			}
		}

		totalTimePlayed := 0
		for _, c := range playerInfo.CharacterInfo {
			totalTimePlayed += c.TimePlayedSeconds
		}
		playerInfo.TimePlayedSeconds = int32(totalTimePlayed)
		result = append(result, playerInfo)
	}

	return result, nil
}

// Create an individual player character info struct based on a stats entry
// This utilizes Redis to fetch several pre-indexed manifest objects
// If querying Redis fails then this method return an error
func createPlayerCharacter(entry *bungie.StatsEntry) (*bungie.CharacterInfo, error) {
	characterInfo := bungie.CharacterInfo{
		ActivityCompleted: entry.Values.Completed == 1.0,
		WeaponInfo:        []bungie.WeaponInfo{}, // empty just in case the player didn't do anything in the activity
	}

	class := bungie.CharacterClass(entry.Player.CharacterClass)
	characterInfo.CharacterId = entry.CharacterId.Int64()
	characterInfo.LightLevel = entry.Player.LightLevel
	characterInfo.CharacterClass = class
	characterInfo.CharacterEmblem = entry.Player.EmblemHash
	characterInfo.TimePlayedSeconds = int(entry.Values.TimePlayedSeconds)
	characterInfo.Kills = int(entry.Values.Kills)
	characterInfo.Deaths = int(entry.Values.Deaths)
	characterInfo.Assists = int(entry.Values.Assists)
	characterInfo.Kda = float64(entry.Values.Kda)
	characterInfo.Kdr = float64(entry.Values.Kdr)

	// Set weapon information
	if entry.Extended != nil {
		for _, weapon := range entry.Extended.Weapons {
			w := bungie.WeaponInfo{
				WeaponHash:     weapon.ReferenceId,
				Kills:          int(weapon.Values.WeaponKills),
				PrecisionKills: int(weapon.Values.PrecisionKills),
				PrecisionRatio: float64(weapon.Values.PrecisionRatio),
			}
			characterInfo.WeaponInfo = append(characterInfo.WeaponInfo, w)
		}

		// Set ability information
		abilityInfo := bungie.AbilityInfo{
			GrenadeKills: int(entry.Extended.Abilities.GrenadeKills),
			MeleeKills:   int(entry.Extended.Abilities.MeleeKills),
			SuperKills:   int(entry.Extended.Abilities.SuperKills),
		}
		characterInfo.AbilityInfo = abilityInfo
	}
	return &characterInfo, nil
}

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
