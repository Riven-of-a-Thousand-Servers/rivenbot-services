package mapper

import (
	"context"
	"log/slog"
	"math"
	"strconv"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/db"
	"pgcr-processing-service/internal/types/manifest"
	"pgcr-processing-service/internal/types/pgcr"
)

// Mapper uses the supplied manifet cache to map elements of a PGCR to concrete
// attributes from both the pgcr itself and the bungie manifest
type Mapper struct {
	ManifestCache cache.ManifestCache[manifest.Entry]
}

func New(cache cache.ManifestCache[manifest.Entry]) *Mapper {
	return &Mapper{
		ManifestCache: cache,
	}
}

const (
	pstTimezone string = "America/Los_Angeles"
)

// Maps a pgcr.WeaponInfo struct to a db.CreateWeaponParams entity
func (m *Mapper) WeaponInfoToDBEntity(ctx context.Context, wep pgcr.WeaponInfo) (db.CreateWeaponParams, error) {
	var params db.CreateWeaponParams
	manifestEntity, err := m.ManifestCache.Get(ctx, strconv.FormatInt(wep.WeaponHash, 10), manifest.InventoryItemDefinition)
	if err != nil {
		return params, err
	}

	params = db.CreateWeaponParams{
		WeaponHash:    wep.WeaponHash,
		IconUrl:       manifestEntity.DisplayProperties.Icon,
		WeaponName:    manifestEntity.DisplayProperties.Name,
		DamageType:    pgcr.GetDamageType(manifestEntity.EquippingBlock.AmmoType).String(),
		EquipmentSlot: pgcr.GetEquippingSlot(manifestEntity.EquippingBlock.EquipmentSlotTypeHash).String(),
	}

	return params, nil
}

func (m *Mapper) ExtractInfo(ctx context.Context, report *pgcr.PostGameCarnageReport) (*pgcr.PgcrInfo, error) {
	enriched, err := m.enrichPgcrInfo(report)
	if err != nil {
		return nil, err
	}
	return enriched, nil
}

func (p *Mapper) enrichPgcrInfo(report *pgcr.PostGameCarnageReport) (*pgcr.PgcrInfo, error) {
	var entity pgcr.PgcrInfo

	// Calculate start and end time
	startTime, err := time.Parse(time.RFC3339, report.Period)
	if err != nil {
		slog.Error("Something went wrong when parsing the period for PGCR", "InstanceId", report.ActivityDetails.InstanceId, "Error", err)
		return nil, err
	}

	if len(report.Entries) == 0 {
		slog.Warn("No entries pgcr. Unable to determine activity duration", "pgcr", report.ActivityDetails.InstanceId)
	}

	// Get the max duration value for all players
	var maxDuration float64 = 0
	for _, e := range report.Entries {
		maxDuration = math.Max(float64(maxDuration), float64(e.Values.ActivityDurationSeconds))
	}

	endTime := startTime.Add(time.Second * time.Duration(maxDuration))

	entity.StartTime = startTime
	entity.EndTime = endTime

	instanceId, err := strconv.ParseInt(report.ActivityDetails.InstanceId, 10, 64)
	if err != nil {
		slog.Error("Unable to convert instanceIdto int64 for some reason?", "InstanceId", report.ActivityDetails.InstanceId)
		return nil, err
	}

	entity.InstanceId = instanceId
	entity.ActivityHash = report.ActivityDetails.ActivityHash
	activitiyHash := report.ActivityDetails.ActivityHash

	res, err := p.ManifestCache.Get(context.Background(),
		strconv.FormatInt(activitiyHash, 10),
		manifest.ActivityDefinition)
	if err != nil {
		slog.Error("Unable to find activity hash in Redis", "ActivityHash", activitiyHash, "Error", err)
		return nil, err
	}

	raidName, raidDifficulty, err := pgcr.GetRaidAndDifficulty(res.DisplayProperties.Name)
	if err != nil {
		slog.Error("Unable to parse activity raid name and raid difficulty", "error", err)
		return nil, err
	}

	entity.RaidName = raidName
	entity.RaidDifficulty = raidDifficulty

	groupedPlayers := make(map[int64][]pgcr.StatsEntry)
	for _, entry := range report.Entries {
		membershipId, err := strconv.ParseInt(entry.Player.DestinyUserInfo.MembershipId, 10, 64)
		if err != nil {
			slog.Error("Something went wrong when parsing membership ID to Int64", "MembershipId", entry.Player.DestinyUserInfo.MembershipId)
			return nil, err
		}
		val, ok := groupedPlayers[membershipId]
		if ok {
			groupedPlayers[membershipId] = append(val, entry)
		} else {
			groupedPlayers[membershipId] = []pgcr.StatsEntry{entry}
		}
	}

	if entity.PlayerInfo, err = processPlayers(groupedPlayers); err != nil {
		return nil, err
	}

	flawless := true

Outerloop:
	for _, players := range groupedPlayers {
		for _, player := range players {
			if player.Values.Deaths > 0.0 {
				flawless = false
				break Outerloop
			}
		}
	}

	fresh, err := resolveFromBeginning(report, flawless)
	if err != nil {
		slog.Error("Failed to determine if PGCR is fresh", "InstanceId", instanceId, "Error", err)
		return nil, err
	}

	trio := len(groupedPlayers) == 3
	duo := len(groupedPlayers) == 2
	solo := len(groupedPlayers) == 1

	entity.Trio = trio
	entity.Duo = duo
	entity.Solo = solo
	entity.Flawless = flawless
	entity.FromBeginning = *fresh
	return &entity, nil
}

// Takes in a map of grouped up PGCR entries by players' membershipIds and returns an array of PlayerInformation structs
// Ensures that each player will have all their characters respectively
func processPlayers(groups map[int64][]pgcr.StatsEntry) ([]pgcr.PlayerInfo, error) {
	result := []pgcr.PlayerInfo{}
	for membershipId, entries := range groups {
		if len(entries) == 0 {
			slog.Info("Player with membershipId has no entries, skipping", "MembershipId", membershipId)
			continue
		}

		playerInfo := pgcr.PlayerInfo{
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
func createPlayerCharacter(entry *pgcr.StatsEntry) (*pgcr.CharacterInfo, error) {
	characterInfo := pgcr.CharacterInfo{
		ActivityCompleted: entry.Values.Completed == 1.0,
		WeaponInfo:        []pgcr.WeaponInfo{}, // empty just in case the player didn't do anything in the activity
	}

	class := pgcr.CharacterClass(entry.Player.CharacterClass)

	characterId, err := strconv.ParseInt(entry.CharacterId, 10, 64)
	if err != nil {
		slog.Error("Unable to parse character Id to int64", "CharacterId", entry.CharacterId)
		return nil, err
	}

	characterInfo.CharacterId = characterId
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
			w := pgcr.WeaponInfo{
				WeaponHash:     weapon.ReferenceId,
				Kills:          int(weapon.Values.WeaponKills),
				PrecisionKills: int(weapon.Values.PrecisionKills),
				PrecisionRatio: float64(weapon.Values.PrecisionRatio),
			}
			characterInfo.WeaponInfo = append(characterInfo.WeaponInfo, w)
		}

		// Set ability information
		abilityInfo := pgcr.AbilityInfo{
			GrenadeKills: int(entry.Extended.Abilities.GrenadeKills),
			MeleeKills:   int(entry.Extended.Abilities.MeleeKills),
			SuperKills:   int(entry.Extended.Abilities.SuperKills),
		}
		characterInfo.AbilityInfo = abilityInfo
	}
	return &characterInfo, nil
}

// Resolves if a raid was fresh or not, courtesy of @Newo
func resolveFromBeginning(pgcr *pgcr.PostGameCarnageReport, flawless bool) (*bool, error) {
	var result *bool = new(bool)

	startTime, err := time.Parse(time.RFC3339, pgcr.Period)
	if err != nil {
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
