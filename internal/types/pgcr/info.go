package pgcr

import (
	"time"
)

// TODO: Do we really need this intermediate representation for PGCRs?
// It seems to me that I just need to map to the database entities and there's
// that
type PgcrInfo struct {
	StartTime      time.Time      `json:"startTime"`
	EndTime        time.Time      `json:"endTime"`
	FromBeginning  bool           `json:"fromBeginning"`
	InstanceId     int64          `json:"instanceId"`
	RaidName       RaidName       `json:"raidName"`
	RaidDifficulty RaidDifficulty `json:"raidDifficulty"`
	ActivityHash   int64          `json:"activityHash"`
	Flawless       bool           `json:"flawless"`
	Solo           bool           `json:"solo"`
	Duo            bool           `json:"duo"`
	Trio           bool           `json:"trio"`
	PlayerInfo     []PlayerInfo   `json:"playerInformation"`
}

type PlayerInfo struct {
	MembershipId          int64           `json:"membershipId"`
	MembershipType        int             `json:"membershipType"`
	DisplayName           string          `json:"displayName"`
	GlobalDisplayName     string          `json:"globalDisplayName"`
	GlobalDisplayNameCode int             `json:"globalDisplayNameCode"`
	Completed             bool            `json:"completed"`
	TimePlayedSeconds     int32           `json:"timePlayedSeconds"`
	IconPath              string          `json:"iconPath"`
	IsPublic              bool            `json:"isPrivate"`
	CharacterInfo         []CharacterInfo `json:"characterInformation"`
}

type CharacterInfo struct {
	CharacterId       int64          `json:"characterId"`
	LightLevel        int            `json:"lightLevel"`
	CharacterClass    CharacterClass `json:"characterClass"`
	CharacterEmblem   int64          `json:"characterEmblem"`
	ActivityCompleted bool           `json:"activityCompleted"`
	Kills             int            `json:"kills"`
	Assists           int            `json:"assists"`
	Deaths            int            `json:"deaths"`
	Kda               float64        `json:"kda"`
	Kdr               float64        `json:"kdr"`
	Efficiency        int            `json:"efficiency"`
	TimePlayedSeconds int            `json:"timePlayedSeconds"`
	WeaponInfo        []WeaponInfo   `json:"weaponInformation"`
	AbilityInfo       AbilityInfo    `json:"abilityInformation"`
}

type WeaponInfo struct {
	WeaponHash     int64   `json:"weaponHash"`
	Kills          int     `json:"kills"`
	PrecisionKills int     `json:"precisionKills"`
	PrecisionRatio float64 `json:"precisionRatio"`
}

type AbilityInfo struct {
	GrenadeKills int `json:"grenadeKills"`
	MeleeKills   int `json:"meleeKills"`
	SuperKills   int `json:"superKills"`
}
