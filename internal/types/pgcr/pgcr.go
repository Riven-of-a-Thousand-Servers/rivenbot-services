package pgcr

import (
	"encoding/json"
	"fmt"
)

type Response struct {
	Response        PostGameCarnageReport `json:"Response"`
	ErrorCode       int                   `json:"ErrorCode"`
	ErrorStatus     string                `json:"ErrorStatus"`
	ThrottleSeconds int                   `json:"ThrottleSeconds"`
}

type PostGameCarnageReport struct {
	ActivityDetails                 ActivityEntry `json:"activityDetails"`
	Period                          string        `json:"period"`
	ActivityWasStartedFromBeginning bool          `json:"activityWasStartedFromBeginning"`
	StartingPhaseIndex              int           `json:"startingPhaseIndex"`
	Entries                         []StatsEntry  `json:"entries"`
}

type ActivityEntry struct {
	ReferenceId    int64  `json:"referenceId"`
	ActivityHash   int64  `json:"directorActivityHash"`
	InstanceId     string `json:"instanceId"`
	Mode           int    `json:"mode"`
	Modes          []int  `json:"modes"`
	IsPrivate      bool   `json:"isPrivate"`
	MembershipType int    `json:"membershipType"`
}

type StatsEntry struct {
	Player      PlayerEntry   `json:"player"`
	CharacterId string        `json:"characterId"`
	Values      StatValues    `json:"values"`
	Extended    *CarnageEntry `json:"extended"`
}

type StatValues struct {
	Efficiency              StatValue `json:"efficiency"`
	Score                   StatValue `json:"score"`
	Completed               StatValue `json:"completed"`
	Kills                   StatValue `json:"kills"`
	Deaths                  StatValue `json:"deaths"`
	Assists                 StatValue `json:"assists"`
	Kda                     StatValue `json:"killsDeathsAssists"`
	Kdr                     StatValue `json:"killsDeathsRatio"`
	TimePlayedSeconds       StatValue `json:"timePlayedSeconds"`
	ActivityDurationSeconds StatValue `json:"activityDurationSeconds"`
}

type StatValue float64

func (s *StatValue) UnmarshalJSON(data []byte) error {
	var f float64

	if err := json.Unmarshal(data, &f); err != nil {
		*s = StatValue(f)
	}

	var nested struct {
		Basic struct {
			Value float64 `json:"value"`
		} `json:"basic"`
	}

	if err := json.Unmarshal(data, &nested); err != nil {
		return fmt.Errorf("StatValue: unrecognized shape: %v", err)
	}

	*s = StatValue(nested.Basic.Value)
	return nil
}

type PlayerEntry struct {
	DestinyUserInfo DestinyUserEntry `json:"destinyUserInfo"`
	CharacterClass  string           `json:"characterClass"`
	ClassHash       int64            `json:"classHash"`
	RaceHash        int64            `json:"raceHash"`
	GenderHash      int64            `json:"genderHash"`
	LightLevel      int              `json:"lightLevel"`
	EmblemHash      int64            `json:"emblemHash"`
}

type DestinyUserEntry struct {
	IconPath                    string `json:"iconPath"`
	IsPublic                    bool   `json:"isPublic"`
	MembershipId                string `json:"membershipId"`
	MembershipType              int    `json:"membershipType"`
	DisplayName                 string `json:"displayName"`
	BungieGlobalDisplayName     string `json:"bungieGlobalDisplayName"`
	BungieGlobalDisplayNameCode int    `json:"bungieGlobalDisplayNameCode"`
}

type CarnageEntry struct {
	Weapons   []WeaponEntry `json:"weapons"`
	Abilities AbilityValues `json:"values"`
}

type WeaponEntry struct {
	ReferenceId int64        `json:"referenceId"`
	Values      WeaponValues `json:"values"`
}

type WeaponValues struct {
	WeaponKills    StatValue `json:"uniqueWeaponKills"`
	PrecisionKills StatValue `json:"uniqueWeaponPrecisionKills"`
	PrecisionRatio StatValue `json:"uniqueWeaponKillsPrecisionKills"`
}

type AbilityValues struct {
	PrecisionKills    StatValue `json:"precisionKills"`
	GrenadeKills      StatValue `json:"weaponKillsGrenade"`
	MeleeKills        StatValue `json:"weaponKillsMelee"`
	SuperKills        StatValue `json:"weaponKillsSuper"`
	ClassAbilityKills StatValue `json:"weaponKillsAbility"`
}
