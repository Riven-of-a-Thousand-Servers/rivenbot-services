package pipeline

import (
	"pgcr-processing-service/internal/telemetry"
	"pgcr-processing-service/internal/types/bungie"
)

const (
	// This is the associated enum value for a raid in the bungie API
	// see: https://bungie-net.github.io/multi/schema_Destiny-HistoricalStats-Definitions-DestinyActivityModeType.html#schema_Destiny-HistoricalStats-Definitions-DestinyActivityModeType
	raidMode = 4
)

func FilterRaids(item telemetry.Job[bungie.PostGameCarnageReport]) bool {
	if item.Skibidi.ActivityDetails.Mode == 4 {
		return true
	}
	item.Task.Skipped.Add(1)
	return false
}
