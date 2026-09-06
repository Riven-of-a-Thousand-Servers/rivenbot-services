package pipeline

import "pgcr-processing-service/internal/types/bungie"

const (
	// This is the associated enum value for a raid in the bungie API
	// see: https://bungie-net.github.io/multi/schema_Destiny-HistoricalStats-Definitions-DestinyActivityModeType.html#schema_Destiny-HistoricalStats-Definitions-DestinyActivityModeType
	raidMode = 4
)

func FilterByMode(item bungie.PostGameCarnageReport) bool {
	return item.ActivityDetails.Mode == 4
}
