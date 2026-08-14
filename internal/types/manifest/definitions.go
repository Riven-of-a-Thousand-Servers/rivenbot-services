package manifest

type EntityDefinition int

const (
	ActivityDefinition EntityDefinition = iota + 1
	DestinationDefinition
	InventoryItemDefinition
)

func (d EntityDefinition) String() string {
	switch d {
	case ActivityDefinition:
		return "DestinyActivityDefinition"
	case DestinationDefinition:
		return "DestinyDestinationDefinition"
	case InventoryItemDefinition:
		return "DestinyInventoryItemDefinition"
	default:
		return "unknown"
	}
}

func ParseEntity(s string) (EntityDefinition, bool) {
	switch s {
	case "DestinyActivityDefinition":
		return ActivityDefinition, true
	case "DestinyDestinationDefinition":
		return DestinationDefinition, true
	case "DestinyInventoryItemDefinition":
		return InventoryItemDefinition, true
	default:
		return 0, false
	}
}
