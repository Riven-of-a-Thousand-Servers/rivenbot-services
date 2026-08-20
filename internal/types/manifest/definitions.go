package manifest

type EntityDefinition int

const (
	ActivityDefinition EntityDefinition = iota + 1
	DestinationDefinition
	InventoryItemDefinition
	DamageTypeDefinition
	EquipmentSlotDefinition
)

func (d EntityDefinition) String() string {
	switch d {
	case ActivityDefinition:
		return "DestinyActivityDefinition"
	case DestinationDefinition:
		return "DestinyDestinationDefinition"
	case InventoryItemDefinition:
		return "DestinyInventoryItemDefinition"
	case DamageTypeDefinition:
		return "DestinyDamageTypeDefinition"
	case EquipmentSlotDefinition:
		return "DestinyEquipmentSlotDefinition"
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
	case "DamageTypeDefinition":
		return DamageTypeDefinition, true
	case "DestinyEquipmentSlotDefinition":
		return EquipmentSlotDefinition, true
	default:
		return 0, false
	}
}
