package pgcr

import "strings"

type EquipmentSlot string

const (
	Primary EquipmentSlot = "Primary"
	Special EquipmentSlot = "Special"
	Heavy   EquipmentSlot = "Heavy"
)

func (e EquipmentSlot) String() string {
	switch e {
	case Primary:
		return "Primary"
	case Special:
		return "Special"
	case Heavy:
		return "Heavy"
	default:
		return ""
	}
}

// TODO: Equipment slot should be fetched from the manifest rather than
// rely on enum types. Seems bungie likes their manifest DB more than that
func ParseEquipmentSlot(s string) (EquipmentSlot, bool) {
	switch s {
	case "Primary":
		return Primary, true
	case "Special":
		return Special, true
	case "Heavy":
		return Heavy, true
	default:
		return "", false
	}
}

type EquippingBlockTypes interface {
	~int64 | ~string | ~int
}

func GetEquippingSlot[T EquippingBlockTypes](enum T) EquipmentSlot {
	switch v := any(enum).(type) {
	case int64:
		switch v {
		case 1498876634:
			return Primary
		case 2465295065:
			return Special
		case 953998645:
			return Heavy
		}
	case string:
		switch strings.ToLower(v) {
		case "kinetic weapons":
			return Primary
		case "energy weapons":
			return Special
		case "power weapons":
			return Heavy
		}
	}
	return ""
}
