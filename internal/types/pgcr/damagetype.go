package pgcr

type DamageType string

const (
	Kinetic DamageType = "Kinetic"
	Arc     DamageType = "Arc"
	Void    DamageType = "Void"
	Solar   DamageType = "Solar"
	Stasis  DamageType = "Stasis"
	Strand  DamageType = "Strand"
)

func GetDamageType(enumValue int) DamageType {
	switch enumValue {
	case 1:
		return Kinetic
	case 2:
		return Arc
	case 3:
		return Solar
	case 4:
		return Void
	case 6:
		return Stasis
	case 7:
		return Strand
	default:
		return ""
	}
}

func (d DamageType) String() string {
	switch d {
	case Kinetic:
		return "Kinetic"
	case Arc:
		return "Arc"
	case Void:
		return "Void"
	case Solar:
		return "Solar"
	case Stasis:
		return "Stasis"
	case Strand:
		return "Strand"
	default:
		return ""
	}
}

func ParseDamageType(s string) (DamageType, bool) {
	switch s {
	case "Kinetic":
		return Kinetic, true
	case "Arc":
		return Arc, true
	case "Void":
		return Void, true
	case "Solar":
		return Solar, true
	case "Stasis":
		return Stasis, true
	case "Strand":
		return Strand, true
	default:
		return "", false
	}
}
