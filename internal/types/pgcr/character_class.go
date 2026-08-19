package pgcr

type CharacterClass string

const (
	Titan   CharacterClass = "Titan"
	Warlock CharacterClass = "Warlock"
	Hunter  CharacterClass = "Hunter"
)

func (c CharacterClass) String() string {
	switch c {
	case Titan:
		return "Titan"
	case Warlock:
		return "Warlock"
	case Hunter:
		return "Hunter"
	}

	return ""
}

func ParseClass(s string) (CharacterClass, bool) {
	switch s {
	case "Titan":
		return Titan, true
	case "Warlock":
		return Warlock, true
	case "Hunter":
		return Hunter, true
	default:
		return "", false
	}
}
