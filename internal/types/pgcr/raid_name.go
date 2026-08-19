package pgcr

type RaidName string

const (
	SalvationsEdge    RaidName = "Salvation's Edge"
	CrotasEnd         RaidName = "Crota's End"
	RootOfNightmares  RaidName = "Root of Nightmares"
	KingsFall         RaidName = "King's Fall"
	VowOfTheDisciple  RaidName = "Vow of the Disciple"
	VaultOfGlass      RaidName = "Vault of Glass"
	DeepStoneCrypt    RaidName = "Deep Stone Crypt"
	GardenOfSalvation RaidName = "Garden of Salvation"
	CrownOfSorrow     RaidName = "Crown of Sorrow"
	LastWish          RaidName = "Last Wish"
	SpireOfStars      RaidName = "Leviathan, Spire of Stars"
	EaterOfWorlds     RaidName = "Leviathan, Eater of Worlds"
	Leviathan         RaidName = "Leviathan"
	ScourgeOfThePast  RaidName = "Scourge of the Past"
)

func (r RaidName) String() string {
	switch r {
	case SalvationsEdge:
		return "Salvation's Edge"
	case CrotasEnd:
		return "Crota's End"
	case RootOfNightmares:
		return "Root of Nightmares"
	case KingsFall:
		return "King's Fall"
	case VowOfTheDisciple:
		return "Vow of the Disciple"
	case VaultOfGlass:
		return "Vault of Glass"
	case DeepStoneCrypt:
		return "Deep Stone Crypt"
	case GardenOfSalvation:
		return "Garden of Salvation"
	case CrownOfSorrow:
		return "Crown of Sorrow"
	case LastWish:
		return "Last Wish"
	case SpireOfStars:
		return "Leviathan, Spire of Stars"
	case EaterOfWorlds:
		return "Leviathan, Eater of Worlds"
	case Leviathan:
		return "Leviathan"
	case ScourgeOfThePast:
		return "Scourge of the Past"
	default:
		return ""
	}
}

func ParseRaidName(s string) (RaidName, bool) {
	switch s {
	case "Salvation's Edge":
		return SalvationsEdge, true
	case "Crota's End":
		return CrotasEnd, true
	case "Root of Nightmares":
		return RootOfNightmares, true
	case "King's Fall":
		return KingsFall, true
	case "Vow of the Disciple":
		return VowOfTheDisciple, true
	case "Vault of Glass":
		return VaultOfGlass, true
	case "Deep Stone Crypt":
		return DeepStoneCrypt, true
	case "Garden of Salvation":
		return GardenOfSalvation, true
	case "Crown of Sorrow":
		return CrownOfSorrow, true
	case "Last Wish":
		return LastWish, true
	case "Leviathan, Spire of Stars":
		return SpireOfStars, true
	case "Leviathan, Eater of Worlds":
		return EaterOfWorlds, true
	case "Leviathan":
		return Leviathan, true
	case "Scourge of the Past":
		return ScourgeOfThePast, true
	default:
		return "", false
	}
}
