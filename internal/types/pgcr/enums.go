package pgcr

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

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

type RaidDifficulty string

const (
	Normal        RaidDifficulty = "Normal"
	Prestige      RaidDifficulty = "Prestige"
	Master        RaidDifficulty = "Master"
	GuidedGames   RaidDifficulty = "Guided Games"
	ChallengeMode RaidDifficulty = "Challenge Mode"
)

func (d RaidDifficulty) String() string {
	switch d {
	case Normal:
		return "Normal"
	case Prestige:
		return "Prestige"
	case Master:
		return "Master"
	case GuidedGames:
		return "Guided Games"
	case ChallengeMode:
		return "Challenge Mode"
	default:
		return ""
	}
}

func ParseRaidDifficulty(s string) (RaidDifficulty, bool) {
	switch s {
	case "Normal":
		return Normal, true
	case "Prestige":
		return Prestige, true
	case "Master":
		return Master, true
	case "Guided Games":
		return GuidedGames, true
	case "Challenge Mode":
		return ChallengeMode, true
	default:
		return "", false
	}
}

type DamageType string

const (
	Kinetic DamageType = "Kinetic"
	Arc     DamageType = "Arc"
	Void    DamageType = "Void"
	Solar   DamageType = "Solar"
	Stasis  DamageType = "Stasis"
	Strand  DamageType = "Strand"
)

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

type ClearType string

const (
	Solo         ClearType = "Solo"
	Duo          ClearType = "Duo"
	Trio         ClearType = "Trio"
	SoloFlawless ClearType = "Solo Flawless"
	DuoFlawless  ClearType = "Duo Flawless"
	TrioFlawless ClearType = "Trio Flawless"
)

func (c ClearType) String() string {
	switch c {
	case Solo:
		return "Solo"
	case Duo:
		return "Duo"
	case Trio:
		return "Trio"
	case SoloFlawless:
		return "Solo Flawless"
	case DuoFlawless:
		return "Duo Flawless"
	case TrioFlawless:
		return "Trio Flawless"
	default:
		return ""
	}
}

func ParseClearType(s string) (ClearType, bool) {
	switch s {
	case "Solo":
		return Solo, true
	case "Duo":
		return Duo, true
	case "Trio":
		return Trio, true
	case "Solo Flawless":
		return SoloFlawless, true
	case "Duo Flawless":
		return DuoFlawless, true
	case "Trio Flawless":
		return TrioFlawless, true
	default:
		return "", false
	}
}

func GetRaidAndDifficulty(label string) (RaidName, RaidDifficulty, error) {
	tokens := strings.Split(label, ":")

	if len(tokens) <= 0 {
		log.Panicf("Unable to tokenize raid Manifest Display Name [%s]", label)
		return "", "", errors.New("Unable to tokenize raid Manifest Display Name")
	}
	name := strings.TrimSpace(tokens[0])
	raidName, ok := ParseRaidName(name)
	if !ok {
		return "", "", fmt.Errorf("Raid name [%s] has no match", name)
	}

	if len(tokens) <= 1 {
		return raidName, Normal, nil
	}

	difficulty := strings.TrimSpace(tokens[1])
	raidDifficulty, ok := ParseRaidDifficulty(difficulty)
	if !ok {
		switch {
		case strings.EqualFold(difficulty, "Standard"):
			raidDifficulty = Normal
		case strings.EqualFold(difficulty, "Expert") || strings.EqualFold(difficulty, "Legend"):
			raidDifficulty = ChallengeMode
		default:
			return "", "", fmt.Errorf("Raid difficulty [%s] has no match", difficulty)
		}
	}

	return raidName, raidDifficulty, nil
}

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
