package utils

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"pgcr-processing-service/internal/types/pgcr"
)

var (
	reverseClassLabels   map[string]pgcr.CharacterClass
	characterClassLabels = map[pgcr.CharacterClass]string{
		pgcr.TITAN:   "Titan",
		pgcr.WARLOCK: "Warlock",
		pgcr.HUNTER:  "Hunter",
	}
)

func ClassLabel(c pgcr.CharacterClass) string {
	if label, exists := characterClassLabels[c]; exists {
		return label
	} else {
		return ""
	}
}

var (
	reverseRaidLabels map[string]pgcr.RaidName
	raidNameLabels    = map[pgcr.RaidName]string{
		pgcr.SALVATIONS_EDGE:     "Salvation's Edge",
		pgcr.CROTAS_END:          "Crota's End",
		pgcr.ROOT_OF_NIGHTMARES:  "Root of Nightmares",
		pgcr.KINGS_FALL:          "King's Fall",
		pgcr.VOW_OF_THE_DISCIPLE: "Vow of the Disciple",
		pgcr.VAULT_OF_GLASS:      "Vault of Glass",
		pgcr.DEEP_STONE_CRYPT:    "Deep Stone Crypt",
		pgcr.GARDEN_OF_SALVATION: "Garden of Salvation",
		pgcr.CROWN_OF_SORROW:     "Crown of Sorrow",
		pgcr.LAST_WISH:           "Last Wish",
		pgcr.SPIRE_OF_STARS:      "Leviathan, Spire of Stars",
		pgcr.EATER_OF_WORLDS:     "Leviathan, Eater of Worlds",
		pgcr.LEVIATHAN:           "Leviathan",
		pgcr.SCOURGE_OF_THE_PAST: "Scourge of the Past",
	}
)

func RaidLabel(rn pgcr.RaidName) string {
	if label, exists := raidNameLabels[rn]; exists {
		return label
	} else {
		return ""
	}
}

var (
	reverseDifficultyLabels map[string]pgcr.RaidDifficulty
	raidDifficultyLabels    = map[pgcr.RaidDifficulty]string{
		pgcr.NORMAL:         "Normal",
		pgcr.PRESTIGE:       "Prestige",
		pgcr.MASTER:         "Master",
		pgcr.GUIDED_GAMES:   "Guided Games",
		pgcr.CHALLENGE_MODE: "Challenge Mode",
	}
)

var once sync.Once

// Initializes the reverse look up maps only once
func initReverseMaps() {
	once.Do(func() {
		reverseDifficultyLabels = make(map[string]pgcr.RaidDifficulty)
		for raidDifficulty, label := range raidDifficultyLabels {
			reverseDifficultyLabels[label] = raidDifficulty
		}

		reverseRaidLabels = make(map[string]pgcr.RaidName)
		for raidName, label := range raidNameLabels {
			reverseRaidLabels[label] = raidName
		}

		reverseClassLabels = make(map[string]pgcr.CharacterClass)
		for characterClass, label := range characterClassLabels {
			reverseClassLabels[label] = characterClass
		}
	})
}

// This method returns an instance of a Raid Name and RaidDifficulty
// given a string, e.g., Last Wish should yield both the RaidName LAST_WISH
// and the RaidDiffculty NORMAL. On the other hand, Salvation's Edge: Master
// should yield RaidName SALVATIONS_EDGE and RaidDifficulty MASTER
func GetRaidAndDifficulty(label string) (pgcr.RaidName, pgcr.RaidDifficulty, error) {
	initReverseMaps()
	tokens := strings.Split(label, ":")

	if len(tokens) <= 0 {
		log.Panicf("Unable to tokenize raid Manifest Display Name [%s]", label)
		return "", "", errors.New("Unable to tokenize raid Manifest Display Name")
	}
	name := strings.TrimSpace(tokens[0])
	raidName, nameExists := reverseRaidLabels[name]

	if !nameExists {
		return "", "", fmt.Errorf("Raid name [%s] has no match", name)
	}

	if len(tokens) <= 1 {
		return raidName, pgcr.NORMAL, nil
	}

	difficulty := strings.TrimSpace(tokens[1]) // Default difficulty
	raidDifficulty, difficultyExists := reverseDifficultyLabels[difficulty]
	if !difficultyExists {
		switch {
		case strings.EqualFold(difficulty, "Standard"):
			raidDifficulty = reverseDifficultyLabels["Normal"]
		case strings.EqualFold(difficulty, "Expert") || strings.EqualFold(difficulty, "Legend"):
			raidDifficulty = reverseDifficultyLabels["Challenge Mode"]
		default:
			return "", "", fmt.Errorf("Raid difficulty [%s] has no match", difficulty)
		}
	}

	return raidName, raidDifficulty, nil
}

func GetDamageType(enumValue int) pgcr.DamageType {
	switch enumValue {
	case 1:
		return pgcr.KINETIC
	case 2:
		return pgcr.ARC
	case 3:
		return pgcr.SOLAR
	case 4:
		return pgcr.VOID
	case 6:
		return pgcr.STASIS
	case 7:
		return pgcr.STRAND
	default:
		return ""
	}
}

type EquippingBlocktypes interface {
	~int64 | ~string | ~int
}

func GetEquippingSlot[T EquippingBlocktypes](enumValue T) pgcr.EquipmentSlot {
	value := any(enumValue)
	if v, ok := value.(int64); ok {
		switch v {
		case 1498876634:
			return pgcr.PRIMARY
		case 2465295065:
			return pgcr.SPECIAL
		case 953998645:
			return pgcr.HEAVY
		}
	} else if s, ok := value.(string); ok {
		switch strings.ToLower(s) {
		case "kinetic weapons":
			return pgcr.PRIMARY
		case "energy weapons":
			return pgcr.SPECIAL
		case "power weapons":
			return pgcr.HEAVY
		}
	}
	return ""
}
