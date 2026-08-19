package pgcr

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

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
