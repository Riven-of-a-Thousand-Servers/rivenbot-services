package ui

import "charm.land/lipgloss/v2"

var (
	CoolBlue = lipgloss.Color("#7DCFFF")
	Purple   = lipgloss.Color("#BB9AF7")
	Pink     = lipgloss.Color("#FF87D7")
	Green    = lipgloss.Color("#9ECE6A")
	Red      = lipgloss.Color("#F7768E")
	Neutral  = lipgloss.Color("#565F89")
)

var (
	NeutralStyle  = lipgloss.NewStyle().Foreground(Neutral)
	PurpleStyle   = lipgloss.NewStyle().Foreground(Purple)
	PinkStyle     = lipgloss.NewStyle().Foreground(Pink)
	GreenStyle    = lipgloss.NewStyle().Foreground(Green)
	RedStyle      = lipgloss.NewStyle().Foreground(Red)
	CoolBlueStyle = lipgloss.NewStyle().Foreground(CoolBlue)
)
