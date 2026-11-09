package ui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

var (
	tableStyle = lipgloss.NewStyle().BorderStyle(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240"))

	barOK   = lipgloss.NewStyle().Foreground(lipgloss.Color("#9ECE6A"))
	barWarn = lipgloss.NewStyle().Foreground(lipgloss.Color("#E0AF68"))
	barFull = lipgloss.NewStyle().Foreground(lipgloss.Color("#F7768E"))

	labelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#565F89"))
	valueStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#C0CAF5")).Bold(true)

	headerSeparator = labelStyle.Render("  |  ")
)

const (
	headerString = "PGCR dataset import — %s\nCompleted Files: %d/%d   Errors: %d   Elapsed: %s   Throughput: %.0f rows/s   In-Flight: %d"
)

func gradientTitle(text string) string {
	runes := []rune(text)
	colors := lipgloss.Blend2D(len(runes), 2, 50, lipgloss.Color("#7D56F4"), lipgloss.Color("#FF6AC1"))
	// colors := lipgloss.Blend1D(len(runes), lipgloss.Color("#7D56F4"), lipgloss.Color("#FF6AC1"))

	var b strings.Builder
	for i, r := range runes {
		b.WriteString(lipgloss.NewStyle().
			Foreground(colors[i]).
			Bold(true).
			Render(string(r)))
	}
	return b.String()
}
