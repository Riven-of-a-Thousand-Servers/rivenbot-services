package ui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

var baseTableStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240"))

const (
	headerString = "PGCR dataset import — %s\nCompleted Files: %d/%d   Errors: %d   Elapsed: %s   Throughput: %.0f rows/s   In-Flight: %d"
)

func gradientTitle(text string) string {
	runes := []rune(text)
	colors := lipgloss.Blend1D(len(runes), lipgloss.Color("#7D56F4"), lipgloss.Color("#FF6AC1"))

	var b strings.Builder
	for i, r := range runes {
		b.WriteString(lipgloss.NewStyle().
			Foreground(colors[i]).
			Bold(true).
			Render(string(r)))
	}
	return b.String()
}
