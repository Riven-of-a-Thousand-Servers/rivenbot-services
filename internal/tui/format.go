package ui

import (
	"math"
	"strconv"
	"strings"
)

func headerItem(label, value string) string {
	return labelStyle.Render(label+" ") + valueStyle.Render(value)
}

func formatNumber(in float64) string {
	abs := math.Abs(in)
	switch {
	case abs >= 1e9:
		return trimFloat(in/1e9) + "B"
	case abs >= 1e6:
		return trimFloat(in/1e6) + "M"
	case abs >= 1e3:
		return trimFloat(in/1e3) + "K"
	}
	return trimFloat(in)
}

func trimFloat(f float64) string {
	return strings.TrimSuffix(strconv.FormatFloat(f, 'f', 1, 64), ".0")
}

func max0(n int) int {
	return max(0, n)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}
