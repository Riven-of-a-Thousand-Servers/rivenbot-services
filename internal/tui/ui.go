package ui

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/telemetry"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/dustin/go-humanize"
)

const (
	rowsPerFile  = 10_000_000
	renderPeriod = 250 * time.Millisecond
)

type uiState int

const (
	databaseLoading uiState = iota + 1
	cacheWarming
	datasetProcessing
)

type (
	HeaderTickMsg time.Time
	RenderTick    time.Time
)

type snapshotTickMsg time.Time

func snapshotTick() tea.Cmd {
	return tea.Tick(250*time.Millisecond, func(t time.Time) tea.Msg {
		return snapshotTickMsg(t)
	})
}

type Model struct {
	tracker *telemetry.Tracker
	snap    []telemetry.TaskView
	state   uiState

	spinner       spinner.Model
	cacheStageMsg string

	tbl        table.Model
	cancelFunc context.CancelFunc
}

func NewModel(tracker *telemetry.Tracker, cancelFunc context.CancelFunc) Model {
	spinnerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	spinner := spinner.New(
		spinner.WithSpinner(spinner.Dot),
		spinner.WithStyle(spinnerStyle))

	return Model{
		tracker:    tracker,
		spinner:    spinner,
		state:      cacheWarming,
		tbl:        newTable(),
		cancelFunc: cancelFunc,
	}
}

func WaitForEvent[T any](ch <-chan T) tea.Cmd {
	return func() tea.Msg {
		e, ok := <-ch
		if !ok {
			return nil
		}
		return e
	}
}

// Start listening for broker events
func (m Model) Init() tea.Cmd {
	return snapshotTick()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case snapshotTickMsg:
		if m.state == datasetProcessing {
			m.snap = m.tracker.SnapshotTasks()
		}
		cmds = append(cmds, snapshotTick())
	case tea.KeyPressMsg:
		switch msg.String() {
		case "esc":
			if m.tbl.Focused() {
				m.tbl.Blur()
			} else {
				m.tbl.Focused()
			}
		case "q", "ctrl+c":
			m.cancelFunc()
			cmds = append(cmds, tea.Quit)
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		if m.state == cacheWarming {
			cmds = append(cmds, cmd)
		}
		// Cache warming events
	case pubsub.Event[cache.CacheEvent]:
		switch msg.Type {
		case pubsub.CacheStarted:
			m.state = cacheWarming
			m.cacheStageMsg = "Initializing cache..."
			cmds = append(cmds, m.spinner.Tick) // Initialize spinner
		case pubsub.CacheLoading:
			m.cacheStageMsg = fmt.Sprintf("Fetching %s", msg.Payload.CurrentDefinition.String())
		case pubsub.CacheFinished:
			// Cache warming finished, now moving to datasetProcessing
			m.cacheStageMsg = fmt.Sprintf("Finished warming up the cache with %d entries", msg.Payload.Size)
			m.state = datasetProcessing
		}
	}

	return m, tea.Batch(cmds...)
}

func (m Model) View() tea.View {
	switch m.state {
	case cacheWarming:
		s := "\nCache Warming Sequence\n"
		s += fmt.Sprintf("\n%s %s", m.spinner.View(), m.cacheStageMsg)
		return tea.NewView(s)
	case datasetProcessing:
		var rows []table.Row
		for _, task := range m.snap {
			row := createRow(task)
			slog.Debug("Rows", "cell", row)
			rows = append(rows, row)
		}
		m.tbl.SetRows(rows)
		return tea.NewView(m.tbl.View())
	default:
	}
	return tea.NewView("Unknown state for the dataset process. Exiting.")
}

func createRow(task telemetry.TaskView) table.Row {
	percentDone := float64(task.LinesRead) / float64(task.LinesTotal)
	linesRead := formatNumber(float64(task.LinesRead))
	bytesRead := humanize.Bytes(uint64(task.BytesRead))
	// inserted := formatNumber(float64(task.Inserted))
	// skipped := formatNumber(float64(task.Skipped))
	// errored := formatNumber(float64(task.Errored))
	var params []string
	params = append(params, task.Filename)
	params = append(params, task.State.String())
	params = append(params, fmt.Sprintf("%.2f%%", percentDone*100))
	params = append(params, linesRead)
	params = append(params, bytesRead)
	params = append(params, strconv.FormatInt(int64(task.Inserted), 10))
	params = append(params, strconv.FormatInt(int64(task.Skipped), 10))
	params = append(params, strconv.FormatInt(int64(task.Errored), 10))

	return params
}

func (m Model) footerView() string {
	return "\n[q/ctrl+c] quit"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-3] + "..."
}

func newTable() table.Model {
	columns := []table.Column{
		{Title: "File", Width: 30},
		{Title: "State", Width: 10},
		{Title: "Progress", Width: 10},
		{Title: "Lines Read", Width: 12},
		{Title: "Bytes Read", Width: 20},
		{Title: "Inserted", Width: 12},
		{Title: "Skipped", Width: 12},
		{Title: "Errored", Width: 12},
	}

	tbl := table.New(table.WithColumns(columns), table.WithFocused(false), table.WithWidth(200), table.WithHeight(12))
	s := table.DefaultStyles()
	s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	tbl.SetStyles(s)
	return tbl
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
