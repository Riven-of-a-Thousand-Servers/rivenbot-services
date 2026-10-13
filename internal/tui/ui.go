package ui

import (
	"context"
	"fmt"
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

type mainPaneTickMsg time.Time

func mainPaneTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return mainPaneTickMsg(t)
	})
}

type Model struct {
	tracker *telemetry.Tracker
	snap    []telemetry.TaskView
	logs    []string
	state   uiState

	spinner       spinner.Model
	cacheStageMsg string

	// TODO: This table will belong only to the main-pane from the TUI
	// However, we still haven't setup panes yet
	table      table.Model
	cancelFunc context.CancelFunc
}

func NewModel(tracker *telemetry.Tracker, cancelFunc context.CancelFunc) Model {
	spinner := spinner.New(
		spinner.WithSpinner(spinner.Dot))

	return Model{
		tracker:    tracker,
		spinner:    spinner,
		state:      cacheWarming,
		table:      newTable(),
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
	return tea.Batch(mainPaneTick(), m.spinner.Tick)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case mainPaneTickMsg:
		if m.state == datasetProcessing {
			m.snap = m.tracker.SnapshotTasks()
			m.logs = m.tracker.SnapshotLogs(18)
		}
		cmds = append(cmds, mainPaneTick())
	case tea.KeyPressMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focused()
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
	filesPane := lipgloss.NewStyle().
		Width(118).
		Height(20).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.BrightCyan).
		Padding(0)

	logsPane := lipgloss.NewStyle().
		Width(100).
		Height(20).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.BrightCyan).
		AlignHorizontal(lipgloss.Left).
		AlignVertical(lipgloss.Left).
		Padding(0)

	switch m.state {
	case cacheWarming:
		s := "\nCache Warming Sequence\n"
		s += fmt.Sprintf("\n%s %s", m.spinner.View(), m.cacheStageMsg)
		return tea.NewView(s)
	case datasetProcessing:
		var v tea.View
		var rows []table.Row
		for _, task := range m.snap {
			row := createRow(task)
			rows = append(rows, row)
		}
		m.table.SetRows(rows)
		logs := strings.Join(m.logs, "\n")

		res := lipgloss.JoinHorizontal(228, filesPane.Render(m.table.View()), logsPane.Render(logs))
		v.Content = res
		v.AltScreen = true
		return v
	default:
	}
	return tea.NewView("Unknown state for the dataset process. Exiting.")
}

func createRow(task telemetry.TaskView) table.Row {
	percentDone := fmt.Sprintf("%.2f%%", (float64(task.LinesRead)/float64(task.LinesTotal))*100)
	linesRead := formatNumber(float64(task.LinesRead))
	bytesRead := humanize.Bytes(uint64(task.BytesRead))
	inserted := formatNumber(float64(task.Inserted))
	skipped := formatNumber(float64(task.Skipped))
	errored := formatNumber(float64(task.Errored))

	var params []string
	params = append(params, task.Filename)
	params = append(params, task.State.String())
	params = append(params, percentDone)
	params = append(params, linesRead)
	params = append(params, bytesRead)
	params = append(params, inserted)
	params = append(params, skipped)
	params = append(params, errored)

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
		{Title: "Lines Read", Width: 10},
		{Title: "Bytes Read", Width: 10},
		{Title: "Inserted", Width: 10},
		{Title: "Skipped", Width: 10},
		{Title: "Errored", Width: 10},
	}

	tbl := table.New(table.WithColumns(columns), table.WithFocused(false))
	s := table.DefaultStyles()
	s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		Bold(true)
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
