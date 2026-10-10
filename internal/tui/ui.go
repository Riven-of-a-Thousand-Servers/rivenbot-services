package ui

import (
	"context"
	"fmt"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/telemetry"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
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

func renderTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return RenderTick(t)
	})
}

func headerTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return HeaderTickMsg(t)
	})
}

type Model struct {
	tracker *telemetry.Tracker
	state   uiState

	// Cache warming state
	spinner       spinner.Model
	cacheStageMsg string

	tbl        table.Model
	dirty      bool
	quitting   bool
	cancelFunc context.CancelFunc
}

func NewModel(tracker *telemetry.Tracker, cancelFunc context.CancelFunc) Model {
	spinnerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	spinner := spinner.New(
		spinner.WithSpinner(spinner.Dot),
		spinner.WithStyle(spinnerStyle))

	return Model{
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
	return m.spinner.Tick
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case RenderTick:
		if m.dirty {
			m.dirty = false
		}
		cmds = append(cmds, renderTick())
	case HeaderTickMsg:
		cmds = append(cmds, headerTick())
	case tea.KeyPressMsg:
		switch msg.String() {
		case "esc":
			if m.tbl.Focused() {
				m.tbl.Blur()
			} else {
				m.tbl.Focused()
			}
		case "q", "ctrl+c":
			m.quitting = true
			m.cancelFunc()
			cmds = append(cmds, tea.Quit)
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		cmds = append(cmds, cmd, m.spinner.Tick)
		// Cache warming events
	case pubsub.Event[cache.CacheEvent]:
		switch msg.Type {
		case pubsub.CacheStarted:
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			cmds = append(cmds, cmd)
			m.state = cacheWarming
			m.cacheStageMsg = "Initializing cache..."
		case pubsub.CacheLoading:
			m.cacheStageMsg = fmt.Sprintf("Fetching %s", msg.Payload.CurrentDefinition.String())
		case pubsub.CacheFinished:
			// Cache warming finished, now moving to datasetProcessing
			m.cacheStageMsg = fmt.Sprintf("Finished warming up the cache with %d entries", msg.Payload.Size)
			m.state = datasetProcessing
		}
		m.dirty = true
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
	default:
	}
	return tea.NewView("Unknown state for the dataset process. Exiting.")
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
		{Title: "State", Width: 12},
		{Title: "Lines", Width: 20},
		{Title: "Inserted", Width: 8},
		{Title: "Skipped", Width: 8},
		{Title: "Errors", Width: 8},
		{Title: "Rate", Width: 12},
	}

	tbl := table.New(table.WithColumns(columns), table.WithFocused(false), table.WithWidth(100), table.WithHeight(12))
	s := table.DefaultStyles()
	s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)

	tbl.SetStyles(s)

	return tbl
}
