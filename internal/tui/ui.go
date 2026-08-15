package ui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	uiEvents "pgcr-processing-service/internal/types/ui"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const (
	rowsPerFile  = 10_000_000
	renderPeriod = 500 * time.Millisecond
)

type uiState int

const (
	DatabaseLoading uiState = iota + 1
	CacheWarming
	DatasetProcessing
)

type (
	HeaderTickMsg time.Time
	RenderTickMsg time.Time
)

func renderTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return RenderTickMsg{}
	})
}

func headerTick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return HeaderTickMsg(t)
	})
}

type fileState struct {
	rowsDone  int
	rowsTotal int
	startedAt time.Time
	errCount  int
}

type Model struct {
	f <-chan uiEvents.FileEvent
	c <-chan uiEvents.CacheEvent

	state      uiState
	spinner    spinner.Model
	tbl        table.Model
	inFlight   map[string]*fileState
	startedAt  time.Time
	filesTotal int
	filesDone  int
	errored    int
	dirty      bool
	done       bool
	quitting   bool
	cancelFunc context.CancelFunc
	logger     *slog.Logger
}

func NewModel(f <-chan uiEvents.FileEvent, c <-chan uiEvents.CacheEvent, filesTotal int, logger *slog.Logger, cancelFunc context.CancelFunc) Model {
	s := spinner.New(spinner.WithSpinner(spinner.Dot), spinner.WithStyle(lipgloss.NewStyle().Foreground(lipgloss.Color("205"))))
	return Model{
		f:          f,
		spinner:    s,
		inFlight:   make(map[string]*fileState),
		filesTotal: filesTotal,
		tbl:        newTable(),
		startedAt:  time.Now(),
		logger:     logger,
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
	return tea.Batch(WaitForEvent(m.f), headerTick(), renderTick())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	m.logger.Info("Msg received", "msg", msg)
	switch msg := msg.(type) {
	case RenderTickMsg:
		if m.dirty {
			m.tbl.SetRows(m.tableRows())
			m.dirty = false
		}
		return m, renderTick()
	case HeaderTickMsg:
		m.headerView()
		return m, headerTick()
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
			return m, tea.Quit
		}

		// Cache warming event
	case uiEvents.CacheEvent:
		switch msg.Type {
		case uiEvents.CacheStarted:
		case uiEvents.CacheLoading:
		case uiEvents.CacheFinished:
		}

		return m, tea.Batch(m.spinner.Tick, WaitForEvent(m.f))

	// Broker events related to uiEvents events
	case uiEvents.FileEvent:
		switch msg.Type {
		case uiEvents.FileStarted:
			m.logger.Info("Received File started event", "file", msg.Filename)
			m.inFlight[msg.Filename] = &fileState{
				rowsTotal: rowsPerFile,
				startedAt: time.Now(),
			}
		case uiEvents.FileProgress:
			m.logger.Info("Received File progress event", "file", msg.Filename, "rowsDone", msg.RowsDone)
			if state, ok := m.inFlight[msg.Filename]; ok {
				state.rowsDone = msg.RowsDone
				if msg.Err != nil {
					state.errCount++
				}
			}
		case uiEvents.FileCompleted:
			delete(m.inFlight, msg.Filename)
			m.filesDone++
			if msg.Err != nil {
				m.errored++
			}
		}
		m.dirty = true
		return m, WaitForEvent(m.f)
	}

	return m, nil
}

func (m Model) View() tea.View {
	switch m.state {
	case DatabaseLoading:
		body := m.spinner.View() + " Initializing database connection"
		return tea.NewView(body)
	case CacheWarming:
	case DatasetProcessing:
		return tea.NewView(m.headerView() + "\n" + m.bodyView() + "\n" + m.footerView())
	default:
	}
	return tea.NewView("Unknown state for the dataset process. Exiting.")
}

func (m Model) footerView() string {
	return "\n[q/ctrl+c] quit"
}

func (m Model) bodyView() string {
	if len(m.inFlight) == 0 && !m.done {
		return "\nwaiting for workers to start...\n"
	}

	return baseTableStyle.Render(m.tbl.View())
}

func (m Model) headerView() string {
	status := "Processing"
	if m.done {
		status = "Done"
	}

	elapsed := time.Since(m.startedAt).Round(time.Second)

	var totalRowsDone int64
	for _, fs := range m.inFlight {
		totalRowsDone += int64(fs.rowsDone)
	}

	completedRows := int64(m.filesDone) * int64(rowsPerFile)
	rate := float64(completedRows+totalRowsDone) / max(elapsed.Seconds(), 0.1)

	return gradientTitle(fmt.Sprintf(
		headerString, status, m.filesDone, m.filesTotal, m.errored, elapsed, rate, len(m.inFlight)))
}

func (m Model) tableRows() []table.Row {
	rows := make([]table.Row, 0, len(m.inFlight))
	for name, state := range m.inFlight {
		pct := 0.0
		if state.rowsTotal > 0 {
			pct = float64(state.rowsDone) / float64(state.rowsTotal)
		}
		elapsed := time.Since(state.startedAt)
		rate := float64(state.rowsDone) / max(elapsed.Seconds(), 0.0001)
		eta := time.Duration(float64(state.rowsTotal-state.rowsDone)/max(rate, 0.0001)) * time.Second

		rows = append(rows, table.Row{
			truncate(name, 28),
			fmt.Sprintf("%.1f%%", pct*100),
			fmt.Sprintf("%d/%d", state.rowsDone, state.rowsTotal),
			fmt.Sprintf("%.0f/s", rate),
			eta.Round(time.Second).String(),
			fmt.Sprintf("%d", state.errCount),
		})
	}

	return rows
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
		{Title: "Progress", Width: 12},
		{Title: "Rows", Width: 20},
		{Title: "Rate", Width: 12},
		{Title: "ETA", Width: 10},
		{Title: "Errors", Width: 8},
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
