package ui

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	events "pgcr-processing-service/internal/types/ui"

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
	databaseLoading uiState = iota + 1
	cacheWarming
	datasetProcessing
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
	consumerEvents <-chan events.FileEvent
	cacheEvents    <-chan events.CacheEvent
	workerEvents   <-chan events.FileEvent

	// Switches from Database loading, cache warming, and actual processing
	state uiState

	// Cache warming state
	spinner       spinner.Model
	cacheStageMsg string

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

// This approach of passing the individual channels is probably the right choice for this sequential
// TUI where database status > cache warming > dataset processing takes place
//
// The alternative to this is to fan-in every channel to the tea.Send(..) method
// so the program gets messages from the outside, however, this would make sense
// if the TUI needs to listen for concurrent messages from several sources at the same time
func NewModel(
	files <-chan events.FileEvent,
	cache <-chan events.CacheEvent,
	worker <-chan events.FileEvent,
	filesTotal int,
	logger *slog.Logger,
	cancelFunc context.CancelFunc,
) Model {
	s := spinner.New(spinner.WithSpinner(spinner.Dot), spinner.WithStyle(lipgloss.NewStyle().Foreground(lipgloss.Color("205"))))
	return Model{
		consumerEvents: files,
		cacheEvents:    cache,
		spinner:        s,
		state:          cacheWarming,
		inFlight:       make(map[string]*fileState),
		filesTotal:     filesTotal,
		tbl:            newTable(),
		startedAt:      time.Now(),
		logger:         logger,
		cancelFunc:     cancelFunc,
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
	return tea.Batch(WaitForEvent(m.cacheEvents), m.spinner.Tick)
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

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, tea.Batch(cmd, WaitForEvent(m.cacheEvents))

		// Cache warming events
	case events.CacheEvent:
		switch msg.Type {
		case events.CacheStarted:
			m.cacheStageMsg = "Initializing cache..."
			m.state = cacheWarming
			return m, WaitForEvent(m.cacheEvents)
		case events.CacheLoading:
			m.cacheStageMsg = fmt.Sprintf("Fetching %s", msg.CurrentDefinition.String())
			return m, WaitForEvent(m.cacheEvents)
		case events.CacheFinished:
			// Cache warming finished, now moving to datasetProcessing
			var cmds []tea.Cmd = []tea.Cmd{WaitForEvent(m.consumerEvents), headerTick(), renderTick()}
			m.cacheStageMsg = fmt.Sprintf("Finished warming up the cache with %d entries", msg.Size)
			m.state = datasetProcessing
			return m, tea.Batch(cmds...)
		}

	// Broker events related to uiEvents events
	case events.FileEvent:
		switch msg.Type {
		case events.FileStarted:
			m.logger.Info("Received File started event", "file", msg.Filename)
			m.inFlight[msg.Filename] = &fileState{
				rowsTotal: rowsPerFile,
				startedAt: time.Now(),
			}
		case events.FileProgress:
			m.logger.Info("Received File progress event", "file", msg.Filename, "rowsDone", msg.RowsDone)
			if state, ok := m.inFlight[msg.Filename]; ok {
				state.rowsDone = msg.RowsDone
				if msg.Err != nil {
					state.errCount++
				}
			}
		case events.FileCompleted:
			delete(m.inFlight, msg.Filename)
			m.filesDone++
			if msg.Err != nil {
				m.errored++
			}
		}
		m.dirty = true
		return m, WaitForEvent(m.consumerEvents)
	}

	return m, nil
}

func (m Model) View() tea.View {
	switch m.state {
	case databaseLoading:
		body := m.spinner.View() + " Initializing database connection"
		return tea.NewView(body)
	case cacheWarming:
		s := "\nCache Warming Sequence\n"
		s += fmt.Sprintf("\n%s %s", m.spinner.View(), m.cacheStageMsg)
		return tea.NewView(s)
	case datasetProcessing:
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
