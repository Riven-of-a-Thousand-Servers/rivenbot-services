package ui

import (
	"fmt"
	"log/slog"
	"time"

	uiEvents "pgcr-processing-service/internal/types/ui"

	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const (
	rowsPerFile  = 10_000_000
	renderPeriod = 500 * time.Millisecond
)

type viewMode int

const (
	compactView viewMode = iota
	detailedView
)

type (
	TickMsg       time.Time
	RenderTickMsg time.Time
)

func renderTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return RenderTickMsg{}
	})
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg {
		return TickMsg(t)
	})
}

type fileState struct {
	bar       progress.Model
	rowsDone  int
	rowsTotal int
	startedAt time.Time
	errCount  int
}

type Model struct {
	ch <-chan uiEvents.Event

	mode       viewMode
	tbl        table.Model
	inFlight   map[string]*fileState
	startedAt  time.Time
	filesTotal int
	filesDone  int
	errored    int
	dirty      bool
	done       bool
	quitting   bool
	logger     *slog.Logger
}

func NewModel(ch <-chan uiEvents.Event, filesTotal int, logger *slog.Logger) Model {
	return Model{
		ch:         ch,
		inFlight:   make(map[string]*fileState),
		filesTotal: filesTotal,
		mode:       compactView,
		tbl:        newTable(),
		startedAt:  time.Now(),
		logger:     logger,
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
	return tea.Batch(WaitForEvent(m.ch), tick(), renderTick())
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
	case TickMsg:
		return m, tick()
	case tea.KeyPressMsg:
		switch msg.String() {
		case "+":
			if m.mode == compactView {
				m.mode = detailedView
			} else {
				m.mode = compactView
			}
			return m, nil
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		}

	// Broker events related to uiEvents events
	case uiEvents.Event:
		switch msg.Type {
		case uiEvents.FileStarted:
			m.logger.Info("Received File started event", "file", msg.Filename)
			m.inFlight[msg.Filename] = &fileState{
				bar:       progress.New(progress.WithDefaultBlend()),
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
			// delete(m.inFlight, msg.Filename)
			m.filesDone++
			if msg.Err != nil {
				m.errored++
			}
		}

		// Update the in-flight progress bars
		// case progress.FrameMsg:
		// 	var cmds []tea.Cmd
		// 	for name, fs := range m.inFlight {
		// 		updated, cmd := fs.bar.Update(msg)
		// 		fs.bar = updated
		// 		cmds = append(cmds, cmd)
		// 		m.inFlight[name] = fs
		// 	}
		//
		// 	return m, tea.Batch(cmds...)

		m.dirty = true
		return m, WaitForEvent(m.ch)
	}

	return m, nil
}

func (m Model) View() tea.View {
	return tea.NewView(m.headerView() + "\n" + m.barsView())
}

func (m Model) barsView() string {
	// if len(m.inFlight) == 0 && !m.done {
	// 	return "\nwaiting for workers to start...\n"
	// }

	return m.tbl.View()
}

func (m Model) headerView() string {
	status := "processing"
	if m.done {
		status = "done"
	}

	elapsed := time.Since(m.startedAt).Round(time.Second)

	var totalRowsDone int64
	for _, fs := range m.inFlight {
		totalRowsDone += int64(fs.rowsDone)
	}

	completedRows := int64(m.filesDone) * int64(rowsPerFile)
	rate := float64(completedRows+totalRowsDone) / max(elapsed.Seconds(), 0.0001)

	return fmt.Sprintf(
		"PGCR dataset import — %s\nFiles: %d/%d   errors: %d   elapsed: %s   throughput: %.0f rows/s\n[+] toggle detail   [q] quit",
		status, m.filesDone, m.filesTotal, m.errored, elapsed, rate,
	)
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
	styles := table.DefaultStyles()
	styles.Header.BorderStyle(lipgloss.NormalBorder()).BorderForeground(lipgloss.Color("240")).BorderBottom(true).Bold(false)
	tbl.SetStyles(styles)

	return tbl
}
