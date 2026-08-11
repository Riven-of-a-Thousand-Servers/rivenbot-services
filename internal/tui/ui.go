package ui

import (
	"time"

	uiEvents "pgcr-processing-service/internal/types/ui"

	"charm.land/bubbles/v2/progress"
	tea "charm.land/bubbletea/v2"
)

type viewMode int

const (
	compactView viewMode = iota
	detailedView
)

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
	inFlight   map[string]*fileState
	filesTotal int
	filesDone  int
	errored    int
	done       bool
	quitting   bool
}

func NewModel(ch <-chan uiEvents.Event, filesTotal int) Model {
	return Model{
		ch:         ch,
		inFlight:   make(map[string]*fileState),
		filesTotal: filesTotal,
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
	return WaitForEvent(m.ch)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "+":
			if m.mode == compactView {
				m.mode = detailedView
			} else {
				m.mode = compactView
			}
			return m, nil
		}

	// Broker events related to uiEvents events
	case uiEvents.Event:
		var cmds []tea.Cmd
		cmds = append(cmds, WaitForEvent(m.ch))

		switch msg.Type {
		case uiEvents.FileStarted:
			m.inFlight[msg.Filename] = &fileState{
				bar:       progress.New(progress.WithDefaultBlend()),
				rowsTotal: 10_000_000,
				startedAt: time.Now(),
			}
		case uiEvents.FileProgress:
			if state, ok := m.inFlight[msg.Filename]; ok {
				state.rowsDone = msg.RowsDone
				if msg.Err != nil {
					state.errCount++
				}
				pct := 0.0
				if state.rowsTotal > 0 {
					pct = float64(state.rowsDone) / float64(state.rowsTotal)
				}

				cmds = append(cmds, state.bar.SetPercent(pct))
			}
		case uiEvents.FileCompleted:
			delete(m.inFlight, msg.Filename)
			m.filesDone++
			if msg.Err != nil {
				m.errored++
			}
		}

		return m, tea.Batch(cmds...)

	// Update the in-flight progress bars
	case progress.FrameMsg:
		var cmds []tea.Cmd
		for name, fs := range m.inFlight {
			updated, cmd := fs.bar.Update(msg)
			fs.bar = updated
			cmds = append(cmds, cmd)
			m.inFlight[name] = fs
		}

		return m, tea.Batch(cmds...)
	}
	return m, nil
}

// TODO: Write the visual portion for each stylized bar and file discovery
func (m Model) View() tea.View {
	return tea.NewView(m.headerView() + "\n" + m.barsView())
}

// TODO: Finish setting up the core view for the progres bars
func (m Model) barsView() string {
	return ""
}

// TODO: Finish setting the dynamic view for the header
func (m Model) headerView() string {
	return ""
}
