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
	renderPeriod = 270 * time.Millisecond
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

type (
	MainPaneTickMsg time.Time
	GaugeTickMsg    time.Time
)

var (
	barOK   = lipgloss.NewStyle().Foreground(lipgloss.Color("#9ECE6A"))
	barWarn = lipgloss.NewStyle().Foreground(lipgloss.Color("#E0AF68"))
	barFull = lipgloss.NewStyle().Foreground(lipgloss.Color("#F7768E"))
)

func mainTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return MainPaneTickMsg(t)
	})
}

func gaugeTick() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(t time.Time) tea.Msg {
		return GaugeTickMsg(t)
	})
}

type Model struct {
	tracker *telemetry.Tracker
	snap    []telemetry.TaskView
	gauges  []telemetry.GaugeView
	logs    []string
	state   uiState

	spinner       spinner.Model
	cacheStageMsg string

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

// Start listening for broker events
func (m Model) Init() tea.Cmd {
	return tea.Batch(mainTick(), gaugeTick(), m.spinner.Tick)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case MainPaneTickMsg:
		if m.state == datasetProcessing {
			m.snap = m.tracker.SnapshotTasks()
			m.logs = m.tracker.SnapshotLogs(18)
		}
		cmds = append(cmds, mainTick())
	case GaugeTickMsg:
		if m.state == datasetProcessing {
			m.gauges = m.tracker.SnapshotGauges()
		}
		cmds = append(cmds, gaugeTick())
	case tea.KeyPressMsg:
		switch msg.String() {
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
		BorderForeground(lipgloss.Color("62")).
		Padding(0)

	logsPane := lipgloss.NewStyle().
		Width(100).
		Height(20).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.Color("62")).
		AlignHorizontal(lipgloss.Left).
		AlignVertical(lipgloss.Left).
		Padding(0)

	gaugePane := lipgloss.NewStyle().
		Width(76).
		Height(7).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.Color("62")).
		AlignHorizontal(lipgloss.Left).
		AlignVertical(lipgloss.Center).
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

		var gaugeRows []string
		for _, gauge := range m.gauges {
			gaugeRows = append(gaugeRows, gaugeRow(gauge, 20, 10))
		}

		gauges := strings.Join(gaugeRows, "\n")

		out := lipgloss.JoinHorizontal(228, filesPane.Render(m.table.View()), logsPane.Render(logs))
		out = lipgloss.JoinVertical(lipgloss.Left, out, gaugePane.Render(gauges))
		out = lipgloss.JoinVertical(lipgloss.Center, out, m.footerView())
		v.Content = out
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
		{Title: PurpleStyle.Render("File"), Width: 24},
		{Title: PurpleStyle.Render("State"), Width: 10},
		{Title: PurpleStyle.Render("Progress"), Width: 10},
		{Title: PurpleStyle.Render("Lines Read"), Width: 10},
		{Title: PurpleStyle.Render("Bytes Read"), Width: 10},
		{Title: GreenStyle.Render("Inserted"), Width: 10},
		{Title: CoolBlueStyle.Render("Skipped"), Width: 10},
		{Title: RedStyle.Render("Errored"), Width: 10},
	}

	tbl := table.New(table.WithColumns(columns), table.WithHeight(12), table.WithWidth(200))
	s := table.DefaultStyles()
	s.Header.
		BorderStyle(lipgloss.InnerHalfBlockBorder()).
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

func gaugeRow(g telemetry.GaugeView, nameW, barW int) string {
	name := lipgloss.NewStyle().Width(nameW).Render(g.Name)
	b := barStyle(g).Render(bar(g.Length, g.Cap, barW))
	nums := fmt.Sprintf(" %d/%d %s", g.Length, g.Cap, g.Unit)
	return name + " " + b + nums
}

func bar(cur, max, width int) string {
	if max <= 0 || width <= 0 {
		return strings.Repeat("░", max0(width))
	}
	filled := min(max0(cur*width/max), width)
	return strings.Repeat("▓", filled) + strings.Repeat("░", width-filled)
}

func max0(n int) int {
	return max(0, n)
}

func barStyle(g telemetry.GaugeView) lipgloss.Style {
	if g.Cap <= 0 {
		return barOK
	}

	ratio := float64(g.Length) / float64(g.Cap)
	switch {
	case ratio >= 1.0:
		return barFull
	case ratio >= 0.8:
		return barWarn
	default:
		return barOK
	}
}
