package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"pgcr-processing-service/internal/cache"
	"pgcr-processing-service/internal/pubsub"
	"pgcr-processing-service/internal/telemetry"

	"charm.land/bubbles/v2/spinner"
	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
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

type MainPaneTickMsg time.Time

func mainTick() tea.Cmd {
	return tea.Tick(renderPeriod, func(t time.Time) tea.Msg {
		return MainPaneTickMsg(t)
	})
}

type Model struct {
	tracker *telemetry.Tracker

	tasks   []telemetry.TaskView
	gauges  []telemetry.GaugeView
	summary telemetry.Summary

	logs   []string
	state  uiState
	layout layout

	width, height int
	cacheStageMsg string

	spinner spinner.Model
	table   table.Model

	cancelFunc context.CancelFunc
}

func NewModel(tracker *telemetry.Tracker, cancelFunc context.CancelFunc) Model {
	spinner := spinner.New(
		spinner.WithSpinner(spinner.Dot))

	return Model{
		tracker: tracker,
		spinner: spinner,

		state:      cacheWarming,
		table:      newTable(),
		cancelFunc: cancelFunc,
	}
}

// Start listening for broker events
func (m Model) Init() tea.Cmd {
	return tea.Batch(mainTick(), m.spinner.Tick)
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.layout = computeLayout(m.width, m.height)
	case MainPaneTickMsg:
		if m.state == datasetProcessing {
			m.tasks = m.tracker.SnapshotTasks()
			m.logs = m.tracker.SnapshotLogs(m.layout.logsInnerH) // -2 is from the border at the top/bottom
			m.gauges = m.tracker.SnapshotGauges()
			m.summary = m.tracker.SnapshotSummary()
		}

		cmds = append(cmds, mainTick())
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
	headerSection := strings.Join([]string{
		headerItem("Files", fmt.Sprintf("%d/%d", m.summary.General.FilesDone, m.summary.General.FilesTotal)),
		headerItem("Elapsed", m.summary.General.Elapsed.Round(time.Second).String()),
		headerItem("Read Throughput", formatNumber(m.summary.General.ReadThroughput)+"ln/s"),
		headerItem("Inserted", formatNumber(float64(m.summary.General.LinesInserted))),
		headerItem("Skipped", formatNumber(float64(m.summary.General.LinesSkipped))),
		headerItem("Errored", formatNumber(float64(m.summary.General.LinesErrored))),
	}, headerSeparator)

	headerSection = lipgloss.NewStyle().
		Height(1).
		AlignHorizontal(lipgloss.Center).
		Width(m.width).
		MaxWidth(m.width).
		Render(headerSection)

	filesPane := lipgloss.NewStyle().
		Width(m.layout.filesW).
		Height(m.layout.topH).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.Color("62"))

	logsPane := lipgloss.NewStyle().
		Width(m.layout.logsW).
		Height(m.layout.topH).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.Color("62")).
		AlignHorizontal(lipgloss.Left).
		AlignVertical(lipgloss.Left)

	gaugePane := lipgloss.NewStyle().
		Width(m.layout.gaugeW).
		Height(m.layout.gaugeH).
		Border(lipgloss.RoundedBorder(), true).
		BorderForeground(lipgloss.Color("62")).
		AlignHorizontal(lipgloss.Left).
		AlignVertical(lipgloss.Center)

	switch m.state {
	case cacheWarming:
		s := "\nStarting...\n"
		s += fmt.Sprintf("\n%s %s", m.spinner.View(), m.cacheStageMsg)
		return tea.NewView(s)
	case datasetProcessing:
		var v tea.View
		var rows []table.Row
		for _, task := range m.tasks {
			row := createRow(task)
			rows = append(rows, row)
		}
		m.table.SetRows(rows)

		lines := make([]string, len(m.logs))
		for i, l := range m.logs {
			lines[i] = ansi.Truncate(l, m.layout.logsInnerW, "...")
		}
		logs := strings.Join(lines, "\n")

		var gaugeRows []string
		for _, gauge := range m.gauges {
			gaugeRows = append(gaugeRows, gaugeRow(gauge, 20, 10))
		}
		gauges := strings.Join(gaugeRows, "\n")

		var histogramRows []string
		maxCount := telemetry.MaxCount(m.summary.Runtime.SchedLatencies)
		for _, bucket := range m.summary.Runtime.SchedLatencies {
			histogramRows = append(histogramRows, histogramRow(bucket, 6, 10, int(maxCount)))
		}

		var res string
		res = lipgloss.JoinHorizontal(lipgloss.Top, filesPane.Render(m.table.View()), logsPane.Render(logs))
		res = lipgloss.JoinVertical(lipgloss.Left, res, gaugePane.Render(gauges))
		res = lipgloss.JoinVertical(lipgloss.Center, headerSection, res)

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
	s.Selected = lipgloss.NewStyle()
	s.Header.
		BorderStyle(lipgloss.InnerHalfBlockBorder()).
		BorderForeground(lipgloss.Color("240")).
		Bold(true)
	tbl.SetStyles(s)
	return tbl
}

func histogramRow(b telemetry.Bucket, nameW, barW, maxCount int) string {
	var name string
	nameStyle := lipgloss.NewStyle().Width(nameW)
	if b.Last {
		name = nameStyle.Render(">" + b.Upper.String())
	} else {
		name = nameStyle.Render("≤")
	}

	bar := bar(int(b.Count), maxCount, barW)
	return name + " " + bar + " " + string(b.Count)
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
