package telemetry

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Queued TaskState = iota + 1
	Started
	Errored
	Finished
)

type (
	gaugeEntry struct {
		id        int
		name      string
		unit      string
		gaugeFunc GaugeFunc
	}

	GaugeView struct {
		Id     int
		Name   string
		Unit   string
		Length int
		Cap    int
	}

	TaskState int32

	// Represents the atomic unit of work for the TUI
	// which is processing a zst-compressed file
	Task struct {
		Filename   string
		LinesTotal int
		LinesDone  int
		State      atomic.Int32
		BytesRead  atomic.Int64
		LinesRead  atomic.Int32
		Inserted   atomic.Int32
		Skipped    atomic.Int32
		Errored    atomic.Int32
		StartedAt  time.Time
		FinishedAt time.Time
	}

	TaskView struct {
		Filename   string
		LinesTotal int
		LinesDone  int
		State      TaskState
		BytesRead  int64
		LinesRead  int32
		Inserted   int32
		Skipped    int32
		Errored    int32

		StartedAt  time.Time
		FinishedAt time.Time
	}

	GaugeFunc func() (queueLength, queueCap int)

	Tracker struct {
		logBuf *RingBuffer
		mu     sync.RWMutex
		nextId int
		gauges []gaugeEntry
		tasks  []*Task
	}

	// Job represents the task a Task Worker is tasked with doing
	// and additionally where to report their progress to which
	// ends up being a Task struct itself
	Job[T any] struct {
		Todo T
		Task *Task
	}
)

func (t TaskState) String() string {
	switch t {
	case Started:
		return "Started"
	case Queued:
		return "Queued"
	case Errored:
		return "Errored"
	case Finished:
		return "Finished"
	default:
		return ""
	}
}

func (t *Task) View() TaskView {
	return TaskView{
		Filename:   t.Filename,
		LinesTotal: t.LinesTotal,
		LinesDone:  t.LinesDone,
		State:      TaskState(t.State.Load()),
		BytesRead:  t.BytesRead.Load(),
		LinesRead:  t.LinesRead.Load(),
		Inserted:   t.Inserted.Load(),
		Skipped:    t.Skipped.Load(),
		Errored:    t.Errored.Load(),
		StartedAt:  t.StartedAt,
		FinishedAt: t.FinishedAt,
	}
}

func (q *gaugeEntry) View() GaugeView {
	length, cap := q.gaugeFunc()
	return GaugeView{
		Id:     q.id,
		Name:   q.name,
		Unit:   q.unit,
		Length: length,
		Cap:    cap,
	}
}

func DefaultTracker(logBuf *RingBuffer) *Tracker {
	return &Tracker{
		logBuf: logBuf,
	}
}

// AddTask registers a Task and subsequently returns a pointer to it
func (t *Tracker) AddTask(file string, linesTotal int) *Task {
	t.mu.Lock()
	defer t.mu.Unlock()

	task := &Task{
		Filename:   file,
		LinesTotal: linesTotal,
	}
	t.tasks = append(t.tasks, task)
	return task
}

func (t *Tracker) SnapshotLogs(n int) []string {
	snapshot := t.logBuf.Last(n)

	var res []string
	for _, snap := range snapshot {
		res = append(res, snap.String())
	}

	return res
}

func (t *Tracker) SnapshotTasks() []TaskView {
	t.mu.RLock()
	entries := slices.Clone(t.tasks)
	t.mu.RUnlock()

	// Ring buffer will only contain entries the top N entries that haven't finished
	// and if they did they will only stay for 10 seconds before the next one is fetched
	ringBuffer := make([]TaskView, 0, len(entries))
	for _, task := range entries {
		ringBuffer = append(ringBuffer, task.View())
	}
	return ringBuffer
}

func (t *Tracker) SnapshotGauges() []GaugeView {
	t.mu.RLock()
	entries := slices.Clone(t.gauges)
	t.mu.RUnlock()

	gauges := make([]GaugeView, 0, len(entries))
	for _, gauge := range entries {
		gauges = append(gauges, gauge.View())
	}

	return gauges
}

// RegisterGauge adds a new queue representing an active Go channel
func (t *Tracker) RegisterGauge(name, unit string, fn GaugeFunc) (unregister func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextId++
	id := t.nextId
	t.gauges = append(t.gauges, gaugeEntry{id: id, name: name, unit: unit, gaugeFunc: fn})
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.gauges = slices.DeleteFunc(t.gauges, func(e gaugeEntry) bool {
			return e.id == id
		})
	}
}

func (t *Task) SetStarted() TaskState {
	state := Started
	t.State.Store(int32(Started))
	t.StartedAt = time.Now()
	return state
}

func (t *Task) SetQueued() TaskState {
	state := Queued
	t.State.Store(int32(Queued))
	return state
}

func (t *Task) SetFinished() TaskState {
	state := Finished
	t.State.Store(int32(state))
	t.FinishedAt = time.Now()
	return state
}

func (t *Task) SetErrored() TaskState {
	state := Errored
	t.State.Store(int32(state))
	t.FinishedAt = time.Now()
	return state
}

func (t *Task) IncrementErrors() int32 {
	return t.Errored.Add(1)
}

func (t *Task) IncrementInserted() int32 {
	return t.Inserted.Add(1)
}

func (t *Task) IncrementLinesRead() int32 {
	return t.LinesRead.Add(1)
}

func (t *Task) IncrementSkipped() int32 {
	return t.Skipped.Add(1)
}

func (t *Task) IncrementBytesRead(amt int64) int64 {
	return t.BytesRead.Add(amt)
}
