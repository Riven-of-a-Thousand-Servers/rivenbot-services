package telemetry

import (
	"slices"
	"sync"
)

type TaskState int32

const (
	Queued TaskState = iota + 1
	Started
	Errored
	Finished
)

type Tracker struct {
	logBuf *RingBuffer
	mu     sync.RWMutex
	nextId int
	gauges []Gauge
	tasks  []*Task
}

// Job represents the task a Task Worker is tasked with doing
// and additionally where to report their progress to which
// ends up being a Task struct itself
type Job[T any] struct {
	Todo T
	Task *Task
}

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

func DefaultTracker(buf *RingBuffer) *Tracker {
	return &Tracker{
		logBuf: buf,
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
	t.gauges = append(t.gauges, Gauge{id: id, name: name, unit: unit, gaugeFunc: fn})
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.gauges = slices.DeleteFunc(t.gauges, func(e Gauge) bool {
			return e.id == id
		})
	}
}
