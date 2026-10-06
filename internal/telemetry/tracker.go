package telemetry

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type (
	queueEntry struct {
		id        int
		name      string
		unit      string
		gaugeFunc GaugeFunc
	}

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

	GaugeFunc func(name, unit string) (len, cap int)

	Tracker struct {
		mu     sync.RWMutex
		nextId int
		queues []queueEntry
		tasks  []*Task
	}

	// Job represents the task a Task Worker is tasked with doing
	// and additionally where to report their progress to which
	// ends up being a Task struct itself
	Job[T any] struct {
		Payload T
		Task    *Task
	}
)

func NewTracker() *Tracker {
	return &Tracker{}
}

// AddTask registers a Task and subsequently returns a pointer to it
func (t *Tracker) AddTask(file string) *Task {
	t.mu.Lock()
	defer t.mu.Unlock()

	task := &Task{Filename: file, StartedAt: time.Now()}
	t.tasks = append(t.tasks, task)
	return task
}

func (t *Tracker) SnapshotTasks() []*Task {
	t.mu.RLock()
	entries := slices.Clone(t.tasks)
	t.mu.RUnlock()
	return entries
}

// RegisterQueue adds a new queue representing an active Go channel
func (t *Tracker) RegisterQueue(name, unit string, fn GaugeFunc) (unregister func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextId++
	id := t.nextId
	t.queues = append(t.queues, queueEntry{id: id, name: name, unit: unit, gaugeFunc: fn})
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.queues = slices.DeleteFunc(t.queues, func(e queueEntry) bool {
			return e.id == id
		})
	}
}
