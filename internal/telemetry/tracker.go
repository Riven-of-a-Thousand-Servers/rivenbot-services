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

	TaskView struct {
		Filename   string
		LinesTotal int
		LinesDone  int
		State      int32
		BytesRead  int64
		LinesRead  int32
		Inserted   int32
		Skipped    int32
		Errored    int32

		StartedAt  time.Time
		FinishedAt time.Time
	}

	GaugeFunc func(name, unit string) (len, cap int)

	Tracker struct {
		bufferSize int
		mu         sync.RWMutex
		nextId     int
		queues     []queueEntry
		tasks      []*Task
	}

	// Job represents the task a Task Worker is tasked with doing
	// and additionally where to report their progress to which
	// ends up being a Task struct itself
	Job[T any] struct {
		ToDo T
		Task *Task
	}
)

func (t *Task) View() TaskView {
	return TaskView{
		Filename:   t.Filename,
		LinesTotal: t.LinesTotal,
		LinesDone:  t.LinesDone,
		State:      t.State.Load(),
		BytesRead:  t.BytesRead.Load(),
		LinesRead:  t.LinesRead.Load(),
		Inserted:   t.Inserted.Load(),
		Skipped:    t.Skipped.Load(),
		Errored:    t.Errored.Load(),
		StartedAt:  t.StartedAt,
		FinishedAt: t.FinishedAt,
	}
}

func EmptyTracker() *Tracker {
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

func (t *Tracker) SnapshotTasks() []TaskView {
	t.mu.RLock()
	entries := slices.Clone(t.tasks)
	t.mu.RUnlock()

	// Ring buffer will only contain entries the top N entries that haven't finished
	// and if they did they will only stay for 10 seconds before the next one is fetched
	ringBuffer := make([]TaskView, t.bufferSize)
	for i := range t.bufferSize {
		curr := entries[i]
		if curr.FinishedAt.IsZero() || curr.FinishedAt.Before(time.Now().Add(10*time.Second)) {
			ringBuffer = append(ringBuffer, curr.View())
		}
	}
	return ringBuffer
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
