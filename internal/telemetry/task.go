package telemetry

import (
	"sync/atomic"
	"time"
)

type Task struct {
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

type TaskView struct {
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
