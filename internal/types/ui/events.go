package ui

import "time"

type EventType int

const (
	FileDiscovery EventType = iota
	FileStarted
	FileProgress
	FileCompleted
	FileError
)

type Event struct {
	Type     EventType
	Filename string
	RowsDone int
	Elapsed  time.Duration
	Err      error
}

// Special message that tells the UI that the context was cancelled
type CtxCancelledMsg struct{}
