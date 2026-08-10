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
