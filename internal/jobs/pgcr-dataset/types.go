package pgcrdataset

import "time"

type DatasetEntry struct {
	Filename string
	Bytes    []byte
	Number   int64
}

type EventType int

const (
	FileDiscovery EventType = iota
	FileStarted
	FileProgress
	FileCompleted
)

type Event struct {
	Type     EventType
	Filename string
	RowsDone int
	Elapsed  time.Duration
	Err      error
}
