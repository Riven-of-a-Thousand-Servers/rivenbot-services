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

type FileEvent struct {
	Type     EventType
	Filename string
	RowsDone int
	Elapsed  time.Duration
	Err      error
}

type CacheState int

// Cache warming states
const (
	CacheStarted CacheState = iota
	CacheLoading
	CacheFinished
)

// Events that describe where the cache-warming process
// is currently at
type CacheEvent struct {
	Type CacheState
	Msg  string
	Done bool
	Size int64
}
