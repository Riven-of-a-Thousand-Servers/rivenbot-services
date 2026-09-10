package pubsub

import (
	"pgcr-processing-service/internal/types/manifest"
)

type EventType int

const (
	// Walker Event
	WalkerProgress EventType = iota + 1

	// File Events
	FileStarted
	FileProgress
	FileCompleted
	FileError

	// Cache Events
	CacheStarted
	CacheLoading
	CacheFinished

	// Database Events
	ConnectionStarted
	PingingContext
	ConnectionSuccesful
)

type CacheEvent struct {
	// Current definition being fetched from bungie
	CurrentDefinition manifest.EntityDefinition

	// Entries in the cache
	Size int
}
