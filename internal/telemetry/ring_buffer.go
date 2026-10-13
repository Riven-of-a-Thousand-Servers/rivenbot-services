package telemetry

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type (
	RingBuffer struct {
		buf   []LogEntry
		total int
		size  int
		mu    sync.RWMutex
	}

	LogEntry struct {
		Timestamp time.Time
		Level     string
		Message   string
	}
)

func (l LogEntry) String() string {
	return fmt.Sprintf("%s [%s] - %s", l.Timestamp.String(), l.Level, l.Message)
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{
		size: size,
		buf:  make([]LogEntry, size),
	}
}

func (b *RingBuffer) Push(e LogEntry) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := b.total % len(b.buf)
	b.buf[pos] = e
	b.total++
}

func (b *RingBuffer) Last(n int) []LogEntry {
	b.mu.RLock()
	entries := slices.Clone(b.buf)
	b.mu.RUnlock()

	count := min(n, b.total, b.size)
	start := b.total - count

	var res []LogEntry
	for i := start; i < b.total; i++ {
		res = append(res, entries[i%b.total])
	}

	return res
}
