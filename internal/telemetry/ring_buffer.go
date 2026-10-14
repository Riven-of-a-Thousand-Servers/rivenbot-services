package telemetry

import (
	"fmt"
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
	defer b.mu.RUnlock()

	// Count should resolve to the lowest of either the
	// the request amount n, the total amount of items, the
	// size of the array in case its full, or zero in worst case
	// This essentially clamps the value so that is always within the
	// valid range
	count := max(0, min(n, b.total, b.size))
	if count == 0 {
		return []LogEntry{}
	}

	start := b.total - count
	res := make([]LogEntry, 0, count)
	for i := start; i < b.total; i++ {
		res = append(res, b.buf[i%b.size])
	}

	return res
}
