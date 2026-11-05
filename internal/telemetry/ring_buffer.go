package telemetry

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

type (
	RingBuffer struct {
		// Reading/Writing from the buf should always be protected by using mutex mu
		buf []LogEntry

		total int
		size  int
		mu    sync.RWMutex
	}

	LogEntry struct {
		Timestamp time.Time
		Level     string
		Message   string

		// Just a list of strings formatted as 'key=value', nothing crazy
		Attributes []string
	}
)

func (l LogEntry) String() string {
	attrs := strings.Join(l.Attributes, " ")
	return fmt.Sprintf("%s [%s] - %s: %s", l.Timestamp.Format(time.RFC1123), l.Level, l.Message, attrs)
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
