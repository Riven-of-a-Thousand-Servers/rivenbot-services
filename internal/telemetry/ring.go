package telemetry

import (
	"sync"
)

type Ring[T any] struct {
	// Reading/Writing from the buf should always be protected by using mutex mu
	buf []T

	total int
	size  int
	mu    sync.RWMutex
}

func NewRing[T any](size int) *Ring[T] {
	return &Ring[T]{
		size: size,
		buf:  make([]T, size),
	}
}

func (b *Ring[T]) Push(e T) T {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := b.total % len(b.buf)
	b.buf[pos] = e
	b.total++
	return e
}

func (b *Ring[T]) Last(n int) []T {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Count should resolve to the lowest of either the
	// the request amount n, the total amount of items, the
	// size of the array in case its full, or zero in worst case
	// This essentially clamps the value so that is always within the
	// valid range
	count := max(0, min(n, b.total, b.size))
	if count == 0 {
		return []T{}
	}

	start := b.total - count
	res := make([]T, 0, count)
	for i := start; i < b.total; i++ {
		res = append(res, b.buf[i%b.size])
	}

	return res
}
