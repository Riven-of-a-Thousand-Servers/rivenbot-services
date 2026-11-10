package telemetry

import (
	"runtime/metrics"
	"sync"
)

type Timeseries[T comparable] struct {
	// Ring buffer of type T
	buf Ring[T]
	max T
	min T

	sample []metrics.Sample
	mu     sync.RWMutex
}
