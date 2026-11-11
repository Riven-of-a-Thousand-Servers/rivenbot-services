package telemetry

import (
	"runtime/metrics"
)

type Timeseries[T comparable] struct {
	// Ring buffer of type T
	buf *Ring[T]
	max T
	min T

	// This is what will be sampled, or at least the result of it
	// therefore in the background sampling function I need to continuously
	// call metrics
	sample []metrics.Sample
}

func NewTimeseries[T comparable](size int) *Timeseries[T] {
	t := &Timeseries[T]{
		buf: NewRing[T](size),
	}

	return t
}
