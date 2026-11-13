package telemetry

import (
	"context"
	"runtime/metrics"
	"slices"
	"time"

	"pgcr-processing-service/internal/pubsub"
)

type MetricType interface {
	~float64 | ~uint64
}

type Timeseries[T MetricType] struct {
	// Ring buffer of type T
	metric          string
	buf             *Ring[T]
	refreshInterval time.Duration
	max, min        T
	hasData         bool

	samples <-chan pubsub.Event[[]metrics.Sample]
}

func NewTimeseries[T MetricType](
	metric string,
	samples <-chan pubsub.Event[[]metrics.Sample],
	refreshInterval time.Duration,
	size int,
) *Timeseries[T] {
	return &Timeseries[T]{
		buf:             NewRing[T](size),
		samples:         samples,
		refreshInterval: refreshInterval,
	}
}

func (t *Timeseries[T]) StartSampling(ctx context.Context, metric string) {
	timer := time.NewTicker(t.refreshInterval)

	var (
		pending bool
		latest  pubsub.Event[[]metrics.Sample]
	)

	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-t.samples:
			if !ok {
				return
			}
			latest, pending = event, true
		case <-timer.C:
			if pending {
				t.pushSample(latest)
			}
		}
	}
}

func (t *Timeseries[T]) pushSample(event pubsub.Event[[]metrics.Sample]) {
	// Literally just a reusable function to compare a metric.Sample
	f := func(e metrics.Sample) bool {
		return e.Name == t.metric
	}
	idx := slices.IndexFunc(event.Payload, f)

	if idx < 0 {
		return
	}

	sample := event.Payload[idx]

	var val T
	switch sample.Value.Kind() {
	case metrics.KindFloat64:
		val = T(sample.Value.Float64())
	case metrics.KindUint64:
		val = T(sample.Value.Uint64())
	default:
	}

	t.buf.Push(val)
	t.observe(val)
}

func (t *Timeseries[T]) observe(val T) {
	if !t.hasData {
		t.max, t.min, t.hasData = val, val, true
	}

	t.max = max(val, t.max)
	t.min = min(val, t.min)
}
