package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime/metrics"
	"slices"
	"sync"
	"time"

	"pgcr-processing-service/internal/log"
	"pgcr-processing-service/internal/pubsub"
)

var ringBufferSize = 50

type MetricType interface {
	~float64 | ~uint64
}

type Timeseries[T MetricType] struct {
	metric          RuntimeMetric
	buf             *log.Ring[T]
	refreshInterval time.Duration
	max, min        T
	hasData         bool

	mu sync.RWMutex

	done      chan struct{}
	closeOnce sync.Once
	samples   <-chan pubsub.Event[[]metrics.Sample]
}

func NewTimeseries[T MetricType](
	metric RuntimeMetric,
	samples <-chan pubsub.Event[[]metrics.Sample],
	refresh time.Duration,
) (*Timeseries[T], error) {
	if refresh <= 0 {
		return nil, fmt.Errorf("refresh interval must be positive, got %v", refresh.Abs())
	}

	if err := validateKind[T](metric); err != nil {
		return nil, err
	}

	t := &Timeseries[T]{
		metric:          metric,
		buf:             log.NewRing[T](ringBufferSize),
		samples:         samples,
		refreshInterval: refresh,
	}

	return t, nil
}

func validateKind[T any](metric RuntimeMetric) error {
	var want metrics.ValueKind
	switch reflect.TypeFor[T]().Kind() {
	case reflect.Float64:
		want = metrics.KindFloat64
	case reflect.Uint64:
		want = metrics.KindUint64
	}

	for _, d := range metrics.All() {
		if d.Name != metric.String() {
			continue
		}

		if d.Kind != want {
			return fmt.Errorf("metric %q has kind %v, which does not match %v", metric, d.Kind, reflect.TypeFor[T]())
		}

		return nil
	}

	return fmt.Errorf("unknown runtime metrics %q", metric)
}

func (t *Timeseries[T]) Run(ctx context.Context) error {
	timer := time.NewTicker(t.refreshInterval)
	defer timer.Stop()

	var (
		pending bool
		latest  pubsub.Event[[]metrics.Sample]
	)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("timeseries was cancelled", "error", ctx.Err())
			return ctx.Err()
		case <-t.done:
			{
				slog.Debug("received done message for timeseries")
				return nil
			}
		case event, ok := <-t.samples:
			if !ok {
				slog.Debug("timeseries sampler channel was suddenly closed, pushing pending sample")
				if pending {
					t.pushSample(latest)
				}
				return nil
			}
			latest, pending = event, true
		case <-timer.C:
			slog.Debug("pushing latest data to buffer", "latest", latest)
			if pending {
				t.pushSample(latest)
				pending = false
			}
		}
	}
}

func (t *Timeseries[T]) pushSample(event pubsub.Event[[]metrics.Sample]) {
	// Literally just a reusable function to compare a metric.Sample to a metric name
	f := func(e metrics.Sample) bool {
		return e.Name == t.metric.String()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasData {
		t.max, t.min, t.hasData = val, val, true
	}

	t.max = max(val, t.max)
	t.min = min(val, t.min)
}

func (t *Timeseries[T]) Last(n int) []T {
	return t.buf.Last(n)
}

// Read max, min and hasData under a Read-lock mutex
func (t *Timeseries[T]) MinMax() (T, T, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.max, t.min, t.hasData
}

func (t *Timeseries[T]) Close() error {
	t.closeOnce.Do(func() { close(t.done) })
	return nil
}
