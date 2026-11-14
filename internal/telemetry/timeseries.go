package telemetry

import (
	"context"
	"fmt"
	"reflect"
	"runtime/metrics"
	"slices"
	"sync"
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

	mu     sync.RWMutex
	cancel context.CancelFunc

	done    chan struct{}
	samples <-chan pubsub.Event[[]metrics.Sample]
}

func NewTimeseries[T MetricType](
	ctx context.Context,
	metric string,
	samples <-chan pubsub.Event[[]metrics.Sample],
	refreshInterval time.Duration,
	size int,
) (*Timeseries[T], error) {
	if refreshInterval <= 0 {
		return nil, fmt.Errorf("refresh interval must be positive, got %v", refreshInterval.Abs())
	}

	if err := validateKind[T](metric); err != nil {
		return nil, err
	}

	newCtx, cancel := context.WithCancel(ctx)

	t := &Timeseries[T]{
		metric:          metric,
		buf:             NewRing[T](size),
		samples:         samples,
		cancel:          cancel,
		refreshInterval: refreshInterval,
		done:            make(chan struct{}),
	}

	go func() {
		defer close(t.done)
		t.startSampling(newCtx)
	}()

	return t, nil
}

func validateKind[T any](metric string) error {
	var want metrics.ValueKind
	switch reflect.TypeFor[T]().Kind() {
	case reflect.Float64:
		want = metrics.KindFloat64
	case reflect.Uint64:
		want = metrics.KindUint64
	}

	for _, d := range metrics.All() {
		if d.Name != metric {
			continue
		}

		if d.Kind != want {
			return fmt.Errorf("metric %q has kind %v, which does not match %v", metric, d.Kind, reflect.TypeFor[T]())
		}

		return nil
	}

	return fmt.Errorf("unknown runtime metrics %q", metric)
}

func (t *Timeseries[T]) startSampling(ctx context.Context) {
	timer := time.NewTicker(t.refreshInterval)
	defer timer.Stop()

	var (
		pending bool
		latest  pubsub.Event[[]metrics.Sample]
	)

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
				pending = false
			}
		}
	}
}

func (t *Timeseries[T]) pushSample(event pubsub.Event[[]metrics.Sample]) {
	// Literally just a reusable function to compare a metric.Sample to a metric name
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasData {
		t.max, t.min, t.hasData = val, val, true
	}

	t.max = max(val, t.max)
	t.min = min(val, t.min)
}

// Read max, min and hasData under a Read-lock mutex
func (t *Timeseries[T]) MinMax() (T, T, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.max, t.min, t.hasData
}

func (t *Timeseries[T]) Close() error {
	t.cancel()
	<-t.done
	return nil
}
