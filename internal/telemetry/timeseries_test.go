package telemetry

import (
	"context"
	"fmt"
	"runtime/metrics"
	"testing"
	"time"

	"pgcr-processing-service/internal/pubsub"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/poll"
)

const (
	DefaultInterval time.Duration = 200 * time.Millisecond
	DefaultSize                   = 10

	DoesntExist RuntimeMetric = "thisShouldntWork"
)

type samplerOpt func(*mockSampler)

func WithBufferSize(size int) samplerOpt {
	return func(ms *mockSampler) {
		ms.values = make([]any, 0, size)
	}
}

func WithInterval(interval time.Duration) samplerOpt {
	return func(ms *mockSampler) {
		ms.interval = interval
	}
}

type mockSampler struct {
	samples  []metrics.Sample
	interval time.Duration
	values   []any
	ch       chan pubsub.Event[[]metrics.Sample]
	done     chan struct{}
}

func newMockSampler(samples []metrics.Sample, opts ...samplerOpt) *mockSampler {
	s := &mockSampler{
		samples:  samples,
		interval: DefaultInterval,
		values:   make([]any, 0, DefaultSize),
		done:     make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// publishSamples will send n amount of samples at a given interval to its declared channel
func (m *mockSampler) publishSamples(count int) chan pubsub.Event[[]metrics.Sample] {
	ch := make(chan pubsub.Event[[]metrics.Sample])
	m.ch = ch

	go func() error {
		defer close(m.done)
		defer close(m.ch)

		for range count {
			time.Sleep(m.interval)
			metrics.Read(m.samples)
			ch <- pubsub.Event[[]metrics.Sample]{
				Type:    pubsub.RuntimeMetricSample,
				Payload: m.samples,
			}
			val := m.samples[0].Value
			switch val.Kind() {
			case metrics.KindFloat64:
				m.values = append(m.values, val.Float64())
			case metrics.KindUint64:
				m.values = append(m.values, val.Uint64())
			default:
				return fmt.Errorf("unable to serve sample kind %v", val.Kind())
			}
		}
		return nil
	}()

	return ch
}

func Test_Timeseries_InvalidInterval(t *testing.T) {
	var tests map[string]time.Duration = map[string]time.Duration{
		"Zero duration":     0 * time.Second,
		"Negative duration": -1 * time.Second,
	}

	for testName, interval := range tests {
		t.Run(testName, func(t *testing.T) {
			samples := []metrics.Sample{
				{Name: MemoryHeapUsed.String()},
			}

			sampler := newMockSampler(samples, WithInterval(1*time.Second))
			ch := sampler.publishSamples(0)

			_, err := NewTimeseries[uint64](MemoryHeapUsed, ch, interval)
			if err == nil {
				t.Fatalf("expecting error, got none")
			}
		})
	}
}

func Test_Timeseries_InvalidKind(t *testing.T) {
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}
	sampler := newMockSampler(samples, WithInterval(500*time.Millisecond))
	ch := sampler.publishSamples(1)
	_, err := NewTimeseries[float64](DoesntExist, ch, 200*time.Millisecond)
	if err == nil {
		t.Fatal("expecting error, got none")
	}
}

func Test_Timeseries_InvalidHistogram(t *testing.T) {
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}

	sampler := newMockSampler(samples, WithInterval(500*time.Millisecond))
	ch := sampler.publishSamples(1)
	_, err := NewTimeseries[float64](GarbageCollectionCycles, ch, 200*time.Millisecond)
	if err == nil {
		t.Fatal("expecting error, got none")
	}
}

func Test_TimeseriesSampling(t *testing.T) {
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}
	sampler := newMockSampler(samples, WithInterval(500*time.Millisecond))

	ch := sampler.publishSamples(5)
	sut, err := NewTimeseries[uint64](MemoryHeapUsed, ch, 200*time.Millisecond)
	assert.NilError(t, err)

	ctx := context.Background()

	err = sut.Run(ctx)
	assert.NilError(t, err)

	<-sampler.done

	poll.WaitOn(t, func(t poll.LogT) poll.Result {
		_, _, hasData := sut.MinMax()
		if hasData && sut.buf.Len() >= 5 {
			return poll.Success()
		}

		return poll.Continue("waiting until timeseries has data and buffer has more than 5 elements")
	}, poll.WithTimeout(2*time.Second))
}

func Test_Timeseries_PushSampleWorksSuccessfully(t *testing.T) {
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}

	sampler := newMockSampler(samples, WithInterval(100*time.Millisecond))
	ch := sampler.publishSamples(2)

	sut, err := NewTimeseries[uint64](MemoryHeapUsed, ch, 50*time.Millisecond)
	assert.NilError(t, err)

	ctx := context.Background()

	err = sut.Run(ctx)
	assert.NilError(t, err)

	<-sampler.done

	poll.WaitOn(t, func(t poll.LogT) poll.Result {
		if sut.buf.Len() >= 2 {
			return poll.Success()
		}
		return poll.Continue("waiting for 2 points")
	}, poll.WithTimeout(2*time.Second))
}
