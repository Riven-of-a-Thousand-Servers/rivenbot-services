package telemetry

import (
	"context"
	"runtime/metrics"
	"testing"
	"time"

	"pgcr-processing-service/internal/pubsub"

	"gotest.tools/v3/assert"
	"gotest.tools/v3/poll"
)

const DoesntExist RuntimeMetric = "thisShouldntWork"

type mockSampler struct {
	samples  []metrics.Sample
	interval time.Duration
	ch       chan pubsub.Event[[]metrics.Sample]
	done     chan struct{}
}

func newMockSampler(samples []metrics.Sample, interval time.Duration) *mockSampler {
	return &mockSampler{
		samples:  samples,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// publishSamples will send n amount of samples at a given interval to its declared channel
func (m *mockSampler) publishSamples(count int) chan pubsub.Event[[]metrics.Sample] {
	ch := make(chan pubsub.Event[[]metrics.Sample])
	m.ch = ch

	go func() {
		defer close(m.done)
		defer close(m.ch)

		for range count {
			time.Sleep(m.interval)
			metrics.Read(m.samples)
			ch <- pubsub.Event[[]metrics.Sample]{
				Type:    pubsub.RuntimeMetricSample,
				Payload: m.samples,
			}
		}
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			samples := []metrics.Sample{
				{Name: MemoryHeapUsed.String()},
			}

			sampler := newMockSampler(samples, 1*time.Second)
			ch := sampler.publishSamples(0)

			_, err := NewTimeseries[uint64](ctx, MemoryHeapUsed, ch, interval, 20)
			if err == nil {
				t.Fatalf("expecting error, got none")
			}
		})
	}
}

func Test_Timeseries_InvalidKind(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}
	sampler := newMockSampler(samples, 500*time.Millisecond)
	ch := sampler.publishSamples(1)
	_, err := NewTimeseries[float64](ctx, DoesntExist, ch, 200*time.Millisecond, 20)
	if err == nil {
		t.Fatal("expecting error, got none")
	}
}

func Test_Timeseries_InvalidHistogram(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}
	sampler := newMockSampler(samples, 500*time.Millisecond)
	ch := sampler.publishSamples(1)
	_, err := NewTimeseries[float64](ctx, GarbageCollectionCycles, ch, 200*time.Millisecond, 20)
	if err == nil {
		t.Fatal("expecting error, got none")
	}
}

func Test_TimeseriesSampling(t *testing.T) {
	ctx := context.Background()
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}
	sampler := newMockSampler(samples, 500*time.Millisecond)

	ch := sampler.publishSamples(5)
	sut, err := NewTimeseries[uint64](ctx, MemoryHeapUsed, ch, 200*time.Millisecond, 20)

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
	ctx := context.Background()
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed.String()},
	}

	sampler := newMockSampler(samples, 100*time.Millisecond)
	ch := sampler.publishSamples(2)
	sut, err := NewTimeseries[uint64](ctx, MemoryHeapUsed, ch, 50*time.Millisecond, 20)
	assert.NilError(t, err)

	<-sampler.done
	poll.WaitOn(t, func(t poll.LogT) poll.Result {
		if sut.buf.Len() >= 2 {
			return poll.Success()
		}
		return poll.Continue("waiting for 2 points")
	}, poll.WithTimeout(2*time.Second))
}
