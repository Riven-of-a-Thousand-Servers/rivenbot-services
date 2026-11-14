package telemetry

import (
	"context"
	"runtime/metrics"
	"testing"
	"time"

	"pgcr-processing-service/internal/pubsub"

	"gotest.tools/v3/assert"
)

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

func (m *mockSampler) PushSamples(count int) chan pubsub.Event[[]metrics.Sample] {
	ch := make(chan pubsub.Event[[]metrics.Sample])
	m.ch = ch

	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		defer close(m.ch)

		for range count {
			select {
			case <-m.done:
				return
			case <-ticker.C:
				metrics.Read(m.samples)
				ch <- pubsub.Event[[]metrics.Sample]{
					Type:    pubsub.RuntimeMetricSample,
					Payload: m.samples,
				}
			}
		}

		m.done <- struct{}{}
	}()

	return ch
}

func Test_TimeseriesSampling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	samples := []metrics.Sample{
		{Name: MemoryHeapUsed},
	}
	sampler := newMockSampler(samples, 500*time.Millisecond)

	ch := sampler.PushSamples(5)
	sut, err := NewTimeseries[uint64](ctx, MemoryHeapUsed, ch, 500*time.Millisecond, 20)
	if err != nil {
		t.Fatalf("not expecting error, got: %v", err)
	}

	// Wait for 5 seconds to be done
	<-ctx.Done()

	assert.Equal(t, sut.hasData, true)
	assert.Equal(t, sut.buf.total, 5)
}

func Test_Timeseries_PushSampleWorksSuccessfully(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	samples := []metrics.Sample{
		{Name: MemoryHeapUsed},
	}

	sampler := newMockSampler(samples, 200*time.Millisecond)
	ch := sampler.PushSamples(2)

	sut, err := NewTimeseries[uint64](ctx, MemoryHeapUsed, ch, 500*time.Millisecond, 20)

	assert.NilError(t, err)

	<-ctx.Done()
	sut.Close()

	max, min, hasData := sut.MinMax()

	assert.Check(t, max != 0)
	assert.Check(t, min != 0)
	assert.Check(t, hasData)
	assert.Equal(t, sut.buf.total, 2)
}
