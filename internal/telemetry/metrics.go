package telemetry

import (
	"context"
	"log/slog"
	"runtime/metrics"
	"slices"
	"sync"
	"time"

	"pgcr-processing-service/internal/pubsub"
)

type RuntimeMetric string

const (
	MemoryHeapUsed          RuntimeMetric = "/memory/classes/heap/objects:bytes"
	GoroutinesRunning       RuntimeMetric = "/sched/goroutines/running:goroutines"
	GoroutinesWaiting       RuntimeMetric = "/sched/goroutines/waiting:goroutines"
	SchedulerLatencies      RuntimeMetric = "/sched/latencies:seconds"
	GarbageCollectionCycles RuntimeMetric = "/gc/cycles/total:gc-cycles"
)

func (r RuntimeMetric) String() string {
	return string(r)
}

var Samples []metrics.Sample = []metrics.Sample{
	{Name: MemoryHeapUsed.String()},
	{Name: GoroutinesRunning.String()},
	{Name: GoroutinesWaiting.String()},
	{Name: SchedulerLatencies.String()},
	{Name: GarbageCollectionCycles.String()},
}

// MetricsSampler is a broker that will publish
// to all subscriber at a given interval all samples
// given a []metrics.Sample slice
type MetricsSampler struct {
	*pubsub.Broker[[]metrics.Sample]
	samples  []metrics.Sample
	interval time.Duration

	done      chan struct{}
	closeOnce sync.Once
}

func NewSampler(
	samples []metrics.Sample,
	interval time.Duration,
) *MetricsSampler {
	return &MetricsSampler{
		interval: interval,
		samples:  samples,
		Broker:   pubsub.NewBroker[[]metrics.Sample](),
	}
}

func (m *MetricsSampler) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Debug("sampler context was cancelled", "error", ctx.Err())
			return ctx.Err()
		case <-m.done:
			slog.Debug("received done message for sampler")
			return nil
		case <-ticker.C:
			metrics.Read(m.samples)
			samples := slices.Clone(m.samples)
			m.Publish(pubsub.RuntimeMetricSample, samples)
		}
	}
}

func (m *MetricsSampler) Close() error {
	m.closeOnce.Do(func() {
		close(m.done)
		if err := m.Broker.Shutdown(); err != nil {
			slog.Error("failed to shutdown metric's sampler broker", "error", err)
		}
	})

	return nil
}
