package telemetry

import (
	"context"
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

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
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

func (m *MetricsSampler) Start(ctx context.Context) {
	ctx, m.cancel = context.WithCancel(ctx)
	m.done = make(chan struct{})
	go func() {
		defer close(m.done)
		m.run(ctx)
	}()
}

func (m *MetricsSampler) run(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.Read(m.samples)
			samples := slices.Clone(m.samples)
			m.Publish(pubsub.RuntimeMetricSample, samples)
		}
	}
}

func (m *MetricsSampler) Close() error {
	m.once.Do(func() {
		if m.cancel != nil {
			m.cancel()
			<-m.done
		}

		m.Broker.Shutdown()
	})

	return nil
}
