package telemetry

import (
	"context"
	"runtime/metrics"
	"time"

	"pgcr-processing-service/internal/pubsub"
)

// MetricsSampler is a broker that will publish
// to all subscriber at a given interval all samples
// given a []metrics.Sample slice
type MetricsSampler struct {
	*pubsub.Broker[[]metrics.Sample]
	samples  []metrics.Sample
	interval time.Duration
}

func NewSampler(
	samples []metrics.Sample,
	queueSize int,
	interval time.Duration,
) *MetricsSampler {
	return &MetricsSampler{
		interval: interval,
		samples:  samples,
		Broker:   pubsub.NewBroker[[]metrics.Sample](queueSize),
	}
}

func (m *MetricsSampler) StartSampling(ctx context.Context) {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.Read(m.samples)
			m.Publish(pubsub.RuntimeMetricSample, m.samples)
		}
	}
}
