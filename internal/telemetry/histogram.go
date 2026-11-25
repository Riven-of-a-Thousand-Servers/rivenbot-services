package telemetry

import (
	"context"
	"log/slog"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"pgcr-processing-service/internal/pubsub"
)

// Histogram has the backend structure for constructing
// a visualization of buckets with values
type Histogram struct {
	// timeRanges are the buckets in which the histogram will
	// try to aggregate points for the specified runtime metric
	timeRanges []time.Duration
	metric     RuntimeMetric

	latest    atomic.Pointer[[]Bucket]
	samplerCh <-chan pubsub.Event[[]metrics.Sample]

	closeOnce sync.Once
	done      chan struct{}
}

type Bucket struct {
	Upper time.Duration
	Count uint64
	Last  bool
}

func NewHistogram(
	metric RuntimeMetric,
	metricsChan <-chan pubsub.Event[[]metrics.Sample],
	ranges ...time.Duration,
) *Histogram {
	h := &Histogram{
		timeRanges: ranges,
		samplerCh:  metricsChan,
		done:       make(chan struct{}),
	}

	h.latest.Store(&[]Bucket{})
	return h
}

// ReadSamples runs as soon as the histogram is made
// It runs every interval and stores the result in latest
// Additionally, stores the previous sample in a function
// variable caled prev
func (h *Histogram) Run(ctx context.Context) error {
	var prev []uint64
	for {
		select {
		case <-ctx.Done():
			slog.Debug("Histogram context was cancelled", "error", ctx.Err())
			return ctx.Err()
		// Each tick we sample and save the latest entry to the atomic.Pointer entry
		// in the Histogram
		case metrics, ok := <-h.samplerCh:
			if !ok {
				slog.Debug("sampler channel was suddenly closed")
				return nil
			}

			hist := metrics.Payload[0].Value.Float64Histogram()
			delta := make([]uint64, len(hist.Counts))
			if prev != nil {
				for i, c := range hist.Counts {
					delta[i] = c - prev[i]
				}
			}

			prev = make([]uint64, len(hist.Counts))
			copy(prev, hist.Counts)

			buckets := make([]Bucket, len(h.timeRanges)+1)
			i := 0
			for d, r := range h.timeRanges {
				b := Bucket{Upper: r}
				for i < len(delta) && hist.Buckets[i+1] <= r.Seconds() {
					b.Count += delta[i]
					i++
				}
				buckets[d] = b
			}

			// Its 'upper' will be the second to last bucket
			overflowBucket := Bucket{Upper: buckets[len(buckets)-2].Upper, Last: true}
			for ; i < len(delta); i++ {
				overflowBucket.Count += delta[i]
			}
			buckets[len(buckets)-1] = overflowBucket

			h.latest.Store(&buckets)
		case <-h.done:
			slog.Debug("received done message for histogram")
			return nil
		}
	}
}

func (h *Histogram) getSample() []Bucket {
	if res := h.latest.Load(); res != nil {
		return *res
	}

	return []Bucket{}
}

func MaxCount(buckets []Bucket) uint64 {
	var m uint64
	for _, b := range buckets {
		m = max(m, b.Count)
	}

	return m
}

func (h *Histogram) GetRanges() []string {
	var ranges []string
	for _, r := range h.timeRanges {
		ranges = append(ranges, r.String())
	}
	return ranges
}

func (h *Histogram) Close() error {
	h.closeOnce.Do(func() { close(h.done) })
	return nil
}
