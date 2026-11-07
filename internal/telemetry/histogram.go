package telemetry

import (
	"runtime/metrics"
	"sync/atomic"
	"time"
)

type Histogram struct {
	samples    []metrics.Sample
	timeRanges []time.Duration
	latest     atomic.Pointer[[]uint64]
	stopChan   chan struct{}
}

func NewHistogram(samples []metrics.Sample, interval time.Duration, ranges ...time.Duration) *Histogram {
	hist := &Histogram{
		timeRanges: ranges,
		samples:    samples,
		stopChan:   make(chan struct{}),
	}

	go hist.BackgroundSampling(interval)

	return hist
}

func (h *Histogram) BackgroundSampling(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var prev []uint64
	for {
		select {
		// Each tick we sample and save the latest entry to the atomic.Pointer entry
		// in the Histogram
		case <-ticker.C:
			metrics.Read(h.samples)

			if h.samples[0].Value.Kind() != metrics.KindFloat64Histogram {
				return
			}

			hist := h.samples[0].Value.Float64Histogram()
			delta := make([]uint64, len(hist.Counts))
			if prev != nil {
				for i, c := range hist.Counts {
					delta[i] = c - prev[i]
				}
			}

			prev = make([]uint64, len(hist.Counts))
			copy(prev, hist.Counts)

			displays := make([]uint64, len(h.timeRanges)+1)
			i := 0
			for d, r := range h.timeRanges {
				for i < len(delta) && hist.Buckets[i+1] <= r.Seconds() {
					displays[d] += delta[i]
					i++
				}
			}

			for ; i < len(delta); i++ {
				displays[len(h.timeRanges)] += delta[i]
			}

			h.latest.Store(&displays)
		case <-h.stopChan:
			return
		}
	}
}

func (h *Histogram) SampleLatest() []uint64 {
	if res := h.latest.Load(); res != nil {
		return *res
	}

	return nil
}

func (h *Histogram) Close() {
	close(h.stopChan)
}
