package telemetry

import (
	"runtime/metrics"
	"sync/atomic"
	"time"
)

// Histogram has the backend structure for constructing
// a visualization of buckets with values
type Histogram struct {
	samples    []metrics.Sample
	timeRanges []time.Duration
	latest     atomic.Pointer[[]Bucket]
	stopChan   chan struct{}
}

type Bucket struct {
	Upper time.Duration
	Count uint64
	Last  bool
}

func NewHistogram(samples []metrics.Sample, interval time.Duration, ranges ...time.Duration) *Histogram {
	hist := &Histogram{
		timeRanges: ranges,
		samples:    samples,
		stopChan:   make(chan struct{}),
	}

	go hist.BackgroundSampling(interval)

	// Store so no nil pointer
	hist.latest.Store(&[]Bucket{})

	return hist
}

// BackgroundSampling runs as soon as the histogram is made
// It runs every interval and stores the result in latest
// Additionally, stores the previous sample in a function
// variable caled prev
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
		case <-h.stopChan:
			return
		}
	}
}

func (h *Histogram) SampleLatest() []Bucket {
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
	close(h.stopChan)
	return nil
}
