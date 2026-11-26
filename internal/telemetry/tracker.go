package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type Tracker struct {
	logBuf     *Ring[LogEntry]
	mu         sync.RWMutex
	nextId     int
	totalFiles int32

	// sampler will periodically fetch runtime metrics from Go
	// histograms and timeseries can then subscribe to it to
	// receive data from it
	sampler *MetricsSampler

	// gcHistogram represents the durations between garbage
	// collections separated by duration buckets
	gcHistogram *Histogram

	// heapTimeseries samples the heap size every interval
	// amount to see how memory is doing
	heapTimeseries *Timeseries[uint64]

	cleanup   []any
	cancel    context.CancelFunc
	closeOnce sync.Once

	// waitgroup that all runnable attributes parts of the tracker use
	// to synchronize and share execution
	wg      sync.WaitGroup
	runOnce sync.Once

	startedAt time.Time
	gauges    []Gauge
	tasks     []*Task
}

type Summary struct {
	App     AppStats
	Runtime RuntimeStats
}

type AppStats struct {
	Elapsed        time.Duration
	ReadThroughput float64
	FilesTotal     int32
	FilesDone      int32
	LinesRead      int32
	LinesInserted  int32
	LinesSkipped   int32
	LinesErrored   int32
}

type RuntimeStats struct {
	SchedLatencies []Bucket
	HeapUsedBytes  []uint64
}

// Job represents the task a Task Worker is tasked with doing
// and additionally where to report their progress to which
// ends up being a Task struct itself
type Job[T any] struct {
	Todo T
	Task *Task
}

func NewTracker(buf *Ring[LogEntry], totalFiles int) (*Tracker, error) {
	var err error

	t := &Tracker{
		logBuf:     buf,
		totalFiles: int32(totalFiles),
		startedAt:  time.Now(),
	}

	defer func() {
		if err != nil {
			t.Close()
		}
	}()

	sampler := NewSampler(Samples, 500*time.Millisecond)
	if err := t.AddToCleanup(sampler.Close); err != nil {
		return nil, err
	}

	buckets := []time.Duration{
		1 * time.Microsecond,
		10 * time.Microsecond,
		100 * time.Microsecond,
		1 * time.Millisecond,
		10 * time.Millisecond,
	}

	histSub, histUnsub := sampler.Subscribe()
	t.gcHistogram = NewHistogram(SchedulerLatencies, histSub, buckets...)
	if err := t.AddToCleanup(t.gcHistogram.Close, histUnsub); err != nil {
		return nil, err
	}

	timeSub, timeUnsub := sampler.Subscribe()
	t.heapTimeseries, err = NewTimeseries[uint64](MemoryHeapUsed, timeSub, 1*time.Second)
	if err != nil {
		slog.Error("failed to create telemetry tracker", "error", err)
		return nil, err
	}

	if err := t.AddToCleanup(t.heapTimeseries.Close, timeUnsub); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Tracker) Run(ctx context.Context) error {
	t.runOnce.Do(func() {
		ctx, t.cancel = context.WithCancel(ctx)
		t.wg.Go(func() { t.sampler.Run(ctx) })
		t.wg.Go(func() { t.gcHistogram.Run(ctx) })
		t.wg.Go(func() { t.heapTimeseries.Run(ctx) })
	})

	return nil
}

// AddTask registers a Task and subsequently returns a pointer to it
func (t *Tracker) AddTask(file string, linesTotal int) *Task {
	t.mu.Lock()
	defer t.mu.Unlock()

	task := &Task{
		Filename:   file,
		LinesTotal: linesTotal,
	}
	t.tasks = append(t.tasks, task)

	return task
}

func (t *Tracker) SnapshotLogs(n int) []string {
	snapshot := t.logBuf.Last(n)

	var res []string
	for _, snap := range snapshot {
		res = append(res, snap.String())
	}

	return res
}

func (t *Tracker) SnapshotSummary() Summary {
	t.mu.RLock()
	tasks := slices.Clone(t.tasks)
	t.mu.RUnlock()

	elapsed := time.Since(t.startedAt)

	var (
		filesDone     int32
		linesRead     int32
		linesSkipped  int32
		linesInserted int32
		linesErrored  int32
	)

	for _, t := range tasks {
		v := t.View()
		if v.State == Finished {
			filesDone++
		}
		linesRead += v.LinesRead
		linesInserted += v.Inserted
		linesSkipped += v.Skipped
		linesErrored += v.Errored
	}

	secs := max(1, elapsed.Seconds())
	readThroughput := float64(linesRead) / secs

	return Summary{
		Runtime: RuntimeStats{
			SchedLatencies: t.gcHistogram.getSample(),
			HeapUsedBytes:  t.heapTimeseries.Last(20), // TODO: Need to dynamically adjust this
		},
		App: AppStats{
			Elapsed:        elapsed,
			ReadThroughput: readThroughput,
			FilesDone:      filesDone,
			FilesTotal:     t.totalFiles,
			LinesRead:      linesRead,
			LinesInserted:  linesInserted,
			LinesSkipped:   linesSkipped,
			LinesErrored:   linesErrored,
		},
	}
}

func (t *Tracker) SnapshotTasks() []TaskView {
	t.mu.RLock()
	tasks := slices.Clone(t.tasks)
	t.mu.RUnlock()

	ringBuffer := make([]TaskView, 0, len(tasks))
	for _, task := range tasks {
		ringBuffer = append(ringBuffer, task.View())
	}
	return ringBuffer
}

func (t *Tracker) SnapshotGauges() []GaugeView {
	t.mu.RLock()
	entries := slices.Clone(t.gauges)
	t.mu.RUnlock()

	gauges := make([]GaugeView, 0, len(entries))
	for _, gauge := range entries {
		gauges = append(gauges, gauge.View())
	}

	return gauges
}

// RegisterGauge adds a new queue representing an active Go channel
func (t *Tracker) RegisterGauge(name, unit string, fn GaugeFunc) (unregister func()) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nextId++
	id := t.nextId
	t.gauges = append(t.gauges, Gauge{id: id, name: name, unit: unit, gaugeFunc: fn})
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.gauges = slices.DeleteFunc(t.gauges, func(e Gauge) bool {
			return e.id == id
		})
	}
}

func cleanupRunAny(a any) error {
	switch f := a.(type) {
	case func() error:
		return f()
	case func():
		f()
		return nil
	default:
		return fmt.Errorf("unsupported type: %T", f)
	}
}

func (t *Tracker) AddToCleanup(closers ...any) error {
	for _, c := range closers {
		switch f := c.(type) {
		case func() error:
			t.cleanup = append(t.cleanup, f)
		case func():
			t.cleanup = append(t.cleanup, f)
		default:
			return fmt.Errorf("unsupported type: %T", f)
		}
	}

	return nil
}

func (t *Tracker) Close() error {
	var err error
	t.closeOnce.Do(func() {
		if t.cancel != nil {
			t.cancel()
		}

		t.wg.Wait()

		var errs []error
		for _, c := range slices.Backward(t.cleanup) {
			if e := cleanupRunAny(c); e != nil {
				errs = append(errs, e)
			}
		}

		err = errors.Join(errs...)
	})

	return err
}
