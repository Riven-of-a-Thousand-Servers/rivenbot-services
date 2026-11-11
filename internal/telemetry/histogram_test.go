package telemetry

//
// import (
// 	"runtime/metrics"
// 	"testing"
// 	"time"
//
// 	"gotest.tools/v3/assert"
// )
//
// // Not really a test but a sanity check
// func Test_HistogramRead(t *testing.T) {
// 	sample := []metrics.Sample{
// 		{Name: "/sched/latencies:seconds"},
// 	}
//
// 	ranges := []time.Duration{1 * time.Microsecond, 10 * time.Microsecond, 100 * time.Microsecond}
// 	defer hist.Close()
//
// 	timer := time.NewTimer(500 * time.Millisecond)
//
// 	// Wait 500 milliseconds
// 	<-timer.C
// 	res := hist.SampleLatest()
// 	assert.Equal(t, len(res), 4)
// }
