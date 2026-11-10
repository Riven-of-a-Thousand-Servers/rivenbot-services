package telemetry

import (
	"testing"

	"gotest.tools/v3/assert"
)

func Test_PushSucces_OverflowShouldOverwriteFirstValue(t *testing.T) {
	r := NewRing[string](15)
	for range 15 {
		r.Push("Hello")
	}

	first := "World!"
	r.Push(first)

	assert.Equal(t, r.total, 16)
	assert.Equal(t, r.buf[0], first)
}

func Test_Last_ShouldReturnAllLastValues(t *testing.T) {
	r := NewRing[int](5)
	for range 3 {
		r.Push(1)
	}

	res := r.Last(3)
	for _, ele := range res {
		assert.Equal(t, ele, 1)
	}
}

func Test_Last_BadInputReturnsEmpty(t *testing.T) {
	r := NewRing[int](10)
	tests := map[string]struct {
		in int
	}{
		"Negative number": {
			in: -1,
		},
		"Zero": {
			in: 0,
		},
		"Bigger than size": {
			in: r.size + 1,
		},
	}

	for test, tt := range tests {
		t.Run(test, func(t *testing.T) {
			res := r.Last(tt.in)
			assert.Equal(t, len(res), 0)
		})
	}
}
