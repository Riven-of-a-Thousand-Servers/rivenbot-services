package writer

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type MockWriter struct {
	buf   []int
	min   int
	count int
}

// Mock Writer just writes item to its internal buffer
// after min attempts are satisfied
func (m *MockWriter) Write(ctx context.Context, item int) error {
	if m.count >= m.min {
		m.buf = append(m.buf, item)
		return nil
	}
	m.count++
	return fmt.Errorf("Count is not 5 or greater")
}

func Test_RetryWriter_ShouldRetryNTimes(t *testing.T) {
	ctx := context.Background()
	item := 10
	mock := &MockWriter{min: 4}
	w := NewRetryWriter(mock, 50*time.Millisecond, 5)

	err := w.Write(ctx, item)
	if err != nil {
		t.Fatalf("not expecting error, got: %v", err)
	}

	if len(mock.buf) <= 0 {
		t.Fatal("buffer has no contents")
	}

	got := mock.buf[0]
	want := item
	if got != want {
		t.Fatalf("want %v, got %v", got, want)
	}
}

func Test_RetryWriter_ShouldFailAfterNTimes(t *testing.T) {
	tests := map[string]struct {
		retryAmt int
	}{
		"Five retries": {
			retryAmt: 5,
		},
		"Six retries": {
			retryAmt: 6,
		},
	}

	for test, tt := range tests {
		t.Run(test, func(t *testing.T) {
			ctx := context.Background()
			item := 10
			mock := &MockWriter{min: tt.retryAmt}
			w := NewRetryWriter(mock, 50*time.Millisecond, 5)

			err := w.Write(ctx, item)
			if err == nil {
				t.Fatal("expecting error, got none")
			}
		})
	}
}
