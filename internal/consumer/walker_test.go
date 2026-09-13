package consumer

import (
	"testing"

	"gotest.tools/v3/assert"
)

func Test_WalkAndAccumulate(t *testing.T) {
	tests := map[string]struct {
		filters  []filterFunc
		expected int
	}{
		"Should filter by extension": {
			filters:  []filterFunc{WithExtension("zst")},
			expected: 2,
		},
		"Should filter hidden": {
			filters:  []filterFunc{ExcludeHidden},
			expected: 5,
		},
		"Should filter reserved": {
			filters:  []filterFunc{ExcludeReserved},
			expected: 5,
		},
		"Should filter both hidden and reserved": {
			filters:  []filterFunc{ExcludeReserved, ExcludeHidden},
			expected: 3,
		},
		"Should show only visible zst files": {
			filters:  []filterFunc{ExcludeHidden, ExcludeReserved, WithExtension("zst")},
			expected: 2,
		},
	}

	for test, tt := range tests {
		t.Run(test, func(t *testing.T) {
			root := "./testdata/"
			sut := NewFileWalker(root)
			idx, err := sut.WalkAndAccumulate(tt.filters...)
			if err != nil {
				t.Fatalf("Not expecting error, got %v", err)
			}
			got := len(idx)
			assert.Equal(t, got, tt.expected, "Expecting %d .zst files, got %d", tt.expected, got)
		})
	}
}
