package walker

import (
	"testing"

	"gotest.tools/v3/assert"
)

func Test_WalkAndAccumulate(t *testing.T) {
	tests := map[string]struct {
		dirs     []FilterFunc
		glob     string
		expected int
	}{
		"Should filter by extension": {
			glob:     "*.zst",
			expected: 8,
		},
		"Should filter hidden directories": {
			dirs:     []FilterFunc{ExcludeHidden},
			expected: 7,
		},
		"Should filter reserved": {
			dirs:     []FilterFunc{ExcludeReserved},
			expected: 7,
		},
		"Should filter both hidden and reserved": {
			dirs:     []FilterFunc{ExcludeReserved, ExcludeHidden},
			expected: 5,
		},
		"Should show only visible zst files": {
			dirs:     []FilterFunc{ExcludeHidden, ExcludeReserved},
			glob:     "*.zst",
			expected: 4,
		},
		"Should filter by glob pattern": {
			glob:     `example[0-9].*`,
			expected: 4,
		},
	}

	for test, tt := range tests {
		t.Run(test, func(t *testing.T) {
			root := "./testdata/"
			idx, err := WalkAndAccumulate(root, tt.glob, tt.dirs...)
			if err != nil {
				t.Fatalf("Not expecting error, got %v", err)
			}
			got := len(idx)
			assert.Equal(t, got, tt.expected, "Expecting %d .zst files, got %d", tt.expected, got)
		})
	}
}
