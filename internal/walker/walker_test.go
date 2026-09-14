package walker

import (
	"testing"

	"gotest.tools/v3/assert"
)

func Test_WalkAndAccumulate(t *testing.T) {
	tests := map[string]struct {
		dirs     []FilterFunc
		files    []FilterFunc
		expected int
	}{
		"Should filter by extension": {
			files:    []FilterFunc{WithExtension("zst")},
			expected: 8,
		},
		"Should filter hidden": {
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
			files:    []FilterFunc{WithExtension("zst")},
			expected: 4,
		},
		"Should filter by regex pattern": {
			files:    []FilterFunc{WithPattern(`^example\.+`)},
			expected: 5,
		},
	}

	for test, tt := range tests {
		t.Run(test, func(t *testing.T) {
			root := "./testdata/"
			var opts []WalkerOption
			for _, dirFn := range tt.dirs {
				opts = append(opts, WithDirFilter(dirFn))
			}

			for _, fileFn := range tt.files {
				opts = append(opts, WithFileFilter(fileFn))
			}

			sut := NewFileWalker(root, opts...)
			idx, err := sut.WalkAndAccumulate()
			if err != nil {
				t.Fatalf("Not expecting error, got %v", err)
			}
			got := len(idx)
			assert.Equal(t, got, tt.expected, "Expecting %d .zst files, got %d", tt.expected, got)
		})
	}
}
