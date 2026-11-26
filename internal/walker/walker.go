package walker

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

type FilterFunc func(fs.DirEntry) bool

type FileEntry struct {
	Filename string
	Path     string
}

// WalkAndAccumulate takes in a list of filters to be applied sequentially
// if any of the directoryEntries fails one of the filters then it is skipped
func WalkAndAccumulate(root string, globPattern string, dirFilters ...FilterFunc) ([]FileEntry, error) {
	var entries []FileEntry
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		if d.IsDir() {
			if path == root {
				return nil
			}

			for _, filter := range dirFilters {
				if !filter(d) {
					return filepath.SkipDir
				}
			}

			return nil
		}

		if globPattern == "" {
			globPattern = "*"
		}

		if matched, err := filepath.Match(globPattern, d.Name()); err != nil {
			return fmt.Errorf("malformed glob pattern: %v", err)
		} else if !matched {
			return nil
		}

		entries = append(entries, FileEntry{
			Path:     path,
			Filename: d.Name(),
		})

		return nil
	}); err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a FileEntry, b FileEntry) int {
		aName, _, _ := strings.Cut(a.Filename, "-")
		bName, _, _ := strings.Cut(b.Filename, "-")
		return strings.Compare(aName, bName)
	})

	return entries, nil
}

func WithExtension(extension string) FilterFunc {
	if !strings.HasPrefix(extension, ".") {
		extension = "." + extension
	}

	return func(d fs.DirEntry) bool {
		return !d.IsDir() && filepath.Ext(d.Name()) == extension
	}
}

var ExcludeHidden FilterFunc = func(de fs.DirEntry) bool {
	return !strings.HasPrefix(de.Name(), ".")
}

var ExcludeReserved FilterFunc = func(de fs.DirEntry) bool {
	return !strings.HasPrefix(de.Name(), "$")
}

func WithPattern(pattern string) FilterFunc {
	regex, _ := regexp.Compile(pattern)
	return func(de fs.DirEntry) bool {
		if strings.TrimSpace(pattern) == "" {
			return true
		}
		return regex.MatchString(de.Name())
	}
}
