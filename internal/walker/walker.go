package walker

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"pgcr-processing-service/internal/pubsub"
)

const defaultEventBroker = 50

type FileIndex []FileEntry

type FilterFunc func(fs.DirEntry) bool

type FileWalker struct {
	*pubsub.Broker[FileEntry]

	root          string
	dirs          []FilterFunc
	files         []FilterFunc
	eventsEnabled bool
}

type FileEntry struct {
	Filename string
	Path     string
	Started  bool
	Done     bool
}

type WalkerOption func(w *FileWalker)

func WithDirFilter(fn FilterFunc) WalkerOption {
	return func(w *FileWalker) {
		w.dirs = append(w.dirs, fn)
	}
}

func WithFileFilter(fn FilterFunc) WalkerOption {
	return func(w *FileWalker) {
		w.files = append(w.files, fn)
	}
}

var WithEventsEnabled WalkerOption = func(w *FileWalker) {
	w.eventsEnabled = true
}

func NewFileWalker(root string, opts ...WalkerOption) *FileWalker {
	w := &FileWalker{root: root}
	for _, opt := range opts {
		opt(w)
	}

	if w.eventsEnabled {
		w.Broker = pubsub.NewBroker[FileEntry](defaultEventBroker)
	}

	return w
}

// WalkAndAccumulate takes in a list of filters to be applied sequentially
// if any of the directoryEntries fails one of the filters then it is skipped
func (f *FileWalker) WalkAndAccumulate() (FileIndex, error) {
	var entries []FileEntry
	if err := filepath.WalkDir(f.root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		// Apply only directory filters first
		if d.IsDir() {
			if path == f.root {
				return nil
			}

			for _, filter := range f.dirs {
				if !filter(d) {
					return filepath.SkipDir
				}
			}
			return nil
		}

		// Apply file-level filters
		for _, filter := range f.files {
			if !filter(d) {
				return nil
			}
		}

		entry := FileEntry{
			Path:     path,
			Filename: d.Name(),
			Started:  false,
			Done:     false,
		}
		entries = append(entries, entry)

		if f.Broker != nil {
			f.Broker.Publish(pubsub.WalkerProgress, entry)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a FileEntry, b FileEntry) int {
		return strings.Compare(a.Filename, b.Filename)
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
