package consumer

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

type filterFunc func(fs.DirEntry) bool

type FileWalker struct {
	*pubsub.Broker[FileEntry]
	Root string
}

type FileEntry struct {
	Filename string
	Path     string
	Started  bool
	Done     bool
}

func NewFileWalker(root string) *FileWalker {
	return &FileWalker{Root: root, Broker: pubsub.NewBroker[FileEntry](defaultEventBroker)}
}

// DiscoverFunc takes in a list of filters to be applied sequentially
// if any of the directoryEntries fails one of the filters then it is skipped
func (f *FileWalker) DiscoverFunc(filters ...filterFunc) (FileIndex, error) {
	var entries []FileEntry
	if err := filepath.WalkDir(f.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		for _, filter := range filters {
			if !filter(d) {
				if d.IsDir() && path != f.Root {
					return filepath.SkipDir
				}
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
		f.Broker.Publish(pubsub.WalkerProgress, entry)

		return nil
	}); err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a FileEntry, b FileEntry) int {
		return strings.Compare(a.Filename, b.Filename)
	})

	return entries, nil
}

func HasExtension(extension string) filterFunc {
	return func(de fs.DirEntry) bool {
		return !de.IsDir() && filepath.Ext(de.Name()) == extension
	}
}

var NotHiddenFile filterFunc = func(de fs.DirEntry) bool {
	return strings.HasPrefix(de.Name(), ".")
}

var NotReserved filterFunc = func(de fs.DirEntry) bool {
	return strings.HasPrefix(de.Name(), "$")
}

func RegexMatch(pattern string) filterFunc {
	return func(de fs.DirEntry) bool {
		res, _ := regexp.Match(pattern, []byte(de.Name()))
		return res
	}
}
