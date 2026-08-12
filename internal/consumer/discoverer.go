package consumer

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type FileIndex map[string]*FileEntry

type FileDiscoverer struct {
	Root      string
	Started   chan string
	Completed chan string
}

type FileEntry struct {
	Name    string
	Started bool
	Done    bool
}

func NewDiscoverer(root string) *FileDiscoverer {
	return &FileDiscoverer{Root: root}
}

func (f *FileDiscoverer) Discover(ctx context.Context, extension string) (FileIndex, error) {
	entries := make(map[string]*FileEntry)
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}

		if filepath.Ext(d.Name()) == extension {
			entries[path] = &FileEntry{
				Name:    d.Name(),
				Started: false,
				Done:    false,
			}
		}
		return nil
	}

	if err := filepath.WalkDir(f.Root, walkFunc); err != nil {
		return nil, err
	}

	return entries, nil
}
