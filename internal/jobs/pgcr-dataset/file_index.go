package pgcrdataset

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
	Files     FileIndex
	Started   chan string
	Completed chan string
}

type FileEntry struct {
	Name     string
	Started  bool
	Done     bool
	Progress chan int64
}

func NewDiscoverer(root string) *FileDiscoverer {
	return &FileDiscoverer{Root: root}
}

func (f *FileDiscoverer) Discover(ctx context.Context, extension string) error {
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

	return filepath.WalkDir(f.Root, walkFunc)
}
