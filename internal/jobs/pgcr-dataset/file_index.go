package pgcrdataset

import (
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
	Path     string
	Started  bool
	Done     bool
	Progress chan int64
}

func NewDiscoverer(root string) *FileDiscoverer {
	return &FileDiscoverer{Root: root}
}

func (f *FileDiscoverer) Discover(ext string) error {
	entries := make(map[string]*FileEntry)
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			{
				if os.IsPermission(err) {
					return filepath.SkipDir
				}
				return err
			}
		}

		if d.IsDir() && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}

		if filepath.Ext(d.Name()) == ext {
			entries[d.Name()] = &FileEntry{
				Path:    path,
				Started: false,
				Done:    false,
			}
		}
		return nil
	}

	return filepath.WalkDir(f.Root, walkFunc)
}
