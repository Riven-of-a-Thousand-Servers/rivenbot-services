package consumer

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"pgcr-processing-service/internal/pubsub"
)

type FileIndex []FileEntry

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
	return &FileWalker{Root: root}
}

func (f *FileWalker) Discover(extension string) (FileIndex, error) {
	var entries []FileEntry
	if err := filepath.WalkDir(f.Root, f.getFilesByExtension(".ext", &entries)); err != nil {
		return nil, err
	}

	slices.SortFunc(entries, func(a FileEntry, b FileEntry) int {
		return strings.Compare(a.Filename, b.Filename)
	})

	return entries, nil
}

func (f *FileWalker) getFilesByExtension(extension string, idx *[]FileEntry) fs.WalkDirFunc {
	return func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsPermission(err) {
				return filepath.SkipDir
			}
			return err
		}

		// Skip any hidden directories or the $RECYBLE_BIN directory
		if d.IsDir() && (strings.HasPrefix(d.Name(), ".") || strings.HasPrefix(d.Name(), "$")) {
			return filepath.SkipDir
		}

		if filepath.Ext(d.Name()) == extension {
			entry := FileEntry{
				Filename: d.Name(),
				Started:  false,
				Done:     false,
			}

			*idx = append(*idx, entry)
			f.Broker.Publish(pubsub.WalkerProgress, entry)
		}
		return nil
	}
}
