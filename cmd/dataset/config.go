package dataset

import (
	"errors"
	"os"
)

func rootDir() (string, error) {
	dir := os.Getenv("RIVENBOT_DATASET_DIR")
	if dir == "" {
		return "", errors.New("RIVENBOT_DATASET_DIR is not set")
	}

	return dir, nil
}
