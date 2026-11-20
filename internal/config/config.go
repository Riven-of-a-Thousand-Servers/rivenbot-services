package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	configFileName       = "config.json"
	configDirName        = ".rivenbot"
	EnvOverrideConfigDir = "RIVENBOT_CONFIG"
)

type Config struct {
	Backfill *BackfillConfig `json:"backfill"`
}

func Init() (*Config, error) {
}

// Loads the Config from the configured location
// Default location is in cwd
func Load() (*Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(cwd, configDirName, configFileName)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("unable to open config file at %s: %v", path, err)
	}

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("unable to decode config: %v", err)
	}

	return &cfg, nil
}
