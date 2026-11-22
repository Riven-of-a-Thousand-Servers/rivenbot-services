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
	logFileName          = "rivenbot.log"
	EnvOverrideConfigDir = "RIVENBOT_CONFIG"
)

type Provider interface {
	GetConfig() *Config
}

type Config struct {
	Path     string
	Database *DatabaseConfig `json:"db"`
	Logging  *LoggingConfig  `json:"log"`
	Backfill *BackfillConfig `json:"backfill"`
}

type LoggingConfig struct {
	FileName string
}

type DatabaseConfig struct {
	URL string `json:"url,omitempty"`
}

func (c *Config) Defaults() {
	c.Logging.FileName = logFileName
	c.Database.URL = "postgres://localhost:5043/"

	c.Backfill.WalkerOpts.Filepattern = "*.zst"
	c.Backfill.WalkerOpts.Files = 0
	c.Backfill.WalkerOpts.Lines = 0
	c.Backfill.WalkerOpts.RootDir = "/Volumes/T7 Shield/"

	c.Backfill.Processor.Workers = 1
}

func candidatePaths() ([]string, error) {
	var paths []string

	if dir := os.Getenv(EnvOverrideConfigDir); dir != "" {
		paths = append(paths, filepath.Join(dir, configFileName))
	}

	if cwd, err := os.Getwd(); err == nil {
		paths = append(paths, filepath.Join(cwd, configDirName, configFileName))
	} else {
		return nil, fmt.Errorf("failed to get current working directory: %v", err)
	}

	if home, err := os.UserHomeDir(); err == nil {
		paths = append(paths, filepath.Join(home, configDirName, configFileName))
	} else {
		return nil, fmt.Errorf("failed to get user's home directory: %v", err)
	}

	if xdg, err := os.UserConfigDir(); err == nil {
		paths = append(paths, filepath.Join(xdg, "rivenbot", configFileName))
	} else {
		return nil, fmt.Errorf("failed to fetch user config directory: %v", err)
	}

	return paths, nil
}

func Find() (string, error) {
	paths, err := candidatePaths()
	if err != nil {
		return "", err
	}

	for _, p := range paths {
		_, err := os.Stat(p)
		if err == nil {
			return p, nil
		}

		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("failed to check %s: %v", p, err)
		}
	}

	return "", nil
}

func Init() (*Config, error) {
	var cfg Config

	if existing, err := Find(); err != nil {
		return nil, err
	} else if existing != "" {
		return decodeConfig(existing)
	}

	cfg.Defaults()
	cwd, err := os.Getwd()
	if err != nil {
		return &cfg, err
	}

	dir := filepath.Join(cwd, configDirName)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, configFileName)
	tmpPath := path + ".tmp"
	tmpFile, err := os.CreateTemp(tmpPath, "")
	if err != nil {
		return nil, err
	}

	if err := json.NewEncoder(tmpFile).Encode(&cfg); err != nil {
		return nil, err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func decodeConfig(path string) (*Config, error) {
	var cfg Config
	r, err := os.Open(path)
	if err != nil {
		return &cfg, err
	}

	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return &cfg, err
	}

	return &cfg, nil
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
