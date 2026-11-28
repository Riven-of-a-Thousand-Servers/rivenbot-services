package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"pgcr-processing-service/internal/utils"
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
	Database DatabaseConfig `json:"db"`
	Logging  LoggingConfig  `json:"log"`
	Backfill BackfillConfig `json:"backfill"`
}

type LoggingConfig struct {
	FileName string `json:"name"`
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
	existing, err := Load()
	if err != nil {
		return nil, err
	}

	if existing != nil {
		return existing, nil
	}

	cfg := new(Config)
	cfg.Defaults()

	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	dir := filepath.Join(cwd, configDirName)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, err
	}

	data, err := json.MarshalIndent(cfg, "", " ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')

	if err := utils.AtomicFileWrite(filepath.Join(dir, configFileName), data, 0o644); err != nil {
		return nil, err
	}

	return cfg, nil
}

func Load() (*Config, error) {
	if existing, err := Find(); err != nil {
		return nil, err
	} else if existing != "" {
		return decodeConfig(existing)
	}

	return nil, nil
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
