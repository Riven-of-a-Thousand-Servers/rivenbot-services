package config

type BackfillConfig struct {
	WalkerOpts FileWalkerConfig `json:"walker"`
	Database   DatabaseConfig   `json:"db"`
	Processor  ProcessorConfig  `json:"processor"`
}

type FileWalkerConfig struct {
	RootDir     string `json:"root_dir,omitempty"`
	Filepattern string `json:"pattern,omitempty"`
	Lines       int    `json:"lines,omitempty"`
	Files       int    `json:"files,omitempty"`
}

type DatabaseConfig struct {
	URL string `json:"url,omitempty"`
}

type ProcessorConfig struct {
	Workers int `json:"workers"`
}
