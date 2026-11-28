package config

type BackfillConfig struct {
	DeadLetter DeadLetterConfig `json:"deadLetter"`
	TuiEnabled bool             `json:"tui"`
	WalkerOpts FileWalkerConfig `json:"walker"`
	Processor  ProcessorConfig  `json:"processor"`
}

type FileWalkerConfig struct {
	RootDir     string `json:"root_dir,omitempty"`
	Filepattern string `json:"pattern,omitempty"`
	Lines       int    `json:"lines,omitempty"`
	Files       int    `json:"files,omitempty"`
}

type ProcessorConfig struct {
	Workers int    `json:"workers"`
	ApiKey  string `json:"apiKey"`
}

type DeadLetterConfig struct {
	Filename string `json:"name"`
}
