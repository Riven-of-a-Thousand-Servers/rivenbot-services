package manifest

type TopLevel struct {
	Version                    string `json:"version"`
	WorldComponentContentPaths Paths  `json:"jsonWorldComponentContentPaths"`
}

type Paths struct {
	English map[string]string `json:"en"`
}
