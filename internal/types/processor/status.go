package processor

import "time"

type Source int

const (
	Crawler Source = iota + 1
	Dataset
)

var Sources map[Source]string = map[Source]string{
	Crawler: "crawler",
	Dataset: "dataset",
}

var ReverseSources map[string]Source = map[string]Source{
	"crawler": Crawler,
	"dataset": Dataset,
}

type Status int

const (
	Started Status = iota + 1
	Processing
	Errored
	Success
)

var Statuses map[Status]string = map[Status]string{
	Started:    "started",
	Processing: "processing",
	Errored:    "error",
	Success:    "success",
}

// This is the duration after which an entry in the Ledger is
// marked as stale
const StaleThreshold = 5 * time.Minute
