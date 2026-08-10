package processor

import "time"

type Source int

const (
	Crawler Source = iota + 1
	Dataset
)

func (s Source) String() string {
	switch s {
	case Dataset:
		return "dataset"
	case Crawler:
		return "crawler"
	default:
		return "unknown"
	}
}

func ParseSource(s string) (Source, bool) {
	switch s {
	case "dataset":
		return Dataset, true
	case "crawler":
		return Crawler, true
	default:
		return 0, false
	}
}

type Status int

const (
	Started Status = iota + 1
	Processing
	Errored
	Success
)

func (s Status) String() string {
	switch s {
	case Started:
		return "started"
	case Processing:
		return "processing"
	case Errored:
		return "error"
	case Success:
		return "success"
	default:
		return "unknown"
	}
}

func ParseStatus(s string) (Status, bool) {
	switch s {
	case "started":
		return Started, true
	case "processing":
		return Processing, true
	case "error":
		return Errored, true
	case "success":
		return Success, true
	default:
		return 0, false
	}
}

// This is the duration after which an entry in the Ledger is
// marked as stale
const StaleThreshold = 5 * time.Minute
