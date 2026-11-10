package telemetry

import (
	"fmt"
	"strings"
	"time"
)

type LogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string

	// Just a list of strings formatted as 'key=value', nothing crazy
	Attributes []string
}

func (l LogEntry) String() string {
	attrs := strings.Join(l.Attributes, " ")
	return fmt.Sprintf("%s [%s] - %s: %s", l.Timestamp.Format(time.RFC1123), l.Level, l.Message, attrs)
}
