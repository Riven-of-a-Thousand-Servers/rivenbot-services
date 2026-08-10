package consumer

import (
	"context"
	"fmt"

	"pgcr-processing-service/internal/types/processor"
)

// Represents each delivery item from amqp.delivery
// we wrap around Ack and Nack functionality so we don't lose these
// when unwrapping the types
type Delivery[T any] struct {
	Payload T
	Headers map[string]any
	Ack     func() error
	Nack    func(requeue bool) error
}

// Consumer represents any construct that relies on an external source
// of information that needs to be processed by various goroutines,
// usually involves I/O operations such as network calls or file operations
type Consumer[T any] interface {
	Consume(context.Context) (<-chan Delivery[T], error)
}

func (d Delivery[T]) GetSource() (processor.Source, error) {
	raw, ok := d.Headers["source"]
	if !ok {
		return 0, fmt.Errorf("missing source header")
	}

	str, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("source header is not a string, got %T", raw)
	}

	source, ok := processor.ParseSource(str)
	if !ok {
		return 0, fmt.Errorf("unrecognized source value: %q", str)
	}

	return source, nil
}
