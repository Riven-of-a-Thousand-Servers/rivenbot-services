package consumer

import (
	"context"

	"pgcr-processing-service/internal/types/constraints"
)

// Represents each delivery item from amqp.delivery
// we wrap around Ack and Nack functionality so we don't lose these
// when unwrapping the types
type Delivery[T constraints.Bytes] struct {
	Payload T
	Headers map[string]any
	Ack     func() error
	Nack    func(requeue bool) error
}

// Consumer represents any construct that relies on an external source
// of information that needs to be processed by various goroutines,
// usually involves I/O operations such as network calls or file operations
type Consumer[T constraints.Bytes] interface {
	Consume(context.Context) (<-chan Delivery[T], error)
}
