package consumer

import (
	"context"
)

// Represents each delivery item from amqp.delivery
// we wrap around Ack and Nack functionality so we don't lose these
// when unwrapping the types
type Delivery[T ~[]byte] struct {
	Payload T
	Headers map[string]any
	Ack     func() error
	Nack    func(requeue bool) error
}

// Default delivery with no Headers, Empty ACK and NACK callback functions
func DefaultDelivery[T ~[]byte](payload T) Delivery[T] {
	return Delivery[T]{
		Payload: payload,
		Headers: map[string]any{},
		Ack: func() error {
			return nil
		},
		Nack: func(requeue bool) error {
			return nil
		},
	}
}

// Consumer represents any construct that relies on an external source
// of information that needs to be processed by various goroutines,
// usually involves I/O operations such as network calls or file operations
type Consumer[T ~[]byte] interface {
	Consume(context.Context) (<-chan Delivery[T], error)
}
