package consumer

import "context"

// Represents each delivery item from amqp.delivery
// we wrap around Ack and Nack functionality so we don't lose these
// when unwrapping the types
type Delivery[T any] struct {
	Item T
	Ack  func() error
	Nack func(requeue bool) error
}

type Consumer[T any] interface {
	Consume(context.Context) (<-chan Delivery[T], error)
}
