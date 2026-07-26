package producer

import (
	"context"
	"io"
)

// Represents anything that can 'produce' something, in this case
// RabbitMQ, STDproducer, etc...
type Producer[T any] interface {
	Produce(context.Context, T) error
}

// This combines both a Producer and something that can close itself
// Like a RabbitMQ channel or a connection
type ProducerCloser[T any] interface {
	Producer[T]
	io.Closer
}

// Returns a single ProducerCloser
type Factory[T any] interface {
	NewProducer(context.Context) (ProducerCloser[T], error)
}
