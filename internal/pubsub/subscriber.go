package pubsub

import "context"

type Subscriber[T any] interface {
	Subscribe(context.Context) <-chan T
}
