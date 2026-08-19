package pubsub

import "context"

// Implementations of subscriber should return a channel
// from where subscribers can listen to messages as well as
// a function that tells the subscriber how to unsubscribe
type Subscriber[T any] interface {
	Subscribe(context.Context) (<-chan T, func())
}
