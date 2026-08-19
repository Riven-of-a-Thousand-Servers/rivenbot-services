package pubsub

import (
	"sync"
)

type Broker[T any] struct {
	bufferSize int
	mu         sync.RWMutex
	done       chan struct{}
	subs       map[chan T]struct{}
}

func NewBroker[T any](bufferSize int) *Broker[T] {
	return &Broker[T]{
		subs:       make(map[chan T]struct{}),
		done:       make(chan struct{}),
		bufferSize: bufferSize,
	}
}

// This function returns the output channel where a subscribe will
// receive messages from as well as an unsubscribe function used to
// cleanup resources
func (b *Broker[T]) Subscribe() (ch <-chan T, unsubscribe func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub := make(chan T, b.bufferSize)
	b.subs[sub] = struct{}{}

	unsubscribe = func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if _, ok := b.subs[sub]; ok {
			delete(b.subs, sub)
			close(sub)
		}
	}

	return sub, unsubscribe
}

// TODO: This implementation is actually not that good since it'll drop
// packets if the sending channel is overwhelmed
func (b *Broker[T]) Publish(msg T) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for sub := range b.subs {
		select {
		case sub <- msg:
		default:
		}
	}
}

func (b *Broker[T]) Shutdown() {
	select {
	case <-b.done:
		return
	default:
		close(b.done)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for ch := range b.subs {
		delete(b.subs, ch)
		close(ch)
	}
}
