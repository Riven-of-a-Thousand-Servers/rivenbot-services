package pubsub

import (
	"log"
	"testing"

	"gotest.tools/v3/assert"
)

type someStruct struct {
	field int
}

func TestSubscription(t *testing.T) {
	s := NewBroker[someStruct](5)
	ch, unsub := s.Subscribe()
	defer unsub()

	in := someStruct{field: 1}
	s.Publish(in)

	select {
	case out := <-ch:
		assert.Equal(t, out.field, in.field)
	default:
		log.Fatal("Should not run this select branch")
	}
}

func TestPublishingDropsPackets(t *testing.T) {
	size := 2
	s := NewBroker[someStruct](size)
	ch, unsub := s.Subscribe()
	defer unsub()

	// Any packets after size should be dropped
	// and not block
	for i := range size + 5 {
		s.Publish(someStruct{field: i})
	}

	assert.Equal(t, len(ch), size)
}

func TestUnsubFunction(t *testing.T) {
	s := NewBroker[someStruct](1)
	_, unsub1 := s.Subscribe()
	_, unsub2 := s.Subscribe()

	assert.Equal(t, len(s.subs), 2, "Should be two subscribers before the first unsub")

	// Once we unsub from unsub the first time then the Broker's subs should go down by one
	unsub1()
	assert.Equal(t, len(s.subs), 1, "Should only be one subscriber after the first unsub")

	unsub2()
	assert.Equal(t, len(s.subs), 0, "There should not be any subscreibers")
}
