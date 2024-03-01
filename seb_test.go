package seb_test

import (
	"context"
	"testing"
	"time"

	"github.com/dcarbone/seb/v3"
)

func TestBus(t *testing.T) {
	var (
		receivedEvent seb.Event
	)

	bus := seb.New()
	bus.AttachFunc("", func(ev seb.Event) {
		receivedEvent = ev
	})

	err := bus.Push(context.Background(), "test-topic", true)
	if err != nil {
		t.Logf("Error while pushing event: %v", err)
		t.Fail()
		return
	}

	time.Sleep(500 * time.Millisecond)

	if receivedEvent.Topic != "test-topic" {
		t.Logf("Expected received event to have topic \"test-topic\", saw %q", receivedEvent.Topic)
		t.Fail()
	}
	if b, _ := receivedEvent.Data.(bool); b != true {
		t.Logf("Expected received event to have data=%[1]T(%[1]v), saw %[2]v(%[2]T)", true, receivedEvent.Data)
		t.Fail()
	}
}
