package seb

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Event describes a specific event with associated data that gets pushed to any registered recipients at the
// time of push
type Event struct {
	ID         string // random
	Originated int64  // unixnano timestamp of when this event was created
	Topic      string
	Data       interface{} // no attempt is made to prevent memory sharing
}

// EventHandler can be provided to a Bus to be called per Event
type EventHandler func(Event)

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel chan Event

type worker struct {
	mu     sync.RWMutex
	closed bool
	in     chan Event
	out    chan Event
	fn     EventHandler
}

func newWorker(fn EventHandler) *worker {
	nw := new(worker)
	nw.in = make(chan Event, 100)
	nw.out = make(chan Event)
	nw.fn = fn
	go nw.publish()
	go nw.process()
	return nw
}

func (nw *worker) close() {
	nw.mu.Lock()

	if nw.closed {
		nw.mu.Unlock()
		return
	}

	nw.closed = true

	close(nw.in)
	close(nw.out)
	if len(nw.in) > 0 {
		for range nw.in {
		}
	}

	nw.mu.Unlock()
}

func (nw *worker) publish() {
	var (
		wait *time.Timer
	)
	defer func() {
		if !wait.Stop() {
			<-wait.C
		}
	}()

	for n := range nw.in {
		// todo: it is probably not necessary to test here as if the worker is closed between this event
		// being processed and the next event in, it is removed from the map of available workers to push
		// to before close == true, meaning it cannot have new messages pushed to it.
		nw.mu.RLock()
		if nw.closed {
			nw.mu.RUnlock()
			return
		}

		// either construct timer or reset existing
		if wait == nil {
			wait = time.NewTimer(5 * time.Second)
		} else {
			wait.Reset(5 * time.Second)
		}

		// attempt to push message to consumer, allowing for up to 5 seconds of blocking
		// if block window passes, drop on floor
		select {
		case nw.out <- n:
			if !wait.Stop() {
				<-wait.C
			}
		case <-wait.C:
		}

		nw.mu.RUnlock()
	}
}

func (nw *worker) process() {
	// nw.out is an unbuffered channel.  it blocks until any preceding event has been handled by the registered
	// handler.  it is closed once the publish() loop breaks.
	for n := range nw.out {
		nw.fn(n)
	}
}

func (nw *worker) push(n Event) {
	// hold an rlock for the duration of the push attempt to ensure that, at a minimum, the message is added to the
	// channel before it can be closed.
	nw.mu.RLock()
	defer nw.mu.RUnlock()

	if nw.closed {
		return
	}

	// attempt to push message to ingest chan.  if chan is full, drop on floor
	select {
	case nw.in <- n:
	default:
	}
}

type Bus struct {
	mu sync.RWMutex
	ws map[string]*worker
	rs rand.Source
}

// New creates a new Bus for immediate use
func New() *Bus {
	b := new(Bus)
	b.ws = make(map[string]*worker)
	b.rs = rand.NewSource(time.Now().UnixNano())
	return b
}

// Push will immediately send a new event to all currently registered recipients
func (b *Bus) Push(topic string, d interface{}) {
	b.sendEvent(topic, d)
}

// AttachHandler immediately adds the provided fn to the list of recipients for new events.
//
// It will:
// - panic if fn is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (b *Bus) AttachHandler(id string, fn EventHandler) (string, bool) {
	if fn == nil {
		panic(fmt.Sprintf("AttachHandler called with id %q and nil handler", id))
	}
	var (
		w        *worker
		replaced bool
	)

	b.mu.Lock()
	defer b.mu.Unlock()

	if id == "" {
		id = strconv.FormatInt(b.rs.Int63(), 10)
	}

	w, replaced = b.ws[id]
	if replaced {
		w.close()
	}

	b.ws[id] = newWorker(fn)

	return id, replaced
}

// AttachChannel immediately adds the provided channel to the list of recipients for new
// events.
//
// It will:
// - panic if ch is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (b *Bus) AttachChannel(id string, ch EventChannel) (string, bool) {
	if ch == nil {
		panic(fmt.Sprintf("AttachChannel called with id %q and nil channel", id))
	}
	return b.AttachHandler(id, func(n Event) {
		ch <- n
	})
}

// DetachRecipient immediately removes the provided recipient from receiving any new events,
// returning true if a recipient was found with the provided id
func (b *Bus) DetachRecipient(id string) bool {
	var (
		w  *worker
		ok bool
	)

	b.mu.Lock()
	defer b.mu.Unlock()

	if w, ok = b.ws[id]; ok {
		w.close()
	}
	delete(b.ws, id)

	return ok
}

// DetachAllRecipients immediately clears all attached recipients, returning the count of those previously
// attached.
func (b *Bus) DetachAllRecipients() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// count how many are in there right now
	cnt := len(b.ws)

	// asynchronously close them all
	for _, w := range b.ws {
		go w.close()
	}

	b.ws = make(map[string]*worker)

	return cnt
}

// sendEvent immediately calls each handler with the new event
func (b *Bus) sendEvent(t string, d interface{}) {
	n := Event{
		ID:         strconv.FormatInt(b.rs.Int63(), 10),
		Originated: time.Now().UnixNano(),
		Topic:      t,
		Data:       d,
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, w := range b.ws {
		w.push(n)
	}
}
