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

type EventAttachResult struct {
	ID        string
	Overwrote bool
}

type worker struct {
	mu     sync.RWMutex
	closed bool
	wg     *sync.WaitGroup
	id     string
	in     chan Event
	out    chan Event
	fn     EventHandler
}

func newWorker(id string, wg *sync.WaitGroup, fn EventHandler) *worker {
	nw := new(worker)
	nw.in = make(chan Event, 100)
	nw.out = make(chan Event)
	nw.id = id
	nw.wg = wg
	nw.fn = fn
	go nw.publish()
	go nw.process()
	return nw
}

func (nw *worker) close() {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if nw.closed {
		return
	}

	nw.closed = true
	close(nw.in)
	close(nw.out)
	if len(nw.in) > 0 {
		for range nw.in {
		}
	}
}

func (nw *worker) publish() {
	for n := range nw.in {
		// todo: it is probably not necessary to test here as if the worker is closed between this event
		// being processed and the next event in, it is removed from the map of available workers to push
		// to before close == true, meaning it cannot have new messages pushed to it.
		nw.mu.RLock()
		if nw.closed {
			nw.mu.RUnlock()
			return
		}

		// attempt to push message to consumer, allowing for up to 5 seconds of blocking
		// if block window passes, drop on floor
		waitForConsumer := time.NewTimer(5 * time.Second)
		select {
		case nw.out <- n:
			if !waitForConsumer.Stop() {
				<-waitForConsumer.C
			}
		case <-waitForConsumer.C:
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

	// mark done only after nw.out loop has exited
	nw.wg.Done()
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
	wg *sync.WaitGroup
	rs rand.Source
}

// New creates a new Bus for immediate use
func New() *Bus {
	b := new(Bus)
	b.ws = make(map[string]*worker)
	b.wg = new(sync.WaitGroup)
	b.rs = rand.NewSource(time.Now().UnixNano())
	return b
}

// Push will immediately send a new event to all currently registered recipients
func (eb *Bus) Push(topic string, d interface{}) {
	eb.sendEvent(topic, d)
}

// AttachHandler immediately adds the provided fn to the list of recipients for new events.
//
// It will:
// - panic if fn is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (eb *Bus) AttachHandler(id string, fn EventHandler) (string, bool) {
	if fn == nil {
		panic(fmt.Sprintf("AttachHandler called with id %q and nil handler", id))
	}
	var (
		w        *worker
		replaced bool
	)

	eb.mu.Lock()
	defer eb.mu.Unlock()

	eb.wg.Add(1)

	if id == "" {
		id = strconv.FormatInt(eb.rs.Int63(), 10)
	}
	w, replaced = eb.ws[id]

	eb.ws[id] = newWorker(id, eb.wg, fn)
	if replaced {
		w.close()
	}
	return id, replaced
}

// AttachHandlers allows you to attach 1 or more event handlers at a time
func (eb *Bus) AttachHandlers(fns ...EventHandler) []EventAttachResult {
	l := len(fns)
	if l == 0 {
		return nil
	}

	results := make([]EventAttachResult, l, l)
	for i, fn := range fns {
		res := new(EventAttachResult)
		res.ID, res.Overwrote = eb.AttachHandler("", fn)
		results[i] = *res
	}

	return results
}

// AttachChannel immediately adds the provided channel to the list of recipients for new
// events.
//
// It will:
// - panic if ch is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (eb *Bus) AttachChannel(id string, ch EventChannel) (string, bool) {
	if ch == nil {
		panic(fmt.Sprintf("AttachChannel called with id %q and nil channel", id))
	}
	return eb.AttachHandler(id, func(n Event) {
		ch <- n
	})
}

// AttachChannels will attempt to attach multiple channels at once
func (eb *Bus) AttachChannels(chs ...EventChannel) []EventAttachResult {
	l := len(chs)
	if l == 0 {
		return nil
	}

	results := make([]EventAttachResult, l, l)
	for i, ch := range chs {
		res := new(EventAttachResult)
		res.ID, res.Overwrote = eb.AttachChannel("", ch)
		results[i] = *res
	}

	return results
}

// DetachRecipient immediately removes the provided recipient from receiving any new events,
// returning true if a recipient was found with the provided id
func (eb *Bus) DetachRecipient(id string) bool {
	var (
		w  *worker
		ok bool
	)

	eb.mu.Lock()
	defer eb.mu.Unlock()

	if w, ok = eb.ws[id]; ok {
		w.close()
	}
	delete(eb.ws, id)

	return ok
}

// DetachAllRecipients immediately clears all attached recipients, returning the count of those previously
// attached.
func (eb *Bus) DetachAllRecipients(wait bool) int {
	eb.mu.Lock()

	cnt := len(eb.ws)
	current := eb.ws
	eb.ws = make(map[string]*worker)

	eb.mu.Unlock()

	go func() {
		for _, w := range current {
			w.close()
		}
	}()

	if wait {
		eb.wg.Wait()
	}

	return cnt
}

// sendEvent immediately calls each handler with the new event
func (eb *Bus) sendEvent(t string, d interface{}) {
	n := Event{
		ID:         strconv.FormatInt(eb.rs.Int63(), 10),
		Originated: time.Now().UnixNano(),
		Topic:      t,
		Data:       d,
	}
	eb.mu.RLock()
	for _, w := range eb.ws {
		w.push(n)
	}
	eb.mu.RUnlock()
}
