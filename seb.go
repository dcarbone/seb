package seb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"time"
)

var GlobalBus *Bus

func init() {
	GlobalBus = New()
}

var (
	ErrRecipientNotFound = errors.New("target recipient not found")

	ErrWorkerClosed     = errors.New("worker is closed")
	ErrWorkerBufferFull = errors.New("worker input buffer is full")
)

type Reply struct {
	Data any
	Err  error
}

// Event describes a specific event with associated data that gets pushed to any registered recipients at the
// time of push
type Event struct {
	ID         string       // random
	Originated int64        // unixnano timestamp of when this event was created
	Topic      string       // topic of event
	Data       any          // no attempt is made to prevent memory sharing
	Reply      chan<- Reply // if defined, hints to recipients that a response is desired.
}

// EventHandler can be provided to a Bus to be called per Event
type EventHandler func(Event)

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel chan Event

type worker struct {
	mu     sync.Mutex
	id     string
	closed bool
	in     chan Event
	out    chan Event
	fn     EventHandler
}

func newWorker(id string, fn EventHandler) *worker {
	w := &worker{
		id:  id,
		in:  make(chan Event, 100),
		out: make(chan Event),
		fn:  fn,
	}
	go w.publish()
	go w.process()
	return w
}

func (w *worker) close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	w.closed = true

	close(w.in)
	close(w.out)
	if len(w.in) > 0 {
		for range w.in {
		}
	}
}

func (w *worker) publish() {
	var wait *time.Timer

	defer func() {
		if wait != nil && !wait.Stop() && len(wait.C) > 0 {
			<-wait.C
		}
	}()

	for n := range w.in {
		// acquire lock to prevent closure while attempting to push a message
		w.mu.Lock()

		// test if worker was closed between message push request and now.
		if w.closed {
			w.mu.Unlock()
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
		case w.out <- n:
			if !wait.Stop() {
				<-wait.C
			}
		case <-wait.C:
		}

		w.mu.Unlock()
	}
}

func (w *worker) process() {
	// w.out is an unbuffered channel.  it blocks until any preceding event has been handled by the registered
	// handler.  it is closed once the publish() loop breaks.
	for n := range w.out {
		w.fn(n)
	}
}

func (w *worker) push(n Event) error {
	// hold a lock for the duration of the push attempt to ensure that, at a minimum, the message is added to the
	// channel before it can be closed.
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWorkerClosed
	}

	// attempt to push message to ingest chan.  if chan is full, drop on floor
	select {
	case w.in <- n:
		return nil
	default:
		return ErrWorkerBufferFull
	}
}

type lockableRandSource struct {
	mu  sync.Mutex
	src rand.Source
}

func newLockableRandSource() *lockableRandSource {
	s := new(lockableRandSource)
	s.src = rand.NewSource(time.Now().UnixNano())
	return s
}

func (s *lockableRandSource) Int63() int64 {
	s.mu.Lock()
	v := s.src.Int63()
	s.mu.Unlock()
	return v
}

type Bus struct {
	mu     sync.RWMutex
	rand   *lockableRandSource
	topics map[string]*worker
}

// New creates a new Bus for immediate use
func New() *Bus {
	b := Bus{
		topics: make(map[string]*worker),
		rand:   newLockableRandSource(),
	}
	return &b
}

// Push will immediately send a new event to all currently registered recipients
func (b *Bus) Push(topic string, data any) error {
	return b.sendEvent(b.buildEvent(topic, data))
}

// PushTo attempts to push an even to a specific recipient
func (b *Bus) PushTo(to, topic string, data any) error {
	return b.sendEventTo(to, b.buildEvent(topic, data))
}

// Request will publish a new event with the Reply chan defined, blocking until a single response has been received
// or the provided context expires
func (b *Bus) Request(ctx context.Context, topic string, data any) (Reply, error) {
	return b.doRequest(ctx, "", topic, data)
}

// RequestFrom attempts to request a response from a specific recipient
func (b *Bus) RequestFrom(ctx context.Context, to, topic string, data any) (Reply, error) {
	return b.doRequest(ctx, to, topic, data)
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
		id = strconv.FormatInt(b.rand.Int63(), 10)
	}

	if w, replaced = b.topics[id]; replaced {
		w.close()
	}

	b.topics[id] = newWorker(id, fn)

	return id, replaced
}

// AttachFilteredHandler attaches a handler that will only be called when events are published to specific topics
func (b *Bus) AttachFilteredHandler(id string, fn EventHandler, topics ...string) (string, bool) {
	if len(topics) == 0 {
		return b.AttachHandler(id, fn)
	}
	return b.AttachHandler(id, eventFilterFunc(topics, fn))
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
	return b.AttachHandler(id, eventChanFunc(ch))
}

// AttachFilteredChannel attaches a channel will only have events pushed to it when they are published to specific
// topics
func (b *Bus) AttachFilteredChannel(id string, ch EventChannel, topics ...string) (string, bool) {
	if len(topics) == 0 {
		return b.AttachChannel(id, ch)
	}
	return b.AttachHandler(id, eventChanFilterFunc(topics, ch))
}

// DetachRecipient immediately removes the provided recipient from receiving any new events,
// returning true if a recipient was found with the provided id
func (b *Bus) DetachRecipient(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if w, ok := b.topics[id]; ok {
		go w.close()
		delete(b.topics, id)
		return ok
	}

	return false
}

// DetachAllRecipients immediately clears all attached recipients, returning the count of those previously
// attached.
func (b *Bus) DetachAllRecipients() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// count how many are in there right now
	cnt := len(b.topics)

	// close all current in separate goroutine
	for _, w := range b.topics {
		go w.close()
	}

	b.topics = make(map[string]*worker)

	return cnt
}

func (b *Bus) buildEvent(t string, d any) Event {
	n := Event{
		ID:         strconv.FormatInt(b.rand.Int63(), 10),
		Originated: time.Now().UnixNano(),
		Topic:      t,
		Data:       d,
	}
	return n
}

// sendEvent immediately calls each handler with the new event
func (b *Bus) sendEvent(ev Event) error {
	b.mu.RLock()

	var (
		err      error
		finalErr []error
	)

	for i := range b.topics {
		if err = b.topics[i].push(ev); err != nil {
			finalErr = append(finalErr, fmt.Errorf("worker %q produced error on push: %w", b.topics[i].id, err))
		}
	}

	b.mu.RUnlock()

	if len(finalErr) > 0 {
		return errors.Join(finalErr...)
	}

	return nil
}

func (b *Bus) sendEventTo(to string, ev Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if w, ok := b.topics[to]; ok {
		return w.push(ev)
	}
	return fmt.Errorf("%w: %s", ErrRecipientNotFound, to)
}

func (b *Bus) doRequest(ctx context.Context, to, topic string, data any) (Reply, error) {
	ch := make(chan Reply)
	defer close(ch)

	ev := b.buildEvent(topic, data)
	ev.Reply = ch

	if err := ctx.Err(); err != nil {
		return Reply{Err: err}, err
	}

	if to == "" {
		if err := b.sendEvent(ev); err != nil {
			return Reply{}, err
		}
	} else if err := b.sendEventTo(to, ev); err != nil {
		return Reply{}, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-ctx.Done():
		return Reply{}, ctx.Err()
	}
}

func eventChanFunc(ch EventChannel) EventHandler {
	return func(event Event) {
		ch <- event
	}
}

func eventFilterFunc(topics []string, fn EventHandler) EventHandler {
	// fastpath when only single topic is being filtered
	if len(topics) == 1 {
		topic := topics[0]
		return func(event Event) {
			if topic == event.Topic {
				fn(event)
			}
		}
	}

	// create local copy of topics
	tps := slices.Clone(topics)
	return func(event Event) {
		for i := range tps {
			if tps[i] == event.Topic {
				fn(event)
				return
			}
		}
	}
}

func eventChanFilterFunc(topics []string, ch EventChannel) EventHandler {
	// fastpath when only single topic is being filtered
	if len(topics) == 1 {
		topic := topics[0]
		return func(event Event) {
			if topic == event.Topic {
				ch <- event
			}
		}
	}

	// create local copy of topics
	tps := slices.Clone(topics)
	return func(event Event) {
		for i := range tps {
			if tps[i] == event.Topic {
				ch <- event
				return
			}
		}
	}
}
