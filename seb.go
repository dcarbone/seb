package seb

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	DefaultRecipientBufferSize = 100
	DefaultBlockedEventTTL     = 5 * time.Second
)

var (
	globalBusMu sync.Mutex
	globalBus   *Bus
)

// GlobalBus returns a bus that is global to this package, allowing its use as a static singleton.
// If you need event isolation or different configuration, you will need to construct a new Bus.
func GlobalBus() *Bus {
	globalBusMu.Lock()
	defer globalBusMu.Unlock()
	if globalBus == nil {
		globalBus = New()
	}
	return globalBus
}

// AttachFunc attaches an event recipient func to the global bus
func AttachFunc(id string, fn EventFunc, topicFilters ...any) (string, bool) {
	return GlobalBus().AttachFunc(id, fn, topicFilters...)
}

// AttachChannel attaches an event recipient channel to the global bus
func AttachChannel(id string, ch EventChannel, topicFilters ...any) (string, bool) {
	return GlobalBus().AttachChannel(id, ch, topicFilters...)
}

// Push pushes an event onto the global bus
func Push(ctx context.Context, topic string, data any) error {
	return GlobalBus().Push(ctx, topic, data)
}

// PushTo attempts to push an event to a particular recipient
func PushTo(ctx context.Context, to, topic string, data any) error {
	return GlobalBus().PushTo(ctx, to, topic, data)
}

// Request pushes an event with the Reply field populated, indicating the pusher expects a response
func Request(ctx context.Context, topic string, data any) (Reply, error) {
	return GlobalBus().Request(ctx, topic, data)
}

// RequestFrom attempts to push a request event to a particular recipient
func RequestFrom(ctx context.Context, to, topic string, data any) (Reply, error) {
	return GlobalBus().RequestFrom(ctx, to, topic, data)
}

var (
	ErrRecipientNotFound = errors.New("target recipient not found")

	ErrRecipientClosed     = errors.New("recipient is closed")
	ErrRecipientBufferFull = errors.New("recipient event buffer is full")
)

type Reply struct {
	Data any
	Err  error
}

// Event describes a specific event with associated data that gets pushed to any registered recipients at the
// time of push
type Event struct {
	// Ctx is the context that was provided at time of event push
	Ctx context.Context

	// ID is the randomly generated ID of this event
	ID int64

	// Originated is the unix nano timestamp of when this event was created
	Originated int64

	// Topic is the topic this event was pushed to
	Topic string

	// Data is arbitrary data accompanying this event
	Data any

	// Reply is used to send a message back to the pusher of this event.  If this field is nil, no reply is expected.
	Reply chan<- Reply // if defined, hints to recipients that a response is desired.
}

// EventFunc can be provided to a Bus to be called per Event
type EventFunc func(Event)

// EventChannel can be provided to a Bus to have new Events pushed to it
type EventChannel chan Event

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

type recipientConfig struct {
	buffSize int
	blockTTL time.Duration
}

type recipient struct {
	mu       sync.Mutex
	id       string
	closed   bool
	blockTTL time.Duration
	in       chan Event
	out      chan Event
	fn       EventFunc
}

func newRecipient(id string, fn EventFunc, cfg recipientConfig) *recipient {
	w := &recipient{
		id:       id,
		blockTTL: cfg.blockTTL,
		in:       make(chan Event, cfg.buffSize),
		out:      make(chan Event),
		fn:       fn,
	}
	go w.send()
	go w.process()
	return w
}

func (r *recipient) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return
	}

	r.closed = true

	close(r.in)
	close(r.out)
	if len(r.in) > 0 {
		for range r.in {
		}
	}
}

func (r *recipient) send() {
	var wait *time.Timer

	defer func() {
		if wait != nil && !wait.Stop() && len(wait.C) > 0 {
			<-wait.C
		}
	}()

	for ev := range r.in {
		// acquire lock to prevent closure while attempting to push a message
		r.mu.Lock()

		// test if recipient was closed between message push request and now.
		if r.closed {
			r.mu.Unlock()
			continue
		}

		// if the block ttl is > 0...
		if r.blockTTL > 0 {

			// construct or reset block ttl timer
			if wait == nil {
				wait = time.NewTimer(r.blockTTL)
			} else {
				if !wait.Stop() && len(wait.C) > 0 {
					<-wait.C
				}
				wait.Reset(r.blockTTL)
			}

			// attempt to send message.  if recipient blocks for more than configured
			select {
			case r.out <- ev:
			case <-ev.Ctx.Done():
			case <-wait.C:
			}
		} else {
			// attempt to push event to recipient
			select {
			case r.out <- ev:
			case <-ev.Ctx.Done():
			}
		}

		r.mu.Unlock()
	}
}

func (r *recipient) process() {
	// r.out is an unbuffered channel.  it blocks until any preceding event has been handled by the registered
	// handler.  it is closed once the push() loop breaks.
	for n := range r.out {
		r.fn(n)
	}
}

func (r *recipient) push(ev Event) error {
	// hold a lock for the duration of the push attempt to ensure that, at a minimum, the message is added to the
	// channel before it can be closed.
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return fmt.Errorf("%w: %q", ErrRecipientClosed, r.id)
	}

	// attempt to push message to ingest chan.  if chan is full or event context expires, drop on floor
	select {
	case <-ev.Ctx.Done():
		return fmt.Errorf("cannot push topic %q event %d to recipient %q: %w", ev.Topic, ev.ID, r.id, ev.Ctx.Err())
	case r.in <- ev:
		return nil
	default:
		return fmt.Errorf("cannot push topic %q event %d to recipient %q: %w", ev.Topic, ev.ID, r.id, ErrRecipientBufferFull)
	}
}

type BusConfig struct {
	// BlockedEventTTL denotes the maximum amount of time a given recipient will be allowed to block before
	// an event before the event is dropped for that recipient.  This only impacts the event for the blocking recipient
	BlockedEventTTL time.Duration

	// RecipientBufferSize will be the size of the input buffer created for your recipient.
	RecipientBufferSize int
}

type BusOpt func(*BusConfig)

func WithBlockedEventTTL(d time.Duration) BusOpt {
	return func(cfg *BusConfig) {
		cfg.BlockedEventTTL = d
	}
}

func WithRecipientBufferSize(n int) BusOpt {
	return func(cfg *BusConfig) {
		cfg.RecipientBufferSize = n
	}
}

type Bus struct {
	mu sync.Mutex

	rand *lockableRandSource

	rcfg recipientConfig

	// recipients is a map of recipient_id => recipient
	recipients map[string]*recipient
}

// New creates a new Bus for immediate use
func New(opts ...BusOpt) *Bus {
	cfg := BusConfig{
		BlockedEventTTL:     DefaultBlockedEventTTL,
		RecipientBufferSize: DefaultRecipientBufferSize,
	}

	for _, fn := range opts {
		fn(&cfg)
	}

	b := Bus{
		rand: newLockableRandSource(),
		rcfg: recipientConfig{
			blockTTL: cfg.BlockedEventTTL,
			buffSize: cfg.RecipientBufferSize,
		},
		recipients: make(map[string]*recipient),
	}

	return &b
}

// Push will immediately send a new event to all currently registered recipients, blocking until completed.
func (b *Bus) Push(ctx context.Context, topic string, data any) error {
	return b.sendEvent(b.buildEvent(ctx, topic, data))
}

// PushAsync pushes an event to all recipients without blocking the caller.  You amy optionally provide errc if you
// wish know about any / all errors that occurred during the push.  Otherwise, set errc to nil.
func (b *Bus) PushAsync(ctx context.Context, topic string, data any, errc chan<- error) {
	ev := b.buildEvent(ctx, topic, data)
	if errc == nil {
		go func() { _ = b.sendEvent(ev) }()
	} else {
		go func() { errc <- b.sendEvent(ev) }()
	}
}

// PushTo attempts to push an even to a specific recipient, blocking until completed.
func (b *Bus) PushTo(ctx context.Context, to, topic string, data any) error {
	return b.sendEventTo(to, b.buildEvent(ctx, topic, data))
}

// PushToAsync attempts to push an event to a specific recipient without blocking the caller.
func (b *Bus) PushToAsync(ctx context.Context, to, topic string, data any, errc chan<- error) {
	ev := b.buildEvent(ctx, topic, data)
	if errc == nil {
		go func() { _ = b.sendEventTo(to, ev) }()
	} else {
		go func() { errc <- b.sendEventTo(to, ev) }()
	}
}

// Request will push a new event with the Reply chan defined, blocking until a single response has been received
// or the provided context expires
func (b *Bus) Request(ctx context.Context, topic string, data any) (Reply, error) {
	return b.doRequest(ctx, "", topic, data)
}

// RequestFrom attempts to request a response from a specific recipient
func (b *Bus) RequestFrom(ctx context.Context, to, topic string, data any) (Reply, error) {
	return b.doRequest(ctx, to, topic, data)
}

// AttachFunc immediately adds the provided fn to the list of recipients for new events.
//
// You may optionally provide a list of filters to only allow specific messages to be received by this func.  A filter
// may be a string of the exact topic you wish to receive events from, or a *regexp.Regexp instance to use when
// matching.
//
// It will:
// - panic if fn is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (b *Bus) AttachFunc(id string, fn EventFunc, topicFilters ...any) (string, bool) {
	if fn == nil {
		panic(fmt.Sprintf("AttachFunc called with id %q and nil handler", id))
	}
	var (
		w        *recipient
		replaced bool
	)

	b.mu.Lock()
	defer b.mu.Unlock()

	if id == "" {
		id = strconv.FormatInt(b.rand.Int63(), 10)
	}

	if w, replaced = b.recipients[id]; replaced {
		w.close()
	}

	if len(topicFilters) > 0 {
		fn = eventFilterFunc(topicFilters, fn)
	}

	b.recipients[id] = newRecipient(id, fn, b.rcfg)

	return id, replaced
}

// AttachChannel immediately adds the provided channel to the list of recipients for new
// events.
//
// It will:
// - panic if ch is nil
// - generate random ID if provided ID is empty
// - return "true" if there was an existing recipient with the same identifier
func (b *Bus) AttachChannel(id string, ch EventChannel, topicFilters ...any) (string, bool) {
	if ch == nil {
		panic(fmt.Sprintf("AttachChannel called with id %q and nil channel", id))
	}
	var fn EventFunc
	if len(topicFilters) > 0 {
		fn = eventChanFilterFunc(topicFilters, ch)
	} else {
		fn = func(event Event) { ch <- event }
	}
	return b.AttachFunc(id, fn)
}

// Detach immediately removes the provided recipient from receiving any new events, returning true if a
// recipient was found with the provided id
func (b *Bus) Detach(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if w, ok := b.recipients[id]; ok {
		go w.close()
		delete(b.recipients, id)
		return ok
	}

	return false
}

// DetachAll immediately clears all attached recipients, returning the count of those previously
// attached.
func (b *Bus) DetachAll() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// count how many are in there right now
	cnt := len(b.recipients)

	// close all current in separate goroutine
	for _, w := range b.recipients {
		go w.close()
	}

	b.recipients = make(map[string]*recipient)

	return cnt
}

func (b *Bus) buildEvent(ctx context.Context, topic string, data any) Event {
	n := Event{
		Ctx:        ctx,
		ID:         b.rand.Int63(),
		Originated: time.Now().UnixNano(),
		Topic:      topic,
		Data:       data,
	}
	return n
}

// sendEvent immediately calls each handler with the new event
func (b *Bus) sendEvent(ev Event) error {
	var (
		wg    sync.WaitGroup
		errCh chan error
		errs  []error
	)

	b.mu.Lock()

	errCh = make(chan error, len(b.recipients)+1)

	for topic := range b.recipients {
		wg.Add(1)
		go func(w *recipient) { errCh <- w.push(ev) }(b.recipients[topic])
	}

	b.mu.Unlock()

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		wg.Done()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (b *Bus) sendEventTo(to string, ev Event) error {
	b.mu.Lock()

	if w, ok := b.recipients[to]; ok {
		b.mu.Unlock()
		return w.push(ev)
	}

	b.mu.Unlock()

	return fmt.Errorf("%w: %s", ErrRecipientNotFound, to)
}

func (b *Bus) doRequest(ctx context.Context, to, topic string, data any) (Reply, error) {
	ch := make(chan Reply)
	defer close(ch)

	ev := b.buildEvent(ctx, topic, data)
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

func eventFilterFunc(topics []any, fn EventFunc) EventFunc {
	var (
		st []string
		rt []*regexp.Regexp
	)
	for i := range topics {
		if s, ok := topics[i].(string); ok {
			st = append(st, s)
		} else if r, ok := topics[i].(*regexp.Regexp); ok {
			rt = append(rt, r)
		} else {
			panic(fmt.Sprintf("cannot handle filter of type %T, expected %T or %T", topics[i], "", (*regexp.Regexp)(nil)))
		}
	}

	return func(event Event) {
		for i := range st {
			if st[i] == event.Topic {
				fn(event)
				return
			}
		}
		for i := range rt {
			if rt[i].MatchString(event.Topic) {
				fn(event)
				return
			}
		}
	}
}

func eventChanFilterFunc(topicFilters []any, ch EventChannel) EventFunc {
	var (
		st []string
		rt []*regexp.Regexp
	)
	for i := range topicFilters {
		if s, ok := topicFilters[i].(string); ok {
			st = append(st, s)
		} else if r, ok := topicFilters[i].(*regexp.Regexp); ok {
			rt = append(rt, r)
		} else {
			panic(fmt.Sprintf("cannot handle filter of type %T, expected %T or %T", topicFilters[i], "", (*regexp.Regexp)(nil)))
		}
	}

	return func(event Event) {
		for i := range st {
			if st[i] == event.Topic {
				ch <- event
				return
			}
		}
		for i := range rt {
			if rt[i].MatchString(event.Topic) {
				ch <- event
				return
			}
		}
	}
}
