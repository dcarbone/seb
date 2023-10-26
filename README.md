# seb
Simple event bus written in go

## Primary Goals

### Accept callback funcs or channels
Depending on the situation, it may be advantageous to receive messages in a func or in a channel and I wanted to support
both without requiring the implementor perform the wrapping

### In-order messaging
Each recipient will always get events in the order they were sent until the recipient starts to block beyond the defined
buffer of 100 delayed messages.  If this buffer is breached, events for that specific recipient are dropped.

### Non-blocking
No recipient will prevent or even delay an event from being pushed to other recipients.  This is designed to somewhat
force event recipients to be as expedient as possible in its implementation, as the rest of the Bus won't
wait for it to handle one event before sending another. 

### Global subscription
For simplicity's sake, you either are listening or you aren't.  If you don't care about a topic, ignore the message.

### No external deps
Only use stdlib modules

## Extra Credit

### Filtered Recipients

If you wish to have a particular event func or channel receive events from subset of topics, you may provide either a string or instance 
of *regexp.Regexp to the `Filtered` series of handler registration funcs on the Bus.  Strings are matched exactly, and regex instances
are matched using `MatchString`.  First match wins, and exact string matches are always compared first.
