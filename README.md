# go-seb
Simple event bus written in go

This is designed with the following primary goals:

### Accept callback funcs or channels
Depending on the situation, it may be advantageous to receive messages in a func or in a channel and I wanted to support
both without requiring the implementor perform the wrapping

### In-order messaging
Each recipient will always get events in the order they were sent until the recipient starts to block beyond the defined
buffer of 100 delayed messages.  If this buffer is breached, events for that specific recipient are dropped.

### Non-blocking
No recipient will prevent or even delay an even from being pushed to other recipients.  This is designed to somewhat
force the handler of the event to be as expedient as possible in its handling of events, as the rest of the Bus won't
wait for it to handle one event before sending another. 

### Global subscription
For simplicity's sake, you either are listening or you aren't.  If you don't care about a topic, ignore the message.

### No external deps
Only use stdlib modules