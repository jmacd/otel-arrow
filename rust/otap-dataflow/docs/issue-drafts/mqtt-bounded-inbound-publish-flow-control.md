# Add bounded inbound PUBLISH flow control

## Summary

Add a bounded, application-configurable flow-control mechanism for inbound
MQTT `PUBLISH` packets.

The client currently transfers decoded inbound publishes from the connection
driver to `Receiver::recv()` through an unbounded channel. This makes it
possible for a broker to cause unbounded memory growth whenever it delivers
messages faster than the application consumes them.

The requested behavior is an end-to-end bound on decoded, application-visible
publishes. Reaching that bound must have explicit protocol behavior and must
never silently turn into an unbounded queue.

## Motivation

Long-running services commonly forward MQTT messages into another bounded
system, such as a telemetry pipeline, database writer, or work queue. Such an
application needs slow downstream processing to propagate pressure toward the
MQTT connection.

For example, an OpenTelemetry dataflow receiver may:

1. Receive an MQTT message.
2. Convert it into an OpenTelemetry log or metric.
3. Send it through a bounded processing pipeline.
4. Acknowledge a QoS 1 message only after the terminal exporter reports
   success.

If the pipeline is full, the receiver must stop admitting more work. An
unbounded queue inside the MQTT client defeats that policy and can exhaust the
process before the application has an opportunity to react.

This is both a reliability and security concern. The broker, or any publisher
allowed to reach it, can create sustained memory growth without sending an
individually oversized packet.

## Current behavior

`new_client()` creates an unbounded channel for inbound publishes. The
connection driver continues reading and placing messages in that channel while
the application is blocked or has stopped polling `Receiver::recv()`.

The application cannot establish a complete bound around this behavior:

- Limiting its own downstream queue does not limit the client's inbound queue.
- Stopping calls to `Receiver::recv()` lets the inbound queue continue growing.
- Stopping `Connection::run_until_disconnect()` also stops required connection
  work, including keepalive and acknowledgement progress.
- MQTT 5 Receive Maximum helps with unacknowledged QoS 1 and QoS 2 messages, but
  it does not bound QoS 0 delivery.
- Maximum Packet Size bounds one packet, not the number of queued packets or
  their aggregate memory.

The result is hidden, unbounded buffering between the network and the
application.

## Requested behavior

Provide a supported way to configure a finite maximum for decoded inbound
publishes that have not yet been received by the application.

The complete implementation does not have to use a Tokio bounded channel, but
it should provide the following observable guarantees:

1. The number and aggregate retained size of decoded inbound publishes are
   bounded by documented configuration.
2. A slow or paused application cannot cause an unbounded allocation backlog.
3. Reaching the bound has an explicit outcome. The client may apply protocol
   backpressure, suspend further application-message admission, or disconnect
   with a classified overload reason, but it must not silently allocate beyond
   the bound.
4. Inbound publish ordering is preserved for messages that are delivered to the
   application.
5. The connection driver remains cancellation-safe and does not require an
   application-created background thread or second Tokio runtime.
6. Any message discarded because bounded delivery cannot continue is reported
   explicitly to the application.

The default should be bounded. An explicitly selected unbounded mode could be
retained for specialized users, but it should not be the safe default.

## Protocol considerations

### QoS 1

For manually acknowledged QoS 1 delivery, the client can combine a bounded
application queue with MQTT 5 Receive Maximum:

- Advertise a finite Receive Maximum.
- Do not complete PUBACK while the corresponding application work is pending.
- Stop admitting additional decoded publishes when the application capacity is
  exhausted.
- Resume when the application receives or resolves pending messages.

The configured queue capacity and Receive Maximum should have a documented
relationship. It should not be possible to negotiate more simultaneously
deliverable QoS 1 messages than the implementation can retain safely.

### QoS 0

MQTT has no acknowledgement-based receive window for QoS 0. The implementation
therefore needs an explicit overload policy. Reasonable behavior includes
temporarily stopping network reads or closing the connection once the bounded
capacity is exhausted.

Silently dropping a QoS 0 publish inside the client should not be the default.
If a drop policy is offered, every drop must be observable through a typed
event or result so applications can count and report data loss.

Stopping reads may eventually cause keepalive or socket-level failure. That is
acceptable if it is deliberate, bounded, documented, and surfaced as an
overload outcome rather than an unexplained I/O or ping timeout.

### Mixed traffic

The bound must remain effective when QoS 0 and QoS 1 messages are interleaved.
QoS 0 traffic must not bypass a capacity reserved for manually acknowledged
QoS 1 messages.

Control packets and protocol progress should not share an unbounded escape
queue. If the implementation reserves capacity for control-plane work, that
reservation should be finite and documented.

## Possible API shape

The exact API is open for discussion. One possible configuration is:

```rust
pub struct ClientOptions {
    pub incoming_publish_capacity: NonZeroUsize,
    pub incoming_publish_overflow: IncomingPublishOverflow,
    // Existing options...
}

pub enum IncomingPublishOverflow {
    Backpressure,
    Disconnect,
}
```

An overload-triggered disconnect should be distinguishable from an ordinary
I/O error:

```rust
pub enum DisconnectedEvent {
    // Existing variants...
    InboundPublishCapacityExceeded {
        capacity: usize,
    },
}
```

Another valid design would expose inbound publishes directly from the
connection-driving API, provided the API still lets the application service
control packets, shutdown, and keepalive while respecting a finite memory
bound.

The essential request is the guarantee, not these particular names.

## Observability requirements

Applications should be able to distinguish at least:

- time spent unable to admit another inbound publish;
- overload-driven disconnects;
- publishes explicitly dropped by a configured drop policy;
- the configured capacity; and
- the current or peak number of retained inbound publishes.

This can be provided through typed events, callbacks, counters, or query
methods. The library does not need to choose an observability framework.

No event should include payload contents by default.

## Acceptance criteria

- A finite inbound capacity can be configured without using private features.
- The default configuration has a finite capacity.
- With capacity `N`, a paused application cannot cause more than the documented
  bounded amount of inbound publish storage.
- A QoS 1 test demonstrates that delivery pauses at the bound and resumes after
  capacity is released.
- A QoS 0 flood test demonstrates bounded memory behavior and the documented
  overload outcome.
- A mixed QoS 0/QoS 1 test demonstrates that neither class bypasses the bound.
- Cancellation while the queue is full cannot lose capacity or leave the
  connection driver permanently wedged.
- Disconnect and reconnect behavior after overload is deterministic and tested.
- Network tests exercise the behavior against Mosquitto with MQTT 5.
- The behavior is covered on every platform supported by the crate, including
  Windows if Windows remains supported.
- Public documentation explains the relationship among inbound capacity,
  Receive Maximum, Maximum Packet Size, and manual acknowledgement.

## Non-goals

- Durable persistence of messages across process restarts.
- An application-level retry queue.
- QoS 2 implementation.
- Choosing a downstream batching or concurrency model for applications.
- Guaranteeing delivery of QoS 0 messages after the connection is overloaded.

## Compatibility

Changing the default from unbounded to bounded can alter overload behavior. For
a pre-1.0 client, making the safe behavior the default is preferable. If
compatibility requires preserving the old behavior temporarily, the unbounded
choice should be explicit, documented as unsafe for untrusted or sustained
traffic, and scheduled for deprecation.

## Related work

This request complements a separate acknowledgement-safety request: dropping a
manual QoS 1 acknowledgement token must not implicitly send a successful
PUBACK. Bounded in-flight delivery cannot provide end-to-end reliability if an
error or cancellation path silently acknowledges pending work.

Relevant implementation and protocol documentation:

- <https://github.com/microsoft/rust-mqtt-client/blob/main/src/client.rs>
- <https://github.com/microsoft/rust-mqtt-client/blob/main/doc/feature-support.md>
- <https://github.com/microsoft/rust-mqtt-client/blob/main/doc/limitations.md>
