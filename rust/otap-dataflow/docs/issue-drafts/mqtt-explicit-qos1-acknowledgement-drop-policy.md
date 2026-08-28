# Make dropped QoS 1 acknowledgement tokens explicit and loss-safe

## Summary

Change inbound QoS 1 manual-acknowledgement handling so that dropping an
unresolved acknowledgement token does not implicitly send a successful
PUBACK.

The current auto-accept-on-drop behavior turns ordinary Rust control flow,
including cancellation, early return, panic unwinding, and accidental token
loss, into a successful broker acknowledgement. An application can therefore
report success to the broker for a message that it did not process or persist.

The requested behavior is a loss-safe default with explicit terminal actions:

- `accept` sends a successful PUBACK;
- `reject` sends the selected unsuccessful PUBACK reason; and
- abandoning or dropping an unresolved token never sends a successful PUBACK.

Because leaving an acknowledgement unresolved indefinitely consumes an MQTT
receive slot, the preferred default drop behavior is to invalidate and close
the current connection epoch without sending PUBACK. A persistent MQTT session
can then redeliver the message after reconnect.

## Motivation

Manual acknowledgement is useful when MQTT is the ingress to another reliable
system. A service may need to delay PUBACK until a database transaction,
filesystem write, or telemetry export completes.

For example:

1. The MQTT client receives a QoS 1 PUBLISH and acknowledgement token.
2. The application converts and forwards the message into a bounded pipeline.
3. The application retains the token while downstream work is pending.
4. A terminal exporter reports success or failure.
5. Only then does the application accept or reject the MQTT message.

In this model, token drop means the application did not reach a known terminal
decision. Treating that state as success violates the purpose of manual
acknowledgement and silently weakens at-least-once delivery.

Rust makes accidental drop possible in many non-exceptional situations:

- a future holding the token is cancelled by `select!`;
- an intermediate operation returns with `?`;
- a task is aborted;
- the connection or application shuts down before downstream completion;
- a panic unwinds through the token owner;
- a map or queue containing pending tokens is cleared during reconnect; or
- a refactor moves the token into a shorter-lived scope.

None of these events proves successful application processing.

## Current behavior

Dropping an unused inbound `PubAckToken` submits a successful PUBACK. The same
behavior can occur when an in-progress acknowledgement operation is cancelled
and its token is dropped.

This has several undesirable properties:

- Success is the implicit fallback rather than an explicit application action.
- Error paths can be indistinguishable from successful processing to the
  broker.
- Code review cannot reliably prove that every possible drop path is intended.
- Panic unwinding changes externally visible delivery state.
- A persistent session cannot redeliver work that was accidentally auto-acked.
- Libraries wrapping the token cannot provide stronger delivery semantics than
  the MQTT client.

The behavior is especially surprising for a type described as a manual
acknowledgement token.

## Requested behavior

### Safe default

Dropping an unresolved inbound QoS 1 acknowledgement token must not send a
successful PUBACK.

The recommended default is:

1. Mark the current connection epoch as having an abandoned inbound PUBLISH.
2. Do not send PUBACK for that packet identifier.
3. Wake the connection driver.
4. Close the connection in a bounded, protocol-safe way.
5. Return a typed disconnect event identifying the abandoned acknowledgement.

Closing the epoch avoids permanently consuming Receive Maximum capacity while
also avoiding a false success. With a non-expired persistent session, the
broker can redeliver the unacknowledged QoS 1 PUBLISH after reconnect.

The event should not contain the message payload. It may contain safe protocol
metadata such as the packet identifier or the count of abandoned tokens.

### Explicit terminal operations

Successful and unsuccessful acknowledgement must remain explicit:

```rust
token.accept(/* properties */).await?;
token.reject(reason, /* properties */).await?;
```

Only an explicit successful operation may send a success reason code.

An explicit rejection is a terminal MQTT outcome and may cause the broker to
discard rather than redeliver the message. Documentation should make this
different from abandonment:

- **Accept:** processing succeeded; send successful PUBACK.
- **Reject:** processing reached a known terminal failure; send an unsuccessful
  PUBACK reason.
- **Abandon:** processing outcome is unknown; do not PUBACK and terminate the
  epoch so persistent-session redelivery remains possible.

### Cancellation safety

Submitting an acknowledgement should have a clearly defined atomic point.

If cancellation occurs before the client has accepted the acknowledgement
submission, the operation must follow the unresolved-token policy and must not
send success.

If cancellation occurs after submission has been accepted, dropping only the
completion waiter may detach observation of the result, but it must not submit a
second acknowledgement or reverse the first one.

The API documentation should identify this boundary. An API that consumes the
token in a short, non-awaiting submission step and returns a separate completion
token would make the boundary particularly clear, but that exact design is not
required.

## Possible API shape

The precise names are open for discussion. One option is a client-level policy:

```rust
pub enum UnresolvedPubAckPolicy {
    Disconnect,
    AutoAccept,
}

pub struct ClientOptions {
    pub unresolved_puback_policy: UnresolvedPubAckPolicy,
    // Existing options...
}
```

`Disconnect` should be the default. `AutoAccept` could preserve current behavior
for applications that deliberately want RAII acknowledgement, but it must be
opt-in and prominently documented as unsuitable for downstream-confirmed
delivery.

The connection result could identify the cause:

```rust
pub enum DisconnectedEvent {
    // Existing variants...
    UnresolvedInboundAcknowledgement {
        count: usize,
    },
}
```

An explicit abandonment operation would also be useful:

```rust
token.abandon();
```

This would let applications deliberately request reconnect and redelivery
without relying on `drop(token)` as control flow. Drop should still have the
same loss-safe behavior as explicit abandonment.

Another viable design is for dropping the token to place a typed event into the
connection state and require the application to choose accept, reject, or
disconnect before further inbound delivery proceeds. Whatever design is
selected, unresolved drop must not imply success.

## Connection-epoch behavior

Acknowledgement tokens are scoped to the connection epoch in which the PUBLISH
was received. The following behavior should be explicit:

- A token from an old epoch can never send PUBACK on a new epoch.
- Dropping a stale token after its epoch has ended performs no network action
  and cannot affect the new connection.
- Reconnect code may discard stale tokens without causing successful PUBACK.
- Pending packet identifiers remain the broker and session's responsibility
  after an unacknowledged connection loss.
- If the broker does not resume the session, the client reports that fact; it
  must not pretend the abandoned message was acknowledged.

If several unresolved tokens exist when one is abandoned, the connection event
should report how many acknowledgements remain unresolved when the epoch ends.

## Interaction with bounded inbound flow control

Manual acknowledgement and inbound capacity are related:

- Each unresolved QoS 1 token consumes protocol and application capacity.
- Receive Maximum should bound broker-side in-flight QoS 1 delivery.
- The application-facing publish queue should also be bounded.
- Resolving a token releases protocol capacity.
- Abandoning a token closes the epoch rather than leaking that capacity.

This request should be implemented consistently with the separate request for
bounded inbound PUBLISH delivery. Neither feature alone is sufficient for a
reliable downstream-confirmed receiver.

## Error handling and observability

The library should expose enough information for an application to distinguish:

- explicit acceptance;
- explicit rejection;
- unresolved-token abandonment;
- connection loss before a decision;
- acknowledgement submission failure;
- acknowledgement completion failure; and
- a stale token from an ended epoch.

These should be typed outcomes where practical rather than message-string
matching.

The unresolved-token path must not panic, block in `Drop`, start an operating
system thread, or silently ignore failure to notify the connection driver.
Internal notification capacity for drop handling must itself be bounded or
otherwise bounded by the negotiated number of in-flight acknowledgements.

## Acceptance criteria

- Dropping an unresolved `PubAckToken` never emits a successful PUBACK under the
  default policy.
- Panic unwinding across an unresolved token never emits a successful PUBACK.
- Cancelling an acknowledgement operation before its submission point never
  emits a successful PUBACK.
- Explicit `accept` emits exactly one successful PUBACK.
- Explicit `reject` emits exactly one PUBACK with the selected failure reason.
- Explicit abandonment causes the documented connection-epoch outcome without
  PUBACK.
- Dropping a token after its connection epoch ends cannot affect a later epoch.
- Multiple out-of-order accepts still produce correct acknowledgements.
- A persistent-session network test demonstrates redelivery after an
  unresolved token causes the connection to close.
- A clean-session test documents that redelivery cannot be promised when no
  session is retained.
- Tests cover application shutdown, server disconnect, I/O failure, and
  simultaneous pending tokens.
- Network behavior is verified against Mosquitto using MQTT 5.
- Public documentation distinguishes acceptance, rejection, and abandonment.
- Existing auto-accept behavior, if retained, is available only through an
  explicit compatibility option.

## Non-goals

- Guaranteeing broker redelivery when the application configured a clean
  session or zero session expiry.
- Persisting acknowledgement tokens across process restarts.
- Defining application retry policy.
- Implementing QoS 2.
- Treating an unsuccessful PUBACK as a request for broker redelivery.

## Compatibility

Changing drop from implicit success to connection termination is behaviorally
breaking for applications that rely on RAII acknowledgement.

The crate is pre-1.0, and the safer default is appropriate before the API is
stabilized. A temporary `AutoAccept` compatibility option can ease migration,
but new applications should have to request it explicitly.

Migration guidance should tell existing users to call `accept` intentionally
when successful processing is complete. Applications that truly want immediate
acknowledgement should do so at the receive site rather than depend on scope
exit.

## Related work

Relevant implementation and protocol documentation:

- <https://github.com/microsoft/rust-mqtt-client/blob/main/src/client/token/acknowledgement.rs>
- <https://github.com/microsoft/rust-mqtt-client/blob/main/src/client.rs>
- <https://github.com/microsoft/rust-mqtt-client/blob/main/doc/feature-support.md>
- <https://github.com/microsoft/rust-mqtt-client/blob/main/doc/limitations.md>

The complementary inbound-flow-control request should be linked here once both
issues exist.
