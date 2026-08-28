# Provide observable, bounded delivery semantics for internal (embedded) subscribers

## Status

Draft, upstream-ready. Written against rumqttd 0.20.0, pinned commit
`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`. Every architectural claim below
was verified by reading that exact commit's source, with file references, not
inferred from documentation or changelogs.

## Summary

An embedded (in-process) subscriber that consumes via `Broker::link` ->
`LinkRx` can lose already-acknowledged QoS 1 data with no notification of any
kind, on either side of the broker. This happens through the combination of
two independently-verified behaviors:

1. The router queues a successful PUBACK for an incoming QoS 1 `PUBLISH`
   before that `PUBLISH` is appended to the topic's commit log, and therefore
   before any subscriber -- including an internal `Link` -- has had a chance
   to observe it.
2. The commit log is a bounded, in-memory ring of segments. When a topic's
   log exceeds its configured bound, the oldest segment is evicted to make
   room for new data, with no event, counter, log line, or callback marking
   that eviction.

Put together: a publisher can receive a successful PUBACK for a message that
is deleted before an embedded subscriber ever reads it, and nothing in
rumqttd's public API lets that subscriber, the embedding application, or an
operator detect that this happened.

This request asks for explicit, observable bounded-delivery semantics for
embedded subscribers: some combination of eviction observability, subscriber
lag/capacity visibility, publisher-facing backpressure or rejection before
data is irrecoverably lost, and/or a documented, configurable overflow
policy. It deliberately does not ask for MQTT-level (wire) PUBACK timing to
change as the only option, since that conflates two different contracts (see
[Scope: two separate contracts](#scope-two-separate-contracts)).

## Verified architecture

### 1. QoS 1 PUBACK is queued before commit-log append or subscriber delivery

In `rumqttd/src/router/routing.rs`, `Router::handle_device_payload` (called
from the connection event-processing loop) processes each decoded `Packet`
in the incoming batch. For `Packet::Publish`, the `QoS::AtLeastOnce` arm
runs first:

```rust
QoS::AtLeastOnce => {
    let puback = PubAck {
        pkid,
        reason: PubAckReason::Success,
    };

    let ackslog = self.ackslog.get_mut(id).unwrap();
    ackslog.puback(puback);
    force_ack = true;
}
```

Only after this match statement completes does the same function call
`append_to_commitlog(id, publish.clone(), properties, &mut self.datalog,
&mut self.notifications, &mut self.connections)`, which is what actually
appends the publish to the topic's `DataLog` / `CommitLog` and enqueues the
wake-up notifications that let a subscriber's `Link` observe the new data.

`ackslog.puback(...)` unconditionally queues a successful acknowledgement for
send back to the publishing connection. It does not know about, and cannot be
made to depend on, whether `append_to_commitlog` below it succeeds, whether
any subscriber is currently connected, or whether a subscriber's `Link` has
actually dequeued the data. There is no code path, option, or callback that
lets an embedder observe or delay this ordering: it is sequential router
logic, not a race that a different call pattern could avoid.

Consequence: "PUBACK sent" means only "the router accepted this PUBLISH for
routing." It does not mean, and cannot currently be made to mean, "at least
one subscriber has this data" or "this data was durably retained."

### 2. The bounded in-memory commit log silently evicts the oldest segment

In `rumqttd/src/segments/mod.rs`, `CommitLog::append` is the single entry
point that adds new data to a topic's log:

```rust
pub fn append(&mut self, message: T) -> (u64, u64) {
    self.apply_retention();
    // ... push into the active segment, roll to a new segment if needed
}

fn apply_retention(&mut self) {
    if self.active_segment().size() >= self.max_segment_size as u64 {
        // ... roll the active segment into the ring of in-memory segments
    }
    // when the ring already holds `max_mem_segments` segments, the oldest
    // one is dropped here to stay within budget
    self.tail += 1;
}
```

`CommitLog::new(max_segment_size, max_mem_segments)` bounds the number of
segments retained in memory. Once that bound is reached, `apply_retention`
(called on every `append`) drops the oldest segment to make room, advancing
`self.tail` past it. This is confirmed by the crate's own unit test,
`inmemory_appends_and_retention_policy_works`, which asserts `log.tail`
advances once the configured segment budget is exceeded.

Nothing in `apply_retention`, `append`, or the surrounding `DataLog` /
`Router` code emits an event, increments a public counter, logs at any level,
or invokes a callback when this eviction happens. A subscriber whose `Link`
has fallen behind by more than `max_segment_size * max_mem_segments` worth of
data for a topic will silently have its unread backlog deleted out from under
it, without an error on its next read; the read cursor is simply clamped or
jumped forward.

### 3. Combined effect: a stalled `LinkRx` can lose already-PUBACKed data

Because (1) and (2) are independent of each other and of subscriber state:

- A publisher can publish QoS 1, receive a successful PUBACK immediately (per
  item 1), and consider the message durably handed off.
- An embedded subscriber holding a `LinkRx` obtained from `Broker::link` may
  be temporarily stalled (for example, applying its own backpressure toward a
  slow downstream system, or busy with other work in the same task).
- If the topic's commit log fills and evicts past that subscriber's read
  position before it resumes draining (per item 2), the stalled data is gone.
- Neither the publisher nor the embedding application receives any signal
  that this occurred. The publisher already has its PUBACK. The subscriber's
  next `LinkRx` read simply resumes from wherever the log now starts, with no
  indication that entries were skipped.

This is the specific failure mode this issue asks to make observable and/or
preventable: **broker-ingest acknowledgement and commit-log retention are
currently decoupled from subscriber consumption with no bridge between them.**

## Scope: two separate contracts

This request deliberately separates two contracts that are easy to conflate:

1. **MQTT broker-ingest ACK semantics** (wire-facing, standards-constrained):
   what a PUBACK means to the connected publisher. MQTT 3.1.1 / 5 define
   PUBACK as acknowledgement of receipt by the broker, not end-to-end
   delivery to every subscriber. rumqttd's current behavior of sending PUBACK
   before persisting/routing is a legitimate implementation choice under that
   contract, and this issue does not ask to change it as the only remedy.
2. **Downstream-consumption semantics** (internal, embedding-facing): what
   guarantees an embedded subscriber obtained via `Broker::link` can rely on
   between "PUBACK was sent" and "this subscriber has read the data or been
   told it cannot."

The request is about (2), and about making the interaction between (1) and
(2) observable where it cannot be eliminated. **We are not asking rumqttd to
delay PUBACK until subscriber consumption by default**, since that would
change wire-visible broker behavior for every user and is not how most MQTT
brokers behave. If maintainers want to offer delayed/deferred PUBACK as an
**opt-in mode** (for embedders who accept the latency and want a stronger
guarantee), that would satisfy this request as one of several acceptable
designs -- see [Option E](#option-e-optional-deferred-puback-for-embedded-subscribers-opt-in-only)
-- but it must not become the only, or the default, fix.

## Requested behavior (goal, not a single mandated design)

At least one of the following must become true for embedded (`Broker::link`)
subscribers. These are independent and additive; a maintainer could
reasonably ship a subset.

1. Data that has already been PUBACKed to a publisher and is subsequently
   evicted from the commit log before an embedded subscriber consumes it is
   **observable**: a counter, event, or query surfaces that it happened, for
   which topic, and (at minimum) an approximate affected range/count.
2. An embedded subscriber can **observe its own lag and remaining capacity**
   relative to the commit log's retention budget, so an embedding application
   can detect "about to lose data" before it actually happens, rather than
   only after the fact.
3. The router can **reject or disconnect a publisher** (or otherwise refuse
   new admission) before irreversible eviction of unread, already-PUBACKed
   data occurs, as an alternative or complement to (1)/(2).
4. Embedders can configure **per-link backpressure** (the router pauses or
   slows delivery/retention progress for a specific topic while a specific
   `Link` is behind) or integrate **durable storage** so that eviction from
   the in-memory ring does not equal permanent loss.
5. The eviction/overflow behavior is **configurable** (for example: evict
   silently as today, evict-with-notification, block new publishes, or
   reject new publishes) rather than a single hard-coded policy.
6. Any of the above is exposed through **typed notifications** an embedder
   can match on, not string-matched logs or best-effort metrics.

## Proposed API/behavior options

These are illustrative starting points for discussion, not a single required
design. Names and shapes are negotiable.

### Option A: Eviction events with exact affected ranges

Extend `CommitLog` (or `DataLog`, which owns per-topic logs) to record, on
each `apply_retention` call that actually drops a segment, the topic, the
offset range being dropped, and an approximate entry count. Surface this
through a new `Notification` variant delivered to interested links (e.g. a
metrics/alerts link, or the affected data link itself), and/or through the
existing `Meter`/`Alert` link mechanisms that already exist for other router
events (`meters`, `alerts` fields on `Router`).

```rust
pub enum Notification {
    // ... existing variants ...
    SegmentEvicted {
        topic: String,
        dropped_start: (u64, u64),
        dropped_end: (u64, u64),
        approx_entries: u64,
    },
}
```

This is the minimum acceptable bar: it does not prevent loss, but it makes
loss observable, which is strictly better than the current silent behavior.

### Option B: Subscriber lag/capacity query

Expose a way for a `LinkRx` (or a companion handle) to query, for its current
read cursor on a topic, how far behind the active segment it is, and how much
retention headroom remains before the next eviction would consume its unread
data:

```rust
pub struct LinkLag {
    pub topic: String,
    pub unread_entries: u64,
    pub unread_bytes: u64,
    pub segments_until_eviction: u64,
}

impl LinkRx {
    pub fn lag(&self) -> Vec<LinkLag>;
}
```

An embedding application can poll or periodically sample this to raise an
alarm, apply its own admission control upstream, or proactively shed load
before data is silently dropped.

### Option C: Publisher-facing rejection/disconnect before loss

Before a publish would be appended in a way that forces an eviction of data
still unread by at least one active subscriber, the router optionally
rejects the publish (an unsuccessful PUBACK/PUBREC reason code, where the
protocol allows one) or disconnects the offending publisher, rather than
silently discarding the older, already-PUBACKed data. This should be
configurable per topic or per router, since not every deployment wants
publish-time backpressure.

### Option D: Per-link backpressure or durable storage integration

Let a `Link` (or a per-topic subscription) declare a minimum retention
requirement, and have the router avoid evicting segments that any such link
has not yet read, up to a configured maximum extra memory/time budget. Once
that extra budget is also exhausted, fall through to one of the other
options (event, rejection, or a documented hard drop) rather than always
defaulting to a fixed two-parameter (`max_segment_size`,
`max_mem_segments`) budget that has no relationship to subscriber state.

Alternatively, allow a pluggable storage backend (the crate already
distinguishes in-memory segments from on-disk retention in some
configurations) so an embedder can trade memory for durability under this
same eviction accounting, rather than losing data outright.

### Option E: Optional deferred PUBACK for embedded subscribers (opt-in only)

For embedders who need a stronger guarantee and can accept the latency cost,
offer an explicit, non-default connection or link option that defers PUBACK
until at least the append-to-commitlog step (or, further, until at least one
subscriber has read the data) has succeeded. This must be:

- opt-in per connection/listener, not a default behavior change;
- documented as changing wire-visible timing for the affected publishers;
- independent of whichever of Options A-D also ship, since deferred PUBACK
  alone does not make later eviction from the commit log observable.

### Configurable overflow policy

Whichever combination of the above ships, the overflow policy for a full
commit log should be an explicit, documented enum rather than a single
hard-coded behavior:

```rust
pub enum SegmentOverflowPolicy {
    /// Current behavior: evict oldest segment silently.
    EvictSilently,
    /// Evict oldest segment but emit a `Notification::SegmentEvicted`.
    EvictWithNotification,
    /// Reject new publishes on this topic until a subscriber catches up.
    RejectNewPublishes,
    /// Disconnect publishers whose data would force an eviction that
    /// leaves unread data for at least one active subscriber.
    DisconnectOnForcedEviction,
}
```

## Invariants

The following invariants should hold regardless of which option(s) are
implemented:

- A successful PUBACK must never become indistinguishable, after the fact,
  from data that was never routed anywhere. If eviction happens, the
  embedder must be able to learn that it happened for a specific topic and
  approximate range, even if the payload itself cannot be recovered.
- Observability of eviction/lag must not itself be unbounded: counters,
  event queues, and notification channels introduced by this request must
  have documented, finite capacity and a defined behavior when that capacity
  is exceeded (e.g. coalescing counts rather than an unbounded `Vec` of
  individual events).
- Existing wire-level MQTT semantics (PUBACK meaning "broker accepted
  responsibility," per the current architecture) must remain the default;
  any behavior change here is additive/opt-in unless a maintainer decides
  otherwise after evaluating compatibility impact.
- A slow or disconnected embedded subscriber must never be able to force
  unbounded memory growth in the commit log as a side effect of fixing this
  issue; any backpressure or durability option must remain itself bounded
  and documented.

## Cancellation

- Querying lag/capacity (Option B) must be safe to cancel at any point
  (e.g. a `select!` losing the race) without leaving router-side state
  inconsistent; it should be a side-effect-free read.
- If a publisher is disconnected or rejected due to forced eviction (Option
  C), that action must be idempotent and safe if the router is concurrently
  shutting down that connection for an unrelated reason (no double-free of
  connection state, no panic if the connection ID is already gone).
- If a per-link backpressure hold (Option D) is released because the
  embedder's task is dropped or cancelled, the router must reclaim that
  reserved budget deterministically rather than leaking it as permanently
  reserved capacity.

## Performance

- Recording eviction events or lag counters must not require an allocation
  or lock acquisition on every `CommitLog::append` call in the common case
  where no eviction occurs; a per-append comparison against an already
  in-hand size/count field, as `apply_retention` already does, is the
  acceptable baseline cost.
- Per-link lag queries (Option B) should be computable from data the router
  or `DataLog` already tracks (cursor offsets, segment head/tail) without a
  new O(n) scan over segment contents.
- Any new notification/event path must not introduce an unbounded queue;
  it should reuse or mirror the existing bounded `notifications` /
  `meters` / `alerts` mechanisms already present in `Router`.

## Compatibility

- All proposed options are additive: new `Notification` variants, new
  query methods, and a new configurable enum with a default that preserves
  today's `EvictSilently` behavior. Existing embedders who never opt in are
  unaffected.
- If a default policy change is ever considered (for example, defaulting to
  `EvictWithNotification` since it has no wire-visible behavior change and
  is strictly more informative than silence), it should be called out
  explicitly in the changelog as observability-only, since it adds a
  notification but changes no acknowledgement or retention behavior.
- Option E (deferred PUBACK) is the only option with wire-visible timing
  impact and must remain strictly opt-in, per
  [Scope: two separate contracts](#scope-two-separate-contracts).

## Security / DoS considerations

- Making eviction observable must not itself become a DoS vector: a
  malicious or misbehaving publisher should not be able to cause unbounded
  memory growth in an eviction-event queue or lag-tracking structure by
  forcing many rapid evictions. Coalesced counters (count + range) are
  preferable to per-event allocation for this reason.
- `RejectNewPublishes` / `DisconnectOnForcedEviction` (Option C) change the
  availability characteristics of the broker: a slow subscriber could, if
  misconfigured, cause legitimate publishers to be rejected or disconnected.
  This must be clearly documented as a tradeoff, and ideally scoped per
  topic/subscription rather than applied router-wide, so one slow internal
  consumer cannot globally throttle unrelated publishers.
- Any new configuration surface (per-link retention reservation, overflow
  policy) must have a safe, bounded default so that enabling this feature
  cannot be used to pin unbounded memory by requesting an arbitrarily large
  reservation.

## Tests

- A unit test on `CommitLog`/`DataLog` demonstrating that, under
  `EvictWithNotification`, an eviction that drops unread data for a
  registered link produces exactly one notification (or one coalesced
  counter increment) with a correct topic and range.
- A unit test demonstrating that `EvictSilently` remains bit-for-bit
  compatible with current behavior (no notification, no behavior change) so
  existing embedders opting out are unaffected.
- An integration test with a real `Router` and `Broker::link` reproducing
  the exact sequence in this issue: publish QoS 1 fast enough to force
  eviction while a `LinkRx` is deliberately not drained, and asserting that
  (a) the publisher still received PUBACK, (b) the subscriber's data is
  gone, and (c) whichever chosen observability mechanism reports it.
- A lag-query test (Option B) asserting the reported `unread_entries` /
  `segments_until_eviction` values track actual router-side state across
  several append/evict cycles.
- If `RejectNewPublishes` or `DisconnectOnForcedEviction` is implemented, a
  test confirming the publisher receives a distinguishable, documented
  failure reason rather than an ordinary connection error.
- If Option E is implemented, a test confirming that PUBACK timing changes
  only for connections that opted in, and that the default connection's
  PUBACK timing is unaffected.
- A soak/backpressure test confirming that none of the new bounded
  structures (event queues, lag caches) grow unbounded under a sustained
  fast-publisher / stalled-subscriber workload.

## Acceptance criteria

- At least one of Options A-D (eviction observability, lag/capacity
  visibility, pre-loss rejection, or per-link backpressure/durable storage)
  is implemented and documented.
- The default configuration remains backward compatible: an embedder that
  does not opt into any new option observes byte-for-byte identical PUBACK
  timing and commit-log eviction behavior to today's rumqttd 0.20.0.
- Whichever mechanism is chosen has a documented, finite resource bound of
  its own (see [Performance](#performance) and
  [Security / DoS considerations](#security--dos-considerations)).
- Public documentation (crate docs and/or README) states plainly, next to
  the QoS 1 PUBACK documentation, that PUBACK does not imply subscriber
  delivery or durable retention, and describes how an embedder can opt into
  whichever new observability/backpressure mechanism ships.
- If Option E ships, it is clearly and separately documented as an optional,
  non-default mode with wire-visible latency impact, distinct from the
  observability-only options.
- A test from [Tests](#tests) exists for each option that ships.

## Non-goals

- Changing the default MQTT wire-level PUBACK contract for existing
  embedders and remote clients (see
  [Scope: two separate contracts](#scope-two-separate-contracts)).
- Guaranteeing delivery to every subscriber before PUBACK by default; this
  issue asks for observability and/or configurable backpressure, not a
  mandatory two-phase commit across all subscribers.
- Implementing full durable, replicated, or clustered persistence; a
  pluggable storage hook (Option D) is a reasonable step toward that, but
  full durability is out of scope for this request.
- Redesigning QoS 2 handling; this issue is scoped to the QoS 1 and QoS 0
  ingest/retention/subscriber-consumption path.
- Prescribing a single mandatory API shape; the options above are starting
  points for maintainer discussion, not a take-it-or-leave-it patch.

## Related work

This request is one of the "verified rumqttd 0.20.0 blockers" tracked
internally while evaluating rumqttd as an embedded broker for an
OpenTelemetry Arrow dataflow receiver. The receiver design document
describes the same architecture from the embedder's side, including why
"pause consumption to apply backpressure" is not currently a safe mitigation
on its own, since pausing a `LinkRx` is exactly the condition under which
this silent eviction occurs.

Relevant source at the pinned commit
(`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`):

- `rumqttd/src/router/routing.rs` -- `Router::handle_device_payload`, the
  `QoS::AtLeastOnce` arm that queues PUBACK, and the subsequent
  `append_to_commitlog` call.
- `rumqttd/src/segments/mod.rs` -- `CommitLog::append` and
  `apply_retention`, and the `inmemory_appends_and_retention_policy_works`
  unit test that documents the eviction behavior.
- `rumqttd/src/link/local.rs` -- `LinkBuilder::build`, `LinkTx`, and
  `LinkRx`, the embedding API through which an in-process subscriber
  receives `Forward`/`Notification` values from the router.
- `rumqttd/src/router/logs.rs` -- `DataLog`, which owns the per-topic
  `CommitLog` instances and is configured from `router.max_segment_size` /
  `router.max_segment_count` (called `max_mem_segments` in
  `segments/mod.rs`).
