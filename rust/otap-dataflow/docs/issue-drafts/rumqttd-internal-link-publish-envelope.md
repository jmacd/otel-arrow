# Feature request: typed publish-context envelope for internal `LinkRx`

Status: draft, ready to file upstream against
[bytebeamio/rumqtt](https://github.com/bytebeamio/rumqtt) (the `rumqttd`
crate lives at `rumqtt/rumqttd`).

Pinned reference version: `rumqttd` 0.20.0, commit
[`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`](https://github.com/bytebeamio/rumqtt/tree/c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74).
All line numbers and code excerpts below are from that exact commit; every
claim of "available" or "not available" was verified by reading the pinned
source referenced, not inferred from documentation.

## Summary

An application that embeds `rumqttd` as an in-process broker (via
`Broker::link(client_id) -> (LinkTx, LinkRx)`) can only observe forwarded
publishes as a `Notification::Forward(Forward)` value. `Forward` currently
carries a topic, payload, retain flag, and (for MQTT 5 publishers) most
`PublishProperties`, but it does not carry the identity of the client that
published the message, the MQTT protocol version that client used, or a
handful of other fields (`dup`, the publisher's own packet identifier, and
any peer/connection metadata). This request asks for a small, additive,
opt-in typed envelope that carries this information alongside `Forward`,
so embedded-broker consumers can attribute, audit, and gateway MQTT
traffic without re-implementing broker-side connection tracking.

## Use cases this blocks today

- **Embedded broker as an ingestion component** (for example, an
  OpenTelemetry collector receiver that embeds `rumqttd` to accept MQTT
  PUBLISH traffic and convert it to another telemetry format). Every
  converted record needs to carry which device/client produced it and
  which MQTT protocol version it used, both of which are standard
  attributes in the OpenTelemetry semantic conventions
  (`messaging.client.id`, `network.protocol.version`). Today the receiver
  has no way to populate them from `Forward` alone.
- **Audit/compliance logging.** A component that logs "client X published
  to topic Y at time T" for security or compliance review needs the
  publisher's client identifier on every forwarded message, not just the
  topic and payload.
- **Protocol gateway / bridge.** A component that re-publishes or
  transforms MQTT traffic onto another transport (Kafka, a different MQTT
  broker, a webhook) typically needs to preserve enough of the original
  PUBLISH envelope (protocol version, DUP, publisher identity) to make
  correct re-publication or de-duplication decisions downstream, and to
  let the receiving side distinguish "the same client re-sent this
  message" from "a different client published the same payload".

None of these consumers control the wire protocol or the remote
connections directly; they only see what the embedded `Broker`'s internal
`Link` API exposes, so the gap has to be closed inside `rumqttd`.

## Verified current behavior

### The `Forward` type carries no identity

```rust
// rumqttd/src/router/mod.rs:114-119
pub struct Forward {
    pub cursor: Option<(u64, u64)>,
    pub size: usize,
    pub publish: Publish,
    pub properties: Option<PublishProperties>,
}
```

`Notification::Forward(Forward)` (`rumqttd/src/router/mod.rs:72-74`) is the
only notification an internal `Link` receives for a matched publish, via
`LinkRx::recv`/`next`/`recv_deadline`
(`rumqttd/src/link/local.rs:319-364`). None of `Forward`'s fields, nor
`Publish`, nor `PublishProperties`, carry a client identifier, connection
id, or protocol version.

### `Publish`'s `dup`, `qos`, and `pkid` are not public

```rust
// rumqttd/src/protocol/mod.rs:171-178
pub struct Publish {
    pub(crate) dup: bool,
    pub(crate) qos: QoS,
    pub(crate) pkid: u16,
    pub retain: bool,
    pub topic: Bytes,
    pub payload: Bytes,
}
```

`retain`, `topic`, and `payload` are public fields. `dup`, `qos`, and
`pkid` are `pub(crate)`, and `impl Publish` (same file, lines 180-241)
defines no accessor method for any of the three. A crate outside
`rumqttd` cannot read these three fields through any stable, typed API
today; the only way to observe them at all is to format-debug the struct
(`Publish` derives `Debug`) and parse the resulting string, which is not a
reliable API contract.

### The publisher's `pkid` is overwritten before it reaches `Forward`

For QoS 1/2 requests, `Outgoing::push_forwards` assigns a new,
per-outgoing-link packet identifier before the `Forward` is queued:

```rust
// rumqttd/src/router/iobufs.rs:136-137
self.last_pkid += 1;
p.publish.pkid = self.last_pkid;
```

`last_pkid` is local to the `Outgoing` buffer of the *subscriber* link
(one per `Link`/remote connection) and wraps at `MAX_INFLIGHT = 100`
(`rumqttd/src/router/iobufs.rs:18-19`). Even if `pkid` were made public,
the value on `Forward.publish` is never the original publisher's packet
identifier; it is a small, wrapping, per-subscriber sequence number
assigned at forward time. Any request for the original publisher's `pkid`
must be satisfied by a separate field, not by exposing this one.

### `retain` is force-cleared on the live-forward path, but not on replay

```rust
// rumqttd/src/router/routing.rs:1230-1238 (append_to_commitlog)
if publish.payload.is_empty() {
    datalog.remove_from_retained_publishes(topic.to_owned());
} else if publish.retain {
    datalog.insert_to_retained_publishes(publish.clone(), properties.clone(), topic.to_owned());
}

// after recording retained message, we also send that message to existing subscribers
// as normal publish message. Therefore we are setting retain to false
publish.retain = false;
```

The retained-message *store* keeps the original `retain = true` copy
(cloned before the line above runs), and `read_retained_messages`
(`rumqttd/src/router/logs.rs:272`) returns that stored copy verbatim when
a new subscription replays retained state. But every live forward of a
newly published message always carries `retain = false` on `Forward`,
regardless of what the publisher actually sent. `retain` is already a
public field today, but its value on a live forward does not reflect the
publisher's original RETAIN flag; only the one-time retained-replay path
does. This is a correctness caveat on an already-available field, called
out here because it directly affects any audit/gateway consumer that
assumes `Forward.publish.retain` mirrors the wire flag.

### MQTT 5 `PublishProperties` mostly *do* survive to `Forward`

Contrary to what might be assumed from the field being easy to overlook,
most `PublishProperties` fields are preserved end to end for MQTT 5
publishers. The per-topic commit log entry type is:

```rust
// rumqttd/src/router/logs.rs:18
type PubWithProp = (Publish, Option<PublishProperties>);
```

and `native_readv` (`rumqttd/src/router/logs.rs:175`) returns exactly this
tuple back out, which `forward_device_data`
(`rumqttd/src/router/routing.rs:1417`) wraps into `Forward` unchanged
except for the topic-alias handling described below. Verified as
surviving to `Forward.properties` for an MQTT 5 publisher:
`payload_format_indicator`, `message_expiry_interval`, `response_topic`,
`correlation_data`, `user_properties` (order-preserving `Vec<(String,
String)>`, duplicate keys intact), and `content_type`
(`rumqttd/src/protocol/mod.rs:247-255`). `subscription_identifiers` on
`Forward` reflects the *current subscriber's own* subscription id(s), not
anything from the publisher (a PUBLISH that arrives with a subscription
identifier is rejected as malformed per
`rumqttd/src/router/routing.rs:1197-1210`, matching the MQTT 5 spec).

The one property that is genuinely discarded and re-purposed is
`topic_alias`, covered in its own section below.

## What is verified missing or unsafe to rely on

1. **Publisher client identity.** No field anywhere in `Forward`,
   `Publish`, or `PublishProperties` carries it. The publishing
   connection's `Connection.client_id`
   (`rumqttd/src/router/connection.rs:14-15`) is available in the router
   at ingestion time (`rumqttd/src/router/routing.rs:544`,
   `let client_id = incoming.client_id.clone();`) but is not attached to
   the `(Publish, Option<PublishProperties>)` tuple that gets persisted
   into the per-topic commit log (`PubWithProp`,
   `rumqttd/src/router/logs.rs:18`). Because `forward_device_data` reads
   back historical, already-persisted entries that may be replayed to a
   slow subscriber long after the original publisher disconnected, the
   identity has to be captured and stored at ingestion time, alongside
   the existing `(Publish, Option<PublishProperties>)` tuple; it cannot be
   recovered later from connection state alone.
2. **Protocol version (MQTT 3.1.1 vs MQTT 5).** Not encoded anywhere on
   `Forward`. `properties.is_some()` is not a reliable proxy: an MQTT 5
   publisher that sets zero properties also produces `None`, because
   `properties::read` returns `Ok(None)` whenever the encoded properties
   length is zero (`rumqttd/src/protocol/v5/publish.rs:136-148`), and
   `protocol::v4::publish::read` (`rumqttd/src/protocol/v4/publish.rs:12`)
   never produces a properties value at all, by construction (MQTT 3.1.1
   PUBLISH has no properties). `properties.is_some()` therefore cannot
   distinguish "v4 publisher" from "v5 publisher that set no properties".
3. **`dup`.** Present on `Publish` but `pub(crate)`; no accessor. Not
   modified anywhere on the forward path we traced, so if exposed it would
   faithfully reflect what the publisher sent on the wire.
4. **The publisher's own packet identifier.** Not preserved anywhere
   (see the `pkid` rewrite above); would need a new field distinct from
   `Publish.pkid`.
5. **Peer/connection metadata.** No `SocketAddr`, listener name, or TLS
   peer identity is attached to `Forward`, `Publish`, `Connection`, or the
   commit log entry. The remote peer address is visible only transiently
   during connection accept in `server::broker::remote`
   (`rumqttd/src/server/broker.rs`) and is never threaded into the router
   or `Connection` model at all in the pinned version, so this is a
   larger ask than the others (see Scope below).

## What is verified stable vs connection-local

| Field | Stability | Notes |
| --- | --- | --- |
| Topic | Stable, message-level | Already public (`publish.topic`), except when a topic-alias reuse clears it (see below); not affected by this request. |
| Payload | Stable, message-level | Already public. |
| Publisher client identifier | Stable, message-level, but see Privacy below | Identifies which client produced the message; does not change based on who reads it. |
| Protocol version | Stable, message-level | Fixed at the moment the publisher sent the PUBLISH; does not change based on who reads it. |
| DUP | Stable, message-level, but see Non-goals | Reflects what the publisher sent; the internal `Link` forward path never sets or clears it and does not itself retransmit with DUP=1. |
| Original (publisher's) QoS | Stable, message-level | Distinct from the delivery QoS `Forward.publish.qos` is rewritten to per-subscription; see [Backwards-compatible API options](#backwards-compatible-api-options). |
| Original (publisher's) packet identifier | Connection-scoped, not message-level | Only meaningful within the lifetime of the publisher's original connection; MQTT packet identifiers are reused across connections and over time, so this value must never be treated as a global message id or dedup key. |
| `Forward.publish.pkid` (as it exists today) | Link-local, not message-level | Assigned per subscriber `Outgoing` buffer at forward time (`iobufs.rs:136-137`); unrelated across two subscribers observing the same message. |
| Topic alias (`properties.topic_alias`) | Connection-local, not message-level | See dedicated section below; never the publisher's alias by the time it reaches `Forward`. |
| `retain` (as it exists today) | Message-level only on the retained-replay path | False on every live forward regardless of the publisher's flag; true only on initial retained-state replay. |
| Peer socket address / listener | Connection-local | Changes if the same client reconnects; must never be used as a stable client identity substitute. |

## Requested typed envelope

Add a small, additive struct populated only when explicitly requested
(see [Backwards-compatible API options](#backwards-compatible-api-options)):

```rust
/// Immutable, per-message context describing the connection that
/// originally published a message, captured at ingestion time and
/// persisted alongside the message so it survives past the publisher's
/// connection lifetime.
#[derive(Debug, Clone)]
pub struct PublishContext {
    /// The publishing client's MQTT Client Identifier, including any
    /// tenant prefix `Connection::new` applies
    /// (rumqttd/src/router/connection.rs:40-56). Cheaply clonable; shared
    /// across every message a given connection publishes.
    pub publisher_client_id: Arc<str>,
    /// The MQTT protocol version negotiated on the publisher's CONNECT.
    pub protocol_version: ProtocolVersion,
    /// The DUP flag as sent by the publisher. Never rewritten by the
    /// internal forward path.
    pub dup: bool,
    /// The QoS the publisher used on the wire, distinct from
    /// `Forward.publish.qos`, which reflects the effective delivery QoS
    /// for a specific subscription.
    pub original_qos: QoS,
    /// The publisher's own packet identifier, `None` for QoS 0. Connection-
    /// scoped: only unique within the lifetime of the publisher's
    /// original connection; MUST NOT be used as a global message id or
    /// cross-connection dedup key.
    pub original_pkid: Option<u16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    V4,
    V5,
}
```

Peer/connection metadata (socket address, listener name) is deliberately
left out of `PublishContext` itself; see [Non-goals](#non-goals) and
[Scope](#scope-peer-metadata-is-a-larger-ask) below for why it is proposed
as a separate, later addition rather than bundled with this request.

## Ownership, copying, and performance requirements

- `publisher_client_id` must be `Arc<str>`, not `String`. `Connection`
  already owns a `String` client id
  (`rumqttd/src/router/connection.rs:15`); wrap it once, at connection
  creation, and clone the `Arc` (a refcount bump, not a heap
  allocation-and-copy) into every `PublishContext` for messages that
  connection publishes, and into every stored commit-log entry.
- The per-topic commit log entry type
  (`type PublishData` derived from `PubWithProp`,
  `rumqttd/src/router/logs.rs:18-33`) must be extended to carry
  `Option<PublishContext>` (or the fields needed to reconstruct it)
  alongside the existing `(Publish, Option<PublishProperties>)` pair,
  because `forward_device_data` reads historical entries that may
  postdate the publisher's connection. This is a real, if moderate,
  internal storage change, not just an API-surface change; it is called
  out explicitly here rather than assumed to be free.
- The feature must be opt-in and default to zero added cost, mirroring
  the existing `LinkBuilder::topic_alias_max`/`dynamic_filters` builder
  pattern (`rumqttd/src/link/local.rs:40-100`) and
  `Connection::topic_alias_max` (`rumqttd/src/router/connection.rs:72-79`,
  which only allocates `BrokerAliases` when `max > 0`). When the opt-in
  flag is off, `Option<PublishContext>` must cost only a `None`
  discriminant on every stored/forwarded entry; no `Arc` clone, no string
  formatting, and no additional heap allocation should occur on that
  path. `Broker::link` (`rumqttd/src/server/broker.rs:150-155`) is the
  existing entry point most embedded consumers use today and should keep
  its current zero-context behavior unless a new opt-in constructor is
  used.
- No copying of `payload`/`topic` bytes beyond what already happens
  today; this request only adds small, fixed-size or `Arc`-shared fields.

## Backwards-compatible API options

Any of the following would satisfy this request; ordered from least to
most invasive so the maintainers can pick whichever fits `rumqttd`'s
design preferences:

1. **Add `pub publish_context: Option<PublishContext>` to `Forward`.**
   `Forward`'s fields are already all `pub`, and (as far as we can tell
   from the pinned source) `Forward` values are only ever constructed
   inside `rumqttd` itself
   (`rumqttd/src/router/routing.rs:1585-1591`); external consumers only
   destructure or read it. Adding a field is source-compatible for any
   consumer that reads `forward.publish`/`forward.properties` by field
   access rather than exhaustive positional destructuring, and the
   opt-in default (`None`) preserves current behavior exactly.
2. **A parallel accessor**, for example `LinkRx::recv_with_context()` /
   `next_with_context()` returning `(Notification, Option<PublishContext>)`,
   leaving `Forward` itself completely unchanged. Fully non-breaking, at
   the cost of a second code path through the router's notification
   plumbing.
3. **Minimal-first step:** add `pub fn dup(&self) -> bool` and
   `pub fn qos(&self) -> QoS` accessors to `Publish` (no storage-format
   change needed, since both fields are already stored in the existing
   commit-log entry and are never rewritten on the internal-Link forward
   path). This alone would close item 3 in
   [What is verified missing](#what-is-verified-missing-or-unsafe-to-rely-on)
   without any of the persistence work required for publisher identity
   or protocol version, and could ship independently and sooner. A
   `pub fn pkid(&self) -> u16` accessor could ship alongside it, but must
   be documented as returning the link-local, per-subscriber value
   described above, not the publisher's original packet identifier.

We do not have a preference among these; whichever best matches
`rumqttd`'s existing API conventions is fine.

## MQTT 3.1.1 vs MQTT 5 behavior

- For an MQTT 3.1.1 (v4) publisher, `protocol_version` must be
  `ProtocolVersion::V4` and `Forward.properties` remains `None` always,
  since `protocol::v4::publish::read`
  (`rumqttd/src/protocol/v4/publish.rs:12-38`) has no properties concept
  at all.
- For an MQTT 5 (v5) publisher, `protocol_version` must be
  `ProtocolVersion::V5` even when the publisher set zero properties (so
  `Forward.properties` is `None`). This is the concrete case that makes
  `properties.is_some()` insufficient as a version proxy today, per item 2
  above.
- `original_qos` and `dup` behave identically across v4 and v5 (both
  fields exist in the shared `Publish` struct;
  `rumqttd/src/protocol/mod.rs:171-177`).

## Topic aliases

This deserves precise treatment because the existing behavior is easy to
misread from the `Forward` value alone:

- On ingestion, if the publisher's PUBLISH carried a topic alias, the
  router uses it to resolve the actual topic and then discards it:

  ```rust
  // rumqttd/src/router/routing.rs:1196-1199 (append_to_commitlog)
  let topic_alias = properties.as_mut().and_then(|p| {
      // clear the received value as it is irrelevant while forwarding publishes
      p.topic_alias.take()
  });
  ```

  So `properties.topic_alias`, as stored and later read back on
  `Forward`, is never the publisher's original alias.
- Separately, if the *subscriber's own* `Link`/connection was created
  with `topic_alias_max > 0`
  (`rumqttd/src/link/local.rs:86-88`,
  `rumqttd/src/router/connection.rs:72-79`), the broker assigns and
  reuses its own outgoing alias for that specific subscriber
  (`rumqttd/src/router/routing.rs:1543-1576`), and on repeat use of an
  already-assigned alias it clears `publish.topic` entirely
  (`rumqttd/src/router/routing.rs:1575`, `publish.topic.clear()`) to mimic
  real wire savings.
- `Broker::link` (`rumqttd/src/server/broker.rs:150-155`) constructs its
  `LinkBuilder` without calling `.topic_alias_max(...)`, so it defaults to
  `0` (`rumqttd/src/link/local.rs:51,64`), meaning `broker_topic_aliases`
  stays `None` (`rumqttd/src/router/connection.rs:72-75`) and this
  topic-clearing behavior does not occur for the common embedding path
  used today. It only becomes relevant if a consumer explicitly opts an
  internal `Link` into `topic_alias_max > 0`, in which case that consumer
  must resolve `properties.topic_alias` back to a topic itself (mirroring
  `validate_and_set_topic_alias`,
  `rumqttd/src/router/routing.rs:1335-1360`) rather than assume
  `publish.topic` is always populated.
- This request does not ask for a change to any of the above; it only
  asks that the behavior be documented at the `Forward`/`PublishContext`
  API boundary so consumers do not have to read the router source to
  discover it, and that `PublishContext.original_pkid`/`original_qos`
  (which are entirely separate from `properties.topic_alias`) are never
  affected by whichever topic-alias mode a given `Link` uses.

## User properties: ordering and duplicates

`PublishProperties.user_properties` is `Vec<(String, String)>`
(`rumqttd/src/protocol/mod.rs:253`), read in wire order and pushed in
that same order (`rumqttd/src/protocol/v5/publish.rs:167-171`,
`user_properties.push((key, value));`), then cloned as-is into the commit
log and back out to `Forward.properties`
(`rumqttd/src/router/logs.rs:18`, `PubWithProp`). Because it is a `Vec`
and not a map, insertion order and duplicate keys both survive
end-to-end today, with no code path we found that sorts, dedups, or
merges entries. This request does not ask for any change to
`user_properties` handling; it asks that this existing, already-correct
guarantee (order-preserving, duplicate-preserving) be pinned down with an
explicit regression test (see [Tests](#tests-and-acceptance-criteria)),
since it is exactly the kind of behavior that a future refactor (for
example, switching to a map for lookup performance) could silently break
without anyone noticing, and downstream consumers depend on it for
semantic fidelity.

## Packet identifier caveats

To avoid this feature being misused, `PublishContext.original_pkid` must
be documented, at minimum, with:

- `None` for QoS 0 publishes, which have no packet identifier on the
  wire (`protocol::v5::publish::read`/`protocol::v4::publish::read`, both
  only read a `pkid` when `qos != QoS::AtMostOnce`).
- Connection-scoped uniqueness only: MQTT packet identifiers are 16-bit
  values reused within a single connection over time and have no
  uniqueness guarantee across different connections or across
  reconnects of the same client. It must never be documented or used as
  a global message identifier, an idempotency key, or a cross-connection
  deduplication key.
- Explicitly distinct from `Forward.publish.pkid`, which (per
  [Verified current behavior](#verified-current-behavior) above) is
  rewritten to an unrelated, per-subscriber, wrapping sequence number by
  `Outgoing::push_forwards`. Both values may legitimately be present and
  different on the same `Forward`/`PublishContext` pair; documentation
  should show a worked example so consumers do not confuse the two.

## Privacy and security concerns

- **Client identifiers may be sensitive.** In many deployments the MQTT
  Client Identifier is, or embeds, a device serial number, account
  identifier, or other value with privacy or security sensitivity for the
  operator. This is why the feature must be opt-in (see
  [Ownership, copying, and performance requirements](#ownership-copying-and-performance-requirements)),
  not default-on, so operators who do not want publisher identity
  surfaced to embedded-broker consumers are not forced to pay for or
  expose it.
- **Tenant prefixing changes the string's meaning.** `Connection::new`
  rewrites `client_id` to `tenant_id + "." + client_id` when a tenant id
  is configured (`rumqttd/src/router/connection.rs:40-56`). Documentation
  for `publisher_client_id` must say plainly that, in a multi-tenant
  deployment, the value already includes this prefix and is not
  necessarily identical to the wire-level MQTT Client Identifier the
  device itself sent.
- **Do not bundle peer network metadata into this request.** Socket
  addresses are a distinct, higher-sensitivity category (they can reveal
  network topology or approximate physical location) and, per
  [Scope](#scope-peer-metadata-is-a-larger-ask) below, are not currently
  threaded through the router at all, so adding them is a larger design
  question. Keeping them out of `PublishContext` lets this request stay
  additive and low-risk, and lets peer metadata be reviewed separately
  with its own opt-in and its own privacy documentation.

## Scope: peer metadata is a larger ask

Unlike `client_id`, protocol version, `dup`, `qos`, and the publisher's
`pkid` (all of which are already known in-router at ingestion time and
"only" need to be threaded through to `Forward`), a remote peer's
`SocketAddr` is not part of `Connection`, `Outgoing`, or any router-level
state at all in the pinned version; it exists only transiently inside
`server::broker::remote` at accept time
(`rumqttd/src/server/broker.rs`). Attaching it to every forwarded message
would require adding a new field to `Connection` and passing it through
connection setup, which is a larger, separate change. This request
intentionally scopes peer metadata out so the core identity/protocol-
version ask above can be reviewed and merged independently; a follow-up
request can propose peer metadata once this is in place.

## Non-goals

- Not requesting any change to QoS 1 PUBACK-before-commit-log-append
  sequencing, the lack of a public broker shutdown API, or commit-log
  eviction behavior; these are separate, already-tracked concerns.
- Not requesting QoS 2 support.
- Not requesting that `retain` be fixed to reflect the publisher's
  original flag on live forwards; that behavior is documented above as a
  correctness caveat for awareness, not requested to change, since it may
  be intentional (retained state has its own, separate replay path).
- Not requesting that the internal `Link` forward path retransmit with
  `DUP=1`, or otherwise change `dup` semantics; `PublishContext.dup`
  should simply mirror whatever the publisher originally sent.
- Not requesting removal of, or a breaking change to, any existing
  `Forward`, `Publish`, or `PublishProperties` field.

## Tests and acceptance criteria

1. **Scenario:** an MQTT 5 publisher sends `user_properties` containing
   both an out-of-alphabetical-order sequence and a repeated key.
   **Guarantee:** an internal `Link` subscriber observes
   `Forward.properties.user_properties` with the exact same order and
   the same duplicate entries (regression test for existing, currently
   correct behavior described in
   [User properties](#user-properties-ordering-and-duplicates)).
2. **Scenario:** the same MQTT 5 publish is delivered to two internal
   `Link` subscribers that subscribed at different QoS levels.
   **Guarantee:** `PublishContext.original_qos` is identical and correct
   on both deliveries, even though `Forward.publish.qos` and
   `Forward.publish.pkid` may legitimately differ per subscriber.
3. **Scenario:** an MQTT 3.1.1 publisher and an MQTT 5 publisher (that
   sets no properties) each publish once. **Guarantee:**
   `PublishContext.protocol_version` distinguishes the two
   (`V4` vs `V5`) even though `Forward.properties` is `None` in both
   cases.
4. **Scenario:** a publisher disconnects, then a slow internal `Link`
   subscriber drains its backlog and receives that publisher's message
   afterward. **Guarantee:** `PublishContext.publisher_client_id` is
   still correctly populated, proving identity was persisted at
   ingestion time rather than looked up from the (now-gone) live
   connection.
5. **Scenario:** an internal `Link` is built with the opt-in feature
   disabled (the default). **Guarantee:** no additional heap allocation
   occurs per forwarded message compared to the pinned baseline (an
   allocation-counting or equivalent micro-benchmark test), and
   `PublishContext`/`publish_context` is `None` throughout.
6. **Scenario:** an internal `Link` opts into `topic_alias_max > 0` and
   receives repeated publishes to the same topic. **Guarantee:**
   existing topic-clearing behavior on repeat aliases is unchanged by
   this feature, and `PublishContext.original_pkid`/`original_qos` are
   populated identically regardless of which topic-alias mode is in use.
7. **Scenario:** a retained message is replayed to a newly subscribing
   `Link`, followed by a live publish to the same topic. **Guarantee:**
   `Forward.publish.retain` is `true` for the replayed message and
   `false` for the live one (regression test documenting the caveat
   above, independent of whether this feature ships).

Acceptance for this request as a whole: all of the above tests pass, the
opt-in default adds no observable behavior change or allocation for
existing consumers, and the new types/fields are documented with the
caveats above (connection-scoped `pkid`, tenant-prefixed `client_id`,
`retain` semantics, topic-alias interaction).

## References

- Repository:
  [bytebeamio/rumqtt](https://github.com/bytebeamio/rumqtt), `rumqttd`
  subdirectory.
- Pinned commit:
  [`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`](https://github.com/bytebeamio/rumqtt/tree/c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74).
- `rumqttd/src/router/mod.rs` (`Notification`, `Forward`).
- `rumqttd/src/protocol/mod.rs` (`Publish`, `PublishProperties`).
- `rumqttd/src/protocol/v4/publish.rs`, `rumqttd/src/protocol/v5/publish.rs`
  (wire parsing, per-version properties handling).
- `rumqttd/src/router/routing.rs` (`append_to_commitlog`,
  `forward_device_data`, topic-alias handling, retain clearing).
- `rumqttd/src/router/iobufs.rs` (`Outgoing::push_forwards`, `pkid`
  rewrite).
- `rumqttd/src/router/logs.rs` (`PubWithProp`, `native_readv`,
  `read_retained_messages`).
- `rumqttd/src/router/connection.rs` (`Connection`, `BrokerAliases`).
- `rumqttd/src/link/local.rs` (`LinkBuilder`, `LinkRx`).
- `rumqttd/src/server/broker.rs` (`Broker::link`).
- Related internal design notes (this repository, not upstream):
  [mqtt-raw-receiver.md](mqtt-raw-receiver.md),
  [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md).
