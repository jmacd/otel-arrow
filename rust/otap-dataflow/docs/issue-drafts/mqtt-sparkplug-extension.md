# Add a Sparkplug B extension composing with `receiver:mqtt` (not embedded in it)

## Status

Draft. This is an implementation-ready architecture for a future Sparkplug B
extension, written against the already-accepted raw MQTT design set:
[mqtt-raw-receiver.md](mqtt-raw-receiver.md),
[mqtt-raw-exporter.md](mqtt-raw-exporter.md), and
[mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md), plus the
extension system's
[requirements](../extension-requirements.md) and
[Phase 1 architecture](../extension-system-architecture.md). It also responds
to three related upstream proposals:
[#3452](https://github.com/open-telemetry/otel-arrow/issues/3452) (pluggable
PData byte representations),
[#3875](https://github.com/open-telemetry/otel-arrow/issues/3875) (pluggable
PData Arrow batch representations), and
[#3876](https://github.com/open-telemetry/otel-arrow/issues/3876) (mixed-signal
PData). Several parts of this design depend on engine capabilities that do not
exist yet; each is called out explicitly in
[Unresolved engine changes](#unresolved-engine-changes) rather than assumed
available. No implementation should begin against a dependency listed there
until it is resolved.

## Summary

Sparkplug B is a payload and session-management specification layered on top
of plain MQTT. It is **not** a transport: every Sparkplug message is an
ordinary MQTT `PUBLISH` (or `STATE` topic message) carrying a Google Protocol
Buffers `Payload` and following a topic-namespace convention
(`spBv1.0/{group_id}/{message_type}/{edge_node_id}[/{device_id}]`). This
document defines a Sparkplug extension that:

- **Composes with `receiver:mqtt`/`exporter:mqtt` instead of being embedded in
  them.** Neither raw component gains any Sparkplug-specific code path; the
  [envelope contract](mqtt-raw-envelope-contract.md) explicitly places
  "Sparkplug B" decoding out of scope for the raw receiver/exporter and assigns
  it to a processor layered on top -- this document is that layering, made
  concrete.
- Manages the **Primary Host Application `STATE` lifecycle** as a genuine,
  long-lived MQTT protocol participant (its own CONNECT, its own Will
  message, its own retained publishes) rather than a one-shot side effect.
- **Observes broker publications** (`NBIRTH`/`NDEATH`/`DBIRTH`/`DDEATH`/
  `NDATA`/`DDATA`) to maintain per-edge-node session state, independent of
  whether that state is ever materialized into OTAP.
- Issues **`NCMD`/`DCMD` rebirth requests** when session state indicates a
  edge node or device must republish its birth certificate before further
  data can be decoded.
- Acquires its MQTT connectivity through a **capability, abstracted from
  `rumqttd`**, so the extension has zero compile-time or runtime dependency
  on the embedded-broker library and works unchanged against an external
  broker.
- Splits **"protocol participant"** (this extension: stateful, PData-free,
  network-facing) from **"codec"** (a separate processor/PDataCodec:
  stateless per call, pdata-facing) along the engine's existing "extensions
  are PData-free" boundary, connected by a small capability the extension
  exposes for the codec to query.

## Non-Goals

- Redefining or modifying the raw envelope contract's PUBLISH<->LogRecord
  mapping. Sparkplug's payload remains opaque bytes as far as
  `receiver:mqtt`/`exporter:mqtt` are concerned; this document does not touch
  that mapping.
- Embedding Sparkplug topic-namespace parsing, protobuf decoding, alias
  resolution, or birth/death handling inside `receiver:mqtt` or
  `exporter:mqtt`. [mqtt-raw-receiver.md](mqtt-raw-receiver.md#non-goals)
  already forbids this; this document does not revisit that decision.
- A complete Sparkplug B "application" (dashboarding, historian, tag
  browsing). This document only covers the dataflow-engine-facing session
  lifecycle, observation, rebirth, and codec split needed to turn Sparkplug
  traffic into OTAP telemetry (or to relay it unmodified).
- Implementing pluggable byte/Arrow representations
  (#3452, #3875) or mixed-signal `SignalType` (#3876) themselves. This
  document specifies how a Sparkplug codec would use those mechanisms once
  they exist, and does not block on them for the earliest milestones (see
  [Staged milestones](#staged-milestones)).
- MQTT 3.1.1 support for the Sparkplug extension's own client connection.
  Sparkplug B requires MQTT 3.1.1 *or* 5 in principle, but this design
  targets MQTT 5 only for the extension's own CONNECT (Will properties,
  clean-start semantics), matching
  [mqtt-raw-exporter.md](mqtt-raw-exporter.md#mqtt5-only-implications-and-qos-2)'s
  existing MQTT5-only posture for outbound clients in this codebase.
- Persisting session/alias state across process restarts. Treated as a later,
  optional milestone (see [Staged milestones](#staged-milestones)); the
  baseline design rebuilds all session state from a fresh round of rebirths
  after a restart.

## Why Sparkplug must compose, not embed

[mqtt-raw-receiver.md](mqtt-raw-receiver.md) and
[mqtt-raw-exporter.md](mqtt-raw-exporter.md) are already-scoped, in-review
designs whose non-goals explicitly exclude Sparkplug. Re-opening either
document to add Sparkplug awareness would:

- Couple two independently useful components (a generic MQTT broker/client)
  to one specific payload convention, breaking the "raw passthrough" use case
  documented in the envelope contract (an operator bridging arbitrary MQTT
  traffic, Sparkplug or not, through OTLP).
- Force every consumer of `receiver:mqtt`, including non-Sparkplug
  deployments, to carry Sparkplug's protobuf dependency and its stateful
  alias/seq/session tracking, even when unused.
- Violate the codebase's own layering precedent: the envelope contract
  already states "[b]ody content parsing (JSON, CBOR, Sparkplug B,
  protobuf, etc.) ... a processor, not the receiver, is responsible for any
  content-aware decoding" (see
  [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md#scope)).

Composing instead of embedding means the Sparkplug extension:

- Never imports `rumqttd` or any raw-receiver/-exporter internal type.
- Can be deployed with `receiver:mqtt`/`exporter:mqtt` absent entirely (talking
  to a wholly external broker), with them present and reused (loopback to the
  same embedded broker), or with only one side present (Sparkplug-aware
  ingestion feeding a non-MQTT exporter, or a non-MQTT receiver feeding a
  Sparkplug-aware NCMD/DCMD command path).
- Is itself an ordinary MQTT client from the broker's point of view --
  Sparkplug does not require, and this design does not assume, any privileged
  broker-internal access.

## Component placement, URNs, and feature gates

| Component | Crate / module | URN | Primary metric set |
| --- | --- | --- | --- |
| Sparkplug host extension | `crates/contrib-extensions/src/sparkplug_host/` | `urn:otel:extension:sparkplug_host` (`extension:sparkplug_host`) | `extension.sparkplug_host` |
| Sparkplug decode processor | `crates/contrib-nodes/src/processors/sparkplug_decode_processor/` | `urn:otel:processor:sparkplug_decode` (`processor:sparkplug_decode`) | `processor.sparkplug_decode` |
| Sparkplug protobuf codec (library) | `crates/contrib-nodes/src/processors/sparkplug_decode_processor/proto/` (or a small shared `sparkplug-proto` crate if reused by both node kinds) | n/a (library, not a node) | n/a |

Both node kinds live in `contrib`, following the same "vendor/spec-specific
dependency behind a Cargo feature" convention already used for
`kafka_receiver`, `etw_receiver`, and (per its own draft)
`receiver:mqtt`/`exporter:mqtt`. Cargo features: `sparkplug-host-extension` and
`sparkplug-decode-processor`, neither part of any default feature set. Both
are first-party OTel-namespaced URNs despite living in `contrib`, per
[urns.md](../urns.md)'s "the namespace reflects ownership/standardization of
the node type, not the Rust crate" rule, and per
[AGENTS.md](../../AGENTS.md#component-naming-conventions)'s
`<component_kind>.<component_name>` metric-set convention.

The Sparkplug protobuf schema (`com.eclipse.tahu.protobuf.sparkplug_b`, the
`Payload`/`Metric`/`DataSet`/`Template` message set defined by the Sparkplug B
specification) is vendored or pulled from a `prost`-generated crate. Its
license (Eclipse Public License 2.0, matching the Eclipse Tahu reference
implementation) must clear a `cargo deny`/`deny.toml` pass before
implementation, mirroring the license-review step already called out in
[mqtt-raw-receiver.md](mqtt-raw-receiver.md#platform-and-security-concerns).

## Architecture overview

```text
                        +-------------------------------------+
                        |     extension:sparkplug_host          |
                        |  (Active, shared execution model)     |
                        |                                       |
                        |  - owns a real MQTT client connection  |
                        |    (CONNECT + Will, not a rumqttd-      |
                        |    internal Link)                     |
                        |  - publishes STATE/{host_id}           |
                        |  - subscribes spBv1.0/#, STATE/#       |
                        |  - tracks per-edge-node session state  |
                        |    (online, seq, bdSeq, alias table)   |
                        |  - issues NCMD/DCMD rebirth requests   |
                        |  - exposes SparkplugSessionView         |
                        |    capability (read-only, per node)    |
                        +-------------------+-------------------+
                                            | capability: SparkplugSessionView
                                            | capability: MqttBrokerEndpoint (consumed)
                                            v
+----------------------+   loopback or external TCP    +----------------------+
| receiver:mqtt / an     |<------------------------------>|  MQTT broker          |
| external broker's own  |                                |  (embedded rumqttd,   |
| listener                |                                |   or any external)    |
+-----------+------------+                                +----------------------+
            | OtapPdata (opaque body: raw Sparkplug protobuf bytes,
            | mqtt.topic, mqtt.qos, mqtt.retain -- per envelope contract)
            v
+----------------------------------------------------------------+
|  processor:sparkplug_decode                                    |
|  - PData-facing, stateless per call                             |
|  - parses topic namespace, decodes protobuf Payload             |
|  - binds SparkplugSessionView capability (read-only query)      |
|  - emits OTAP (mixed-signal where applicable) or a pluggable    |
|    "sparkplug-protobuf-bytes" representation for passthrough    |
+----------------------------------------------------------------+
```

Two deployment topologies are both first-class:

1. **Co-located.** `receiver:mqtt` embeds `rumqttd` as today's accepted draft
   specifies; the Sparkplug host extension dials the *same* broker's listener
   over loopback TCP as one more ordinary MQTT client. No new capability is
   required on the broker side for this topology beyond what is described in
   [Capability model](#capability-model-abstracting-broker-access-from-rumqttd).
2. **Decoupled.** The Sparkplug host extension (and, separately,
   `receiver:mqtt`/`exporter:mqtt`, if present at all) each connect
   independently to a wholly external broker (Mosquitto, EMQX, HiveMQ, a
   cloud MQTT service). This is the common real-world Sparkplug deployment
   shape and must work with zero code differences from topology 1 -- only
   configuration (a remote host:port instead of loopback) differs.

## Primary Host Application `STATE` lifecycle

A Sparkplug Primary Host Application announces its own liveness on a
dedicated topic, separate from the `spBv1.0/#` namespace used by edge nodes
and devices:

- **Topic:** `STATE/{host_id}`, where `host_id` is the operator-configured,
  stable identifier for this host application instance (config field
  `host_id`, required, no default -- a random or auto-generated host id would
  break edge-node configuration that references it, mirroring
  [mqtt-raw-exporter.md](mqtt-raw-exporter.md#session-and-client-ids)'s
  reasoning against inventing client identity automatically).
- **Birth:** on successful CONNECT, the extension publishes retained
  `STATE/{host_id}` = `ONLINE` (QoS 1, `retain = true`) so any edge node or
  device that subscribes afterward immediately observes the host's current
  state without waiting for a fresh publish.
- **Death:** the extension registers an MQTT Will message at CONNECT time:
  topic `STATE/{host_id}`, payload `OFFLINE`, QoS 1, `retain = true`. The
  broker publishes this automatically if the extension's connection drops
  without a clean DISCONNECT (crash, network partition, killed process) --
  this is the mechanism edge nodes rely on to detect an unreachable host and
  stop expecting inbound `NCMD`/`DCMD`.
- **Graceful shutdown:** on `NodeControlMsg`-equivalent
  `ExtensionControlMsg::Shutdown`, the extension explicitly publishes
  `STATE/{host_id}` = `OFFLINE` (retained) *before* disconnecting, then
  disconnects cleanly so the Will is not triggered redundantly. Explicit
  OFFLINE-before-disconnect and Will-on-crash are complementary, not
  duplicate: exactly one of them fires per session end, and both leave the
  same retained value behind.
- **Why this requires a real client connection, not a broker-internal
  link.** MQTT Will registration is a property of the CONNECT packet on a
  real client session; it does not exist for `rumqttd`'s internal
  `Broker::link` subscriber mechanism used by `receiver:mqtt`
  (see [mqtt-raw-receiver.md](mqtt-raw-receiver.md#internal-brokerlink-use)),
  which models an in-process subscriber, not a full client session. A
  correct Primary Host STATE implementation is therefore only achievable by
  giving this extension its own genuine MQTT CONNECT, independent of whether
  the broker is embedded or external. This is the central reason the
  Sparkplug host extension cannot reuse `receiver:mqtt`'s internal
  `LinkTx`/`LinkRx` path for its STATE responsibilities, even in the
  co-located topology.
- **Newer Sparkplug revisions.** Some Sparkplug spec revisions define a
  structured (JSON) STATE payload carrying a timestamp instead of the plain
  `ONLINE`/`OFFLINE` string. The extension's STATE payload format is a config
  enum (`state_payload_format: plain | timestamped_json`, default `plain`)
  so operators can match whichever revision their edge-node fleet expects;
  this document does not mandate one, since the wire format is an
  interoperability choice, not an architectural one.
- **Multiple Primary Hosts.** Sparkplug permits more than one Primary Host
  Application to observe the same namespace (for redundancy). This extension
  does not implement Primary Host election or failover coordination between
  multiple instances of itself; each configured instance is independently
  responsible for its own `host_id` and STATE lifecycle. Coordinating
  multiple redundant hosts is out of scope (see
  [Non-Goals](#non-goals)) and, if ever needed, is a separate concern layered
  on top, not a change to this document's STATE handling.

## Observing broker publications

The same client connection used for the STATE lifecycle subscribes to:

- `spBv1.0/#` -- every group, message type, edge node, and device in the
  Sparkplug namespace this host application observes (or a narrower
  `spBv1.0/{group_id}/#` per configured group, if `groups: [...]` is set,
  to bound subscription scope in multi-tenant deployments).
- `STATE/#` (or a narrower `STATE/{other_host_id}` set), only if this
  instance needs to observe *other* Primary Host Applications' liveness;
  optional, default off, since most deployments run exactly one Primary
  Host.

Observation is **read-only bookkeeping**, not decoding. For every inbound
message on `spBv1.0/{group_id}/{message_type}/{edge_node_id}[/{device_id}]`,
the extension:

1. Parses only the **topic** (group id, message type, edge node id, optional
   device id) and the **`seq`** and **`bdSeq`** fields, which requires
   decoding the Sparkplug protobuf envelope's fixed fields but *not* its
   metric list. This is a deliberately small, fast parse distinct from the
   full metric decode the codec performs later (see
   [Why Sparkplug is both protocol participant and codec](#why-sparkplug-is-both-protocol-participant-and-codec)).
2. Updates the session table entry for `(group_id, edge_node_id[,
   device_id])`: marks it online/offline, records the new `seq`, and (for
   `NBIRTH`/`DBIRTH`) captures the full alias table and the announced
   `bdSeq`. See [Stateful session model](#stateful-alias-seq-bdseq-and-session-model).
3. On `NDEATH`, matches the message's `bdSeq` metric against the session's
   last-recorded `bdSeq` from its `NBIRTH`. A match means this death
   corresponds to the current session (mark it, and every device under it,
   offline/stale); a mismatch is logged as an anomaly (a delayed or
   out-of-order `NDEATH` from a prior session incarnation) and does not
   change current session state.
4. Detects `seq` discontinuity: if the received `seq` is not
   `(last_seq + 1) mod 256` (and this is not itself an `NBIRTH`, which
   legitimately resets the counter), the session is marked
   `seq_gap_detected` and a rebirth request is scheduled (see next section).

This bookkeeping never touches the metric payload's *values*; it only reads
the small set of protocol-level fields listed above. Full metric decode is
strictly a separate, on-demand step performed by the codec, not by this
extension.

## `NCMD`/`DCMD` and rebirth

- **Node rebirth (`NCMD`):** published to
  `spBv1.0/{group_id}/NCMD/{edge_node_id}` as a Sparkplug `Payload` containing
  one metric, conventionally named `Node Control/Rebirth`, datatype
  `Boolean`, value `true`. This is the standardized mechanism by which a host
  application asks an edge node to republish `NBIRTH` (and, as a consequence
  of the edge node's own behavior, every attached device's `DBIRTH`).
- **Device commands (`DCMD`):** published to
  `spBv1.0/{group_id}/DCMD/{edge_node_id}/{device_id}` for arbitrary
  device-addressed commands (writes, actions). The Sparkplug specification
  does not define a standardized per-device rebirth metric analogous to
  `Node Control/Rebirth`; a device's data is re-established by its owning
  edge node's node-level rebirth cycle, which causes that edge node to
  reissue every attached device's `DBIRTH`. This document therefore treats
  rebirth as **node-scoped only**: a detected device-level anomaly (for
  example, a `DDATA` referencing an unknown device alias, with the parent
  node otherwise healthy) triggers a node-level `NCMD` rebirth request
  targeting that device's owning edge node, not a device-targeted `DCMD`.
  `DCMD` publication capability is still exposed (for future
  command-and-control use cases outside rebirth), but no rebirth logic uses
  it directly.
- **Rebirth triggers.** A rebirth request is scheduled when any of the
  following is observed for a session already marked online:
  - `seq_gap_detected` (see previous section).
  - A `NDATA`/`DDATA` message references a metric alias that is not present
    in the session's current alias table (an "unknown alias" condition,
    which by definition means metric values cannot be decoded correctly).
  - The codec (via the capability described below) reports a decode failure
    classified as "requires rebirth" (as opposed to "malformed, drop and
    count" -- see [Failure handling](#failure-handling)).
- **Rebirth delivery is at-least-once but not exactly-once.** `NCMD` is
  published at QoS 1: the broker's PUBACK only confirms broker acceptance,
  never that the target edge node processed the command (ordinary MQTT
  PUBACK semantics, the same asymmetry already documented for
  [receiver:mqtt's QoS boundary](mqtt-raw-receiver.md#qos-semantics-and-the-ack-boundary)).
  The extension therefore retries an outstanding rebirth request with bounded
  exponential backoff (reusing the same `initial_interval`/`max_interval`/
  `multiplier`/`max_attempts` field names as
  [mqtt-raw-exporter.md](mqtt-raw-exporter.md#reconnect-and-backoff)'s
  reconnect block, but scoped to one rebirth request) until either a fresh
  `NBIRTH` with a consistent alias table is observed (success; cancel
  retries) or `max_attempts` is exhausted (give up; emit a
  `rebirth.exhausted` event and leave the session `stale` until the next
  externally-triggered `NBIRTH`).
- **Rebirth storms are bounded.** A per-edge-node minimum interval between
  self-triggered rebirth requests (`min_rebirth_interval`, default 30s)
  prevents a persistently inconsistent edge node (or a decode bug) from
  flooding `NCMD` traffic. A request suppressed by this interval is counted,
  not silently dropped (see [Telemetry](#telemetry)).

## Capability model: abstracting broker access from `rumqttd`

The Sparkplug host extension must never depend on `rumqttd` directly. Two
capabilities are introduced, both defined as engine-core capability traits
(per [extension-requirements.md](../extension-requirements.md#non-goals),
capability interfaces are defined and maintained in engine core, not by
individual extensions) under a new `capability::mqtt` domain in
`crates/engine/src/capability/`:

### `MqttBrokerEndpoint` (discovery/coordination only)

```rust
#[capability(name = "mqtt_broker_endpoint")]
trait MqttBrokerEndpoint {
    /// Resolves the network target for a real MQTT CONNECT-based client
    /// session against this broker. Does not itself open a connection.
    async fn connect_target(&self) -> Result<MqttConnectTarget, CapabilityError>;
}
```

`MqttConnectTarget` is a plain, engine-owned struct (`{ addr: SocketAddr,
tls: Option<TlsClientConfig> }`) -- no `rumqttd` type appears anywhere in
this trait's signature. Two providers implement it:

- **`extension:embedded_mqtt_broker`** (co-located topology): owns the
  process-wide `rumqttd` `Broker` singleton and its listener configuration.
  `connect_target()` resolves once the listener socket is confirmed bound,
  retrying internally with backoff if called before that point (see
  [Ownership and lifecycle ordering](#ownership-and-lifecycle-ordering-between-extension-and-receiver)
  for why this internal retry, not framework-level ordering, is what makes
  this safe). This is a **new, small extension**, not `receiver:mqtt`
  itself -- see the ownership-relocation discussion below.
- **`extension:external_mqtt_broker_target`** (decoupled topology): a
  trivial, config-only provider that returns a statically configured
  remote `addr`/`tls` pair. No embedded broker involved; exists purely so
  the Sparkplug host extension's code path is identical in both topologies.

A deployment that only ever uses an external broker does not need to bind
this capability at all -- the Sparkplug host extension's own config can
carry `broker.addr`/`broker.tls` directly, exactly like
[mqtt-raw-exporter.md](mqtt-raw-exporter.md#endpoints-tls-and-auth)'s
endpoint config. The capability exists specifically for the co-located case,
where the loopback address is owned by another component's runtime state
(the bound listener) rather than being known statically at config time.

### `SparkplugSessionView` (exposed by this extension, consumed by the codec)

```rust
#[capability(name = "sparkplug_session_view")]
trait SparkplugSessionView {
    /// Read-only snapshot of one edge node/device session, as of the last
    /// birth/death/data message observed. `None` if never seen.
    fn session(&self, key: &SparkplugSessionKey) -> Option<SparkplugSessionSnapshot>;
}
```

`SparkplugSessionSnapshot` carries the fields the codec needs to decode a
`NDATA`/`DDATA` message correctly: the current alias table (numeric alias ->
metric name/datatype), `online`, `last_seq`, and `bdSeq`. This is a
**read-only, point-in-time query capability**, not a stream: the codec calls
`session()` once per decoded message (see next section), and the extension's
own background bookkeeping (previous section) is the only writer. The
capability is registered as **shared only** (the extension's underlying
session table is `Send + Sync`, guarded internally, because the codec runs as
an ordinary pdata-processing node that may be replicated per core while the
session-tracking extension instance is not necessarily co-located on every
core -- see the next section for the resulting cross-core cost this implies
and why it is accepted).

## Ownership and lifecycle ordering between extension and receiver

Three ordering constraints interact here, and they do not compose for free:

1. **"Extensions start first, shut down last"** is an engine-wide guarantee
   (see
   [extension-system-architecture.md](../extension-system-architecture.md#key-design-decisions),
   item 1): all extensions are spawned before any data-path node, and
   drained only after every data-path node has drained.
2. **`receiver:mqtt` is a node, not an extension.** Its embedded `rumqttd`
   `Broker` singleton, as specified in
   [mqtt-raw-receiver.md](mqtt-raw-receiver.md#proposed-architecture), is
   created inside the receiver's `create()`/`start()` path -- which, per
   constraint 1, runs *after* every extension (including the Sparkplug host
   extension) has already started.
3. **The Sparkplug host extension needs a bound broker listener before its
   first CONNECT can succeed** (co-located topology only).

Constraints 1 and 2 combine to an ordering inversion: if the embedded broker
stays owned by `receiver:mqtt` (a node), the Sparkplug host extension (an
extension) is guaranteed to start *before* that broker exists. This is not a
timing race to paper over with a longer timeout; it is a structural ordering
guarantee running in the wrong direction for this composition.

**Recommended resolution:** relocate the embedded-broker singleton's
ownership out of `receiver:mqtt` and into a new, small, capability-providing
extension, `extension:embedded_mqtt_broker` (introduced above). Under this
arrangement:

- `extension:embedded_mqtt_broker` starts as part of "extensions start
  first," creates the `rumqttd` `Broker`, and binds its listener(s).
- `receiver:mqtt` becomes a **capability consumer**: instead of owning the
  `OnceLock`-guarded singleton itself, it binds `MqttBrokerEndpoint` (or,
  more directly, an internal-link-shaped capability carrying the already
  documented `LinkTx`/`LinkRx` semantics -- the exact shape of what
  `receiver:mqtt` binds is an implementation detail of relocating that
  existing draft, not something this document redefines) and starts after
  extensions, per the engine's existing node-after-extension ordering. This
  requires **no new engine ordering primitive**: it only requires that the
  broker in fact live behind an extension, which today's capability system
  already supports for any node (nodes-consuming-extension-capabilities is
  the system's whole purpose).
- The Sparkplug host extension, also an extension, starts concurrently with
  `extension:embedded_mqtt_broker` (the engine does not order two
  extensions relative to each other -- see
  [Unresolved engine changes](#unresolved-engine-changes) item 2). Its
  `connect_target()` call against `MqttBrokerEndpoint` must therefore
  tolerate "listener not yet bound" as an ordinary, retryable condition,
  using the same bounded-backoff posture
  [mqtt-raw-exporter.md](mqtt-raw-exporter.md#reconnect-and-backoff) already
  applies to reconnects, rather than assuming any start-order guarantee
  between sibling extensions.

This relocation is a **scoped change to the accepted raw-receiver design**,
not a decision this document can make unilaterally -- it must be coordinated
with [mqtt-raw-receiver.md](mqtt-raw-receiver.md)'s owners before either
document's implementation begins. It is listed again, explicitly, under
[Unresolved engine changes](#unresolved-engine-changes).

**Shutdown ordering** follows the same relocation: because extensions shut
down only after every data-path node has drained (constraint 1, in
reverse), `extension:embedded_mqtt_broker` (and, transitively, the
loopback-connected Sparkplug host extension) remain available for the
entire time `receiver:mqtt` is draining -- this is the *correct* direction
for shutdown, unlike startup, and is one more reason the relocation is
beneficial rather than merely a workaround: it fixes both ends of the
lifecycle, not just the startup half.

## Stateful alias, seq, bdSeq, and session model

Session state is keyed by `SparkplugSessionKey { group_id, edge_node_id,
device_id: Option<String> }` (device sessions nest under their edge node's
key). Per key, the extension retains:

| Field | Source | Purpose |
| --- | --- | --- |
| `online: bool` | `NBIRTH`/`DBIRTH` (true) vs. matching `NDEATH`/`DDEATH` (false) | Gate whether decode should proceed or a message should be treated as stale. |
| `alias_table: HashMap<u64, (String, Datatype)>` | Metric definitions in the most recent `NBIRTH`/`DBIRTH` | Resolve numeric aliases carried by subsequent `NDATA`/`DDATA` back to metric name and declared datatype. |
| `last_seq: u8` | Every message's Sparkplug envelope `seq` field | Detect gaps: next expected is `(last_seq + 1) mod 256`. |
| `bdSeq: u64` | The `bdSeq` metric carried in `NBIRTH` (and matched against the paired `NDEATH`'s `bdSeq`) | Identify which birth/death cycle (session incarnation) current data belongs to; a `NDEATH` whose `bdSeq` does not match the currently tracked value is a stale/reordered signal, not a live state transition. |
| `last_activity: Instant` | Any message for this key | Drives bounded LRU eviction (below). |

**Bounded eviction.** The session table is bounded by `max_tracked_sessions`
(a required, finite config field; no unbounded default, consistent with the
project's bounded-resources requirement already enforced for
`receiver:mqtt`'s own limits table). When inserting a new key would exceed
the bound, the least-recently-active *existing* key is evicted. Eviction of
a session currently `online = true` is **not** silent: it is logged as a
`session.evicted_while_online` event and the key is recorded (briefly, in a
small bounded "recently evicted" set) so that a subsequent message for that
key is recognized as "unknown due to eviction, not a new deployment" and
immediately triggers a rebirth request rather than being decoded against a
stale or absent alias table. This distinguishes eviction-under-pressure
(recoverable via rebirth) from a first-ever sighting of a new edge node
(no rebirth needed -- simply wait for its first natural `NBIRTH`).

**Per-session alias table size is also bounded** (`max_aliases_per_session`),
independent of the session-count bound, because a single malicious or
malfunctioning `NBIRTH` announcing an unbounded metric list must not be able
to exhaust memory through one session alone. Exceeding the bound is treated
as a malformed birth: the session is marked `invalid`, not partially
populated, and a rebirth is requested (a half-applied alias table is worse
than none, since it would silently misdecode some metrics and correctly
reject others with no visible pattern).

## Why Sparkplug is both protocol participant and codec

Sparkplug's two responsibilities do not fit one component kind in this
engine's architecture, and forcing them into one would violate an existing
invariant:

- **Protocol participant:** maintaining `STATE`, tracking `seq`/`bdSeq`/alias
  state across an unbounded stream of MQTT messages over the *lifetime of a
  connection*, and issuing rebirth commands, is inherently a stateful,
  long-lived, network-facing responsibility. It needs its own MQTT client
  session (CONNECT, Will, retained publish), background retry timers for
  rebirth, and bounded eviction sweeps -- exactly the shape of an **Active
  extension** (`start()` owning an event loop, background tasks, capability
  exposure), not a per-message pipeline node.
- **Codec:** turning one Sparkplug protobuf `Payload` into OTAP records
  (or into a pluggable byte representation) is, per message, a **pure,
  content-aware transformation** -- decode topic, decode protobuf, resolve
  aliases, emit records. This is exactly what a processor (or a PDataCodec,
  per [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452)) is
  for: pdata in, pdata out, replicated per core, no persistent session of
  its own.

The engine already enforces the boundary between these two shapes: "\[e\]
xtensions are PData-free -- they never process PData, only control messages"
(see
[extension/mod.rs](../../crates/engine/src/extension/mod.rs)). A single
component cannot simultaneously be the thing that holds the MQTT session and
the thing that touches `OtapPdata` -- the codec's decode step, which
necessarily consumes and produces pdata, cannot live inside the extension
without violating that boundary, no matter how convenient it would be to
have one component do both.

This is why the design splits into an extension (protocol participant:
STATE, observation bookkeeping, rebirth) exposing a small, read-only,
per-message-queryable capability (`SparkplugSessionView`), consumed by a
stateless-per-call processor (codec: topic/protobuf decode, alias
resolution using the queried snapshot, OTAP emission). The extension is the
single writer of session state; the codec is a pure reader. Neither
component could do the other's job without breaking either the PData-free
extension invariant or the stateful-session requirement.

## Pluggable byte representation vs. mixed-signal OTAP materialization

A decoded Sparkplug `NBIRTH`/`DBIRTH`/`NDATA`/`DDATA` message routinely
contains **more than one kind of thing** in a single wire payload: ordinary
numeric telemetry metrics, alongside bookkeeping metrics like `bdSeq` and
command metrics like `Node Control/Rebirth`, all sharing one `group_id`/
`edge_node_id`/`device_id` identity (which maps naturally to resource
attributes) and one birth/session scope. This is precisely the situation
[#3876](https://github.com/open-telemetry/otel-arrow/issues/3876) (mixed-signal
PData) was filed to support: "the primary requirement is that signals share
their resource and scope tables," which Sparkplug's `group_id`/`edge_node_id`/
`device_id` identity already satisfies.

The codec supports two output modes, chosen lazily and per consumer, mirroring
[#3452](https://github.com/open-telemetry/otel-arrow/issues/3452)'s "\[c\]
onversion should occur only when required" principle:

1. **Passthrough / pluggable byte representation.** If no downstream
   consumer needs decoded values (a router, a retry buffer, or a relay
   forwarding to another Sparkplug-aware system), the codec need not run at
   all -- the raw protobuf bytes continue to flow as the opaque `body` the
   raw envelope contract already defines. Once
   [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452) lands,
   registering a `sparkplug-protobuf-bytes` `PDataEncoding` (with `decode`/
   `encode` functions backed by this same codec logic) lets pipelines
   declare the representation explicitly and lets compatible downstream
   exporters (including `exporter:mqtt`, republishing byte-for-byte) consume
   it without ever materializing OTAP. This is the mechanism behind
   [Raw passthrough to the MQTT exporter](#raw-passthrough-to-the-mqtt-exporter)
   below.
2. **Mixed-signal OTAP materialization.** When a consumer needs actual
   values (a metrics backend, an alerting processor), the codec decodes the
   protobuf `Payload` into OTAP records, using the `SparkplugSessionView`
   snapshot to resolve aliases, and emits every signal type present in that
   one message (metrics for ordinary telemetry; a control/event
   representation for command-shaped metrics such as `Node Control/Rebirth`)
   sharing one resource (group/edge-node/device identity) and one scope, per
   the mixed-signal proposal in
   [#3876](https://github.com/open-telemetry/otel-arrow/issues/3876). Until
   that proposal lands, the interim baseline (see
   [Staged milestones](#staged-milestones)) emits ordinary OTLP metrics only,
   dropping (and counting) command/bookkeeping metrics that have no
   sensible metrics-signal representation, and documents this as a known,
   temporary gap rather than silently misrepresenting a command metric as a
   telemetry value.
3. A Sparkplug-specific Arrow batch representation (a `flat-sparkplug-batch`
   analogous to the `NetTraceV6` example in
   [#3875](https://github.com/open-telemetry/otel-arrow/issues/3875)) is not
   proposed here; Sparkplug's payload is already small and message-oriented
   compared to `NetTraceV6`'s dense multi-table format, so the case for a
   dedicated Arrow representation (versus per-message OTAP or protobuf-byte
   passthrough) is not made in this document and is left for a future
   proposal if a measured need appears.

Neither mode is a prerequisite for the other: passthrough works today (it is
just "don't invoke the codec"), and mixed-signal materialization can start
as ordinary metrics-only OTAP now and be revised once
[#3876](https://github.com/open-telemetry/otel-arrow/issues/3876) is
implemented, without changing the extension side of this design at all.

## Retry, durable-buffer, and order constraints

- **Per-key ordering is load-bearing, not incidental.** Sparkplug decode
  correctness depends on observing `NBIRTH` before any `NDATA` referencing
  its aliases, and on `seq` being contiguous per
  `(group_id, edge_node_id[, device_id])` key. Any component sitting
  between MQTT ingestion and the Sparkplug decode path -- a durable buffer,
  a `processor:retry`, a content-based router -- **must preserve FIFO order
  within that key** or the session model in this document will observe
  spurious `seq` gaps and trigger unnecessary rebirths. This is a
  documented constraint on pipeline topology, not something the codec can
  detect and correct after the fact: a reordering component that is safe
  for opaque MQTT relay (per the envelope contract's batching rules) is
  *not* automatically safe upstream of Sparkplug decode.
  - Components that are safe by construction: anything that only splits or
    merges batches without reordering within a partition key
    (`processor:batch`), or that routes deterministically by the same key
    this document uses (topic-prefix-keyed routing that never interleaves
    two edge nodes' messages relative to each other -- reordering *across*
    keys is harmless, since sessions are independent).
  - Components that are unsafe without additional keying: any retry or
    buffering stage whose replay order is not guaranteed FIFO per key
    (for example, a naive parallel-fan-out retry pool). Using such a
    component upstream of `processor:sparkplug_decode` requires
    per-key-affinitized retry (keying retries by the same
    `group_id`/`edge_node_id` this document uses for session state), which
    is a deployment-topology responsibility, not something this extension
    can enforce at runtime beyond detecting the resulting `seq` gaps and
    requesting rebirth as a (lossy, best-effort) recovery path.
- **Rebirth requests need their own bounded retry**, described in
  [`NCMD`/`DCMD` and rebirth](#ncmddcmd-and-rebirth) above: at-least-once
  delivery of the rebirth *command* (broker-confirmed via QoS 1 PUBACK) is
  not the same as confirmation the edge node *acted* on it, so the extension
  retries until a fresh, consistent `NBIRTH` is observed, not until PUBACK.
- **This design inherits, and does not fix, `receiver:mqtt`'s existing
  durability caveats.** If the co-located embedded broker silently evicts
  backlog under memory pressure (see
  [mqtt-raw-receiver.md](mqtt-raw-receiver.md#commit-log-retention-and-silent-data-loss)),
  the Sparkplug session model has no way to distinguish "edge node stopped
  publishing" from "messages were evicted before this extension's
  subscription observed them" -- both present as a `seq` gap on the next
  observed message, and both are handled identically (rebirth request).
  This is an accepted, already-documented risk of the co-located topology,
  not a new gap introduced by this document.
- **Durable buffering does not need to be Sparkplug-aware for the
  passthrough mode.** Since passthrough forwards opaque bytes keyed by MQTT
  topic (which already encodes `group_id`/`edge_node_id`/`device_id`), any
  buffer that preserves per-topic order is sufficient for a Sparkplug-aware
  *relay* even without running this extension at all -- session tracking is
  only required once a consumer needs decoded values or rebirth automation.

## Raw passthrough to the MQTT exporter

A pipeline that only needs to *relay* Sparkplug traffic (ingest via
`receiver:mqtt`, apply generic processors that do not need decoded values,
republish via `exporter:mqtt`) requires **no Sparkplug awareness at all**:
the envelope contract already treats the payload as opaque bytes end to end,
and `exporter:mqtt` already republishes `body` verbatim per its own
validation rules (see
[mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md#exporter-side-validation-logrecord---publish)).
This document must not regress that path, and does not: nothing about the
Sparkplug host extension or the decode processor is required for a
Sparkplug-oblivious MQTT-to-MQTT relay to keep working exactly as the raw
draft set already specifies.

The Sparkplug extension and processor are additive: they matter only when a
deployment needs one or more of (a) Primary Host `STATE` presence, (b)
rebirth automation, or (c) materialized OTAP values from Sparkplug metrics.
A relay-only deployment binds none of them.

When a deployment *does* run the decode processor but wants to preserve the
ability to relay unmodified bytes downstream (for example, decode for local
alerting while also mirroring raw Sparkplug traffic to a second, external
Sparkplug-consuming system), the pluggable-byte-representation path (mode 1
in the previous section) is the mechanism: the processor need not decode at
all for the branch feeding `exporter:mqtt`, and the codec's `encode()` side
(once [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452)
lands) reproduces the original protobuf bytes exactly for any branch that
did decode and needs to re-encode for onward Sparkplug delivery, without the
lossy, single-signal-type flattening OTAP materialization would otherwise
imply.

## Security and auth boundaries

- **The Sparkplug host extension holds its own credentials, distinct from
  any edge node's identity.** Auth configuration (username/password, mTLS
  client cert, or a bound `BearerTokenProvider`/`BearerTokenAuthorizer`
  capability) follows the same shape as
  [mqtt-raw-exporter.md](mqtt-raw-exporter.md#endpoints-tls-and-auth)'s
  outbound-client auth model; it is never derived from anything observed on
  the wire.
- **Least-privilege topic ACLs.** The extension's broker-side ACL should
  grant only: SUBSCRIBE `spBv1.0/#` (or the narrower configured group set)
  and `STATE/#` (if observing other hosts); PUBLISH `STATE/{host_id}` and
  `spBv1.0/{group_id}/NCMD/{edge_node_id}` (and `DCMD/.../{device_id}` if
  device commands beyond rebirth are used). It must **not** be granted
  PUBLISH on `NBIRTH`/`NDATA`/`DBIRTH`/`DDATA` topics -- a compromised or
  misconfigured host application must not be able to impersonate an edge
  node's data publications. This is a defense-in-depth requirement on the
  broker's own ACL configuration (external or `receiver:mqtt`'s, per
  [mqtt-raw-receiver.md](mqtt-raw-receiver.md#acl)), not something this
  extension can enforce from the client side alone, and must be documented
  as an operator responsibility.
- **No trust derived from `mqtt.*`-sourced attributes.** Mirroring the
  envelope contract's
  [trust and security policy for exporter replay](mqtt-raw-envelope-contract.md#trust-and-security-policy-for-exporter-replay),
  the decode processor never uses topic-derived or payload-derived values to
  select broker endpoints, credentials, or transport security -- those
  remain the extension's/processor's own configuration.
- **Payload bytes are never executed or interpreted beyond the documented
  protobuf schema.** Decode failures (malformed protobuf, unknown datatype
  tags) are rejected and counted, never used to drive control flow beyond
  this document's rebirth-trigger rules.
- **Rebirth requests are rate-bounded** (see
  [`NCMD`/`DCMD` and rebirth](#ncmddcmd-and-rebirth)'s `min_rebirth_interval`)
  specifically to prevent a compromised or malfunctioning decode path from
  being used to flood `NCMD` traffic at edge nodes, which is itself a
  potential denial-of-service vector against field devices with limited
  processing capacity.

## Telemetry

Two independent metric sets, matching the two-component split:

- `extension.sparkplug_host`: `sessions_tracked` (gauge), `rebirth_requested`
  (counter, labeled by outcome: sent / suppressed_by_interval / exhausted),
  `seq_gap_detected` (counter), `bdseq_mismatch` (counter),
  `session_evicted_while_online` (counter), `state_publish_failure`
  (counter, treated as a health-critical signal per
  [Failure handling](#failure-handling)), `connection_status` (up/down
  gauge).
- `processor.sparkplug_decode`: `decode_success`/`decode_error` (counters,
  labeled by error class), `unknown_alias` (counter), `metrics_materialized`
  (counter, labeled by OTel signal type once mixed-signal emission lands).

Events: `host.state_transition` (online/offline, with cause: startup /
graceful_shutdown / will_triggered), `session.rebirth_requested`,
`session.rebirth_exhausted`, `session.evicted_while_online`. No payload
content (metric names or values) appears in telemetry by default, consistent
with
[security-privacy-guide.md](../telemetry/security-privacy-guide.md) and the
same principle already stated for the raw receiver/exporter/envelope
contract.

## Failure handling

| Condition | Classification | Behavior |
| --- | --- | --- |
| Broker connection lost (co-located or external) | Transient | Reconnect with bounded backoff (mirrors [mqtt-raw-exporter.md](mqtt-raw-exporter.md#reconnect-and-backoff)); re-publish retained `STATE` = `ONLINE` immediately upon reconnect (the broker's own retained copy from before disconnect may have been replaced by the Will's `OFFLINE` in the interim). |
| `STATE` publish fails (PUBACK error or send failure) | Health-critical | Not folded into ordinary per-message Nack accounting: a failed STATE publish means every edge node in the namespace may believe this host is unreachable even though the process is running. Retried immediately and surfaced as a dedicated lifecycle event/alarm, not a silent counter increment. |
| Malformed Sparkplug protobuf (decode processor) | Permanent | Drop and count; never partially decode. |
| Unknown metric alias referenced by `NDATA`/`DDATA` | Requires rebirth | Do not guess; drop the offending metric (not the whole message, since other metrics in the same payload may resolve correctly), count it, and schedule a rebirth request per [`NCMD`/`DCMD` and rebirth](#ncmddcmd-and-rebirth). |
| `seq` gap | Requires rebirth | Same as above; does not by itself invalidate already-resolved aliases, only casts doubt on completeness of the data stream since the gap. |
| Session table at `max_tracked_sessions` | Bounded eviction | Evict least-recently-active; see [Stateful session model](#stateful-alias-seq-bdseq-and-session-model) for the online-eviction special case. |
| Rebirth request exhausted (`max_attempts`) | Give up, do not loop forever | Mark session `stale`; emit `session.rebirth_exhausted`; resume normal tracking if the edge node eventually publishes an unsolicited `NBIRTH` on its own. |
| `MqttBrokerEndpoint::connect_target()` fails repeatedly (co-located topology, broker never becomes ready) | Startup health failure | Surface as an extension health event distinct from steady-state reconnects, so operators can distinguish "misconfigured composition" (broker extension never starts) from "broker restarted." |

## Configuration shape

```yaml
extensions:
  sparkplug_main:
    type: extension:sparkplug_host
    config:
      host_id: scada-primary-1
      state_payload_format: plain # or timestamped_json
      broker:
        # Either a capability binding (co-located topology) ...
        capability: embedded_broker_main
        # ... or direct endpoint config (decoupled topology):
        # addr: "mqtt.example.com:8883"
        # tls: { ... }
      auth:
        mode: basic
        username: sparkplug-host
        password_env: SPARKPLUG_HOST_PASSWORD
      groups: [] # empty = subscribe spBv1.0/# unfiltered
      observe_other_hosts: false
      session:
        max_tracked_sessions: 10000
        max_aliases_per_session: 4096
      rebirth:
        min_rebirth_interval: 30s
        initial_interval: 5s
        max_interval: 60s
        multiplier: 2.0
        max_attempts: 10

  embedded_broker_main:
    type: extension:embedded_mqtt_broker
    config:
      # rumqttd listener/router config, relocated here from receiver:mqtt's
      # own config surface per "Ownership and lifecycle ordering" above.
      listeners: [...]
      router: {...}

nodes:
  mqtt_recv:
    type: receiver:mqtt
    capabilities:
      mqtt_broker_link: embedded_broker_main
    config:
      topics:
        subscribe: ["spBv1.0/#"]

  sparkplug_decode:
    type: processor:sparkplug_decode
    capabilities:
      sparkplug_session_view: sparkplug_main
    config:
      emit_command_metrics: false # v1: drop Node Control/* metrics, see Staged milestones
```

### Validation rules

- `host_id` is required and non-empty; there is no generated default (an
  auto-generated host id would silently change on every restart, breaking
  edge-node-side allowlists that may reference it).
- Exactly one of `broker.capability` or `broker.addr` must be set, never
  both and never neither.
- `max_tracked_sessions` and `max_aliases_per_session` are required, finite,
  positive integers; there is no "unbounded" sentinel, consistent with the
  project's bounded-resources requirement already enforced for
  `receiver:mqtt`'s own limits.
- `rebirth.max_attempts` must be a finite positive integer (unlike
  `exporter:mqtt`'s connection-level `max_elapsed_time: null` default,
  which intentionally retries forever -- a single rebirth request must not
  retry forever, since an edge node that never rebirths needs an operator
  to notice, not an unbounded background loop).

## Tests

### Unit tests

- Alias table resolution: known alias resolves; unknown alias is dropped,
  counted, and schedules rebirth.
- `seq` wraparound: `255 -> 0` is accepted as contiguous; any other
  discontinuity is flagged.
- `bdSeq` matching: an `NDEATH` whose `bdSeq` matches the tracked value
  transitions the session offline; a mismatched `bdSeq` does not.
- Bounded eviction: inserting past `max_tracked_sessions` evicts the
  least-recently-active session; evicting an online session marks it for
  rebirth-on-next-sighting rather than silent re-treatment as new.
- STATE payload construction for both `plain` and `timestamped_json` formats.
- Rebirth backoff/retry state machine: cancels on a consistent fresh
  `NBIRTH`; gives up and emits `rebirth_exhausted` after `max_attempts`.

### Integration tests

- End-to-end against an embedded test broker (the same kind of harness
  `receiver:mqtt`'s own test plan already requires): publish a synthetic
  `NBIRTH`, then `NDATA` referencing its aliases; confirm decoded OTAP
  values. Then publish `NDATA` with an out-of-range `seq`; confirm a
  rebirth `NCMD` is published and the session is marked pending.
- STATE lifecycle: confirm `STATE/{host_id}` = `ONLINE` retained publish on
  connect, and `OFFLINE` (via Will) on an abrupt disconnect simulated by
  killing the extension's connection without a clean DISCONNECT.
- Co-located topology ordering: start the pipeline with
  `extension:embedded_mqtt_broker` intentionally delayed (test-only
  injection point) and confirm the Sparkplug host extension's
  `connect_target()` retries rather than failing permanently.

### Interop tests

- Validate against a reference Sparkplug edge-node simulator (for example,
  the Eclipse Tahu Sparkplug test/reference implementations) for topic
  namespace, protobuf schema compatibility, and rebirth response handling,
  the same "validate against real, not merely inferred, behavior" posture
  [mqtt-raw-receiver.md](mqtt-raw-receiver.md#verified-rumqttd-0200-blockers-gate-implementation)
  already establishes for this draft set.

### Performance tests

- Bounded memory under a configured `max_tracked_sessions` with sustained
  churn (sessions constantly created and evicted) to confirm the eviction
  bound holds under load, not just under a single-session unit test.

## Staged milestones

1. **M0 -- Spike and engine coordination.** Validate the ownership
   relocation described in
   [Ownership and lifecycle ordering](#ownership-and-lifecycle-ordering-between-extension-and-receiver)
   against the actual, current state of `receiver:mqtt`'s implementation (or
   its still-gated draft), and confirm whether `rumqttd`'s real client
   CONNECT path (as opposed to `Broker::link`) supports Will registration
   for the co-located topology. Coordinate scope with
   [mqtt-raw-receiver.md](mqtt-raw-receiver.md)'s owners before either
   proceeds.
2. **M1 -- STATE lifecycle and observation only, no decode.** Ship
   `extension:sparkplug_host` with STATE publish/Will, `spBv1.0/#`
   observation, session bookkeeping (`seq`/`bdSeq`/alias-table tracking),
   and rebirth requests, but no decode processor yet -- useful on its own
   for operators who need correct Primary Host presence and rebirth
   automation while still relaying raw bytes downstream (see
   [Raw passthrough](#raw-passthrough-to-the-mqtt-exporter)).
3. **M2 -- Decode processor, metrics-only OTAP.** Ship
   `processor:sparkplug_decode` consuming `SparkplugSessionView`, emitting
   ordinary OTLP metrics for numeric telemetry and dropping (with a counted,
   documented gap) command/bookkeeping metrics that have no sensible
   metrics-signal representation yet.
4. **M3 -- Pluggable byte representation integration.** Once
   [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452) lands,
   register `sparkplug-protobuf-bytes` as a `PDataEncoding` backed by this
   codec, enabling true decode-on-demand passthrough instead of "just don't
   run the processor."
5. **M4 -- Mixed-signal materialization.** Once
   [#3876](https://github.com/open-telemetry/otel-arrow/issues/3876) lands,
   revise the codec to emit every signal type present in one Sparkplug
   message (metrics plus command/event representations) sharing one
   resource/scope, replacing M2's metrics-only interim behavior.
6. **M5 (optional) -- Cross-restart session persistence.** Investigate
   persisting session/alias state so a process restart does not require a
   full rebirth sweep of every tracked edge node; depends on a
   not-yet-existing extension-scoped durable-state capability (see
   [Unresolved engine changes](#unresolved-engine-changes)).

## Unresolved engine changes

1. **Node-owned resources cannot be exposed as capabilities today; only
   extensions register capabilities.** `ExtensionCapabilities` is tied to
   `ExtensionFactory` (see
   [capability/mod.rs](../../crates/engine/src/capability/mod.rs)), not to
   any receiver/processor/exporter factory. This document's
   [ownership-relocation](#ownership-and-lifecycle-ordering-between-extension-and-receiver)
   works around this by moving the embedded broker singleton out of
   `receiver:mqtt` into a new extension, rather than asking the engine to
   let nodes provide capabilities -- but that relocation is itself a scoped
   change to an already-accepted design and needs explicit coordination,
   not silent adoption by this document.
2. **No engine-level ordering primitive exists between two extensions.**
   "Extensions start first" orders extensions before nodes as a group; it
   does not order `extension:sparkplug_host` relative to
   `extension:embedded_mqtt_broker`. This design compensates with
   application-level retry/backoff on the capability call
   (`connect_target()`), not a framework guarantee. A future
   extension-dependency-ordering primitive (if ever added) could simplify
   this, but is not assumed here.
3. **Pluggable PData byte/Arrow representations and mixed-signal
   `SignalType` do not exist yet.** M3 and M4 above are written against the
   *proposed* interfaces in
   [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452),
   [#3875](https://github.com/open-telemetry/otel-arrow/issues/3875), and
   [#3876](https://github.com/open-telemetry/otel-arrow/issues/3876), all
   still open at time of writing, and will need revision once those land
   with concrete APIs.
4. **No shared "generic MQTT client" capability or crate exists yet.** This
   document and [mqtt-raw-exporter.md](mqtt-raw-exporter.md) both need a
   real MQTT client connection (CONNECT, Will, publish, subscribe); they
   should converge on one client abstraction (very likely built on the same
   `ms-mqtt-client` crate the exporter draft already selected) rather than
   each growing an independent client integration. This convergence is not
   yet designed and must happen before either the Sparkplug host extension
   or a shared client capability is implemented.
5. **Whether a real MQTT client CONNECT to a `rumqttd`-embedded broker can
   register a Will message needs a spike**, not an assumption. The raw
   receiver draft only exercises `Broker::link` (no CONNECT, no Will); this
   document's STATE lifecycle requires a genuine CONNECT-based session
   against the same embedded broker, which is a code path
   [mqtt-raw-receiver.md](mqtt-raw-receiver.md) never verified.
6. **No extension-scoped durable-state capability exists** for the optional
   M5 cross-restart session-persistence milestone; this is noted as a
   dependency for that milestone only and does not block M0-M4.

## Acceptance criteria

- [ ] `extension:sparkplug_host` publishes retained `STATE/{host_id}` =
      `ONLINE` on connect and registers a Will publishing retained
      `STATE/{host_id}` = `OFFLINE`, verified against both a graceful
      shutdown and a simulated abrupt disconnect.
- [ ] The extension tracks per-`(group_id, edge_node_id[, device_id])`
      session state (`online`, `alias_table`, `last_seq`, `bdSeq`) from
      observed `spBv1.0/#` traffic, bounded by `max_tracked_sessions` and
      `max_aliases_per_session`, with no unbounded growth path.
- [ ] `seq` gaps, `bdSeq` mismatches, and unknown-alias references each
      schedule a rate-bounded `NCMD` rebirth request with bounded retry, per
      [`NCMD`/`DCMD` and rebirth](#ncmddcmd-and-rebirth).
- [ ] `processor:sparkplug_decode` is a separate, PData-facing node that
      queries `SparkplugSessionView` per message; it holds no session state
      of its own beyond what the capability call returns.
- [ ] Neither `receiver:mqtt` nor `exporter:mqtt` gains any Sparkplug-aware
      code path; a Sparkplug-oblivious MQTT relay pipeline continues to work
      with neither the extension nor the processor configured.
- [ ] The Sparkplug host extension has zero compile-time dependency on the
      `rumqttd` crate; its broker access goes only through
      `MqttBrokerEndpoint` or direct endpoint config.
- [ ] The co-located topology's startup-ordering inversion (extensions
      start before the node-owned broker would exist) is resolved via the
      documented ownership relocation, not via an undocumented timing
      assumption.
- [ ] All telemetry listed in [Telemetry](#telemetry) is implemented and
      contains no payload content by default.

## Related work

- [mqtt-raw-receiver.md](mqtt-raw-receiver.md)
- [mqtt-raw-exporter.md](mqtt-raw-exporter.md)
- [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md)
- [mqtt-bounded-inbound-publish-flow-control.md](mqtt-bounded-inbound-publish-flow-control.md)
- [mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md)
- [extension-requirements.md](../extension-requirements.md)
- [extension-system-architecture.md](../extension-system-architecture.md)
- [k8s-service-account-token-auth-extension.md](../k8s-service-account-token-auth-extension.md)
  (a worked example of a Passive, capability-only extension; this document's
  extension is Active, for contrast)
- [#3452](https://github.com/open-telemetry/otel-arrow/issues/3452) --
  pluggable PData byte representations
- [#3875](https://github.com/open-telemetry/otel-arrow/issues/3875) --
  pluggable PData Arrow batch representations
- [#3876](https://github.com/open-telemetry/otel-arrow/issues/3876) --
  mixed-signal PData
