---
Proposal Name: mqtt-service-capabilities
Start Date: 2026-08-28
RFC PR: TBD
Tracking Issue: TBD
---

# RFC 0003: MQTT as a Service Capability, Not a Client/Server Split

## Summary

MQTT is a bidirectional, connection-owning protocol: a single logical
"datalogger" role can require either an MQTT *client* (when a customer already
runs a broker) or an MQTT *server* (when the DFE is the only thing the field
devices can talk to). Rather than building that split into `receiver:mqtt` and
`exporter:mqtt` directly, this RFC proposes that MQTT connection/session
ownership live in **extensions** (`extension:mqtt_client`,
`extension:mqtt_server`) that expose two engine-owned, pdata-free capabilities,
**`mqtt_ingress`** and **`mqtt_egress`**. `receiver:mqtt` and `exporter:mqtt`
become protocol-agnostic consumers of those capabilities. This lets the same
pair of receiver/exporter nodes work whether MQTT connectivity is supplied by
an external broker or by a DFE-embedded one.

This RFC makes two additional decisions:

- The client implementation will be Microsoft's
  [`rust-mqtt-client`](https://github.com/microsoft/rust-mqtt-client). The
  earlier `rumqttc` exporter was a plaintext transitional implementation and
  has now been removed. `exporter:mqtt` now names the capability-based node
  bound to `mqtt_egress`. We will work upstream with that team to resolve the
  crypto-provider, bounded-ingress, and acknowledgment issues that currently
  prevent broader adoption.
- Sparkplug support will not wait for extension-to-extension dependencies.
  `extension:mqtt_sparkplug` will be a sibling MQTT service implementation
  built from the same common MQTT client/router code as the generic service
  extensions, with additional Sparkplug Primary Host and session logic. It
  will expose capabilities to data-path nodes; it will not consume capabilities
  from another extension.

The topic runtime implements broadcast `all` acknowledgment, but configuration
cannot select it yet and mixed topics remain `first` only. Where an
acknowledgment guarantee depends on completion from multiple downstream
consumers, this document says so and requires a safe reduced scope.

## Motivation

The existing and in-flight MQTT work in this repository is organized as a
protocol/role split:

- `exporter:mqtt` is now the capability-based MQTT egress node described by
  this RFC. Before this consolidation it existed as a `rumqttc`-based,
  plaintext-only client exporter whose policy, bounds, and Ack/Nack behavior
  informed the capability design.
- `receiver:mqtt` (blocked, see Prior art) was designed as an embedded broker
  using `rumqttd`, because the target scenario is standalone Sparkplug/MQTT
  field equipment with no existing SCADA/broker infrastructure -- the receiver
  has to be the thing devices connect to.
- A prospective `extension:mqtt_sparkplug` needs to both observe broker
  traffic and originate `NCMD`/`DCMD`/`STATE` messages back to devices. It is
  therefore an MQTT service with Sparkplug behavior, not a codec layered on an
  already-running MQTT extension.

Two customer topologies are both legitimate and both must be supported:

1. **Existing broker present.** The DFE's MQTT component(s) are ordinary
   *clients* of that broker, symmetric with any other exporter/receiver pair
   in the engine.
2. **No existing broker (standalone edge).** The DFE must itself act as the
   *server* devices connect to. Devices publish telemetry and, for Sparkplug
   and similar protocols, expect commands and state messages back.

Baking "client" and "server" behavior separately into `receiver:mqtt` and
`exporter:mqtt` has three costs:

- It duplicates connection lifecycle, TLS/crypto policy, reconnect/backoff,
  and credential handling logic between whichever nodes need to originate
  MQTT traffic and whichever need to receive it, once both directions matter
  (which Sparkplug requires).
- It gives the receiver two very different jobs -- "be an MQTT endpoint" and
  "turn PUBLISH packets into pdata" -- forcing every future protocol
  extension (Sparkplug, and anything after it) to either reimplement broker
  ownership or reach into the receiver's internals.
- It gives no natural home for a component that needs to *publish back* to
  connected devices without being "the exporter" -- Sparkplug's `NCMD`,
  `STATE`, and rebirth-request messages are broker-observation and
  command-origination in the same breath, not a batch of otel data leaving
  the pipeline.

The user's proposed reframing is: because otap-dataflow is fundamentally a
routing and processing engine, and MQTT is bidirectional, the right owner of
"is there a socket, and who's on the other end of it" is the same kind of
component the engine already uses for cross-cutting, non-pdata concerns --
an **extension** -- not the receiver or exporter themselves. Receivers and
exporters should stay protocol-agnostic pdata boundaries; MQTT-specific
connection state, socket ownership, and session bookkeeping should live
behind a capability those nodes consume.

## Guide-level explanation

### The MQTT service family

```text
extension:mqtt_client                      extension:mqtt_server
(dials out to an existing broker)          (accepts connections; is the broker)

  +-----------------------+                  +-----------------------+
  | owns: TCP connection   |                  | owns: TCP listener(s) |
  | session, keep-alive,   |                  | per-client sessions,  |
  | reconnect/backoff,     |                  | subscriptions,        |
  | TLS/crypto policy      |                  | retained store, wills |
  +-----------------------+                  +-----------------------+
       exposes                                     exposes
  mqtt_ingress / mqtt_egress                 mqtt_ingress / mqtt_egress
```

Both extensions expose the *same two capabilities*. A pipeline author who
writes `receiver:mqtt` + `exporter:mqtt` against `mqtt_ingress`/`mqtt_egress`
does not need to know or care whether the pipeline is talking to an existing
broker or embedding one -- that decision is made once, in the extension
configuration, and the receiver/exporter graph is identical either way.

`extension:mqtt_sparkplug` is a third member of this family:

```text
extension:mqtt_sparkplug
  |
  +-- common MQTT client or router/session implementation
  +-- Primary Host STATE/Will lifecycle
  +-- spBv1.0 topic parsing and ordered session state
  +-- alias, seq, and bdSeq tracking
  +-- NCMD/DCMD rebirth publication
  |
  +-- mqtt_ingress / mqtt_egress
  +-- sparkplug-b representation tagging and decode context
```

It does not bind to `extension:mqtt_client` or `extension:mqtt_server`.
Instead, the three extensions depend at compile time on the same common MQTT
service crate. This is ordinary Rust code reuse and is compatible with the
engine rule that extensions cannot consume other extensions' capabilities.

In external-broker mode, `extension:mqtt_sparkplug` uses
`rust-mqtt-client` through that common crate. In standalone mode, it composes
the common server/router directly and creates a local Primary Host session
inside the router. That local session must implement the same CONNECT, Will,
publish-completion, and disconnect semantics as a network client without
paying for a loopback TCP hop or an extra payload copy.

### Example: existing-broker topology

```yaml
version: otel_dataflow/v1
engine: {}
groups:
  default:
    pipelines:
      main:
        policies:
          resources:
            core_allocation:
              type: core_count
              count: 1
        extensions:
          mqtt_client:
            type: extension:mqtt_client
            config:
              broker: "tcp://scada-broker.example.internal:1883"
              client_id: "otap-dfe-datalogger-01"
        nodes:
          mqtt_in:
            type: receiver:mqtt
            capabilities:
              mqtt_ingress: mqtt_client
            config:
              topic_filters: ["spBv1.0/#"]
          file:
            type: exporter:file
            config:
              path: "/var/log/otap/mqtt-{signal}-{core_id}-{generation}.jsonl"
        connections:
          - from: mqtt_in
            to: file
```

### Example: standalone edge topology

```yaml
version: otel_dataflow/v1
engine: {}
groups:
  default:
    pipelines:
      main:
        policies:
          resources:
            core_allocation:
              type: core_count
              count: 1
        extensions:
          mqtt_server:
            type: extension:mqtt_server
            config:
              listen: "0.0.0.0:1883"
              max_clients: 64
        nodes:
          mqtt_in:
            type: receiver:mqtt
            capabilities:
              mqtt_ingress: mqtt_server
            config:
              topic_filters: ["spBv1.0/#"]
          file:
            type: exporter:file
            config:
              path: "/var/log/otap/mqtt-{signal}-{core_id}-{generation}.jsonl"
        connections:
          - from: mqtt_in
            to: file
```

The `mqtt_in` node and its topic-filter-shaped configuration are identical in
both examples. Only the bound extension changes.

### Why this is a "broker-like configuration," not a general-purpose broker

`extension:mqtt_server` provides only the parts of broker behavior that are
inherently protocol-level and stateful per MQTT connection: accepting TCP/TLS
connections, tracking per-client sessions, subscriptions, retained messages,
last-will, and packet IDs, and multiplexing multiple devices' PUBLISH/SUBSCRIBE
traffic onto the two capabilities. It deliberately does **not** implement
cross-client routing policy, durable storage strategy, or export destinations
-- those are supplied by composing ordinary DFE receivers, processors, and
exporters downstream of `mqtt_ingress` and upstream of `mqtt_egress`. In other
words: the extension is the "broker's front door and session table"; the rest
of the pipeline is the "broker's routing and persistence logic," expressed in
terms the engine already understands (nodes, topics, backpressure, retries).

The minimum router does still implement real MQTT behavior. A remote PUBLISH
must be routed to matching remote subscriptions and local `mqtt_ingress`
subscribers; a local `mqtt_egress` PUBLISH must be routed to matching connected
clients. Retained publications, last wills, QoS state, keep-alive, and bounded
client sessions are protocol responsibilities, not pipeline processors.
The first milestone supports QoS 0 and QoS 1, which cover the target Sparkplug
traffic, and rejects QoS 2 rather than exposing a partially implemented state
machine. MQTT 5 advertises Maximum QoS 1 and disconnects a peer that still
sends QoS 2. MQTT 3.1.1 cannot negotiate Maximum QoS, so receiving a QoS 2
PUBLISH closes that connection; SUBSCRIBE requests are downgraded or rejected
through SUBACK.

The excluded functionality is product-level broker machinery: clustering,
arbitrary plugin systems, rule engines, database integrations, management
dashboards, and broker-to-broker federation. OTAP pipelines provide routing,
transformation, durability, and export integration after protocol admission.

### Sparkplug standalone datalogger

The initial Sparkplug topology is the standalone edge case: field devices have
no SCADA system or independently managed broker and connect directly to the
DFE MQTT service.

```text
Sparkplug devices
       |
       v
extension:mqtt_sparkplug
  - basic MQTT router
  - Primary Host STATE
  - birth/death/session logic
       |
       +---- mqtt_ingress ------> receiver:mqtt
       |                              |
       |                              +--> sparkplug-b encoded pdata
       |                                      |
       |                                      +--> raw relay
       |                                      |
       |                                      +--> PData codec
       |                                               |
       |                                               +--> OTAP metrics/logs
       <---- mqtt_egress -------- exporter:mqtt / local commands
```

The Sparkplug extension remains pdata-free. It emits the same MQTT ingress
domain type as every other MQTT service, but tags Sparkplug publications with
a stable encoded-representation identifier and immutable decode context.
`receiver:mqtt` wraps that payload as encoded pdata without interpreting it.
The registered Sparkplug PData codec converts it into OTAP only when a
downstream component requires native telemetry.

The first useful Sparkplug milestone does not require complete metric
materialization:

1. Relay every device PUBLISH unchanged through `mqtt_ingress`.
2. Publish the configured profile's retained Primary Host online STATE and
   register its matching offline Will.
3. Recognize NBIRTH, DBIRTH, NDEATH, and DDEATH and maintain bounded session
   identity.
4. Mark NDEATH/DDEATH representations as logs and attach their resolved
   entity/session context.
5. Have the Sparkplug PData codec materialize node and device deaths as OTAP
   logs when native telemetry is requested.

An NDEATH transition also marks every tracked device under that edge node
offline and emits a device-death event for each one. Sparkplug registers
NDEATH, not one DDEATH per device, as the edge node's MQTT Will; without this
cascade a disconnected edge node would leave all of its devices appearing
alive indefinitely. An explicit DDEATH affects only the identified device.

Primary Host STATE is profile-specific:

- Sparkplug 2.2 uses `STATE/{host_id}` with `ONLINE`/`OFFLINE`.
- Sparkplug 3.0 uses `spBv1.0/STATE/{host_id}` with a timestamped JSON body.

The service does not mix a topic from one profile with the payload from
another. In standalone mode, STATE activates connected devices while the
router is alive. Its offline Will cannot outlive a process crash that also
destroys the embedded router and retained store; the Will is meaningful when
the local Primary Host session fails independently and remains fully effective
in external-broker mode. Graceful shutdown publishes the profile-specific
offline state before the router stops.

Metric alias resolution and OTAP metric production follow in a later
milestone. Raw relay remains available throughout, so adding semantic decoding
never forces a decode/re-encode cycle on pipelines that only forward
Sparkplug bytes.

An illustrative pipeline binds the generic MQTT receiver to the Sparkplug
service extension. The file exporter requires native OTAP JSON, so conversion
through the registered Sparkplug codec occurs before export:

```yaml
version: otel_dataflow/v1
engine: {}
groups:
  default:
    pipelines:
      main:
        policies:
          resources:
            core_allocation:
              type: core_count
              count: 1
        extensions:
          sparkplug:
            type: extension:mqtt_sparkplug
            config:
              mode: server
              listen: "0.0.0.0:1883"
              host_id: "otap-datalogger-01"
              state_profile: sparkplug_3
              max_clients: 64
        nodes:
          mqtt:
            type: receiver:mqtt
            capabilities:
              mqtt_ingress: sparkplug
            config:
              topic_filters: ["spBv1.0/#"]
          file:
            type: exporter:file
            config:
              path: "/var/log/otap/sparkplug-{signal}-{core_id}-{generation}.jsonl"
        connections:
          - from: mqtt
            to: file
```

The encoded representation owns one immutable MQTT envelope and payload
buffer. A passthrough path clones a `Bytes`/reference-counted handle, not the
payload. Native materialization allocates only when a processor or exporter
requests it. Each capability consumer has an explicit bounded reservation; the
extension must not hide an unbounded queue between router admission and the
receiver. Stronger completion guarantees remain subject to the topic-runtime
consensus limitation below.

## Reference-level explanation

### Capabilities: `mqtt_ingress` and `mqtt_egress`

The MQTT capabilities are engine-owned and pdata-free, following the existing
capability pattern (`crates/engine/src/capability/`): they are typed Rust
interfaces resolved once per consuming node at pipeline build time via
`Capabilities::require_local`/`require_shared`, not pdata payloads flowing
through topics.

`mqtt_ingress` (consumed by `receiver:mqtt`):

- Yields inbound MQTT PUBLISH events (topic, QoS, retain flag, payload,
  MQTT5 user properties/content-type where available, and an
  extension-assigned monotonic sequence number for de-duplication).
- Carries the connection/session metadata needed for the envelope contract
  already specified in `docs/issue-drafts/mqtt-raw-envelope-contract.md`
  (client identity, timestamps, protocol version) without requiring the
  receiver to know whether that metadata came from a client connection or a
  server-side per-device session.
- Exposes an acknowledgment boundary (see below) so the extension knows when
  it is safe to PUBACK (server role) or to consider a message durably
  admitted (client role, for QoS 1 subscriptions it originates).

`mqtt_egress` (consumed by `exporter:mqtt`):

- Accepts outbound PUBLISH requests (topic, QoS, retain, payload, properties)
  and returns per-message completion status (accepted/refused/failed),
  reusing the classification work already done in the existing
  `exporter:mqtt` (`is_refusal` and related error categorization, see
  Prior art).
- Does not require the caller to know whether "publish" means "send to the
  broker we dialed" or "fan out to whichever connected devices are
  subscribed," which is entirely the concern of the `mqtt_server` extension's
  internal routing.

### Sparkplug as a pluggable byte representation

`extension:mqtt_sparkplug` still provides ordinary `mqtt_ingress`. Its ingress
items add a representation descriptor that `receiver:mqtt` copies into encoded
pdata:

```rust
struct MqttIngressItem {
    event_id: MqttEventId,
    envelope: MqttEnvelope,
    payload: Bytes,
    representation: EncodedRepresentation,
}

enum EncodedRepresentation {
    MqttPublish,
    SparkplugB {
        version: SparkplugPayloadVersion,
        message_type: SparkplugMessageType,
        signal: SignalType,
        context: SparkplugDecodeContext,
    },
}
```

The reference types are illustrative. `SparkplugDecodeContext` is an immutable,
serializable, cheaply cloned snapshot containing interned group/edge/device
identifiers, the current `bdSeq`, only the alias definitions referenced by
this payload, and derived lifecycle transitions. It is carried with the
representation so the codec never reaches back into a live extension or races
with a later session update.

The context makes each encoded item independently decodable after durable
buffering or process restart. A codec that depends on replaying every preceding
NBIRTH in order would make an alias-only NDATA/DDATA item unsafe to retry or
move between DFE instances. The pluggable-representation design must therefore
support serializable representation metadata in addition to the payload
`Bytes`, or define a Sparkplug envelope encoding that contains this context
without changing the original payload bytes. This is a concrete requirement
on open-telemetry/otel-arrow#3452, not hidden mutable codec state.

Death origin is best-effort:

- A server-side router knows when it emitted a registered MQTT Will and marks
  that event `router_will`.
- An explicit NDEATH/DDEATH received on a live connection is
  `explicit_publish`.
- A client connected to an external broker receives a Will as an ordinary
  PUBLISH; MQTT carries no "this was a Will" flag, so its origin is `unknown`.

The `sparkplug-b` PData codec initially maps deaths to OTAP logs:

- `event_name = "sparkplug.node.death"` or
  `"sparkplug.device.death"`;
- resource attributes identify group, edge node, and device;
- log attributes carry `bdSeq`, death origin, MQTT topic, QoS, and retain
  status when known;
- the body preserves the original NDEATH/DDEATH payload bytes.

It may additionally emit a liveness metric with value zero. Mixed-signal
support is not a prerequisite for the first codec: message type determines the
initial signal classification. NDEATH/DDEATH materialize as logs;
NBIRTH/DBIRTH/NDATA/DDATA materialize as metrics. A future payload that truly
produces both signals depends on mixed-signal pdata or returns separate pdata
messages through an explicit conversion API. Until the metric mapping is
specified, the death log is authoritative and no synthetic metric is required.

The active Sparkplug extension and the PData codec are separate registrations
supplied by the same feature and built from the same Sparkplug common crate:

- the active extension owns ordered protocol state, Primary Host behavior, and
  rebirth side effects;
- the codec performs a pure conversion from payload bytes plus immutable decode
  context to OTAP;
- neither registration consumes a capability from the other; and
- the common Sparkplug state machine returns typed state transitions and
  protocol effects so its behavior is tested independently of both wrappers.

This is the same separation intended for variable syslog encodings: transport
framing produces an encoded representation, while a registered codec owns
format-specific interpretation. Sparkplug is a stateful MQTT representation,
not a second receiver.

The MQTT capabilities carry direction-specific provenance. An
`mqtt_ingress` item has an ingress origin: `remote_publish`,
`router_will`, `local_service`, or `unknown_remote` (when an external broker
does not expose whether a publication was a Will). An `mqtt_egress` request
has an egress source: `local_pipeline` or `local_service`. Separate enums make
invalid combinations unrepresentable.
This is required to prevent feedback loops: without it, a pipeline that both
consumes `mqtt_ingress` and produces `mqtt_egress` against the same
`mqtt_server` extension (the standalone/broker-like topology) could
re-ingest its own published output as if it were device traffic. Loop
prevention for DFE-local capability subscribers is the extension's
responsibility: by default it does not deliver `local_pipeline` or
`local_service` publications back through local `mqtt_ingress`. This is a
local pipeline policy, not standard broker behavior. Remote MQTT clients retain
normal MQTT echo semantics, including receiving their own matching
publications unless they requested MQTT 5 No Local.

The extension-assigned event identifier remains stable when encoded pdata is
materialized. A raw relay and an OTAP death log are two views of the same
representation, not two independently delivered ingress events. Fan-out may
send the encoded view to one branch and materialize another branch; downstream
policy can correlate them by event identifier without the MQTT service
duplicating admission or completion tickets.

### Common MQTT service code

Code sharing happens below the extension boundary. A workspace crate,
tentatively `otel-arrow-dfe-mqtt`, owns:

- backend-neutral MQTT envelope and completion types;
- bounded subscription and local-delivery queues;
- client session traits and the `rust-mqtt-client` adapter;
- the minimal server/router state machines;
- listener-independent protocol limits and validation;
- retained, Will, QoS, and packet-identifier bookkeeping; and
- deterministic test fixtures shared by all MQTT extensions and nodes.

The hot-path types use `Bytes`, interned identifier handles, and immutable
session snapshots. Core-local implementations use `Rc` for shared service
state rather than imposing atomic reference counts on every state access.
Routing and capability fan-out must not copy payloads or allocate a new topic
string per local subscriber. Allocation and copy budgets are part of the
common crate's API contract and benchmark suite, not implementation details
left to each extension wrapper.

Bounds apply to bytes as well as slots. Every service configuration includes
at least:

- maximum MQTT packet and payload bytes;
- maximum aggregate in-flight payload bytes;
- maximum bytes retained per session and across the service;
- maximum clients, subscriptions, retained publications, and in-flight packet
  identifiers; and
- maximum encoded local-delivery and cached-materialization bytes.

Holding a `Bytes` handle pins its backing allocation, so a slot count alone is
not a memory bound. Admission reserves both one slot and the message's retained
byte size before accepting it.

The first implementation registers local capability variants and uses
core-local `Rc` state; `receiver:mqtt` and `exporter:mqtt` are local nodes.
Shared variants are not synthesized from local state. A later shared
implementation must justify its synchronization and cross-core allocation
behavior independently.

### Ingress registration, activation, and quiescence

Moving endpoint ownership into an extension reverses the normal receiver-first
shutdown order unless the capability has an explicit lifetime contract.
`mqtt_ingress` therefore uses a two-stage registration:

1. During node construction, the receiver declares its topic filters, byte and
   item limits, and acknowledgment policy. This happens while capability
   bindings are resolved, before runtime tasks start.
2. In `Receiver::start`, the receiver activates the registration and holds an
   `IngressLease`. Dropping that lease deactivates the consumer. An exporter
   similarly holds an `EgressLease`.

The extension may bind a listener or construct a client actor during its own
startup, but it must not accept application PUBLISH traffic or issue SUBSCRIBE
until an ingress lease is active. A server begins accepting clients when any
ingress or egress lease is active, so an egress-only server can still serve
remote subscribers. When the last ingress lease drops, the service immediately
quiesces ingress:

- server mode stops accepting new clients and stops admitting new PUBLISHes;
- client mode unsubscribes or stops acknowledging new deliveries;
- pending QoS work follows the configured transient-Nack action below; and
- no message is PUBACKed after all consumers capable of admitting it are gone.

This makes receiver lifetime, not extension shutdown order, control admission.
The extension remains alive and can finish protocol cleanup when it later
receives `ExtensionControlMsg::Shutdown`.

Readiness signals only successful local initialization: listener bind, client
state-machine construction, validated registrations, and bounded queue
allocation. It does not wait for an external broker to be reachable, a receiver
to activate, or Sparkplug STATE to receive PUBACK. Those are runtime health
states. In server mode the bound listener's accept gate remains closed until
activation; in client mode connection attempts may proceed, but subscriptions
and device telemetry admission remain inactive.

A separate Sparkplug module or crate depends on this common MQTT crate and adds
topic parsing, Primary Host behavior, and bounded Sparkplug session state.
It exposes both an active-service adapter used by
`extension:mqtt_sparkplug` and a pure `sparkplug-b` PData codec. There is no
runtime dependency between those registrations and therefore no need for an
extension startup-order graph.

The common crate must not depend on the engine capability registry. Engine
extensions adapt common service handles into `mqtt_ingress` or `mqtt_egress`.
The PData registration adapts immutable representation values into the engine's
codec interface. This keeps the protocol implementation testable without a
running DFE pipeline and prevents capability concerns from leaking into MQTT
state machines.

### Client implementation and upstream partnership

`rust-mqtt-client` is the selected client implementation for
`extension:mqtt_client`, the external-broker mode of
`extension:mqtt_sparkplug`, and eventually `exporter:mqtt` through
`mqtt_egress`.

The choice is based on implementation properties that are directly important
to the DFE:

- explicit submission, protocol-completion, and broker-verdict stages;
- per-operation completion tokens instead of reconstructing packet ownership
  from a global event stream;
- application-owned connection driving, reconnect policy, and task structure;
- no hidden runtime or worker threads;
- epoch-scoped manual acknowledgments and explicit MQTT session state; and
- a correctness-first test, fuzzing, and interoperability posture.

The current library cannot yet be adopted because OpenSSL is unconditional,
its TLS API exposes OpenSSL types, it has no ambient rustls
`CryptoProvider` path, and it is not yet published as a versioned crate. Its
unbounded inbound path and successful PUBACK-on-token-drop behavior also need
resolution before it is used for ingress.

These are upstream collaboration items, not reasons to maintain a competing
client indefinitely. We will:

1. File focused, source-backed issues and avoid duplicate reports.
2. Offer patches, tests, and OTAP's rustls/SymCrypt provider experience.
3. Preserve backend-neutral capability and common-code boundaries so upstream
   changes do not leak into nodes.
4. Pin reviewed revisions until the library publishes releases suitable for
   downstream use.
5. Avoid a permanent fork; if a temporary branch is needed to validate a
   patch, keep it small and intended for upstream submission.
6. Run the same Mosquitto/Paho interoperability and dependency-tree checks
   against upstream changes that we require for our own implementation.

The three known engagement items are:

- contribute bounded-ingress requirements and tests to the existing
  microsoft/rust-mqtt-client#105 discussion instead of filing a duplicate;
- discuss loss-safe unresolved QoS 1 acknowledgment semantics separately from
  the already-resolved token-drop thread-allocation problem; and
- file the pluggable TLS/no-TLS/ambient `CryptoProvider` request captured in
  `docs/issue-drafts/ms-mqtt-client-pluggable-tls-crypto.md`.

The removed `rumqttc` exporter served as the transitional conformance oracle
for this migration. The current MQTT architecture does not expose `rumqttc`
types or make its event-correlation model part of a public capability.

Sparkplug service publications use a reserved egress lane. Primary Host STATE,
rebirth NCMD/DCMD, and graceful offline STATE cannot wait behind bulk
`exporter:mqtt` traffic for all packet identifiers or queue capacity. The lane
is bounded, has a small reserved packet/byte budget, and is served before
ordinary local-pipeline egress without starving it. Exhausting the reserved
lane is a health-critical error, never an unbounded allocation or silent drop.

### Acknowledgment boundary

MQTT QoS 1/2 durability and the DFE's own Ack/Nack model do not automatically
line up, and prior investigation of `rumqttd` and the existing
`exporter:mqtt` found this to be one of the most failure-prone seams (see
`docs/issue-drafts/rumqttd-observable-bounded-delivery.md` and the ack/nack
sections of `docs/issue-drafts/mqtt-raw-exporter.md`). This RFC proposes four
named ack-boundary levels that an `mqtt_ingress` implementation may support,
in increasing order of durability guarantee:

1. **`protocol`** -- the extension PUBACKs (or, client role, completes the
   MQTT-level handshake) as soon as the packet is parsed, before the DFE
   pipeline has seen it at all. Weakest guarantee; matches "fire and forget"
   ingestion.
2. **`admitted`** -- the extension withholds PUBACK/QoS completion until the
   message has been accepted into the receiver's admission path (bounded
   queue space reserved), but not necessarily processed or exported.
3. **`durable`** -- the extension withholds PUBACK/QoS completion until the
   DFE pipeline reports the message as durably handed off (e.g., accepted by
   a downstream exporter, or written to a durable buffer). This is the
   recommended long-term default for any topology claiming "at least once"
   semantics end to end.
4. **`terminal`** -- the extension withholds PUBACK/QoS completion until
   every configured downstream exporter has confirmed terminal delivery.
   Strongest guarantee, highest latency; appropriate only for low-volume,
   high-value topics (e.g., Sparkplug `NBIRTH`/`DBIRTH`).

For QoS 1, the MQTT service snapshots the eligible ingress leases when it
routes the publication. Each eligible lease receives its own one-shot ticket
inside an `IngressCompletionGroup`; no receiver shares or races on one ticket.
The group resolves only after every lease in that immutable snapshot
contributes an outcome. A transient or permanent Nack resolves the group
according to the Nack table below; all-Ack is required for success. Dropping an
eligible lease contributes a transient Nack for every outcome it still owes.

Each eligible `receiver:mqtt` stores its ticket's compact identifier in pdata
return-path context and calls the capability when its selected boundary is
reached. At `admitted`, it resolves its ticket immediately after reserving
local capacity. At `durable` or `terminal`, it resolves its ticket from the
corresponding DFE Ack/Nack. The extension remains pdata-free: it sees only
ticket identifiers and classified outcomes, never `OtapPdata`.

Routing policy determines eligibility before the snapshot. Two MQTT receivers
with overlapping filters each owe one outcome; a receiver whose filters do not
match owes none. Sparkplug materialization is a representation conversion
inside a receiver's downstream path and therefore remains covered by that
receiver's one ticket rather than creating a second capability-level delivery.

Completion groups, tickets, and retained payload bytes share one configured
item-and-byte budget. Dropping a receiver or capability handle never implies
successful PUBACK.

**Current engine gap:** `durable` and `terminal` both require the topic
runtime to report a completion signal aggregated across potentially multiple
downstream consumers. `TopicBroadcastAckMode::All` is implemented for
broadcast-only topics, including subscriber-disappearance Nack behavior.
What remains unavailable is the user-facing per-topic configuration that
selects `all`; mixed balanced/broadcast topics also remain `first` only.

Until that configuration lands, `durable` and `terminal` are valid only for a
single downstream completion path. After it lands, they may fan out through a
broadcast-only topic configured with `ack_mode: all`; they remain invalid for
mixed topics. Capability-level fan-out is separate from topic fan-out: the
MQTT service aggregates admission reservations across its own ingress leases
before producing pdata, while the topic runtime aggregates downstream
completion after a receiver publishes pdata.

### Nack actions

An acknowledgment boundary also defines what failure means on the MQTT wire:

<!-- markdownlint-disable MD013 -->
| Input and outcome | MQTT action |
| --- | --- |
| QoS 0, any downstream Nack | No protocol retry exists. Record an observable loss; no delivery guarantee is claimed. |
| QoS 1, `protocol` or `admitted`, later downstream Nack | PUBACK was already sent. Record the downstream failure; it cannot trigger MQTT redelivery. |
| QoS 1, `durable`/`terminal`, transient Nack, client role | Do not PUBACK; disconnect cleanly enough to preserve the MQTT session, then reconnect and accept broker redelivery. |
| QoS 1, `durable`/`terminal`, transient Nack, server role | Do not PUBACK and close the publishing connection. Redelivery depends on the publisher retaining and retrying its QoS 1 session state. |
| QoS 1, permanent Nack, MQTT 5 | Send a suitable unsuccessful PUBACK reason and record the terminal rejection. |
| QoS 1, permanent Nack, MQTT 3.1.1 | Apply an explicit configured policy: `acknowledge_and_drop` to stop a poison retry loop, or `disconnect_without_puback` to request redelivery. Default to `acknowledge_and_drop` and make the loss observable. |
| Router-generated Will or local-service event | No upstream acknowledgment exists. The boundary begins at admission to local consumers. |
<!-- markdownlint-restore -->

No mode claims exactly-once delivery. Transient redelivery may duplicate work,
and permanent rejection may intentionally terminate delivery.

### Extension lifecycle and current Phase-1 constraints

Per `docs/extension-system-architecture.md` and
`crates/engine/src/extension/builder.rs`, extensions in the engine today are:

- **Pipeline-scoped and instantiated per core** (Phase 1) -- there is no
  process-wide singleton extension. For `extension:mqtt_client`, this is
  natural only for egress: each core's copy owns one `rust-mqtt-client`
  connection. The configured `client_id` is a base; when `num_cores > 1`, the
  resolved identifier appends `-c{core_id}` and validation applies MQTT's
  encoded-length limit after expansion. A client
  extension bound to ingress also requires a one-core source pipeline unless
  every subscription uses an explicitly configured shared-subscription group
  supported by the broker. Reusing one client identifier would cause
  connection takeover; unique identifiers with ordinary subscriptions would
  duplicate every message on every core. The initial `extension:mqtt_server` and
  `extension:mqtt_sparkplug` implementations instead require a one-core source
  pipeline and reject a larger allocation. Independent per-core routers would
  split retained, session, subscription, and Will state; `SO_REUSEPORT` would
  only distribute sockets and cannot repair that protocol-state split (and has
  no Windows equivalent). Multi-core server scaling requires a separate design
  for listener distribution and session ownership; this RFC does not introduce
  shared mutable broker state as a shortcut. Because `all_cores` is the engine
  default, validation errors must name the required override:

  ```yaml
  policies:
    resources:
      core_allocation:
        type: core_count
        count: 1
  ```

- **Active** (per the engine's Active/Passive/Background classification):
  `mqtt_client`, `mqtt_server`, and `mqtt_sparkplug` need to start before, and
  shut down after, the receivers/exporters that depend on their capabilities.
  The existing `ReadinessSignaller`/`ReadinessProbe` mechanism gates only local
  initialization: listener bind, registration validation, and bounded queue
  allocation. It does not wait for an external broker connection or Primary
  Host STATE publication. The Sparkplug extension is ready when its local
  service state can accept an ingress activation. STATE publication starts
  after the first capability lease on a `mqtt_sparkplug` service activates,
  regardless of whether the pipeline bound raw MQTT ingress, semantic
  Sparkplug ingress, or egress. It is reported as runtime health, not extension
  readiness.
- **Extensions cannot consume another extension's capabilities.** Config
  validation rejects `capabilities:` bindings on an extension. This RFC does
  not require that feature: `extension:mqtt_sparkplug` owns its connection or
  router and composes with the common MQTT crate directly. Nodes consume the
  capabilities it provides. Supporting extension dependencies later may enable
  other compositions, but it is not on the Sparkplug critical path.

### MQTT host service: server framework and a multi-core follow-on design

The single-core restriction above is a correct Phase-1 constraint, not a
permanent ceiling. This section evaluates the server-side protocol framework
and lays out the sharding design a follow-on RFC would need, so the
single-core decision is a deliberate staging choice with a known next step
rather than an open-ended limitation.

#### Framework choice: `ntex-mqtt` as protocol codec, not as broker or runtime

`ntex-mqtt` (currently `9.0.0-beta.0`) is confirmed, by direct inspection of
its published `Cargo.toml`, to depend only on `ntex-io`, `ntex-net`,
`ntex-codec`, `ntex-service`, `ntex-router`, `ntex-rt`, and `ntex-util` -- it
has no TLS or crypto dependency of its own, forced or optional. TLS is an
opt-in feature of the top-level `ntex` crate (`rustls` or `openssl` features),
selected by the embedder, not by `ntex-mqtt`. This clears the crypto-provider
bar that eliminated `rumqttd`, RMQTT, and Akasa, and it is the reason this RFC
treats `ntex-mqtt` as the leading server framework candidate rather than
"another broker to reject."

`ntex-mqtt` is explicitly a **protocol framework**, matching this RFC's
"broker's front door and session table, not the whole broker" scope from
above: its `v5`/`v3` modules provide handshake, publish, and control
`ServiceFactory` hooks (`Handshake`/`HandshakeAck`, `Publish`/`PublishAck`,
`Control`/`ControlAck`) plus MQTT wire codec, keep-alive timers, and
packet-identifier bookkeeping. It does not provide cross-client routing,
a retained-message store, or a subscription table -- exactly the router
and session-table responsibilities this RFC already assigns to the common
MQTT crate rather than to a broker dependency.

Two integration questions were flagged for resolution by an implementation
spike before this framework choice would be final; the first is now resolved
by direct inspection of the pinned `ntex-mqtt`/`ntex-net`/`ntex-service`
source (rev `0772703f035f80ce78a387c2b898058151098b92` and their resolved
dependency versions `ntex-net 4.0.0-beta.0`, `ntex-service 5.0.0-beta.3`), the
second remains open:

1. **Runtime ownership -- RESOLVED, clean boundary confirmed.** `ntex`'s
   ordinary server entry point (`ntex::server::build()...bind(name, addr,
   factory)...run()`) does spawn and own its own worker pool and reactor, and
   that entry point is indeed unusable for a DFE core driving its own
   accept/read/write loop -- but it is not the only integration point, and it
   is not required:
   - `ntex-net`'s `tokio` I/O backend (`ntex-net-4.0.0-beta.0/src/tokio/mod.rs`)
     exports a public `tokio::Reactor` type, and that type's public
     `crate::Reactor` impl provides
     `fn from_tcp_stream(&self, stream: std::net::TcpStream, cfg: SharedCfg)
     -> io::Result<ntex_io::Io>`. The `extension:mqtt_server` scaffold drives
     that trait method directly as
     `ntex_net::Reactor::from_tcp_stream(&ntex_net::tokio::Reactor, ...)`,
     not through the convenience free function `ntex_net::from_tcp_stream(...)`
     (which requires a current ntex driver to already be installed). The
     method takes an already-accepted, plain `std::net::TcpStream`
     (obtainable from a `tokio::net::TcpStream` via `.into_std()`), sets it
     non-blocking, wraps it back into a `tokio::net::TcpStream`, and returns
     an `ntex_io::Io` -- entirely independent of `ntex::server`'s own accept
     loop or worker pool. This means the DFE core's own `SO_REUSEPORT` accept
     loop (the same pattern already used by other DFE receivers, see
     `docs/load-balancing.md`) can accept every connection itself, on its own
     tokio task, and only hand the accepted socket to `ntex-net` for
     wire-level `Io` wrapping.
   - `ntex_mqtt::MqttServer<St, V3, V5, Err>` itself implements
     `ntex_service::Service<St, IoBoxed>` and `Service<St, Io<F>>` directly
     (`ntex-mqtt/src/server.rs`): its `call(&self, io, ctx)` reads the MQTT
     protocol version off the buffered/received bytes and dispatches to the
     configured `v3`/`v5` sub-service. There is no requirement to go through
     `ntex::server`'s `ServiceFactory`/`bind` machinery to invoke it.
   - `ntex_service::Pipeline::new<S, St>(f: impl IntoService<S, St, Req>) ->
     Self` (`ntex-service/src/pipeline.rs`) constructs a runnable pipeline
     directly from any `Service` value (including a configured `MqttServer`)
     without an external server harness; `Pipeline::call_static(req)` (or
     `call_nowait`) drives one request (one accepted connection's `Io`)
     through it and returns a future the DFE core's own executor can just
     `.await`.
   - Net result: the common MQTT crate's server path is `accept (own
     SO_REUSEPORT loop) -> std::net::TcpStream -> Reactor::from_tcp_stream
     -> ntex_io::Io -> Pipeline::new(configured MqttServer).call_static(io)
     -> await`, all driven on the DFE core's own tokio task, with `ntex-net`
     contributing only the codec/protocol machinery, never runtime or socket
     ownership. This confirms `ntex-mqtt` can be adopted as a pure protocol
     library under this RFC's thread-per-core model without its fallback
     "wire-level codec only, hand-rolled dispatch" position being necessary --
     though that fallback remains available if a future spike finds a
     correctness issue in this integration path (e.g. an undocumented
     assumption inside `ntex-io`/`ntex-rt` about being driven only from
     `ntex::server`'s own reactor).
2. **Maturity.** `9.0.0-beta.0` is pre-1.0. Adoption should pin a reviewed
   revision, the same posture already applied to `rust-mqtt-client`, and
   should not block the client-side milestone, which does not depend on this
   framework at all.

#### Thread-per-core mapping

The sharding design keeps this RFC's core invariant: **no shared mutable
broker state**. Instead of one router owning every session, each core owns a
disjoint shard of client connections and the full protocol state for only
that shard.

```text
core 0                    core 1                    core 2
+---------------+         +---------------+         +---------------+
| accept (SO_    |        | accept (SO_    |        | accept (SO_    |
| REUSEPORT)     |        | REUSEPORT)     |        | REUSEPORT)     |
| shard sessions |        | shard sessions |        | shard sessions |
| shard subs     |        | shard subs     |        | shard subs     |
| shard retained |        | shard retained |        | shard retained |
+-------+--------+        +-------+--------+        +-------+--------+
        |  local PUBLISH from this shard's clients            |
        +---------------------> broadcast topic <--------------+
                                (every shard subscribes to every
                                 other shard's local PUBLISH)
        <---------------------  filtered by each shard's own  --------------------+
                                 local subscription table
```

- **Connection distribution** uses the same per-CPU `SO_REUSEPORT` listener
  pattern already documented for other DFE receivers in
  `docs/load-balancing.md`: one socket per core, kernel-hashed 4-tuple
  selection, no shared accept queue. This has no Windows equivalent, matching
  the platform caveat already noted for the single-core constraint above; a
  Windows deployment stays on the single-core allocation until an accept
  distribution strategy exists for that platform.
- **Session, subscription, and retained-message state is per-shard, not
  global.** Each core's router only knows about the clients it accepted.
  There is no cross-core session table, no cross-core lock, and no
  cross-core `Rc`/`Arc` sharing of protocol state -- consistent with the
  common crate's existing `Rc`-based, core-local state design.
- **Cross-shard delivery reuses the existing broadcast/topic-split pattern**
  from `docs/load-balancing.md` ("Internal Topic-Based Split") instead of
  inventing new cross-core broker plumbing: every shard publishes its
  locally-received PUBLISHes (from its own clients, and from any
  `mqtt_egress` submissions routed through it) onto a bounded, broadcast-mode
  in-memory topic. Every other shard subscribes to that same topic and
  matches each item against its own local subscription table, forwarding
  only matching messages to its own connected sessions. A shard never
  forwards a message it also originated back to its own clients as if it
  were remote traffic (the same `local_pipeline`-origin loop-prevention rule
  from the capability definitions above applies here, per-shard).
- **Retained messages** are carried the same way: a `retain=true` PUBLISH is
  an ordinary broadcast item, and each shard updates its own copy of the
  retained-message table from the broadcast stream in delivery order, in
  addition to matching it against live subscriptions. This makes the
  retained table eventually consistent across shards rather than globally
  atomic: a client subscribing on core 2 microseconds after a retained
  update was accepted on core 0 may briefly see the prior retained value.
  This is an explicit, bounded trade-off, not a silent one, and must be
  documented as such rather than presented as broker-equivalent retained
  semantics.
- **Cost model.** Broadcast fan-out means every PUBLISH is delivered to every
  shard (`O(cores)` per message) regardless of how many shards actually have
  a matching subscriber, trading some wasted per-shard filtering work for
  avoiding any shared state or cross-core lock. This is the same trade-off
  already accepted by the engine's existing broadcast topic delivery mode
  and is expected to be acceptable at the sharded-client-count and
  message-rate this RFC targets (standalone Sparkplug/IoT edge fleets, not a
  general-purpose multi-tenant broker); it should be re-examined with real
  benchmarks before being assumed at materially larger scale.
- **Primary Host STATE and Sparkplug Will/NDEATH** remain a single logical
  identity even when sharded: only the shard that owns the Primary Host's own
  session publishes STATE and registers its Will, and the retained-table
  propagation above ensures every shard's subscribers still observe it.
  NDEATH-triggered device-death cascades (see the Sparkplug standalone
  datalogger discussion above) are resolved locally by whichever shard owns
  the affected edge node's session; a device connected through a different
  shard learns of that node's death through the same broadcast/retained path
  as any other subscriber, not through a separate cross-shard session
  lookup.

This design is not required for, and must not gate, the first milestone or
the Sparkplug standalone single-core topology; it is recorded here so that
the common MQTT crate's session/subscription/retained state machines are
written in a shard-shaped way from the start (keyed and scoped so that
running many independent instances side by side is a deployment change, not
a rewrite), even though only one instance is created until this design is
implemented and validated.

### Migration path for the existing `exporter:mqtt`

That migration is now complete. The former, hardened `rumqttc`-based
`exporter:mqtt` (102 tests, plaintext-only) has been removed, and the newer
capability-based exporter originally introduced as `exporter:mqtt_publish` now
reclaims the canonical `exporter:mqtt` name and `urn:otel:exporter:mqtt` URN.

Historically, the migration consisted of:

1. Moving MQTT client ownership out of the exporter node and into
   `extension:mqtt_client`.
2. Replacing direct client calls with the `mqtt_egress` capability shape.
3. Dropping the temporary `rumqttc` adapter rather than preserving two
   parallel exporter implementations.
4. Keeping `extension:mqtt_server` and other MQTT service implementations free
   to provide the same `mqtt_egress`/`mqtt_ingress` capability pair without
   further exporter renames.

The current `exporter:mqtt` therefore documents the post-migration steady
state this RFC describes, not a future plan.

### What this RFC does not decide

- Final confirmation of `ntex-mqtt` as the server-side protocol/codec
  framework. No embeddable Rust MQTT broker crate has cleared this
  repository's crypto-policy, lifecycle, and thread-per-core bar (`rumqttd`,
  RMQTT, and Akasa were found unsuitable). `ntex-mqtt` has no forced
  TLS/crypto dependency of its own and is this RFC's leading candidate (see
  "MQTT host service: server framework and a multi-core follow-on design"
  above); the runtime-ownership integration question there is resolved and
  confirmed by the compiling `extension:mqtt_server` scaffold
  (`crates/contrib-extensions/src/mqtt_server/`). The server implementation
  must live behind the common MQTT router API regardless of which framework
  backs it.
- Whether and when the multi-core sharded host-service design in that same
  section is implemented. It is recorded as the deliberate follow-on to the
  Phase-1 single-core constraint, not as part of this RFC's initial
  milestone.
- The exact wire/API shape of the `mqtt_ingress`/`mqtt_egress` Rust traits.
  That is reference-level implementation work for the tracking issue, not an
  architectural decision.
- The client implementation: this RFC selects `rust-mqtt-client`. The timing
  of migration depends on the adoption gates in
  `docs/issue-drafts/ms-mqtt-client-pluggable-tls-crypto.md` and the related
  ingress reports.

## Drawbacks

- **Indirection cost.** Every MQTT-touching node now goes through a
  capability boundary instead of owning a client/broker directly. For the
  simple, single-broker, single-receiver-plus-exporter baseline milestone
  (the user's stated first milestone: mosquitto/paho to filelog via OTLP
  JSON), this is strictly more moving parts than a receiver that just
  contains an MQTT client, for no immediate behavioral benefit -- the payoff
  only appears once a second component (a standalone server, or Sparkplug)
  needs to share the same connection/session state.
- **More than one active service implementation.** Generic MQTT and Sparkplug
  extensions share common code but own separate runtime instances. That avoids
  unsupported extension dependencies, but it places a strong requirement on
  the common crate: connection, router, limits, and completion behavior must
  not drift between wrappers.
- **Server mode is initially single-core.** This preserves share-nothing
  correctness and portability but limits one process's broker-side scaling.
  Multi-core listener and session distribution requires a separate RFC.
- **New capability surface to maintain.** `mqtt_ingress`/`mqtt_egress` become
  long-lived engine-facing contracts (origin tagging, ack-boundary levels)
  that must be versioned carefully once real components depend on them.

## Rationale and alternatives

- **Keep client logic in the exporter, add a separate embedded-broker
  receiver with no shared abstraction (status quo direction).** This is what
  the existing `exporter:mqtt` and the blocked `receiver:mqtt` design
  already do. It is simpler for the client-only baseline milestone, but it
  does not generalize: a future Sparkplug host that needs to both observe
  broker traffic and originate commands would either duplicate the
  receiver's or the exporter's connection-owning code, or reach into one of
  them directly, coupling protocol-specific behavior to node-specific
  internals. The capability-based design avoids that duplication at the
  cost of the indirection described above.
- **Model MQTT connectivity as a processor instead of an extension.**
  Rejected: processors operate on pdata in the topic graph; MQTT connection
  and session state (subscriptions, retained store, wills, packet IDs) is
  not pdata and has its own lifecycle (connect/reconnect, listener
  bind/shutdown) independent of any particular pipeline's data flow. The
  engine's extension abstraction already exists specifically for this kind
  of cross-cutting, non-pdata, lifecycle-bearing concern.
- **Give `receiver:mqtt` an optional embedded-broker mode flag instead of a
  separate extension.** Considered and rejected: this combines endpoint
  lifecycle with pdata construction and makes Sparkplug command publication
  reach into receiver internals. Service extensions own MQTT state; receivers
  only convert capability events into pdata.
- **Make `extension:mqtt_sparkplug` consume `mqtt_ingress`/`mqtt_egress` from
  another extension.** Rejected because the engine does not support extension
  capability consumers or dependency ordering. Compile-time composition over
  the common MQTT crate achieves reuse without adding that engine feature.
- **Implement Sparkplug only as a codec, without an active service
  extension.** Rejected because Primary Host STATE, Will registration,
  sequence tracking, and rebirth publication are active protocol behavior. A
  codec cannot own that lifecycle. The generic MQTT receiver carries the
  representation; the codec owns only OTAP materialization.

## Prior art

- `crates/contrib-nodes/src/exporters/mqtt_exporter/` -- the current
  capability-based `exporter:mqtt`, which consumes `mqtt_egress`. The removed
  `rumqttc` exporter that previously occupied this path supplied the historical
  hardening work and migration context referenced elsewhere in this RFC.
- `docs/issue-drafts/mqtt-raw-receiver.md` -- the blocked `rumqttd`-based
  embedded receiver design; documents the verified `rumqttd` blockers
  (no graceful shutdown, no `SO_REUSEPORT` support, silent
  post-PUBACK eviction) that motivated broader broker research.
- `docs/issue-drafts/rumqttd-graceful-embedded-shutdown.md` and
  `rumqttd-observable-bounded-delivery.md` -- specific upstream gaps found
  in `rumqttd`; the ack-boundary levels in this RFC generalize the durability
  problem identified there beyond one specific broker crate.
- `docs/issue-drafts/mqtt-raw-envelope-contract.md` -- the PUBLISH-to-
  LogRecord mapping contract this RFC's `mqtt_ingress` capability must carry
  enough metadata to satisfy, independent of client vs. server origin.
- `docs/issue-drafts/mqtt-sparkplug-extension.md` -- prior design sketch for
  a Sparkplug extension. This RFC supersedes its extension-dependency shape
  with compile-time composition over common MQTT code.
- `docs/issue-drafts/ms-mqtt-client-pluggable-tls-crypto.md` -- upstream
  crypto-provider adoption issue for the selected `rust-mqtt-client`.
- `docs/issue-drafts/mqtt-bounded-inbound-publish-flow-control.md` and
  `mqtt-explicit-qos1-acknowledgement-drop-policy.md` -- the other focused
  upstream collaboration items needed before using `rust-mqtt-client` for
  ingress.
- Broker survey research (RMQTT, Akasa, `ntex-mqtt`, Mosquitto, and other
  Rust MQTT broker crates) conducted during this design's development:
  `rumqttd`, RMQTT, and Akasa were all found unsuitable as an embeddable
  `extension:mqtt_server` foundation (crypto-provider hardcoding, missing
  shutdown/drain APIs, or `!Send`-incompatible runtime ownership); `ntex-mqtt`
  was identified as the most promising building block (protocol framework,
  not a broker, with app-controlled PUBACK timing and `!Send`-friendly
  types). Direct inspection of its published `Cargo.toml`
  (`ntex-mqtt` 9.0.0-beta.0) confirms it has no forced TLS/crypto dependency
  of its own; see "MQTT host service: server framework and a multi-core
  follow-on design" above for the remaining runtime-ownership question that
  must be resolved by an implementation spike.
- `docs/load-balancing.md` -- the existing per-CPU `SO_REUSEPORT` listener
  guidance and the "Internal Topic-Based Split" broadcast pattern this RFC's
  multi-core MQTT host service design reuses directly for cross-shard
  PUBLISH relay, rather than introducing new cross-core broker plumbing.
- `docs/extension-system-architecture.md`, `docs/extension-requirements.md`,
  `docs/topic-architecture.md`, `crates/engine/src/capability/`,
  `crates/engine/src/extension/builder.rs`,
  `crates/engine/src/extension/readiness.rs` -- current engine mechanics this
  RFC's design is constrained by and must not overstate.

## Unresolved questions

- What is the exact Rust trait/async signature for `mqtt_ingress` and
  `mqtt_egress`? (Reference-level implementation detail, deferred to the
  tracking issue.)
- What is the minimal immutable `SparkplugDecodeContext` carried by the
  representation? It must be sufficient for deterministic alias and death
  materialization without retaining the whole mutable session table or
  creating per-message copies of unchanged definitions.
- Should `extension:mqtt_server` support more than one listener (e.g.,
  plaintext + TLS on different ports) behind a single capability instance,
  or should multiple listeners require multiple extension instances?
- What is the correct default ack-boundary level for `receiver:mqtt` when a
  user does not specify one -- `admitted` (safe today, weaker guarantee) or
  `durable` (stronger guarantee, but silently degraded in fan-out topic
  graphs until the topic runtime's `all`-consensus mode is complete)?
- How should loop prevention be configured, if at all -- always-on
  DFE-local suppression, or an explicit opt-in for pipelines that intentionally
  want to observe local-service and local-pipeline publications?
- Which subset of Sparkplug events is materialized in the first release beyond
  node/device deaths: births, sequence gaps, rebirth requests, or all protocol
  transitions?
- Which Sparkplug STATE profile is the default? Sparkplug 2.2 uses
  `STATE/{host_id}` with `ONLINE`/`OFFLINE`; Sparkplug 3.0 uses
  `spBv1.0/STATE/{host_id}` with a timestamped JSON payload. The configured
  profile determines both topic and payload and must be tested against the
  target device fleet.
- How should a future multi-core server preserve client sessions, retained
  messages, and Will ownership without shared mutable broker state? This RFC
  proposes an answer (per-shard state plus broadcast/topic-split relay, see
  "MQTT host service: server framework and a multi-core follow-on design"
  above); it remains unvalidated by an implementation or benchmark and should
  be treated as a design sketch, not a proven result.
- Can the common MQTT crate drive `ntex-mqtt`'s handshake/publish/control
  service factories directly against a DFE-core-owned connection, or does
  `ntex-net`'s own worker/reactor ownership force either an adapter layer or
  falling back to `ntex-mqtt` as a pure wire-codec dependency with a
  hand-written dispatch loop? **Resolved** by source inspection (see "MQTT
  host service: server framework and a multi-core follow-on design" above):
  `ntex-net`'s public `ntex_net::tokio::Reactor` plus its `Reactor`-trait
  `from_tcp_stream` method, together with
  `ntex_mqtt::MqttServer`'s direct `Service<St, IoBoxed>` implementation plus
  `ntex_service::Pipeline::new(...).call_static(io)` together let a
  DFE-core-owned accept loop drive `ntex-mqtt` without any adapter layer and
  without ceding socket or runtime ownership to `ntex-net`'s own server/worker
  harness. This is now confirmed against a running implementation: the
  `extension:mqtt_server` scaffold (`crates/contrib-extensions/src/mqtt_server/`)
  builds this exact call chain, compiles, passes `cargo clippy -D warnings`,
  and its dependency tree carries no `ring`/`aws-lc-rs`/`openssl`. Remaining
  work is functional hardening (retained messages, Will, multi-listener,
  thread-per-core sharding), not re-validating this integration path.

## Future possibilities

- Additional protocol-specialized MQTT service extensions using the same
  compile-time composition pattern as `extension:mqtt_sparkplug`, without
  requiring extension dependencies.
- Additional pluggable MQTT representations using the same transport/codec
  split, including syslog-like payload families and application-specific
  device protocols.
- Mixed-signal Sparkplug materialization after the pdata model can represent
  metrics and lifecycle logs in one message while sharing resource/scope
  identity.
- A conformance/interop test suite driven by Mosquitto and `paho` clients
  against both `extension:mqtt_client` (dialing a real Mosquitto instance)
  and `extension:mqtt_server` (accepting real `paho` client connections),
  satisfying the user's stated first milestone using standard external
  tooling rather than a bespoke test harness.
