# Add an opt-in contrib `receiver:mqtt` embedding rumqttd

## Status

Draft, and **gated**. This is an implementation-ready feature specification
for the receiver's design, but it is not a green light to start
implementation against stock rumqttd 0.20.0 as embedded today. A spike
against the actual pinned rumqttd 0.20.0 source and behavior has already
been performed (not merely inferred from documentation), and it verified
several blocking gaps -- not open questions -- that make rumqttd 0.20.0, used
as-is, unsuitable for a production-grade receiver in this codebase. These are
summarized in
[Verified rumqttd 0.20.0 blockers (gate implementation)](#verified-rumqttd-0200-blockers-gate-implementation)
immediately below and must each be resolved, by upstream contribution,
adapter, fork, or an explicit and reviewed scope reduction, before
implementation begins. A smaller set of remaining, genuinely open questions
is tracked separately in
[Decisions requiring a rumqttd spike](#decisions-requiring-a-rumqttd-spike).

## Summary

Add `receiver:mqtt`, an opt-in, feature-gated contrib receiver that embeds
[`rumqttd`](https://github.com/bytebeamio/rumqtt/tree/main/rumqttd) (an
embeddable MQTT 3.1.1 / MQTT 5 broker library) directly inside the OTAP
Dataflow process. The receiver terminates MQTT client connections itself
(there is no external broker dependency), and converts every inbound
`PUBLISH` packet into exactly one OTLP `LogRecord`, using the mapping already
specified by
[mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md). This document
does not redefine that mapping; it defines the receiver that produces
`PUBLISH` packets for the contract to consume.

This is a "raw" receiver: it does not parse, validate, or interpret payload
content. In particular, it must not implement Eclipse Sparkplug B semantics
(topic namespace conventions, protobuf payload decoding, alias resolution,
birth/death certificate handling, or any Sparkplug-specific behavior). A
Sparkplug-aware component, if ever built, is a separate processor or receiver
layered on top of this one's raw output.

## Motivation

MQTT is a common ingestion path for IoT and OT telemetry. Operators who
already run OTAP Dataflow at the edge want to accept MQTT `PUBLISH` traffic
from field devices without standing up and operating a separate broker
process (Mosquitto, EMQX, VerneMQ) purely to bridge into OTLP. Embedding a
broker library keeps the deployment footprint to a single process and a
single supported artifact.

## Verified rumqttd 0.20.0 blockers (gate implementation)

The following were confirmed against rumqttd 0.20.0 itself, not inferred
from documentation, and are treated as **production blockers**, not minor
uncertainties. Implementation must not proceed against stock rumqttd 0.20.0
until each is resolved through one of: (a) an upstream rumqttd contribution,
(b) a maintained fork carrying equivalent patches, (c) a thin internal
adapter crate that enforces the missing guarantee at a layer this project
controls, or (d) an explicit, reviewed, and prominently documented scope
reduction where a workaround is genuinely acceptable. This document
specifies the receiver assuming that gate is satisfied; closing the gate
itself is a prerequisite piece of work, tracked here, not something this
design document can resolve by itself.

1. **No public shutdown/stop API; threads are process-lifetime.**
   `Broker::start()` is blocking and spawns the router thread and one
   thread per configured listener (each running its own current-thread
   `tokio` runtime) for the lifetime of the process. rumqttd 0.20.0 exposes
   no public API to stop the router, close a listener's bound socket, or
   join those threads on demand. Consequence: this receiver's node-level
   drain and shutdown (see
   [Drain, shutdown, and reload](#drain-shutdown-and-reload)) cannot
   actually release listener sockets or stop broker threads; only process
   exit does. This blocks graceful pipeline reconfiguration, config reload,
   and any test or deployment scenario that expects a stopped node to free
   its listening ports.
2. **QoS 1 PUBACK is queued before commit-log append and before internal
   `Link` consumption; no path exists for downstream ack.** Verified
   sequencing in the pinned version: the router queues PUBACK for send
   *before* the PUBLISH is appended to the bounded in-memory commit log,
   and therefore before this receiver's internal `LinkRx` (or any other
   subscriber `Link`) has consumed it. This is not a current-API
   limitation that a different call sequence could work around; the
   sequencing itself precludes downstream (OTAP pipeline / export) success
   from ever influencing PUBACK. This receiver must be documented as
   providing broker-ingest-only QoS semantics with no end-to-end delivery
   guarantee, full stop -- see
   [QoS semantics and the ACK boundary](#qos-semantics-and-the-ack-boundary).
3. **The bounded commit log silently evicts oldest segments after PUBACK
   has already been sent.** When a topic's commit log exceeds its
   configured bound (`max_segment_size` / `max_segment_count`), rumqttd
   evicts the oldest segment to stay within budget. This eviction happens
   after the publisher already received a successful QoS 1 PUBACK for the
   evicted data, and is not surfaced through any public event, counter, or
   callback. A backpressured or slow-draining internal `Link` (exactly the
   condition this receiver's own admission/backpressure design induces
   under memory pressure; see
   [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure))
   can therefore have its unread backlog silently discarded with no
   observability on either side of the broker. See
   [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss)
   for why this is rejected as a default and what is required before v1 can
   rely on rumqttd's built-in retention as its only safeguard.
4. **The internal `Link` forward path omits the publishing client's
   identity and most MQTT 5 properties.** The `Forward` value delivered to
   an internal `Link` subscriber (as opposed to data observed directly on
   a remote connection) does not reliably carry the original publisher's
   client identity, and most MQTT 5 `PublishProperties` (payload format
   indicator, message expiry interval, content type, response topic,
   correlation data, topic alias, subscription identifiers, user
   properties) are not populated on that path in the pinned version. This
   narrows what
   [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md) can
   actually populate when this receiver is the source: every "when
   present" row in that contract remains contract-compliant if the
   property is genuinely unavailable, but the practical MQTT 5
   fidelity of this receiver is materially lower than a raw wire capture
   would suggest, and that gap must be stated plainly rather than implied
   away by the contract's "when present" wording. See
   [Internal `Broker::link` use](#internal-brokerlink-use) for the two
   options (documented v1 scope reduction vs. an upstream/adapter hook that
   exposes full wire-level properties to internal subscribers) and pick one
   explicitly before implementation, rather than discovering the gap in a
   test written after the fact.
5. **Threading model confirmed as one router thread plus one current-thread
   `tokio` runtime per configured listener.** This matches what was
   inferred from source review in
   [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component)
   and is restated here as a verified fact, not an assumption: every
   design decision in that section (the `shared::Receiver` requirement, the
   process-wide singleton `Broker`, the single-core allocation requirement)
   stands as specified, with higher confidence than the original source
   read alone provided.

## Non-Goals

- Sparkplug B decoding, topic-namespace interpretation, or any
  payload-content-aware behavior. This receiver is content-agnostic; see the
  [envelope contract](mqtt-raw-envelope-contract.md#design-principle-mechanical-projection-not-semantic-interpretation)
  for the "mechanical projection only" rule this receiver must follow.
- QoS 2 (rumqttd's router accepts QoS 2 on the wire in places, but this
  receiver does not promise QoS 2 delivery semantics; see
  [QoS semantics and the ACK boundary](#qos-semantics-and-the-ack-boundary)).
- Acting as an MQTT bridge or client that connects outward to another broker
  (that would be a `receiver:mqtt_client` or `exporter:mqtt`, not this
  component; the future MQTT exporter described in the envelope contract is
  out of scope here).
- MQTT over WebSocket in v1 (rumqttd supports it behind its own `websocket`
  feature; left for a follow-up once the TCP/TLS path is validated).
- Broker clustering / multi-node MQTT (rumqttd's `cluster` config is
  unimplemented upstream at the time of writing; not used here).
- Redefining the PUBLISH-to-LogRecord attribute mapping, which is owned by
  [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md).
- Guaranteeing that MQTT PUBACK reflects successful OTLP export; see
  [QoS semantics and the ACK boundary](#qos-semantics-and-the-ack-boundary)
  for the verified reason this is not achievable at all with rumqttd
  0.20.0's internal ack sequencing (not merely unexposed by its current
  public API), and is therefore explicitly out of scope for v1.

## Component placement, feature gate, and URN

- Crate: `crates/contrib-nodes`, alongside `kafka_receiver` and
  `etw_receiver`, at `src/receivers/mqtt_receiver/`. This follows the
  existing convention that vendor-library-backed receivers with an optional,
  potentially heavy native/transitive dependency live in `contrib-nodes`
  behind a Cargo feature, not in `core-nodes`
  (`crates/contrib-nodes/Cargo.toml` gates `kafka-receiver` behind
  `dep:rdkafka`; the same pattern applies here with `dep:rumqttd`).
- Cargo feature: `mqtt-receiver`, added to `contrib-nodes`'s
  `[features]` table and to the `contrib-receivers` umbrella feature,
  mirroring `kafka-receiver` and `etw-receiver`. Not part of any default
  feature set; a user must opt in explicitly, consistent with "opt-in
  contrib" in the task description.
- URN: `urn:otel:receiver:mqtt`, expandable to the shortcut form
  `receiver:mqtt` per
  [urns.md](../urns.md#otel-shortcut-form). Despite living in the `contrib`
  crate, this is a first-party OTel-namespaced URN, following the existing
  precedent that "the namespace reflects ownership/standardization of the
  node type, not the Rust crate ... that implements it"
  (see [urns.md](../urns.md)).
- Component naming: primary metric set `receiver.mqtt`, matching the
  `<component_kind>.<component_name>` convention in
  [AGENTS.md](../../AGENTS.md#component-naming-conventions).
- Component inventory: register with
  `#[otel_arrow_dfe_engine::component_inventory(category = Receiver, ...)]`
  next to the `#[distributed_slice(OTAP_RECEIVER_FACTORIES)]` static, per
  [component-inventory.md](../component-inventory.md). The `protocol`
  attribute value must come from the controlled vocabulary in
  `crates/component-inventory-syntax/src/lib.rs`'s `Protocol` enum,
  which currently has no `Mqtt` variant (only `Grpc`, `Http`, `Tcp`, `Udp`,
  `Kafka`, `Syslog`, `Otlp`, `Otap`). Because this is a first-party
  (`urn:otel:`) component, it must use a known value, not `Custom`
  (see [component-inventory.md](../component-inventory.md#controlled-attribute-values-protocol-auth)).
  **Action required as part of this change:** add an `Mqtt` variant to that
  enum (and its `as_str`/`parse` tables) before annotating the factory;
  this is a small, mechanical addition to a shared vocabulary file, not a
  Cargo manifest or unrelated implementation change, and is called out here
  so implementers do not skip it and fall back to `Custom` by mistake.
  `auth` should report `none`, `basic`, or `custom` depending on which
  authentication mode is active (see [Auth](#auth)).

## Runtime placement: rumqttd is a shared, cross-thread component

This is the most consequential architectural decision for this receiver, so
it is called out before configuration details.

### rumqttd is not a `!Send`, single-core library

The engine's default and preferred shape for a receiver is a `!Send` local
receiver run via `spawn_local` on one core's `LocalSet`, per
[rust-review.instructions.md](../../.github/instructions/rust-review.instructions.md)
rule 1 ("prefer `!Send` futures") and the pattern used by
`syslog_cef_receiver` and `kafka_receiver`
(`ReceiverWrapper::local`). rumqttd cannot be embedded that way:

- `rumqttd::Config` -> `Broker::new(config)` immediately spawns the
  `Router` event loop on its own dedicated OS thread
  (`router.spawn()` in `rumqttd/src/router/routing.rs`, a blocking
  `thread::Builder::spawn` running a synchronous `flume::Receiver::recv`
  loop -- not a `tokio` task).
- `Broker::start()` is **blocking** and spawns one additional OS thread
  **per configured listener** (`v4`, `v5`, `ws`, plus `bridge` and `metrics`
  timer threads if configured), each running its own single-threaded
  `tokio` current-thread runtime (`rumqttd/src/server/broker.rs`). None of
  this is exposed as a future we could `spawn_local` ourselves;
  `Broker::start()` owns thread creation, and -- confirmed against rumqttd
  0.20.0, not merely read from source -- these threads run for the
  lifetime of the process: there is no public API to stop the router, close
  a listener's bound socket, or join any of these threads on demand. See
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  item 1 and
  [Drain, shutdown, and reload](#drain-shutdown-and-reload) for the
  consequences this has for this receiver's own lifecycle contract.
- The public embedding API, `Broker::link(client_id) -> (LinkTx, LinkRx)`,
  crosses that thread boundary using `flume` channels and a
  `parking_lot::Mutex`-guarded buffer (`rumqttd/src/link/local.rs`). `LinkTx`
  and `LinkRx` are `Send`. This is the intended "embed a local subscriber"
  API and is what "internal `Broker::link` use" in this document's scope
  refers to.
- rumqttd's own external-auth hook,
  `ConnectionSettings::set_auth_handler`, requires
  `Fn(ClientId, AuthUser, AuthPass) -> O + Send + Sync + 'static` where
  `O: IntoFuture<Output = bool>` (`rumqttd::AuthHandler` is a boxed
  `dyn Fn(...) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync`).
  Any auth extension capability handle passed into it must therefore be
  `Send + Sync`, i.e. a `shared`-execution-model extension per
  [extension-requirements.md](../extension-requirements.md#extension-scopes),
  not a `local` one, regardless of how lightweight the check is.

Consequence: `receiver:mqtt` cannot be authored as a bare `!Send`
`local::Receiver`. Per
[rust-review.instructions.md](../../.github/instructions/rust-review.instructions.md)
rule 2, this cross-thread relationship must be justified in code comments
and in the PR description, and it is justified precisely by the points
above: the router and listener threads are owned and started by the
embedded library, not chosen by us.

### Proposed architecture

- Use `ReceiverWrapper::shared` (the engine already supports `Send`
  receivers for exactly this situation; see `crates/engine/src/receiver.rs`,
  `shared::Receiver`), not `ReceiverWrapper::local`.
- The `shared::Receiver` implementation owns one `LinkRx` obtained from
  `Broker::link(client_id)`, having pre-subscribed to the receiver's
  configured topic filters through that link
  (`LinkTx::subscribe`/`try_subscribe`). It polls `LinkRx` for
  `Notification::Forward(Forward { publish, properties, .. })` values inside
  the receiver's normal `recv`/effect-handler loop (the same loop shape as
  every other `shared::Receiver`), and forwards each decoded `Forward` to
  the envelope-contract mapping function to produce one `OtapPdata` log
  message per `PUBLISH`.
- The embedded `Broker` itself (`Router` thread, listener threads) is a
  **process-wide singleton**, not one instance per pipeline replica. It is
  created once, on first use, guarded by a `std::sync::OnceLock` (or
  equivalent) keyed by node identity, and subsequent pipeline replicas that
  reference the same configured `receiver:mqtt` node reuse the same
  `Broker` handle to create their own additional `Broker::link` connections
  if and when multi-replica fan-out is supported (see next section). This
  mirrors the `Router` being fundamentally singular (it owns the one
  in-process subscription/routing table); there is no way to run two
  independent `Router`s and have them see each other's publishes or
  retained state (see [Decisions requiring a rumqttd spike](#decisions-requiring-a-rumqttd-spike)).

### Why this cannot silently scale across the default `all_cores` allocation

The engine documents that "the pipeline engine will start multiple instances
of the same pipeline in parallel on different cores, each with its own
receiver instance" (`crates/engine/src/shared/receiver.rs`), and
`policies.resources.core_allocation` defaults to `all_cores`
(see [configuration.md](../configuration-model.md)). Existing socket-based
receivers (OTLP, syslog) are safe under that default because each per-core
instance opens its own independent listening socket (optionally via
`SO_REUSEPORT`) and has no state that must be shared across cores.

rumqttd's `Router` is exactly the opposite: it is the single, process-wide
place where topic subscriptions are matched against published topics, and
where retained-message and (for persistent sessions) subscription state
lives. If N independent `Broker` instances were started, one per core
replica, a publisher whose TCP connection happens to land (via
`SO_REUSEPORT` or independent listen sockets) on instance A's `Router` would
never be visible to a subscriber connected to instance B's `Router` --
publish/subscribe matching would silently and non-deterministically break
depending on which core accepted which connection. This is a correctness
bug, not a performance question, so it must be prevented rather than
tuned around.

**v1 decision:** the pipeline containing `receiver:mqtt` must run with a
resolved core allocation of exactly one core
(`policies.resources.core_allocation: { type: core_count, count: 1 }` or an
equivalent single-core `core_set`). The receiver factory's `create()`
function fails fast with an actionable `InvalidUserConfig` error, using
whatever mechanism the pipeline-instance bootstrap exposes for reporting the
resolved replica count/index to a node at construction time, if it is
invoked for a second replica of the same pipeline (the process-wide
`OnceLock` guard doubles as this detection: a second `create()` call for a
node id that already owns a started `Broker` on a *different* pipeline
instance is rejected rather than silently sharing one `LinkRx` across two
receiver instances, which would split -- not duplicate -- delivery in an
unspecified way). Multi-core fan-out of the *decoded* OTLP output (after the
receiver, using ordinary downstream routing) remains available; only the
embedded broker/receiver ingestion path is single-core in v1.

## Configuration shape and validation

Config type, following the `syslog_cef_receiver` pattern (plain
`#[derive(Deserialize)]` structs with `#[serde(deny_unknown_fields)]`,
bounded numeric types, `humantime_serde` durations) rather than the
`kafka_receiver` free-form `HashMap<String, String>` pass-through, because
rumqttd's config surface is small and well-typed enough to model directly:

```yaml
type: receiver:mqtt
config:
  listeners:
    - name: mqtt-tcp
      protocol: v5              # v4 | v5 (one physical listener speaks one version)
      listening_addr: "0.0.0.0:1883"
      tls: null                 # optional TlsServerConfig-shaped block, see below
      max_connections: 1024
      max_payload_bytes: 262144
      max_inflight_count: 100
      connection_timeout: "30s"
      next_connection_delay: "1ms"
  auth:
    mode: none                  # none | static | extension
    # static:
    #   credentials:
    #     device-1: "s3cret"
    # extension:
    #   auth_check: my_auth_extension  # capability binding, see Auth
  topics:
    subscribe:
      - "telemetry/#"
      - "devices/+/status"
    max_topic_bytes: 512
  limits:
    max_user_properties: 32
    max_user_property_bytes: 4096
    max_body_bytes: 262144
    client_id_prefix: "otap-mqtt-"
  queue:
    inbound_capacity: 4096       # bounded bridge channel from the Link into the pipeline
  client_id: "otap-dataflow-mqtt"
  router:
    max_outgoing_packet_count: 200
    max_segment_size: 10485760
    max_segment_count: 10
```

Validation rules (enforced in `TryFrom<&Value>`/`from_config`, returning
`otel_arrow_dfe_config::error::Error::InvalidUserConfig`, matching the
pattern used across existing receivers):

- `listeners` must be non-empty; at least one physical listener is required
  (mirrors rumqttd's own `Broker::start()` check that at least one of
  `v4`/`v5`/`ws` is configured).
- Each listener's `name` must be unique (rumqttd keys `v4`/`v5` server maps
  by name).
- `listening_addr` must parse as a `SocketAddr`; binding two listeners to
  the same address is a config error, not a runtime bind-failure surprise.
- `protocol` must be `v4` or `v5`; `ws` is rejected in v1 (see
  [Non-Goals](#non-goals)) with a clear "not yet supported" error rather
  than silently ignored.
- `max_connections`, `max_payload_bytes`, `max_inflight_count` must be
  positive (`NonZeroU32`/`NonZeroUsize`); these map directly onto rumqttd's
  `ConnectionSettings` fields (`max_payload_size`, `max_inflight_count`) and
  `RouterConfig::max_connections`.
- `topics.subscribe` must be non-empty and every entry must be a
  syntactically valid MQTT subscription filter (no empty levels, `#` only
  as the final level, `+` only as a whole level) -- validated with the same
  rigor a publish-side topic validator would use, since a malformed filter
  silently subscribing to nothing would be a confusing failure mode.
- `queue.inbound_capacity` must be positive; there is no unbounded option
  (see [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure)).
- `auth.mode: static` requires a non-empty `credentials` map; `extension`
  requires a `capabilities.auth_check` binding to resolve (validated at
  pipeline-resolution time the same way other capability bindings are, per
  [extension-requirements.md](../extension-requirements.md#capability-binding)).
- `tls`, when present, is validated the same way `TlsServerConfig` is
  validated elsewhere (cert/key pair present together, files exist or PEM
  parses), before being translated into rumqttd's own `TlsConfig` shape
  (see [Listeners, TLS, auth, and ACL](#listeners-tls-auth-and-acl)).

## Listeners, TLS, auth, and ACL

### Listeners

Each entry in `listeners` becomes one rumqttd `ServerSettings` inside
`Config.v4` or `Config.v5` (keyed by `name`). Because rumqttd owns the
`TcpListener::bind` call inside its own per-listener OS thread (see
[Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component)),
this receiver does not use the engine's shared listener-group /
`SO_REUSEPORT` machinery
(`crates/controller/src/listener_group.rs`) the way OTLP and syslog do --
there is exactly one listener per configured entry, owned by rumqttd, not
one per core. `receiver:mqtt` is therefore intentionally **not** added to
that module's `KNOWN_RECEIVER_URNS` list; doing so would misrepresent it as
a per-core listener.

### TLS

rumqttd's own `TlsConfig` enum (`Rustls { capath, certpath, keypath }` or
`NativeTls { pkcs12path, pkcs12pass }`) is file-path-only and has no PEM-blob
or hot-reload story, unlike the engine's `TlsServerConfig`
(`crates/config/src/tls.rs`), which supports in-memory PEM and
`watch_client_ca` hot-reload. This receiver accepts the engine's standard
`TlsServerConfig` shape in its YAML for consistency with every other
receiver, but at startup it is translated into rumqttd's narrower
`Rustls` variant:

- `cert_file`/`key_file` map directly to `certpath`/`keypath`.
- In-memory `cert_pem`/`key_pem` are rejected with a clear "file path
  required for the embedded MQTT broker" error in v1, since rumqttd has no
  in-memory certificate API.
- `client_ca_file` maps to `capath` if rumqttd's `Rustls` variant is
  extended to accept client-cert verification for mTLS, or is rejected if
  rumqttd's public API cannot express mTLS (see the spike list -- this is
  not yet confirmed from the public source alone).
- `watch_client_ca` / `reload_interval` are rejected as unsupported: rumqttd
  reads certificate files once at `Broker::new`/listener-thread start and
  has no reload path in its public API.
- `handshake_timeout` has no rumqttd equivalent exposed; document it as
  not enforced for this receiver rather than silently dropping the field
  without comment.

### Auth

Three modes, matching rumqttd's actual auth surface
(`rumqttd::ConnectionSettings.auth: Option<HashMap<String, String>>` and
`ConnectionSettings::set_auth_handler`):

- `none`: no CONNECT-time credential check (still subject to `tls` client
  certificate requirements if configured).
- `static`: a fixed username/password map, passed straight into
  `ConnectionSettings.auth`. Intended for simple device-fleet deployments
  with pre-shared credentials, not for anything requiring rotation.
- `extension`: binds a `capabilities.auth_check` extension per
  [extension-requirements.md](../extension-requirements.md), and installs it
  via `ConnectionSettings::set_auth_handler`. Because that handler's
  signature requires `Send + Sync`, the bound extension must use the
  `shared` execution model (see
  [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component)),
  not `local`; this receiver's own `create()` should reject a capability
  binding to a `local`-only extension instance with a clear
  "auth_check must be a shared-scoped extension for receiver:mqtt" error
  at validation time rather than a confusing generic type error at compile
  or bind time.

### ACL

rumqttd has **no per-topic publish/subscribe authorization hook** in its
current public API: `set_auth_handler` receives only
`(client_id, username, password)` at CONNECT time and returns a single
accept/reject decision for the whole connection; there is no callback
invoked per PUBLISH or per SUBSCRIBE with the topic name. Consequently:

- This receiver does not claim to provide topic-level ACLs in v1. This must
  be stated plainly in the component's README and its
  `component_inventory` `auth` attribute must not imply per-topic
  authorization exists.
- The closest available control is `topics.subscribe` (this receiver's own
  subscription list) combined with rumqttd's `dynamic_filters` connection
  setting (`ConnectionSettings.dynamic_filters`): when `false`, an external
  client's PUBLISH to a topic with no pre-existing filter/subscription is
  accepted by the router but not stored durably against a fresh filter,
  bounding (not eliminating) the resource impact of clients publishing to
  arbitrary topics. This receiver sets `dynamic_filters: false` by default
  and exposes it as an advanced override, defaulting to the safer,
  bounded behavior.
- A future per-topic ACL, if ever required, would need either a rumqttd
  upstream contribution or a fork; it is out of scope for this receiver and
  is called out as a known gap rather than worked around silently.

## Topic filters and OTLP log mapping

- `topics.subscribe` lists the MQTT subscription filters (may include `+`
  and `#` wildcards) this receiver subscribes to via its internal
  `Broker::link` connection, using `LinkTx::subscribe` once at startup for
  each configured filter (subscribing again after every reconnect is not
  applicable here since the internal link does not "reconnect" the way an
  external client would -- it is a long-lived in-process connection to the
  same `Router`).
- Which topic a given `PUBLISH` arrived on is carried on the decoded
  `Forward.publish.topic` value and mapped verbatim into `mqtt.topic` /
  `messaging.destination.name`, per the envelope contract's
  [standard attributes](mqtt-raw-envelope-contract.md#standard-attributes-messaging-network-client-server)
  table. This receiver does not decompose topic segments, does not derive
  routing decisions from topic content, and does not filter *out* messages
  post-subscription based on topic pattern beyond what `topics.subscribe`
  itself expresses -- consistent with the contract's explicit
  ["topic-based routing or filtering policy" being out of scope for the mapping](mqtt-raw-envelope-contract.md#scope)
  and squarely in scope for *this* receiver's subscription configuration
  instead.
- Retained messages: a `PUBLISH` with the `RETAIN` flag set that arrives
  because of a fresh SUBSCRIBE match is delivered like any other `Forward`
  and produces one `LogRecord` with `mqtt.retain = true`, per the contract.
  This receiver does not special-case retained delivery (no
  deduplication, no "retained snapshot vs. live" distinction in the
  emitted attributes) beyond what the contract already defines.

## Client, connection, packet, and queue limits

| Limit | Config field | rumqttd mapping | Purpose |
| --- | --- | --- | --- |
| Max simultaneous MQTT client connections | `router.max_connections` (process-wide) | `RouterConfig::max_connections` | Bounds total connection/session memory; matches upstream default order of magnitude (rumqttd's own example config uses 10010). |
| Max connections accepted per listener before backing off | `listeners[].max_connections` | `ServerSettings`/accept-loop pacing via `next_connection_delay` | Bounds accept-rate abuse per physical listener. |
| Max payload size per PUBLISH | `listeners[].max_payload_bytes` and `limits.max_body_bytes` | `ConnectionSettings::max_payload_size` | Rejects oversized packets at the protocol layer before they reach this receiver's own body-size validation from the envelope contract. |
| Max unacknowledged QoS 1/2 packets per connection | `listeners[].max_inflight_count` | `ConnectionSettings::max_inflight_count` | Bounds broker-side in-flight state per external client connection. |
| Max outgoing packets per subscription scheduling pass | `router.max_outgoing_packet_count` | `RouterConfig::max_outgoing_packet_count` | Bounds how much one scheduling pass can push toward any one subscriber, including this receiver's own internal `Link`. |
| Commitlog segment size / count | `router.max_segment_size`, `router.max_segment_count` | `RouterConfig::max_segment_size`/`max_segment_count` | Bounds per-topic in-memory retained/backlog storage inside the router; this is process memory, separate from and in addition to this receiver's own `queue.inbound_capacity`. |
| Internal bridge queue from `Link` to the pipeline | `queue.inbound_capacity` | n/a (this receiver's own bounded channel) | See [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure). |
| MQTT 5 User Properties per message | `limits.max_user_properties`, `limits.max_user_property_bytes` | n/a (enforced by this receiver per the envelope contract) | Matches the [envelope contract's validation table](mqtt-raw-envelope-contract.md#validation) exactly; not a rumqttd-level limit. |

All limits are finite by default; none of these fields accept an "unbounded"
sentinel, consistent with the project's bounded-resources review
requirement.

## Internal `Broker::link` use

- Exactly one `LinkTx`/`LinkRx` pair is created per running `Broker`
  instance (see [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component)),
  via `broker.link(client_id)`, where `client_id` is `client_id` from
  config (defaulting to a stable, receiver-owned identifier, not a random
  UUID, so that logs and rumqttd's own console/metrics can be correlated to
  "the receiver" across restarts of the calling process without the
  identifier changing).
- Immediately after obtaining the link, the receiver issues one
  `LinkTx::subscribe` (or `try_subscribe`) per entry in `topics.subscribe`.
  These are synchronous, non-blocking sends into the router's event queue;
  failures (e.g. `TrySendError`) are treated as startup errors.
- The receive loop calls the async form of receive on `LinkRx`
  (`recv_async`/equivalent) inside the `shared::Receiver`'s normal
  `recv()`/select loop, alongside the control-message channel, exactly like
  every other receiver's main loop shape -- the only difference from a
  `local::Receiver` is the `Send` bound on the receiver type itself, not the
  loop structure.
- `LinkTx` is retained only for the initial subscribe calls (and,
  optionally, control-plane operations like dynamic unsubscribe on
  config reload -- see [Drain, shutdown, and reload](#drain-shutdown-and-reload));
  it is not used to publish application data back into the broker, since
  this is a receiver, not a bridge.
- The receiver never calls `Broker::link` more than once per running
  `Broker`; a second internal link (for example, to parallelize draining)
  would receive its own independent copy of every matching `PUBLISH`
  (MQTT delivers to each subscriber, and rumqttd's `Link` is modeled as a
  subscriber), which would duplicate, not shard, the data -- this is the
  same reasoning that rules out one link per pipeline replica in
  [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component).
- **Confirmed limitation:** the `Forward` value delivered on this internal
  `Link` does not reliably carry the original publisher's client identity,
  and does not carry most MQTT 5 `PublishProperties` (payload format
  indicator, message expiry interval, content type, response topic,
  correlation data, topic alias, subscription identifiers, user
  properties) -- see
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  item 4. Two options exist and one must be chosen explicitly before
  implementation:
  1. **Documented v1 scope reduction.** Ship with the understood
     limitation that most MQTT 5 property-derived attributes in
     [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md#mqtt-specific-attributes-mqtt-namespace)
     will be absent for this receiver even when the original wire packet
     carried them, document this prominently in the README next to the
     QoS-semantics caveat, and treat the contract's "when present" wording
     as the (unsatisfying but technically compliant) escape hatch.
  2. **Upstream/adapter hook.** Pursue an upstream change (or an adapter
     that taps the connection/decode layer before properties are dropped
     on the way into the internal `Link` path) that preserves full
     `PublishProperties` through to `Forward`. This is materially more
     work and is not assumed available for v1 planning; it belongs in the
     same upstream/fork/adapter track as the other items in
     [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation).

  Absent a decision, this document defaults to option 1 for v1 scope, with
  option 2 tracked as a follow-up once the blocking gate items are
  resolved.
- A related, smaller consequence: because the original publisher's
  identity and connection are not observable through the internal `Link`,
  the envelope contract's `network.peer.address` / `network.peer.port` /
  `client.address` / `client.port` fields
  (see [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md#standard-attributes-messaging-network-client-server))
  describe this receiver's own internal connection to the embedded broker,
  which has no real socket -- these fields should be left unset by this
  receiver rather than populated with a synthetic or misleading value, since
  they were defined by the contract with an external-broker, client-mode
  receiver in mind, not an embedded-broker one.

## DFE admission, backpressure, and memory pressure

This receiver participates in the engine's admission and memory-pressure
model the same way `syslog_cef_receiver` does
(`otel_arrow_dfe_engine::admission`, `otel_arrow_dfe_engine::memory_limiter`),
adapted for the fact that inbound data arrives from a cross-thread `LinkRx`
rather than directly from a socket read:

- A `LocalReceiverAdmissionState`-equivalent **shared** admission state
  (the `Send` counterpart, since this is a `shared::Receiver`) is bootstrapped
  from `pipeline.memory_pressure_state()` at construction and updated on
  every `NodeControlMsg::MemoryPressureChanged` control message, exactly
  like the syslog receiver's `admission_state.apply(update)`.
- `AdmissionDimension::Messages` is the metered dimension (one `PUBLISH` is
  one framed application message, matching the dimension's own
  documentation in `crates/engine/src/admission/models/dimension.rs`), bound
  via `AdmissionBinder` against the pipeline's configured
  `rate_limiters: [name]` policy, the same opt-in mechanism documented in
  the [syslog receiver's pressure-aware rate admission section](../../crates/core-nodes/src/receivers/syslog_cef_receiver/README.md).
- **Before** dequeuing the next `Forward` from `LinkRx` (not after
  decoding it into a `LogRecord`), the receiver checks
  `admission_state.should_shed_ingress()`. If ingress should be shed under
  hard memory pressure, the receiver stops draining `LinkRx` for that tick
  rather than pulling the message and dropping it after decode. This is
  where [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  item 3 becomes directly relevant, not just theoretical: leaving data in
  rumqttd's per-topic commit log while this receiver sheds ingress is
  exactly the condition under which rumqttd silently evicts the oldest
  segment once its bound is exceeded, discarding already-PUBACKed data with
  no notification. See
  [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss)
  for the resulting requirement on this admission path: pausing `Link`
  consumption is bounded backpressure from this receiver's point of view,
  but it is not safe backpressure from an at-least-once-delivery point of
  view unless the commit log is sized and monitored accordingly.
- `queue.inbound_capacity` (see
  [Client, connection, packet, and queue limits](#client-connection-packet-and-queue-limits))
  is a second, independent bound: the internal channel used to move a
  decoded `Forward`/mapped `OtapPdata` from wherever it is decoded to the
  point where it is handed to the pipeline's downstream `Sender`. Because
  this receiver is `Send`/shared, this bridge uses `tokio::sync::mpsc` (or
  the engine's `SharedSender`/`SharedReceiver` channel types directly, if
  the mapping step runs inline in the same task as the `LinkRx` poll,
  which is the preferred v1 shape and avoids needing a second bounded
  channel at all -- see the spike list for whether inlining is safe given
  `LinkRx`'s locking).
- Memory-pressure disconnects (the syslog receiver's
  `memory_pressure.disconnect` pattern) do not apply the same way here: this
  receiver does not own individual external TCP connections (rumqttd does),
  so "disconnect" is not an available lever without either (a) exposing a
  disconnect-by-client-id call through `Broker` (not confirmed public; see
  spike list) or (b) temporarily unsubscribing via `LinkTx::unsubscribe`
  under hard pressure, which stops new deliveries without severing external
  client connections. Option (b) is the v1 default: under hard pressure,
  the receiver unsubscribes from all configured filters and re-subscribes
  once pressure returns to normal, rather than attempting to disconnect
  external MQTT clients it does not directly manage. Unsubscribing does not
  stop the commit log from continuing to receive and evict data for that
  topic on behalf of *other* subscribers or future re-subscription, so this
  option reduces this receiver's own backlog risk but does not, by itself,
  address item 3's silent-eviction risk for data published while
  unsubscribed.

## QoS semantics and the ACK boundary

This section deliberately narrows scope versus the client-side ack
discussion in
[mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md)
and
[mqtt-bounded-inbound-publish-flow-control.md](mqtt-bounded-inbound-publish-flow-control.md).
Those two documents describe a **client** library's manual-acknowledgement
API (an outbound MQTT client connecting to someone else's broker). This
receiver is the opposite role: it **is** the broker for its external MQTT
publishers.

**Verified, not inferred:** in rumqttd 0.20.0, the router queues the QoS 1
PUBACK for send *before* the PUBLISH is appended to the bounded in-memory
commit log, and therefore before this receiver's internal `Link` (or any
other subscriber `Link`) has consumed it. This is stronger than "the current
API does not expose a way to defer PUBACK" -- the sequencing itself makes
deferring PUBACK until downstream (OTAP pipeline / export) success
architecturally impossible without changing rumqttd's router internals. See
[Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
item 2.

Concretely:

- **QoS 0**: no acknowledgement exists at the MQTT level. The receiver
  reports delivery-to-pipeline success/failure only through its own
  telemetry (see [Telemetry](#telemetry)), never through MQTT.
- **QoS 1**: the external publisher's PUBACK reflects "the embedded broker
  accepted responsibility for this message," not "this message was
  successfully exported as an OTLP log," and cannot be made to reflect the
  latter with rumqttd 0.20.0's current internal sequencing. This must be
  documented prominently in the component's README as an explicit,
  permanent-until-upstream-changed limitation: **`receiver:mqtt` provides
  "broker-ingest-only" QoS 1 semantics to its external publishers, with no
  end-to-end delivery guarantee to OTLP, full stop.** This is not phrased
  as a spike question in this revision because the sequencing has already
  been verified; what remains open (tracked under
  [Decisions requiring a rumqttd spike](#decisions-requiring-a-rumqttd-spike))
  is only whether any upstream rumqttd change could alter this sequencing
  in a future version, not whether 0.20.0 itself behaves this way.
- **QoS 2**: not supported as a delivery guarantee by this receiver (see
  [Non-Goals](#non-goals)); if a publisher negotiates QoS 2 at the protocol
  level and rumqttd's router accepts it, the resulting `PUBLISH`, if
  forwarded to this receiver's `Link` at all, is mapped with
  `mqtt.qos = 2` per the envelope contract, but no additional delivery
  guarantee beyond what rumqttd itself provides is claimed.
- Backpressure from this receiver toward the pipeline (see
  [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure))
  therefore cannot be translated into "stop acking publishers" the way the
  client-side flow-control document envisions; the only backpressure lever
  available to this receiver is pausing its own `Link` consumption
  (leaving data in the router's bounded commit log) or temporarily
  unsubscribing, both described above -- and, per
  [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss),
  pausing consumption is itself bounded only by a silent-eviction risk, not
  a safe, observable backpressure signal. This asymmetry -- broker
  admission is decoupled from, and already acknowledged ahead of, subscriber
  consumption -- is the single most important semantic difference this
  document adds relative to the two client-focused issue drafts, and it
  should be called out in review as the reason this receiver cannot inherit
  those documents' acceptance criteria verbatim.

## Commit log retention and silent data loss

**Verified, and rejected as an acceptable default.** rumqttd's per-topic
commit log is bounded by `router.max_segment_size` and
`router.max_segment_count` (see
[Client, connection, packet, and queue limits](#client-connection-packet-and-queue-limits)).
When a topic's log exceeds that bound, rumqttd evicts the oldest segment to
stay within budget. Confirmed against rumqttd 0.20.0: this eviction happens
*after* the publisher has already received a successful QoS 1 PUBACK for the
data being evicted, and it is not surfaced through any public event,
counter, log line, or callback that this receiver could observe. A
publisher that received a successful PUBACK, and a subscriber (including
this receiver's own internal `Link`) that has fallen behind for longer than
the configured segment budget allows, can therefore both be left with no
indication that at-least-once delivery was violated.

This directly conflicts with this codebase's bounded-resources posture:
bounding memory is necessary but not sufficient if the bound is enforced by
silently discarding already-acknowledged data with no observability. A
receiver in this codebase must not present a QoS 1 accept to an external
party and then let that data disappear without any counter incrementing
anywhere.

**This is treated as a blocking gap, not a documented limitation to ship
around silently.** Before this receiver can claim any meaningful QoS 1
behavior, one of the following must be true:

1. **Upstream or adapter observability.** rumqttd gains (via upstream
   contribution, fork, or an adapter that instruments segment eviction at
   the point it occurs) a way for this receiver to detect eviction and
   increment a counter / emit an event identifying the affected topic and
   an approximate evicted-message count, even if it cannot prevent the
   eviction. This is the minimum acceptable bar: silent loss becomes
   observable loss.
2. **Upstream or adapter backpressure.** rumqttd gains a way to apply
   backpressure toward publishers (delay or reject new PUBLISH packets on
   a topic) before its commit log reaches the eviction threshold, so that
   the bound is enforced by refusing new admission rather than discarding
   old, already-acknowledged data. This is the stronger, preferred fix, but
   is not assumed available for v1 planning.
3. **A reviewed and bounded mitigation**, accepted only as an interim
   measure and only if items 1 and 2 are not feasible in the implementation
   timeframe: size `router.max_segment_size` / `router.max_segment_count`
   large enough, relative to this receiver's own `queue.inbound_capacity`
   and admission/backpressure recovery-time expectations (see
   [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure)),
   that eviction cannot occur within the maximum tolerated backpressure
   duration under the receiver's own configured limits, and document the
   derived bound (bytes and expected wall-clock tolerance) explicitly in
   the README and in `router.max_segment_size`/`max_segment_count`'s
   config documentation. This increases the process memory footprint (see
   [Client, connection, packet, and queue limits](#client-connection-packet-and-queue-limits))
   and is a mitigation, not a fix -- it narrows the silent-loss window, it
   does not close it, and it must be labeled as such everywhere it is
   documented.

Shipping v1 relying solely on rumqttd's built-in segment retention, with no
mitigation from the list above and no README disclosure, is explicitly
rejected by this design.

## Telemetry

Following the `syslog_cef_receiver` telemetry conventions
(`receiver.<name>` metric set, bounded `outcome`/`error.type` attributes, no
payload content in any signal).

### Metric sets

`receiver.mqtt`:

| Metric | Unit | Description |
| --- | --- | --- |
| `receiver.mqtt.received.items` | `{item}` | Number of `PUBLISH` packets observed from the internal `Link` before mapping. |
| `receiver.mqtt.forwards.items` | `{item}` | Number of mapped `LogRecord`s delivered to the pipeline send path, grouped by `outcome`. |
| `receiver.mqtt.rejections.items` | `{item}` | Number of `PUBLISH` packets rejected before pipeline admission, grouped by bounded `error.type` (`oversized_payload`, `oversized_topic`, `too_many_user_properties`, `oversized_user_property`, `memory_pressure`, `queue_full`). |
| `receiver.mqtt.connections.active` | `{connection}` | Number of external MQTT client connections currently accepted by the embedded broker, sampled from rumqttd's own connection count if exposed, else tracked via CONNECT/DISCONNECT notifications. |
| `receiver.mqtt.subscriptions.active` | `{subscription}` | Number of active internal subscription filters (normally equal to `len(topics.subscribe)`, dropping to zero while unsubscribed under hard memory pressure). |
| `receiver.mqtt.broker.errors` | `{error}` | Errors surfaced from the embedded broker/router/listener threads (bind failures, TLS handshake failures at the rumqttd layer, panics recovered at a thread boundary). |
| `receiver.mqtt.commit_log.evictions` | `{segment}` | Count of detected commit-log segment evictions, per topic, that occurred while data had not yet been consumed by this receiver's internal `Link`. **Not populated in v1 without the upstream/adapter observability described in [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss)**; until that gate is closed, this metric is either absent or defined as always zero, and that fact must be documented, not left implicit. |

Rate-admission outcomes are reported by the shared engine metric set
`admission.rate_limiter`, as with every other receiver
(see [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure)).

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `mqtt_receiver.start` | `info` | Embedded broker starting, with listener addresses and protocol versions. |
| `mqtt_receiver.broker.started` | `info` | Embedded broker's router and listener threads reported ready. |
| `mqtt_receiver.tls_enabled` | `info` | TLS was enabled for a listener. |
| `mqtt_receiver.subscribe.failed` | `error` | An initial `LinkTx::subscribe` call failed at startup; treated as a fatal startup error. |
| `mqtt_receiver.memory_pressure.unsubscribe` | `warn` | All configured subscriptions were dropped because hard memory pressure was active. |
| `mqtt_receiver.memory_pressure.resubscribe` | `info` | Subscriptions were restored after memory pressure returned to normal. |
| `mqtt_receiver.mapping.rejected` | `warn` | A `PUBLISH` failed envelope-contract validation and was dropped and counted (mirrors the contract's [validation table](mqtt-raw-envelope-contract.md#validation)). |
| `mqtt_receiver.drain_ingress.timeout` | `warn` | Ingress drain timed out while the internal `Link` still had buffered notifications. |
| `mqtt_receiver.broker.thread_panic` | `error` | A rumqttd-owned thread (router, listener, bridge, or metrics timer) terminated unexpectedly; the receiver transitions to a fatal error state rather than silently running with a degraded broker. |
| `mqtt_receiver.shutdown.listeners_not_released` | `warn` | Emitted on every `Shutdown` control message as a standing reminder that rumqttd 0.20.0 provides no API to release bound listener sockets or stop broker threads; see [Drain, shutdown, and reload](#drain-shutdown-and-reload). |
| `mqtt_receiver.commit_log.eviction_detected` | `warn` | A commit-log segment eviction was detected for a topic with unconsumed data. **Requires the upstream/adapter observability from [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss); not emittable in v1 without it.** |

## Errors

A dedicated `error.rs` (matching `kafka_receiver::error::KafkaReceiverError`
and the syslog receiver's use of `otel_arrow_dfe_engine::error::{Error,
ReceiverErrorKind}`) defines an `MqttReceiverError` enum covering at least:

- `InvalidConfig` (validation failures listed in
  [Configuration shape and validation](#configuration-shape-and-validation)).
- `SingleInstanceViolation` (a second pipeline replica attempted to start
  the same `receiver:mqtt` node; see
  [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component)).
- `BrokerStartFailed` (wraps rumqttd's own `server::broker::Error`, e.g.
  listener bind failure, TLS cert load failure, "at least one server config
  must be specified").
  `Config(String)` variant does not implement `std::error::Error`'s source
  chain particularly richly, so this receiver's error should preserve the
  original message text verbatim rather than re-deriving it.
- `SubscribeFailed` (wraps `rumqttd::link::local::LinkError`).
- `MappingRejected` (delegates to whatever typed rejection the envelope
  contract's implementation defines, per its own
  [validation](mqtt-raw-envelope-contract.md#validation) section).
- `BrokerThreadPanicked` (surfaced if a spawned rumqttd-owned thread's
  `JoinHandle` completes unexpectedly; see
  [Platform and security concerns](#platform-and-security-concerns) for how
  this is detected without blocking the receiver's own async loop).

Every error surfaced to the pipeline follows the existing
`format_error_sources` convention used by `syslog_cef_receiver` and
`kafka_receiver` so error chains remain readable in logs without leaking
payload content.

## Drain, shutdown, and reload

**This section is gated by [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
item 1: confirmed, not merely suspected, rumqttd 0.20.0 has no public API to
stop the router, close a listener's bound socket, or join the router/listener
threads.** The behavior below reflects what is actually achievable against
stock rumqttd 0.20.0, and is deliberately weaker than every other receiver's
shutdown contract in this codebase; that gap must be closed (per the gate)
before this receiver ships, not silently accepted as "how MQTT receivers
work here."

- **`NodeControlMsg::DrainIngress`**: the receiver stops issuing new
  `LinkTx::subscribe` work (there is none ongoing) and, per the standard
  "receiver-first drain" pattern, continues draining and forwarding
  already-buffered `Notification`s from `LinkRx` until the buffer is empty
  or the deadline elapses, then reports a drain-timeout event if buffered
  notifications remain (mirrors the syslog receiver's
  `drain_ingress.timeout` event). It does not unsubscribe or disconnect
  external clients during drain; MQTT clients remain connected to the
  embedded broker even if this pipeline node is draining, since the broker
  is a process-wide resource, not owned exclusively by one drain scope.
- **`NodeControlMsg::Shutdown`**: after ingress drain completes (or its
  deadline elapses), the receiver unsubscribes its internal `Link`. That is
  the full extent of what this receiver can do on stock rumqttd 0.20.0.
  **Confirmed: there is no way to stop the router thread, close any
  listener's bound TCP socket, or join any of the broker's OS threads
  through rumqttd's public API.** Dropping the `Broker`/`Router` handles
  this receiver holds does not stop the listener threads, which own their
  own `tokio` current-thread runtimes and their own blocking accept loops
  independent of whether any particular `router_tx` sender is still
  live. Consequently, `NodeControlMsg::Shutdown` for this receiver:
  - logs `mqtt_receiver.shutdown.listeners_not_released` (see
    [Telemetry](#telemetry)) every time, as a standing, non-optional
    disclosure rather than a one-time surprise;
  - leaves every configured listener's port bound and accepting new MQTT
    connections until the process exits;
  - cannot be used to free a port for reuse by a different configuration
    within the same process, and cannot be relied on to make repeated
    start/stop cycles within one test binary safe (each broker instance
    started in a test process leaks its threads and its bound port for
    the remainder of that process's life; see
    [Testing plan](#testing-plan) for the resulting test-isolation
    requirement).
  This is treated as a **production blocker for anything expecting a
  logical node shutdown to release its resources** (config reload,
  rolling per-node restart, or repeated test setup/teardown within one
  process), not a cosmetic gap. It must be resolved by one of the same
  paths as the rest of
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  (upstream shutdown API, fork, or an adapter that manages the listener
  accept loop itself instead of delegating to `Broker::start()`) before
  this receiver can be considered safe for any deployment that expects
  ordinary node lifecycle semantics.
- **Reload**: rumqttd has no live-reconfiguration API (listener addresses,
  TLS material, and `RouterConfig` are all consumed once at
  `Broker::new`/`start`), and -- per the confirmed absence of a shutdown
  path above -- there is also no way to tear down and recreate the
  listeners in place even by brute force. Consequently this receiver does
  not support in-place config reload for anything other than
  `topics.subscribe` (which can be changed by issuing new
  `LinkTx::subscribe`/`unsubscribe` calls against the already-running
  broker) and the `auth.mode: extension` binding's own internal refresh
  (handled by the extension itself, per
  [extension-requirements.md](../extension-requirements.md#5-use-background-tasks-for-slow-path-work)).
  Any change to `listeners`, `tls`, `router`, or `auth.mode`/`static`
  credentials requires a full **process** restart, not just a node
  restart (since the leaked listener threads and bound sockets survive a
  logical node stop); this must be documented plainly, and is one more
  reason the shutdown-API gap in item 1 above is a hard prerequisite
  rather than a nice-to-have.

## Platform and security concerns

- **Platforms**: rumqttd is pure Rust and builds on Linux, Windows, and
  macOS; unlike `kafka_receiver` (which requires `librdkafka` via
  `cmake-build` on Windows, per `contrib-nodes/Cargo.toml`), this receiver
  should not need per-target build workarounds. This should be confirmed
  during the spike by building `rumqttd` on the project's supported
  targets before relying on it in CI.
- **Dependency footprint**: rumqttd pulls in `flume`, `parking_lot`,
  `slab`, and (optionally, behind its own features) `rustls`/`native-tls`
  and `async-tungstenite`. Only the minimal feature set needed for TCP +
  TLS (no `websocket`, no `cluster`) should be enabled in
  `contrib-nodes/Cargo.toml`'s `mqtt-receiver` feature, consistent with the
  project's general preference for minimal optional dependencies. This
  needs a `cargo deny`/license check pass (`deny.toml`) as part of the
  implementation PR, not this design doc.
- **Cross-thread primitives**: per
  [rust-review.instructions.md](../../.github/instructions/rust-review.instructions.md)
  rule 2, every `Send`/cross-thread point introduced by this receiver
  (the `shared::Receiver` bound itself, the `LinkTx`/`LinkRx` channel,
  the `OnceLock`-guarded singleton `Broker` handle, and the
  `Send + Sync` auth-extension bound) must carry an explicit code comment
  explaining why it is required, referencing this document.
- **Unsafe code**: none expected beyond the existing
  `#[allow(unsafe_code)]` already used for the `distributed_slice`
  registration macro pattern shared by every receiver factory.
- **Attack surface**: this receiver terminates untrusted network
  connections directly (it is a broker, not a client), so it inherits the
  general "inbound network listener" review posture applied to OTLP and
  syslog: bounded connection/payload/property limits (see
  [Client, connection, packet, and queue limits](#client-connection-packet-and-queue-limits)),
  no panics on malformed input, and no payload content in logs or metrics
  by default (see [Telemetry](#telemetry) and the envelope contract's own
  [trust and security policy](mqtt-raw-envelope-contract.md#trust-and-security-policy-for-exporter-replay),
  which, while written for the exporter side, states the same
  no-payload-in-telemetry principle this receiver must also follow).
- **No built-in per-topic ACL**: restated from
  [ACL](#acl) because it is a security-relevant gap, not just a
  functionality gap; it must be prominent in the README, not buried.
- **Component inventory attributes**: `listen_port` (from
  `listeners[].listening_addr`), `protocol = "mqtt"` (pending the vocabulary
  addition described in
  [Component placement, feature gate, and URN](#component-placement-feature-gate-and-urn)),
  and `auth` reflecting the configured mode, are security-relevant and must
  stay accurate through the `cargo xtask component-inventory` baseline
  process described in [component-inventory.md](../component-inventory.md).

## Testing plan

Following [testing-guide.md](../testing-guide.md)'s "smallest layer that
proves the property" rule.

### Unit tests

- Config validation: every rule in
  [Configuration shape and validation](#configuration-shape-and-validation)
  has a positive and negative test (missing listeners, duplicate names,
  bad `SocketAddr`, invalid topic filter syntax, zero-valued limits,
  `ws` protocol rejection, in-memory PEM rejection).
- Envelope mapping: delegated to the envelope contract's own test plan
  (referenced, not duplicated); this receiver's tests only need to confirm
  that a `Forward` value produced by a real (in-process) rumqttd broker is
  handed to that mapping function unmodified, using a small fixture
  `Forward`/`Publish` value, not a full broker.
- Admission/backpressure: unit tests for
  `should_shed_ingress()`-gated `Link` draining and the unsubscribe/
  resubscribe transition under `MemoryPressureChanged`, following the
  existing `syslog_cef_receiver` test pattern
  (`udp_sheds_ingress_under_hard_memory_pressure` and neighboring tests).
- Single-instance guard: a unit test asserts that a second `create()` call
  for the same node identity while a `Broker` is already running returns
  `SingleInstanceViolation` rather than silently starting a second broker.

### Integration tests

- A node-level harness test (per
  [testing-guide.md](../testing-guide.md#current-test-surfaces)) starts a
  real embedded broker on an ephemeral loopback port, connects a real MQTT
  client (see [Interop tests](#interop-tests)), publishes at QoS 0 and
  QoS 1, and asserts the pipeline receives the corresponding mapped
  `OtapPdata` log records, including via `DrainIngress`/`Shutdown` in
  sequence to confirm receiver-first drain behavior.
- A small pipeline liveness test (`crates/otap/tests`-style) wires
  `receiver:mqtt` into a minimal pipeline (console or noop exporter) and
  confirms sustained publish traffic keeps making progress and that
  shutdown completes within the expected deadline (deadline here meaning
  drain completion and internal `Link` unsubscribe, not listener socket
  release -- see [Drain, shutdown, and reload](#drain-shutdown-and-reload)).
- **Test-process isolation is required, not optional, because of the
  confirmed no-shutdown-API gap** (see
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  item 1): every broker instance started inside a test process leaks its
  threads and its bound listener port for the remainder of that process's
  life. Test suites must therefore either (a) run each broker-starting test
  in its own child process, (b) use a single shared broker fixture across
  all tests in a binary and design tests to avoid needing independent
  broker instances, or (c) accept a documented, deliberately small number
  of long-lived broker instances per test binary and pick distinct
  ephemeral ports for each -- but must not assume repeated start/stop of
  independent brokers within one process is safe, since it is not, on
  stock rumqttd 0.20.0.

### Interop tests

- Exercise the receiver against at least one independent, well-known MQTT
  client implementation not otherwise used in this codebase (for example
  `rumqttc`, the companion client crate in the same upstream repository, or
  a CLI client such as `mosquitto_pub`/`mosquitto_sub` if available in the
  test environment) for both MQTT 3.1.1 and MQTT 5, covering CONNECT with
  and without credentials, SUBSCRIBE with `+`/`#` wildcards, PUBLISH at
  QoS 0/1, and retained-message delivery to a subscriber that connects
  after the retaining publish.
- A TLS interop test using a self-signed test certificate, covering both
  the plain-TLS and (if confirmed supported per the spike list) mTLS paths.
- An auth interop test covering `static` credential rejection/acceptance
  and, if the extension path is implemented in the same change, a fake
  `auth_check` extension exercising accept/reject.
- A commit-log-eviction interop test: publish QoS 1 messages faster than
  this receiver's internal `Link` drains (simulated pressure) until the
  configured `router.max_segment_size`/`max_segment_count` bound would be
  exceeded, and assert whichever of the
  [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss)
  mitigations was implemented actually fires (an observable eviction
  event/counter, or a publisher-visible backpressure/rejection, or -- only
  if that section's interim mitigation option was explicitly chosen -- a
  test proving the configured segment bound is provably larger than the
  test's maximum induced backlog). A test that merely confirms rumqttd
  does not crash under this scenario, without confirming loss is either
  prevented or observable, does not satisfy this criterion.

### Performance tests

- A continuous/nightly benchmark scenario (per
  [testing-guide.md](../testing-guide.md)) measuring sustained PUBLISH
  throughput at fixed payload size across a range of concurrent client
  connections, tracking: end-to-end publish-to-pipeline-admission latency,
  `receiver.mqtt.rejections.items` under intentionally induced memory
  pressure, and behavior at the `queue.inbound_capacity` boundary
  (confirming bounded, not growing, memory use under sustained overload).
- A specific "publisher on connection A, subscriber verification via
  receiver B" style test is not applicable here since there is only one
  embedded broker instance in v1; instead, a regression test should assert
  that starting a second pipeline replica against the same configured
  `receiver:mqtt` node fails fast (see
  [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component))
  rather than silently duplicating or splitting delivery, since that
  failure mode would otherwise only be caught in production under
  `all_cores` allocation.

## Documentation and example configuration

- Add `crates/contrib-nodes/src/receivers/mqtt_receiver/README.md`,
  following the structure already used by
  `crates/core-nodes/src/receivers/syslog_cef_receiver/README.md`
  (Metadata / Overview / Getting Started / Configuration / Telemetry /
  Feature Flags / Examples / Limits / Related Docs), explicitly stating:
  the single-core requirement, the "broker-admitted, not export-confirmed"
  QoS 1 ack semantics, the absence of per-topic ACL, that
  `NodeControlMsg::Shutdown` cannot release listener sockets until the
  gating shutdown-API work is complete (or, once it is complete, exactly
  what guarantee replaced this limitation), the reduced MQTT 5
  property/publisher-identity fidelity through the internal `Link`, and a
  link to [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md)
  for the attribute mapping.
  the README's own Feature Flags table should list `mqtt-receiver`
  matching the pattern used for `kafka-receiver`/`etw-receiver`.
- Add a top-level example pipeline config, `configs/mqtt-console.yaml`,
  mirroring the shape of `configs/syslog-console.yaml`:

  ```yaml
  version: otel_dataflow/v1
  engine: { }
  policies:
    resources:
      core_allocation:
        type: core_count
        count: 1
  groups:
    default:
      pipelines:
        main:
          nodes:
            mqtt:
              type: receiver:mqtt
              config:
                listeners:
                  - name: mqtt-tcp
                    protocol: v5
                    listening_addr: "0.0.0.0:1883"
                topics:
                  subscribe:
                    - "telemetry/#"
            console:
              type: exporter:console
              config: {}
          connections:
            - from: mqtt
              to: console
  ```

## Changelog

A user-facing new component requires a `.chloggen` entry per
[AGENTS.md](../../AGENTS.md#changelog-entries): copy
`.chloggen/TEMPLATE.yaml` to a new file (for example
`.chloggen/mqtt-receiver.yaml`) in the implementation PR (not in this design
doc, and not staged/committed as part of this task) with
`change_type: new_component`, `component: pipeline` (per
`.chloggen/config.yaml`'s comment that `pipeline` covers
`core-nodes`/`contrib-nodes` receivers/processors/exporters), a `note`
describing the opt-in `receiver:mqtt` addition in under 200 ASCII
characters, and the tracking issue number.

## Risks

- **Blocking risk -- no shutdown/stop API**: confirmed in rumqttd 0.20.0
  (see [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  item 1); this is not a residual uncertainty, it is a known gap that
  prevents this receiver from meeting the codebase's normal node lifecycle
  contract (listener release on shutdown, safe repeated start/stop, config
  reload). Must be resolved via upstream/fork/adapter before this receiver
  is used in any deployment that depends on those properties.
- **Blocking risk -- silent commit-log eviction after PUBACK**: confirmed
  in rumqttd 0.20.0 (see
  [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss));
  an already-acknowledged QoS 1 message can be discarded with zero
  observability on either the publisher or subscriber side. This violates
  the project's bounded-resources-with-observability posture and must be
  mitigated or made observable before this receiver's QoS 1 support is
  considered meaningful, let alone acceptance-tested.
- **Correctness risk**: the single-broker-instance constraint (see
  [Runtime placement](#runtime-placement-rumqttd-is-a-shared-cross-thread-component))
  is easy to violate silently under the engine's `all_cores` default if the
  fail-fast guard is not implemented carefully; this remains the highest
  risk among the items that do not also require upstream rumqttd changes,
  and should be prototyped and tested first once the blocking items above
  are otherwise addressed.
- **Fidelity risk -- reduced MQTT 5 property visibility**: confirmed that
  the internal `Link` forward path omits most MQTT 5 `PublishProperties`
  and the publisher's client identity (see
  [Internal `Broker::link` use](#internal-brokerlink-use) item 4 in the
  blockers list); operators expecting full MQTT 5 property passthrough per
  [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md) will not
  get it from this receiver without further upstream/adapter work. This is
  a fidelity risk, not a correctness risk (the contract's "when present"
  wording keeps this technically compliant), but it must be prominently
  disclosed rather than left to be discovered empirically.
- **Ack-semantics risk**: operators accustomed to "PUBACK means my data
  reached the destination" (a reasonable assumption for many MQTT
  deployments) may be surprised that this receiver's PUBACK only means
  "the embedded broker accepted it," not "OTLP export succeeded," and that
  this is architecturally fixed by rumqttd's internal ack sequencing, not a
  tunable option. This must be documented prominently, not just in this
  design doc.
- **Dependency risk**: embedding a broker (not just a client) increases the
  attack surface and the blast radius of any rumqttd defect (a router
  thread panic or resource leak affects the whole process, not just one
  receiver instance), which is why
  [Telemetry](#telemetry) includes an explicit
  `mqtt_receiver.broker.thread_panic` event and
  [Errors](#errors) includes `BrokerThreadPanicked`.
- **Version-pinning risk**: rumqttd does not appear to publish frequent
  stable releases with a strict semver/compatibility policy; the
  implementation should pin an exact version (0.20.0, as verified for this
  document, or a later version re-verified against every claim in
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation))
  and re-review this design's assumptions against any version bump before
  assuming a newer release has silently fixed any of the blocking items.

## Decisions requiring a rumqttd spike

The blocking items verified against rumqttd 0.20.0 are tracked in
[Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation),
not repeated here. The following remain genuinely open questions -- not yet
confirmed either way -- that should be resolved during the same spike effort
or during initial implementation:

1. **Whether any upstream rumqttd change (current main branch, an open PR,
   or a maintainer-acknowledged roadmap item) already addresses any of the
   four blocking items**, before assuming a fork or adapter is the only
   path forward. Re-check this before committing to a specific mitigation
   strategy for each blocking item.
2. **mTLS / client certificate verification.** Does rumqttd's `TlsConfig`
   (`Rustls`/`NativeTls`) support verifying client certificates at all, and
   if so, through what field? This determines whether
   [TLS](#tls) can offer mTLS or must document it as unsupported.
3. **Per-connection disconnect by client id.** Is there a public `Broker`
   or `Router` API to forcibly disconnect a specific external client
   connection (as opposed to this receiver's own internal `Link`)? This
   would change the memory-pressure response described in
   [DFE admission, backpressure, and memory pressure](#dfe-admission-backpressure-and-memory-pressure)
   from "unsubscribe" to "shed the heaviest external publishers directly."
4. **Connection/session count and health introspection.** Confirm what,
   if anything, `Broker`/`Router`/the `console`/`prometheus` subsystems
   expose publicly for reading active connection counts, so
   `receiver.mqtt.connections.active` (see [Telemetry](#telemetry)) can be
   populated accurately rather than approximated from CONNECT/DISCONNECT
   notification counting on this receiver's own `Link`.
5. **`LinkRx` cancellation and async-recv semantics.** Confirm `LinkRx`
   exposes a cancellation-safe async receive suitable for use inside a
   `tokio::select!` alongside the control-message channel (needed for the
   receive loop described in
   [Internal `Broker::link` use](#internal-brokerlink-use)), and whether
   its internal `parking_lot::Mutex`-guarded buffer has any behavior that
   is unsafe or incorrect to poll concurrently with control-message
   handling.
6. **Thread panic propagation.** Confirm what happens to the process (and
   to `Broker`'s already-returned `router_tx` sender) if a rumqttd-owned
   router, listener, or bridge thread panics, so
   `mqtt_receiver.broker.thread_panic` (see [Telemetry](#telemetry)) and
   `BrokerThreadPanicked` (see [Errors](#errors)) can be triggered
   reliably rather than leaving the receiver silently stuck on a dead
   channel.
7. **`dynamic_filters` and topic/segment resource growth.** Confirm the
   exact resource-growth behavior when `dynamic_filters: false` and an
   external client publishes to many distinct topics with no matching
   subscription, to validate the bounded-resource claim in
   [ACL](#acl).
8. **Minimum supported rumqttd version and platform build matrix.**
   Confirm which published `rumqttd` version to pin (0.20.0 is the version
   verified for this document; a later release must be re-verified against
   every claim in
   [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
   before being substituted), and validate that it (and its default-enabled
   transitive dependencies) builds cleanly for every platform this
   workspace supports, per
   [design-principles.md](../design-principles.md#constraints).

## Acceptance criteria

Gating criteria (must be satisfied before any of the criteria below are
attempted against a real implementation):

- Each item in
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation)
  has an explicit, reviewed resolution: an upstream contribution merged
  upstream or in a pinned fork, an adapter that enforces the missing
  guarantee, or -- only for item 4 (reduced MQTT 5 property/identity
  fidelity) -- an explicitly accepted, documented v1 scope reduction. Items
  1 (shutdown) and 3 (commit-log eviction) specifically must not be
  resolved by "document and ship anyway"; they require a functional
  mitigation per
  [Drain, shutdown, and reload](#drain-shutdown-and-reload) and
  [Commit log retention and silent data loss](#commit-log-retention-and-silent-data-loss)
  respectively.

Once the gate above is satisfied:

- `receiver:mqtt` exists behind the `mqtt-receiver` Cargo feature in
  `contrib-nodes`, registered under URN `urn:otel:receiver:mqtt`, with a
  `component_inventory` annotation using a newly added `Mqtt` `Protocol`
  vocabulary value (not `Custom`).
- The receiver embeds rumqttd (or the adapter/fork resolving the gate above)
  and terminates real MQTT 3.1.1 and MQTT 5 client connections without any
  external broker process.
- Every inbound `PUBLISH` matching a configured subscription produces
  exactly one `OtapPdata` log record via the
  [envelope contract](mqtt-raw-envelope-contract.md) mapping, verified by
  an interop test using an independent MQTT client implementation.
- The receiver does not implement or claim any Sparkplug B behavior.
- Starting a second pipeline replica of the same configured `receiver:mqtt`
  node fails fast with a typed, actionable error instead of silently
  starting a second broker or corrupting delivery.
- All connection/payload/property/queue limits in
  [Client, connection, packet, and queue limits](#client-connection-packet-and-queue-limits)
  are finite by default and independently tested.
- Under sustained hard memory pressure, the receiver stops draining new
  `PUBLISH` notifications (or unsubscribes, per the configured response)
  and resumes automatically once pressure clears, without unbounded memory
  growth, verified by a test mirroring
  `udp_sheds_ingress_under_hard_memory_pressure`, **and** the commit-log
  eviction interop test in [Interop tests](#interop-tests) demonstrates
  that this backpressure does not silently discard already-PUBACKed data.
- `NodeControlMsg::Shutdown` actually releases every listener socket this
  receiver's node bound, and a repeated start/stop integration test (not
  merely a single-shutdown test) demonstrates this across at least two
  cycles within one test process. This criterion is only satisfiable once
  the shutdown-API gate item above is resolved; it explicitly supersedes
  the weaker "receiver-first drain is observed" bar that would otherwise be
  sufficient for other receivers in this codebase.
- The README explicitly documents: the single-core requirement, the
  broker-admitted (not export-confirmed) QoS 1 ack semantics, the absence
  of per-topic ACL, and the resolution chosen for each item in
  [Verified rumqttd 0.20.0 blockers](#verified-rumqttd-0200-blockers-gate-implementation).
- A `.chloggen` entry is added in the implementation PR with
  `change_type: new_component` and `component: pipeline`.
- All items in
  [Decisions requiring a rumqttd spike](#decisions-requiring-a-rumqttd-spike)
  have been resolved (confirmed true, confirmed false with a documented
  workaround, or confirmed false and descoped with an explicit non-goal
  update) before this design is considered implementation-complete.

## Related work

- [mqtt-raw-envelope-contract.md](mqtt-raw-envelope-contract.md) -- the
  authoritative PUBLISH-to-LogRecord mapping this receiver produces input
  for; this document does not duplicate it.
- [mqtt-bounded-inbound-publish-flow-control.md](mqtt-bounded-inbound-publish-flow-control.md)
  and
  [mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md)
  -- client-side (outbound MQTT client) flow-control and ack-safety
  proposals for a different component; see
  [QoS semantics and the ACK boundary](#qos-semantics-and-the-ack-boundary)
  for why this receiver's ack model is not the same problem.
- [journald-receiver.md](../journald-receiver.md) and
  `crates/core-nodes/src/receivers/syslog_cef_receiver/README.md` --
  reference designs for mechanical envelope projection, admission/
  memory-pressure integration, and receiver README structure.
- [extension-requirements.md](../extension-requirements.md) -- capability
  binding and `local`/`shared` execution model background for the
  `auth.mode: extension` path.
- [component-inventory.md](../component-inventory.md) and
  [urns.md](../urns.md) -- component registration and URN rules.
- rumqttd upstream:
  [repository](https://github.com/bytebeamio/rumqtt/tree/main/rumqttd),
  [docs.rs](https://docs.rs/rumqttd/latest/rumqttd/).
