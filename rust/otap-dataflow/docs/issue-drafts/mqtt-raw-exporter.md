# Add a raw MQTT exporter (`exporter:mqtt`)

<!-- markdownlint-disable MD013 -->

## Summary

Add an opt-in, contrib-only `exporter:mqtt` node that is the **exporter side**
of
[`mqtt-raw-envelope-contract.md`](mqtt-raw-envelope-contract.md).

This exporter uses a **two-stage MQTT client strategy**, introduced in full in
[Client strategy: baseline vs. long-term replacement](#client-strategy-baseline-vs-long-term-replacement):

- **Baseline** (this document's primary target): [`rumqttc`](https://github.com/bytebeamio/rumqtt)
  `0.25.1`, using its MQTT 5 (`rumqttc::v5`) API, added with
  `default-features = false` so the dependency graph is **plaintext-only**:
  no TLS backend, no certificate-parsing crate, and no cryptography crate of
  any kind is compiled in or linked, verified by an automated `cargo tree`
  CI assertion (see
  [No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement)).
  Baseline therefore supports TCP transport only and explicitly rejects any
  TLS-shaped configuration at validation time; it never connects over TLS and
  never silently downgrades a TLS request to plaintext (see
  [Endpoints, TLS, and auth](#endpoints-tls-and-auth)).
- **Long-term, preferred replacement**: [`microsoft/rust-mqtt-client`](https://github.com/microsoft/rust-mqtt-client)
  (crate `ms-mqtt-client`) remains this exporter's intended eventual client,
  once its own separate, already-drafted pluggable-TLS/ambient-provider
  request lands upstream (see
  [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md)).
  Until that lands, `ms-mqtt-client` cannot satisfy the same plaintext-only,
  zero-cryptography-dependency requirement baseline needs today, because it
  currently hard-depends on OpenSSL unconditionally, even in a "no TLS" build
  (see [Client strategy](#client-strategy-baseline-vs-long-term-replacement)).
  Every `ms-mqtt-client`-specific API detail in this document is discussed as
  **future direction only**; it is not a dependency of the baseline design.

To keep network-library specifics out of the exporter's own semantics, and to
bound the cost of the eventual client swap to one module, this design defines
a narrow, crate-internal **publish-client adapter** trait (see
[The publish-client adapter](#the-publish-client-adapter)). All
topic/QoS/retain policy, per-record validation, fan-out, Ack/Nack reduction,
and telemetry logic is written once against that trait; only the adapter's
baseline implementation references `rumqttc` types.

Baseline consumes `LogRecord`s produced according to the envelope contract
(each one representing exactly one previously-received MQTT `PUBLISH`, but not
necessarily via the paired `receiver:mqtt` -- see
[Trust and security policy for exporter replay](mqtt-raw-envelope-contract.md#trust-and-security-policy-for-exporter-replay))
and republishes **each individual `LogRecord`'s body as one raw MQTT
`PUBLISH`**, using the `mqtt.*` attributes the contract defines for topic,
QoS, retain, and properties, bounded by this exporter's own configured trust
policy. **This exporter never publishes an entire OTLP export request (or any
other multi-record container) as a single MQTT payload.** One `LogRecord` in
is always exactly one `PUBLISH` out; see
[Supported input and the one-LogRecord-to-one-PUBLISH invariant](#supported-input-and-the-one-logrecord-to-one-publish-invariant).

Sparkplug B and any other domain-specific payload encoding remain explicitly
out of scope for this baseline; a Sparkplug encoding, if ever wanted, belongs
in a follow-on design once this raw exporter's plumbing (connection
lifecycle, backpressure, per-record Ack/Nack mapping) is proven.

This document is a feature request and implementation-ready specification,
not an implementation. It follows the
[Reference-Informed OTAP-Native Capability Design](../ai/reference-informed-otap-native-capability-design.md)
approach: `rumqttc`, `ms-mqtt-client`, the existing Kafka/OTLP exporters, and
this project's own envelope contract and raw receiver design are evidence for
the design, not an oracle to copy mechanically.

## Relationship to the envelope contract and the raw receiver

This document does not redefine the PUBLISH-to-LogRecord mapping; that
mapping, its attribute names, its body policy, its round-trip guarantees, and
its exporter-side validation and trust rules are all defined once in
[`mqtt-raw-envelope-contract.md`](mqtt-raw-envelope-contract.md) and are
**normative for this exporter**. This document only defines:

- how the exporter obtains individual `LogRecord`s from an accepted pdata
  message (which may contain many), since the contract is expressed per
  `LogRecord`, not per pdata batch (see
  [Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords));
- how the exporter's own configuration bounds and reconciles with the
  contract's per-record `mqtt.*` attributes (see
  [Configuration and validation](#configuration-and-validation) and
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy));
- how the [publish-client adapter](#the-publish-client-adapter) (backed by
  `rumqttc` in baseline) is driven, and how its submission/enqueue model and
  this exporter's own bounded PubAck correlation layer (see
  [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation))
  reduce this exporter's own N-publishes-per-batch fan-out to the DFE
  engine's one-Ack-or-Nack-per-pdata-message model (see
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction));
  and
- exporter-specific lifecycle, telemetry, and test concerns.

[`mqtt-raw-receiver.md`](mqtt-raw-receiver.md) (a separate, currently gated
design) is the intended producer of contract-compliant `LogRecord`s in a
typical deployment, but this exporter must not assume the receiver is present
in the same pipeline; any `LogRecord` satisfying the contract is valid input,
per the contract's own "receiver and exporter are separate, independently
deployable components" principle.

## Motivation

MQTT is a common transport at the network edge (IoT gateways, industrial
telemetry relays, constrained links) where a full OTLP/gRPC or OTLP/HTTP
endpoint is unavailable or undesirable. Operators who already run an MQTT
broker (Mosquitto, EMQX, HiveMQ, an IoT Hub-compatible endpoint, and so on)
want a way to forward pipeline output -- including MQTT traffic captured
elsewhere, transformed, and replayed -- to a topic on that broker without
standing up an intermediate bridge process.

Client selection for this exporter is discussed in full in
[Client strategy: baseline vs. long-term replacement](#client-strategy-baseline-vs-long-term-replacement)
immediately below; in short, `rumqttc` lets this exporter ship a genuinely
plaintext-only, zero-cryptography-dependency baseline today, while
`ms-mqtt-client` remains the preferred long-term client once its own
pluggable-TLS work lands upstream.

## Client strategy: baseline vs. long-term replacement

### Why two stages

This design commits to two named client stages rather than one, because no
single client available today satisfies both requirements this exporter
needs simultaneously:

1. **A true plaintext-only, zero-cryptography-dependency build, available
   now.** Some deployments of this exporter run in environments where any
   cryptography crate in the dependency graph -- even one that is never
   exercised at runtime because TLS is not configured -- is a compliance or
   supply-chain-review obstacle (FIPS-adjacent build policies, vendored
   dependency audits, or simply minimizing attack surface for a
   network-facing contrib component). This requirement is about the
   **dependency graph**, not just runtime behavior: a crate that merely
   ships an unused TLS code path still fails it.
2. **A long-term client whose tiered, application-driven completion model,
   already-drafted pluggable-TLS design, and Microsoft ownership make it the
   preferred fit for this project once it can also satisfy requirement 1.**

- `rumqttc` `0.25.1` satisfies requirement 1 **today**, as pinned: its
  `[features]` table declares `default = ["use-rustls"]` with `use-rustls`,
  `use-rustls-no-provider`, `use-native-tls`, `websocket`, and `proxy` all
  gated behind optional, non-default Cargo features and `dep:` entries
  ([`rumqttc/Cargo.toml`](https://github.com/bytebeamio/rumqtt/blob/main/rumqttc/Cargo.toml)).
  Declaring the dependency as `rumqttc = { version = "0.25.1",
  default-features = false }` compiles in none of `rustls`,
  `tokio-rustls`, `rustls-webpki`, `rustls-pemfile`, `rustls-native-certs`,
  `native-tls`, `tokio-native-tls`, `async-tungstenite`, or
  `async-http-proxy`; only the plaintext TCP client on `tokio` remains. This
  is a published, crates.io-versioned crate (unlike `ms-mqtt-client`'s
  current pre-publication `0.1.0`), so baseline can depend on it with a
  normal semver requirement, not a floating git-revision pin.
- `ms-mqtt-client` does **not** satisfy requirement 1 today. This
  workspace's own dependency declaration
  (`ms-mqtt-client = { git = "...", rev = "032c3ee2...", default-features =
  false }`, `rust/otap-dataflow/Cargo.toml`) already demonstrates the
  problem: `default-features = false` on `ms-mqtt-client` does not remove
  its `openssl`/`tokio-openssl` dependency, because that crate declares
  those as plain, non-optional dependencies rather than gating them behind a
  Cargo feature -- there is currently no `ms-mqtt-client` build configuration
  that omits OpenSSL from the dependency graph. This gap, and the requested
  fix (a true no-TLS build plus a `rustls`-backed backend that uses whatever
  ambient `rustls::crypto::CryptoProvider` the host process installs, rather
  than a backend bundled and built by the crate itself), is filed as a
  separate, already-drafted upstream issue:
  [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md).
  This exporter design does not restate that issue's content; it only
  depends on its outcome as the trigger for stage 2 below.
- Choosing to wait for that upstream change, rather than shipping baseline
  against `ms-mqtt-client` with its OpenSSL dependency simply unused, would
  mean either accepting a cryptography crate in the graph today (failing
  requirement 1 outright) or blocking this exporter's baseline entirely on
  an unpublished, upstream, cross-organization dependency change with no
  committed timeline. Neither is acceptable when `rumqttc` already meets
  requirement 1 unmodified.

### Stage 1 (baseline, this document's primary target): rumqttc

- Dependency: `rumqttc = { version = "0.25.1", default-features = false }`,
  gated behind this exporter's own `mqtt-exporter` Cargo feature (see
  [URN and feature gate](#urn-and-feature-gate)).
- API surface: `rumqttc::v5::{MqttOptions, AsyncClient, EventLoop, Event,
  Incoming, Outgoing, Request}` and `rumqttc::v5::mqttbytes::v5::*` (`Publish`,
  `PubAck`, `PublishProperties`, and related packet types), confirmed present
  in the pinned version's source
  (`rumqttc/src/v5/mod.rs` on the `bytebeamio/rumqtt` `main` branch at the
  time of evidence review). Baseline uses **only** the `v5` submodule; the
  crate's root module (`rumqttc::{MqttOptions, Client, AsyncClient,
  Connection}`, re-exported at the crate root) implements MQTT 3.1.1, not
  MQTT 5, and must not be used anywhere in this exporter (see
  [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)).
- Transport: TCP only, matching requirement 1. Baseline never constructs a
  TLS transport and, because no TLS feature is compiled in, the binary
  contains no TLS code path to construct even if a bug attempted it (see
  [Endpoints, TLS, and auth](#endpoints-tls-and-auth) for this design's
  defense-in-depth reasoning).
- `rumqttc` has no per-operation completion token: `AsyncClient::publish(...)`
  only reports whether the publish request was accepted into an internal
  channel, not whether or how the broker eventually acknowledged it. This
  exporter must not claim otherwise; it builds its own bounded PubAck
  correlation layer on top of the polled `v5` `EventLoop`'s event stream to
  recover that information (see
  [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation)).
  Several exact API details this correlation layer depends on are flagged
  throughout this document as items for **the parallel rumqttc integration
  spike** to confirm against the pinned crate version before implementation
  begins; this design does not assume they exist.
- License: `rumqttc` is Apache-2.0 licensed (per its repository and
  `docs.rs` listing), compatible with this project's own Apache-2.0
  licensing for a dependency.

### Stage 2 (long-term, preferred): ms-mqtt-client

`ms-mqtt-client` remains this exporter's **preferred** long-term client, not
merely a fallback, because of properties already documented as evidence for
this design: a low-level, application-driven MQTT 5 client with an explicit
tiered submission/completion/PUBACK result model (giving per-operation
completion tokens `rumqttc` does not provide), no internal retry loop, and
Microsoft ownership within the same ecosystem this project already engages
with. None of `ms-mqtt-client`'s API is a dependency of baseline; every
`ms-mqtt-client`-specific detail elsewhere in this document (its
`ConnectHandle`/`Connection`/`Receiver` ownership-typed API, its
`CompletionToken`/`DetachedError`/`CompletionError` model, its `TlsConfig`)
is recorded strictly as **future direction**, evaluated from the same
evidence-review discipline this project's reference-informed design process
requires, so that the eventual migration is planned rather than improvised.

### The publish-client adapter

To keep this exporter's core semantics -- topic/QoS/retain/property policy,
per-record validation, bounded in-flight fan-out, whole-batch Ack/Nack
reduction, reconnect/backoff policy, and telemetry -- independent of which
concrete MQTT client crate is linked, this design introduces a narrow,
crate-internal adapter trait (illustrative shape; exact method signatures
are an implementation detail, not fixed by this document):

```rust
/// Crate-internal only; never re-exported. No exporter logic outside
/// `mqtt_exporter::client` may reference `rumqttc` (or, later,
/// `ms-mqtt-client`) types directly.
trait PublishClient {
    /// Submit one record's PUBLISH. Returns quickly; does not itself imply
    /// broker acknowledgement. See
    /// "Publish submission and PUBACK correlation".
    fn submit_publish(&mut self, request: OutboundPublish) -> Result<PublishHandle, ClientUnavailable>;

    /// Drive the underlying client's network loop and yield the next
    /// correlatable event (a submitted publish's terminal outcome, or a
    /// connection-lifecycle transition).
    async fn next_event(&mut self) -> AdapterEvent;

    async fn disconnect(&mut self);
}
```

- Exactly one module (the adapter's baseline implementation) imports
  `rumqttc` types. Every other module in this exporter -- config, validation,
  policy, fan-out/Ack-Nack reduction, telemetry -- compiles against
  `PublishClient` (or the concrete request/event types it exchanges) only.
- This bounds the blast radius of the eventual stage-2 migration to
  replacing the adapter's implementation and its unit tests; it does not
  require touching the exporter's config schema, validation rules, fan-out
  logic, Ack/Nack reduction, or telemetry, all of which are expressed in
  terms of the adapter's own request/event types, not the client crate's.
- The adapter is also where the [PubAck correlation layer](#publish-submission-and-puback-correlation)
  lives in baseline, since that layer exists specifically to compensate for
  `rumqttc`'s lack of completion tokens; a stage-2 adapter implementation
  backed by `ms-mqtt-client` would not need it, since that crate reports
  completion natively.
- See [Acceptance criteria](#acceptance-criteria) for how this boundary is
  verified (no direct `rumqttc::` references outside the adapter module).

### Replacement criteria (stage 1 to stage 2)

Migrating this exporter's baseline off `rumqttc` and onto `ms-mqtt-client`
requires, at minimum, all of the following; this document does not schedule
or commit to that migration, only records what must be true before it is
undertaken:

- [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md)'s
  requested no-TLS build (zero cryptography dependencies with no `tls-*`
  feature enabled) and pluggable, ambient-`CryptoProvider` `rustls` backend
  have landed and shipped in a released version of `ms-mqtt-client`.
- `ms-mqtt-client` has reached a state this project is willing to pin
  normally (a tagged, crates.io-published release, or at minimum a stable
  git revision with a committed maintenance posture), rather than the
  current unpublished `0.1.0` pinned by git revision.
- The publish-client adapter's `ms-mqtt-client`-backed implementation passes
  the same test suite (unit, pdata-decoding, network-integration,
  cross-client interoperability; see [Tests](#tests)) unmodified from the
  exporter-core side, demonstrating the adapter boundary held.
- Any behavioral difference the migration would introduce -- most notably,
  `ms-mqtt-client` requiring the application to drive its own reconnect loop
  where `rumqttc`'s `EventLoop` reconnects automatically while polled (see
  [Reconnect and backoff](#reconnect-and-backoff)) -- is re-verified against
  this document's reconnect, cancellation, and shutdown requirements before
  the switch ships, not assumed to carry over unchanged.

## Scope

### In scope (baseline)

- A `logs` signal only. Each accepted pdata message is decoded into zero or
  more individual `LogRecord`s (see
  [Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)),
  and each `LogRecord` becomes exactly one MQTT `PUBLISH`, per the envelope
  contract.
- **TCP transport only.** MQTT 5 CONNECT (username/password auth), clean-start
  and persistent-session configuration, keep-alive configuration. TLS
  (including mutual TLS) is explicitly **out of scope for baseline** and is
  rejected at configuration-validation time, never silently downgraded to
  plaintext; it is deferred to the long-term `ms-mqtt-client`-backed
  replacement (see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement) and
  [Endpoints, TLS, and auth](#endpoints-tls-and-auth)).
- Per-record topic, QoS, retain, and a bounded set of MQTT 5 PUBLISH
  properties, all sourced from the `LogRecord`'s `mqtt.*` attributes per the
  envelope contract and bounded by this exporter's configured policy (see
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)),
  plus a static per-signal fallback topic/QoS/retain for records that do not
  carry the corresponding attribute.
- Bounded in-flight publishing across the individual records of one or more
  accepted batches, with explicit backpressure, adapting the Kafka exporter's
  `max_in_flight` model to per-record granularity (see
  [Bounded in-flight publishing](#bounded-in-flight-publishing)).
- Mapping the publish-client adapter's submission/enqueue result plus this
  exporter's own bounded PubAck correlation layer, fanned out across N
  per-batch publishes, onto exactly one DFE `AckMsg`/`NackMsg` per accepted
  pdata message (permanent vs. transient), consistent with the project's "no
  built-in retry loop; emit a classified Nack for an upstream
  `processor:retry`" convention, with an explicit, documented at-least-once/
  duplicate-on-retry behavior (see
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)).
- Reconnect with bounded exponential backoff, layered as needed around
  `rumqttc`'s own automatic reconnect-while-polled behavior (see
  [Reconnect and backoff](#reconnect-and-backoff)).

### Explicitly out of scope for baseline

- **TLS of any kind (including mutual TLS).** Baseline is TCP-only by
  Cargo-dependency construction (`default-features = false` compiles out
  every TLS backend `rumqttc` offers) and by configuration validation (any
  TLS-shaped config is a rejected, non-startable configuration, not a
  silently-ignored one). See
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement) and
  [Endpoints, TLS, and auth](#endpoints-tls-and-auth).
- **Sparkplug B (or any other structured payload) encoding.** Baseline
  publishes each `LogRecord`'s `body` verbatim, per the envelope contract's
  body policy; it never wraps, transforms, or aggregates payload content.
- **Publishing a whole OTLP export request, an OTAP batch, or any other
  multi-record container as a single MQTT PUBLISH payload.** This is a hard
  non-goal, not a deferred feature; see
  [Supported input and the one-LogRecord-to-one-PUBLISH invariant](#supported-input-and-the-one-logrecord-to-one-publish-invariant).
- `traces` and `metrics` signals. The envelope contract itself is
  logs-specific (MQTT PUBLISH maps to `LogRecord`, not to a span or a metric
  data point); there is currently no equivalent contract for those signals,
  so they are out of scope until one exists, not merely deferred for
  config-shape reasons.
- QoS 2. The envelope contract itself conditions QoS 2 rejection on "the
  current client library's stated non-goal"; baseline keeps QoS 2 out of
  scope pending a dedicated design and test pass, regardless of whether
  `rumqttc`'s own QoS 2 protocol support (its `v5` `Request` enum includes
  `PubRec`/`PubRel`/`PubComp` variants, suggesting some support exists) turns
  out to be usable -- see
  [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2).
- WebSocket transport, enhanced (SASL/AUTH-style) authentication, shared
  subscriptions, and any subscribe path -- this is a publish-only exporter.
  `websocket` and `proxy` are additionally excluded from baseline simply by
  not being enabled Cargo features on the `rumqttc` dependency.
- An application-level durable queue. As with the Kafka exporter, add
  `processor:durable_buffer` upstream for cross-restart durability.
- A persistent MQTT session store shared across restarts; the exporter treats
  each process lifetime as owning its own client identity and session state.
- Any mechanism to detect or suppress duplicate publishes caused by retrying
  a partially-succeeded batch; see
  [Partial success and retry duplicates](#partial-success-and-retry-duplicates).

## Supported input and the one-LogRecord-to-one-PUBLISH invariant

Per the envelope contract, the mapping between MQTT PUBLISH and `LogRecord`
is 1:1: "no PUBLISH is split across multiple LogRecords, and no LogRecord
aggregates more than one PUBLISH." This exporter is the inverse direction of
that same invariant: **no `LogRecord` is split across multiple PUBLISH
packets, and no PUBLISH packet aggregates more than one `LogRecord`.**
Concretely:

- An accepted pdata message for the `logs` signal ordinarily contains many
  `LogRecord`s (one OTLP `ExportLogsServiceRequest` or one OTAP logs batch
  nests `ResourceLogs -> ScopeLogs -> LogRecord`, and an upstream
  `processor:batch` routinely combines many originally-separate `LogRecord`s
  -- each from a different MQTT PUBLISH, if sourced from `receiver:mqtt` --
  into one larger pdata message for transport efficiency).
- This exporter never treats that whole pdata message as one payload. It
  decodes the message into its constituent `LogRecord`s (see next section)
  and publishes each one as its own PUBLISH.
- For a pdata batch containing `N` `LogRecord`s, the exporter always attempts
  exactly `N` PUBLISH operations, never fewer (no coalescing) and never more
  (no re-splitting a single `LogRecord`'s body).
- A pdata message for any signal other than `logs` is a permanent
  (non-retryable) Nack, mirroring the Kafka exporter's "unconfigured signal
  is permanently nacked" behavior.

### Decoding pdata into individual LogRecords

This exporter must decode whichever pdata representation the pipeline
delivers -- OTAP Arrow records or OTLP protobuf bytes -- into individual
`LogRecord`s using **existing pdata APIs**, not a new decoder:

- The `crates/pdata` "views" abstraction (`otel_arrow_dfe_pdata_views::views`,
  re-exported via `crates/pdata/src/views/mod.rs`) defines backend-agnostic,
  zero-copy traits `LogsDataView -> ResourceLogsView -> ScopeLogsView ->
  LogRecordView`, following exactly the traversal hierarchy the envelope
  contract assumes.
- `OtapLogsView` (`crates/pdata/src/views/otap/logs.rs`) implements
  `LogsDataView` directly over `OtapArrowRecords` (`TryFrom<&OtapArrowRecords>`).
- `RawLogsData` (`crates/pdata/src/views/otlp/bytes/logs.rs`) implements
  `LogsDataView` directly over serialized OTLP `LogsData`/
  `ExportLogsServiceRequest` bytes, without first decoding into an owned
  Prost object tree.
- Both view implementations expose the same `resources() -> scopes() ->
  log_records()` iteration and the same `LogRecordView::attributes()` /
  `LogRecordView::body()` accessors, so the exporter's per-record mapping
  logic (envelope contract body policy and `mqtt.*` attribute reads) is
  written once, generically over `T: LogsDataView`, and works unmodified for
  either input representation. This mirrors how `crates/pdata/src/otlp/json/mod.rs`
  already consumes `OtapLogsView` generically for JSON encoding.
- Given an accepted `PayloadData::OtapArrowRecords(_)` or
  `PayloadData::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(_))` (see
  `crates/pdata/src/payload.rs`), the exporter selects the matching view
  implementation and iterates it once per accepted pdata message, yielding
  the batch's `LogRecordView`s **in traversal order** (resource index, then
  scope index, then log-record index within scope) -- this order is later
  load-bearing for the
  [deterministic publish order](#deterministic-publish-order) requirement.
- Both view implementations are documented as intentionally `!Send` (the
  OTLP-bytes views share scan state through `Rc<Cell<_>>`); this is
  compatible with, and reinforces, this project's `!Send` future preference
  for the exporter node's own task (see [Multicore](#multicore)).
- Any other `PayloadData` variant for the `logs` signal (there is currently
  none besides these two) or a decode failure while constructing the view
  (for example, an OTAP batch missing a column the view requires) is a
  permanent, whole-batch Nack: the exporter cannot enumerate any `LogRecord`s
  to publish, so there is nothing to fan out.
- A decode or validation failure scoped to **one** `LogRecordView` while
  iterating an otherwise-valid batch (for example, a malformed nested
  attribute) does not abort the batch's traversal; it is treated as that one
  record's publish failing per the envelope contract's exporter-side
  validation table (see
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)),
  and the traversal continues to the remaining records. This "reject this
  record, keep going" behavior mirrors the best-effort per-field behavior the
  raw OTLP-bytes views already document for malformed nested fields.

**API uncertainty:** `RawLogsData`'s best-effort behavior for a malformed or
wrongly-typed nested field is documented at the module level as "may appear
absent," which does not obviously distinguish "this record has no
`mqtt.topic` attribute" (a normal, contract-anticipated condition) from "this
record's attributes could not be parsed at all" (a decode error that might
warrant a different classification or telemetry signal). Confirm, before
implementation, whether `RawLogsData`/`RawLogRecord` expose enough error
detail to distinguish these two cases, or whether the exporter must treat
both identically as "attribute absent" per the envelope contract's
omit-don't-guess principle.

## Evidence reviewed

- `rumqttc` `0.25.1` (`bytebeamio/rumqtt`, `main` branch at time of review):
  `rumqttc/Cargo.toml` (`[features]` table: `default = ["use-rustls"]`,
  optional `use-rustls`/`use-rustls-no-provider`/`use-native-tls`/
  `websocket`/`proxy`), `rumqttc/src/v5/mod.rs` (`MqttOptions`,
  `Request`/`Incoming` = `Packet`, re-exported `AsyncClient`/`Client`/
  `Connection`/`EventLoop`/`Event`, `mqttbytes::v5` re-export), top-level
  crate documentation on `docs.rs` (synchronous/asynchronous usage examples,
  the "Automatic reconnections by just continuing the
  eventloop.poll()/connection.iter() loop" behavior).
- `microsoft/rust-mqtt-client` `main` branch, pinned at commit
  `032c3ee282f425c19f5130d11cb7ad16a7525cfa` (the same revision this
  workspace's `Cargo.toml` currently pins): `README.md`, `src/lib.rs`
  (crate-level operation-completion contract), `src/client.rs` (`new_client`,
  `ClientOptions`, `Client::publish_qos0`/`publish_qos1`,
  `ConnectHandle::connect`, `Receiver`), `src/transport.rs`
  (`ConnectionTransportConfig`, `ConnectionTransportType`, `TlsConfig`,
  `Proxy`), `src/packet.rs` (`QoS`, `RetainOptions`, `PublishProperties`,
  `PubAck::as_result`), `src/error.rs` (`DetachedError`, `CompletionError`,
  `ConnectError`), `Cargo.toml` (dependency pins, license, MSRV),
  `doc/feature-support.md`, `doc/limitations.md`. Reviewed strictly as
  future-direction evidence (see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement));
  none of it is a baseline dependency.
- This repository's
  [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md),
  the upstream-ready issue draft, grounded in the same pinned commit, that
  requests the no-TLS build and pluggable ambient-`CryptoProvider` `rustls`
  backend that this design's [replacement criteria](#replacement-criteria-stage-1-to-stage-2)
  depend on; and this workspace's own `rust/otap-dataflow/Cargo.toml`
  `ms-mqtt-client` dependency declaration, whose `default-features = false`
  (which does not remove the OpenSSL dependency) is direct, first-party
  evidence for that issue's problem statement.
- This repository's [`mqtt-raw-envelope-contract.md`](mqtt-raw-envelope-contract.md)
  (normative for this document; see
  [Relationship to the envelope contract and the raw receiver](#relationship-to-the-envelope-contract-and-the-raw-receiver))
  and [`mqtt-raw-receiver.md`](mqtt-raw-receiver.md) (the intended, but not
  required, upstream producer of contract-compliant `LogRecord`s).
- `crates/pdata/src/views/mod.rs`, `crates/pdata/src/views/otap/logs.rs`,
  `crates/pdata/src/views/otlp/bytes/logs.rs`, and
  `crates/pdata/src/payload.rs` for the backend-agnostic view traits and the
  `PayloadData` representation this exporter must decode.
- This repository's `crates/contrib-nodes/src/exporters/kafka_exporter/`
  (`README.md`, `config.rs`, `error.rs`) for the established exporter
  conventions this design reuses: per-signal config, dynamic-vs-static
  routing with an operator allowlist, bounded in-flight publishing,
  permanent-vs-transient Nack classification, no built-in retry loop,
  telemetry metric-set and event shape.
- `crates/core-nodes/src/exporters/otlp_grpc_exporter/mod.rs` for the
  `FuturesUnordered`-based bounded in-flight pattern used by an async,
  network-backed exporter in this codebase.
- `crates/core-nodes/src/processors/batch_processor/README.md` for this
  codebase's existing precedent of fanning one accepted request out into
  multiple downstream units while tracking "ACK/NACK-sensitive request
  state" back to a single terminal decision on the original request -- the
  same shape this exporter needs for N publishes per one accepted pdata
  message.
- `crates/core-nodes/src/processors/retry_processor/README.md` for the
  upstream retry-processor contract that this exporter is expected to
  interoperate with.
- `crates/engine/src/control.rs` for the `AckMsg`/`NackMsg`/`NackCause` types
  the exporter must produce, in particular that each carries the **entire**
  accepted/refused `PData` (`accepted: Box<PData>` / `refused: Box<PData>`),
  which is why this exporter's fan-out must reduce to exactly one Ack or
  Nack per accepted pdata message (see
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)).
- This repository's own `docs/issue-drafts/mqtt-bounded-inbound-publish-flow-control.md`
  and `docs/issue-drafts/mqtt-explicit-qos1-acknowledgement-drop-policy.md`,
  which describe outstanding gaps in `ms-mqtt-client`'s *inbound* (receiver)
  path. Both are receiver-side and future-direction; this exporter is
  publish-only and, in baseline, does not depend on `ms-mqtt-client` at all,
  but the second draft's acceptance-vs-abandonment distinction directly
  informs how this exporter must interpret PUBACK outcomes regardless of
  which client backs the adapter (see
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)).

## Classification of findings

Using the categories from
[Reference-Informed OTAP-Native Capability Design](../ai/reference-informed-otap-native-capability-design.md#classify-findings):

| Finding | Classification | Rationale |
| --- | --- | --- |
| An earlier draft of this design published one whole accepted pdata message (an entire OTLP-encoded request) as a single MQTT payload | Reject | Conflicts directly with the envelope contract's 1:1 PUBLISH-to-LogRecord invariant and with this exporter's role as the contract's exporter side; superseded by the per-`LogRecord` model in this revision. |
| An earlier draft of this design depended directly on `ms-mqtt-client` for baseline | Reject (for baseline) | `ms-mqtt-client` unconditionally links OpenSSL even with `default-features = false` (confirmed against this workspace's own pinned dependency and `ms-mqtt-client-pluggable-tls-crypto.md`), which fails this exporter's plaintext-only, zero-cryptography-dependency requirement; superseded by the two-stage `rumqttc`-baseline/`ms-mqtt-client`-long-term strategy in this revision. See [Client strategy](#client-strategy-baseline-vs-long-term-replacement). |
| `rumqttc`'s `default-features = false` build removes every TLS/websocket/proxy dependency (`use-rustls`, `use-native-tls`, `websocket`, `proxy` are all optional, non-default features) | Preserve/Compose | Exactly the property baseline needs; adopted directly as the mechanism satisfying the no-crypto dependency-graph requirement (see [No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement)). |
| `rumqttc`'s `EventLoop` reconnects automatically as long as the application keeps calling `eventloop.poll()`/`connection.iter()` | Compose | A materially different property from `ms-mqtt-client`, which never reconnects on its own; reduces (but does not eliminate) the reconnect logic this exporter must own itself. See [Reconnect and backoff](#reconnect-and-backoff) for what still must be verified and layered. |
| `rumqttc`'s `AsyncClient::publish(...)` returns only an enqueue-acceptance result, with no completion token or PUBACK visibility | Investigate/Compose | Not a defect, but a materially different shape from `ms-mqtt-client`'s tiered completion model; this design does not claim `rumqttc` provides completion tokens and instead composes a bounded, exporter-owned PubAck correlation layer over the polled `EventLoop`'s event stream (see [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation)). Several exact API details this layer depends on are flagged to the parallel spike, not assumed. |
| No built-in retry loop; explicit tiered result reporting (informs the correlation layer's design, not a `rumqttc` property itself) | Preserve/Compose | Matches the Kafka exporter's "emit classified Nack, let `processor:retry` retry" convention already established in this codebase, adapted to per-batch (not per-record) Ack/Nack granularity (see [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)). |
| QoS 2 protocol types exist in `rumqttc`'s `v5` `Request` enum (`PubRec`/`PubRel`/`PubComp`) | Investigate (not adopted for baseline) | Suggests some QoS 2 support exists, unlike `ms-mqtt-client`'s panicking stubs, but this design does not verify or rely on it; QoS 2 stays out of scope per the envelope contract's own conditional non-goal, pending a dedicated design and test pass (see [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)). |
| Sparkplug B encoding | Reject (for baseline) | Out of scope; revisit only as a separate, later design once the raw path is stable. |
| Kafka exporter's static-topic-plus-header-routing-plus-allowlist shape | Preserve/Compose | Reused directly, substituting the envelope contract's `mqtt.topic` LogRecord attribute for Kafka's transport header as the dynamic source; see [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy). |
| `crates/pdata` views abstraction already backend-agnostic over OTAP and OTLP bytes | Compose | Directly reusable for per-`LogRecord` decoding; no new pdata surface area needed (see [Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)). |
| Engine `AckMsg`/`NackMsg` carry one whole `PData`, not a sub-selection | Investigate | This is a genuine architectural constraint, not a defect to fix in this design: it forces an all-or-nothing per-batch Ack/Nack decision and a documented duplicate-on-retry behavior (see [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)). A future engine capability for partial batch acknowledgement is noted as a possible follow-on, not invented here. |

## URN and feature gate

- URN: `urn:otel:exporter:mqtt` (shortcut form `exporter:mqtt`), per
  [`docs/urns.md`](../urns.md).
- Crate: `crates/contrib-nodes` (package `otel-arrow-dfe-contrib-nodes`),
  module `src/exporters/mqtt_exporter/` (mirrors `kafka_exporter`'s module
  layout: `config.rs`, `error.rs`, `exporter.rs`, `metrics.rs`, `mod.rs`,
  `client.rs` for the [publish-client adapter](#the-publish-client-adapter),
  `README.md`).
- Feature gate: `mqtt-exporter`, additionally enabled by the umbrella
  `contrib-exporters` feature. This document proposes the following target
  shape for baseline; it does not itself change `Cargo.toml`:

  ```toml
  [dependencies]
  rumqttc = { workspace = true, optional = true }

  [features]
  mqtt-exporter = [
      "dep:rumqttc",
  ]
  contrib-exporters = [
      "geneva-exporter",
      "azure-monitor-exporter",
      "kafka-exporter",
      "clickhouse-exporter",
      "mqtt-exporter",
  ]
  ```

  and, in the workspace root `Cargo.toml`'s `[workspace.dependencies]`:

  ```toml
  rumqttc = { version = "0.25.1", default-features = false }
  ```

  `default-features = false` is load-bearing, not cosmetic: it is the
  mechanism that keeps every TLS/websocket/proxy crate out of the
  dependency graph (see
  [No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement)).
  Do not add `features = [...]` to this dependency declaration for baseline;
  doing so could silently reintroduce a TLS or crypto crate, which is why an
  automated CI assertion, not only this documented intent, guards the
  requirement.
- Primary metric-set prefix: `exporter.mqtt` (per-op detail metrics) alongside
  the shared `exporter.exports.*` metric set (see
  [Telemetry and error categories](#telemetry-and-error-categories)),
  following the naming convention in `AGENTS.md`.
- Stability: Experimental, matching every other contrib exporter at
  introduction (Kafka, ClickHouse, Azure Monitor, Geneva all start there).

## Configuration and validation

Proposed top-level shape. Compared to a naive "one PUBLISH per pdata"
design, the `logs` block separates **exporter-side policy** (ceilings,
allowlists, static fallbacks) from **per-record values**, which come from
each `LogRecord`'s `mqtt.*` attributes per the envelope contract:

```yaml
type: exporter:mqtt
config:
# Required. Broker connection. Baseline is TCP-only; see the `tls` block
# below for how a TLS-shaped configuration is rejected, not downgraded.
endpoint:
  host: "broker.example.com"
  port: 1883                        # The standard MQTTS port (8883) is
                                     # itself treated as a signal the
                                     # operator intends TLS; see Validation.
client_id: "otap-exporter-1"        # Required in baseline; see
                                     # Session and client IDs.
clean_start: true                   # Default: true.
session_expiry_seconds: 0           # Default: 0 (no persistent session).
keep_alive_seconds: 30              # Default: 30.

auth:
  username: "device-01"             # Optional.
  password: "..."                   # Optional; SHOULD be sourced via a secret
                                     # reference, not inline plaintext (see
                                     # Endpoints, TLS, and auth). Sent over
                                     # plaintext TCP in baseline; see
                                     # Endpoints, TLS, and auth for the
                                     # operator guidance this implies.

tls:
  enabled: false                    # MUST be false (or omitted) in baseline.
                                     # Any other value is a hard config-time
                                     # validation error; baseline never
                                     # attempts, and never silently skips,
                                     # a TLS handshake. This field, and the
                                     # rest of this block, exist only so the
                                     # config schema does not need to change
                                     # shape when the long-term
                                     # ms-mqtt-client-backed replacement
                                     # adds real TLS support; see
                                     # Client strategy and
                                     # Endpoints, TLS, and auth.
  ca_file: null                     # Reserved for the future TLS-capable
  cert_file: null                   # implementation. Setting any of these
  key_file: null                   # in baseline alongside `enabled: false`
                                     # is itself a validation error (see
                                     # Validation rules), since it almost
                                     # always indicates operator intent to
                                     # use TLS that baseline cannot honor.
  insecure: false

logs:
  # --- Topic reconciliation: static config vs. per-record mqtt.topic ---
  topic: "otel/logs"               # Optional static fallback/default topic.
  topic_policy: "attribute"        # "attribute" (default) | "static_only".
                                    # "attribute": use the record's mqtt.topic
                                    # when present and authorized (below); fall
                                    # back to static `topic` when the attribute
                                    # is absent and `topic` is configured;
                                    # otherwise reject that record (permanent).
                                    # "static_only": always use static `topic`,
                                    # ignoring mqtt.topic entirely (for sources
                                    # that never went through receiver:mqtt).
  allowed_topics: []                # Exact-match allowlist for attribute-
                                    # supplied topics (mirrors the Kafka
                                    # exporter's field of the same name).
                                    # Empty = unrestricted (default).
  allowed_topics_regex: []          # Anchored regex allowlist, same semantics
                                    # as the Kafka exporter's field.

  # --- QoS reconciliation: static ceiling/default vs. per-record mqtt.qos ---
  qos_ceiling: 1                    # Maximum permitted QoS: 0 or 1. Default: 1.
  qos_over_ceiling_policy: "reject" # "reject" (default) | "downgrade".
                                    # Applies when mqtt.qos > qos_ceiling.
  qos_default_when_absent: null     # null (default, reject when mqtt.qos is
                                    # absent) or 0 (never 1; see Validation).

  # --- Retain reconciliation ---
  retain_policy: "forbid"           # "forbid" (default) | "allow".
                                    # When "forbid", mqtt.retain = true on a
                                    # record is downgraded or rejected per
                                    # retain_over_policy_action.
  retain_over_policy_action: "reject" # "reject" (default) | "downgrade".
  retain_default_when_absent: false # Used only when mqtt.retain is absent.

  # --- Properties: attribute-sourced per envelope contract, with static
  # --- fallbacks applied only when the corresponding attribute is absent.
  default_content_type: null        # Optional static fallback for mqtt.content_type.
  default_message_expiry_seconds: null # Optional static fallback for
                                       # mqtt.message_expiry_interval.

max_in_flight: 100                 # Bounded outstanding individual PUBLISH
                                    # operations (per LogRecord, not per
                                    # pdata batch). Default: 100. Must be in
                                    # range 1..=100000. See
                                    # Bounded in-flight publishing.

reconnect:
  initial_interval: 1s
  max_interval: 30s
  multiplier: 2.0
  max_elapsed_time: null           # null = retry indefinitely (a connection-level
                                    # outage is not a per-message failure; see
                                    # Reconnect and backoff).
```

### Validation rules

- `tls.enabled` MUST be `false` (or omitted) in baseline. Any truthy value is
  a config-time validation error with a distinct, actionable error message
  ("TLS is not supported by the current MQTT exporter baseline; see
  <link to this document/README> for the long-term replacement status"),
  never a silent fallback to plaintext. Setting `ca_file`, `cert_file`,
  `key_file`, or `insecure: true` while `tls.enabled` is `false` (its only
  legal baseline value) is **also** a validation error, not a no-op, since a
  populated TLS field almost always signals the operator believes TLS is in
  effect. See
  [No silent TLS downgrade](#no-silent-tls-downgrade) for the full rule.
- `endpoint.port: 8883` (the IANA-registered "secure MQTT" port) without an
  accompanying, explicit `tls.enabled: false` is treated the same as any
  other TLS-implying signal and rejected at config time with an error
  directing the operator to either use the plaintext port their broker also
  listens on, or set `tls.enabled: false` explicitly to confirm the port
  choice is intentional (for a broker that, unusually, serves plaintext MQTT
  on 8883). This is a heuristic safety check, not a protocol requirement;
  document it as such so it is not mistaken for an MQTT specification rule.
- `qos_ceiling` MUST be `0` or `1`; `2` (or any other value) is a config-time
  validation error, per this design's out-of-scope QoS 2 decision (see
  [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)). A
  per-record `mqtt.qos = 2` attribute is a **runtime**, per-record validation
  error under the same rule (see
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)).
- `qos_default_when_absent`, if set, MUST be `0`. It is a config-time error to
  set it to `1` or `2`: per the envelope contract, an absent `mqtt.qos`
  attribute "must never silently become QoS 1 or 2" -- only an explicit,
  present attribute value may request QoS 1.
- `topic_policy: "static_only"` REQUIRES `topic` to be set. `topic_policy:
  "attribute"` MAY omit `topic`; if omitted, a record whose `mqtt.topic`
  attribute is absent is rejected (permanent) rather than silently
  discarded or misrouted.
- `client_id` is REQUIRED in baseline (see
  [Session and client IDs](#session-and-client-ids)); it is a config-time
  error to omit it.
- `session_expiry_seconds > 0` with `clean_start: true` is legal per MQTT 5
  (clean start discards prior state but a nonzero expiry keeps the *new*
  session around across a later reconnect); document this combination rather
  than rejecting it.
- `allowed_topics_regex` entries follow the Kafka exporter's rule: each
  pattern is anchored (`\A(?:<pattern>)\z`) and must be a valid standalone
  regular expression on its own, compiled once at construction/reconfigure;
  an invalid pattern is a config-time error.
- `max_in_flight` bounds follow the same rationale as the Kafka exporter:
  reject `0` (no forward progress possible) and cap the upper bound to avoid
  unbounded in-process buffering. This bound is now over individual
  in-flight PUBLISH operations, not accepted pdata messages; see
  [Bounded in-flight publishing](#bounded-in-flight-publishing).

### No silent TLS downgrade

Baseline enforces the "never silently downgrade TLS to plaintext" rule in
two independent layers, deliberately redundant with each other:

1. **Dependency-graph layer**: `rumqttc` is compiled with
   `default-features = false`, so the binary contains no TLS client code at
   all -- there is no `Transport::Tls`-equivalent construction path for a bug
   to accidentally reach, regardless of what configuration validation does
   or fails to do.
2. **Configuration-validation layer**: independent of (1), any config that
   asks for TLS is rejected at startup with a clear, distinct error (see
   Validation rules above), so an operator who intended to run this exporter
   with TLS gets a loud failure to fix before deploying it, not a quiet
   plaintext connection they believe is encrypted.

Both layers must hold; neither depends on the other, and neither is
allowed to regress silently (see
[No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement)
for how layer (1) is continuously verified in CI).

## Endpoints, TLS, and auth

- Transport: baseline supports TCP only, via `rumqttc::v5::MqttOptions`'s
  default transport. TLS (of any kind, including mutual TLS) and WebSocket
  transport are both out of scope for baseline; neither the corresponding
  `rumqttc` Cargo features nor any TLS-shaped configuration are enabled (see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement),
  [Scope](#explicitly-out-of-scope-for-baseline), and
  [No silent TLS downgrade](#no-silent-tls-downgrade)).
- Auth: baseline supports MQTT 5 CONNECT username/password only. Enhanced
  authentication (AUTH/reauthentication flows) is out of scope; flag it as a
  follow-on if a user scenario needs it. Per the envelope contract's trust
  policy, broker endpoint and credentials are always exporter configuration;
  they are never derived from `LogRecord` attributes (see
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)).
- **Credentials travel in plaintext in baseline.** Because baseline never
  negotiates TLS, a configured `auth.username`/`auth.password` is sent over
  an unencrypted TCP connection, exactly as the wire-level MQTT CONNECT
  packet's cleartext username/password fields always are absent a TLS (or
  equivalent) tunnel underneath. Document this prominently in the exporter's
  README: operators who need both immediate delivery and TLS-protected
  credentials while this exporter is on the `rumqttc` baseline should
  terminate TLS outside the exporter's own process -- for example, with a
  broker-side TLS listener reached through an operator-managed TLS-terminating
  proxy or sidecar on the same trusted host or network segment -- rather than
  sending credentials over an untrusted network in cleartext. This is
  different from, and does not conflict with, the "never silently downgrade
  TLS to plaintext" rule: the exporter itself still never claims to provide
  TLS and never silently drops a TLS request; an operator-managed external
  proxy is a deployment topology choice made outside this exporter's control
  and configuration surface entirely.
- Secrets: `password` SHOULD be resolvable through this project's existing
  secret-reference/extension mechanism (whatever the Kafka exporter's `auth`
  config uses for SASL credentials) rather than requiring plaintext in the
  pipeline YAML. **API uncertainty:** confirm the exact secret-reference
  convention used elsewhere in this codebase (e.g. an extension capability
  or `${env:...}` substitution) before implementation; this document assumes
  parity with whatever the Kafka exporter's `auth.sasl.password` already
  supports today.
- The long-term, `ms-mqtt-client`-backed replacement is expected to restore
  TLS (including mutual TLS) support once the pluggable-TLS/ambient-provider
  request lands (see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement));
  this section will need a corresponding revision at that time. Until then,
  this section, and the `tls` config block it governs, describe baseline's
  TCP-only, TLS-rejecting behavior only.

## MQTT5-only implications and QoS 2

Baseline uses `rumqttc`'s `v5` submodule (`rumqttc::v5::{MqttOptions,
AsyncClient, EventLoop}`) exclusively, which implements MQTT 5.0. The
crate's root module (re-exported at `rumqttc::{MqttOptions, Client,
AsyncClient, Connection}`) implements MQTT 3.1.1 instead and must never be
used by this exporter; a code-review or lint check should confirm no import
path reaches the crate root's MQTT 3.1.1 types (see
[Acceptance criteria](#acceptance-criteria)). Operators pointing this
exporter at a broker or managed endpoint that only speaks MQTT 3.1.1 will
see the CONNECT rejected or the transport error out. The exporter should
surface this clearly (a distinct connect-failure event/error category, not a
generic I/O failure) since it is a common deployment-time misconfiguration,
not a transient network issue.

QoS 2: unlike `ms-mqtt-client` (whose QoS 2 methods are documented to panic
if invoked), `rumqttc`'s `v5` `Request` enum includes `PubRec`, `PubRel`, and
`PubComp` variants alongside `Publish` and `PubAck`, suggesting the crate has
some genuine QoS 2 protocol implementation. This design does **not** rely on
that, for two independent reasons:

- The envelope contract itself conditions QoS 2 rejection on "the current
  client library's stated non-goal," not on a specific library's
  capabilities; baseline keeps QoS 2 out of scope as a deliberate scope
  decision (see [Scope](#explicitly-out-of-scope-for-baseline)), pending a
  dedicated design and test pass specifically for it, not as a stopgap
  around a library limitation.
- Whether `rumqttc`'s QoS 2 implementation is complete and robust enough to
  depend on for this exporter has not been verified from the evidence
  reviewed for this document. **Flag to the parallel rumqttc integration
  spike:** confirm the actual behavior and completeness of `rumqttc`'s v5
  QoS 2 support before ever reconsidering it for a future revision of this
  design; do not assume it is production-ready merely because the request
  types exist.

Baseline therefore:

- rejects `qos_ceiling: 2` at config validation time, and rejects any
  per-record `mqtt.qos = 2` attribute at the point that record's publish is
  evaluated (see
  [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)),
  in both cases without ever calling any QoS-2-specific adapter method;
- documents this as a scope decision consistent with the envelope contract,
  not an unconditional claim that the underlying client cannot do it, so it
  is easy to revisit with dedicated evidence later.

## Reconnect, backoff, session and client IDs, cancellation, multicore

### Reconnect and backoff

`rumqttc`'s `v5` `EventLoop` reconnects **automatically** as long as the
application keeps calling `eventloop.poll()` in a loop, per the crate's own
documented behavior ("Automatic reconnections by just continuing the
eventloop.poll()/connection.iter() loop"). This is a materially different
property from `ms-mqtt-client`, which never reconnects on its own and
requires the application to explicitly call `ConnectHandle::connect(...)`
again after every disconnect. Concretely, for baseline:

- The exporter's own driving task's normal "poll the adapter for the next
  event" loop (needed anyway to drive publishes and the PubAck correlation
  layer; see
  [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation))
  is, by itself, sufficient to keep `rumqttc` retrying a dropped connection,
  with no separate reconnect-supervisor code required to *trigger* a
  reconnect attempt.
- **API uncertainty -- flag to the parallel rumqttc integration spike:**
  whether the crate's internal reconnect cadence is configurable (a
  minimum/maximum delay, jitter, or only a fixed retry interval) and, if so,
  how to configure it so it can honor this exporter's own `reconnect`
  config block (`initial_interval`/`max_interval`/`multiplier`/
  `max_elapsed_time`, reusing `processor:retry`'s field names for
  configuration consistency). If the crate's internal cadence is not
  independently configurable to match, the exporter must layer its own
  outer supervisory bound around the automatic reconnect behavior (for
  example, tracking elapsed time since the last successful connection and
  surfacing a "still reconnecting past `max_interval`-scaled backoff"
  health event) so the configuration promise made to operators is honored
  regardless of which internal mechanism `rumqttc` turns out to use. Do not
  assume the crate's default cadence already matches this project's
  configured defaults without confirming it.
- `max_elapsed_time: null` (the recommended default) means the exporter
  never gives up on the connection entirely; a network exporter going
  offline is not the same failure class as a batch being permanently
  refused, and giving up on reconnecting entirely would silently stop all
  future export, not just the failed batch.
- While disconnected, publish submissions must not block indefinitely: apply
  the same bounded in-flight/backpressure behavior described in
  [Bounded in-flight publishing](#bounded-in-flight-publishing), and Nack
  (transient) any pdata batch whose records cannot be submitted before the
  node's shutdown or drain deadline.
- On every reconnect (every new connection epoch), the exporter's own
  connection-scoped state -- most importantly, the
  [PubAck correlation table](#publish-submission-and-puback-correlation),
  since MQTT packet identifiers are only meaningful within one connection's
  session lifetime -- must be entirely cleared, and every entry it held
  resolved as a transient per-record failure. Nothing from the previous
  connection epoch may be assumed valid against a new one; a stale
  correlation-table entry surviving a reconnect could otherwise be matched
  against an unrelated later PUBACK using a reused packet identifier, which
  must never be allowed to happen silently.

### Session and client IDs

- `client_id`: a stable, operator-configured client ID is REQUIRED in
  baseline (see [Validation rules](#validation-rules)), and is REQUIRED for
  any deployment using `clean_start: false` in particular (a persistent
  session is meaningless without a stable identity across reconnects).
  **API uncertainty -- flag to the parallel rumqttc integration spike:**
  whether `rumqttc`'s `v5::MqttOptions` API supports an MQTT 5 zero-length
  ("server-assigned") Client Identifier end-to-end; `MqttOptions::new`'s
  public constructor signature takes a client ID argument directly, which
  suggests server-assigned IDs may not be a first-class path in this
  crate's API, unlike `ms-mqtt-client`'s explicit `client_id: None` support.
  Until confirmed, baseline requires an operator-supplied, non-empty
  `client_id` unconditionally, rather than relying on unverified
  zero-length-identifier behavior.
- Because pipeline nodes can be replicated (for example, multiple pipeline
  instances or hot-reload creating a new exporter instance), the exporter
  MUST NOT invent client IDs automatically at random on every start if
  `clean_start: false` -- that would silently discard persistent session
  state on every restart. Static or externally-derived client IDs are the
  operator's responsibility to keep unique.

### Cancellation

Cancellation covers two distinct situations, both of which the
[PubAck correlation layer](#publish-submission-and-puback-correlation) and
the adapter must handle explicitly, not merely as a side effect of dropping
a future:

- **Reconnect-triggered cancellation**: as described above, every
  correlation-table entry outstanding when a new connection epoch begins is
  cancelled (resolved transient) as part of clearing the table, because its
  packet identifier is no longer meaningful once the session it belonged to
  is gone.
- **Shutdown-triggered cancellation**: on node shutdown (see
  [Lifecycle, drain, and shutdown](#lifecycle-drain-and-shutdown)), any
  correlation-table entry still outstanding when the drain timeout expires
  is cancelled (resolved transient, unless a permanent failure was already
  observed for that batch) and its slot freed, so shutdown itself is bounded
  and does not wait indefinitely on a broker that stops responding.
- In both cases, cancelling a correlation entry must be idempotent with
  respect to a PUBACK that arrives immediately afterward: if the adapter
  observes a PUBACK for a packet identifier whose entry was already
  cancelled (reconnect cleared the table, or shutdown's drain timeout
  already fired), that late PUBACK is discarded rather than resolving a
  record twice or resolving a since-reused packet identifier's new entry
  incorrectly.

### Multicore

The OTAP dataflow engine is thread-per-core and share-nothing (see
`AGENTS.md` and `.github/instructions/rust-review.instructions.md`). Each
exporter node instance owns exactly one publish-client adapter instance (one
`rumqttc::v5::AsyncClient`/`EventLoop` pair, in baseline) on its own core;
there is no cross-core sharing of a single MQTT connection. If a pipeline
runs multiple parallel instances of this exporter (for throughput), each
instance is a fully independent MQTT client with its own `client_id`, and
the config must make per-instance client ID uniqueness the operator's
responsibility (for example, by requiring a per-instance suffix), the same
way a multi-instance Kafka producer would need distinct `client.id`s.

This design is also consistent with the pdata decoding side: the views used
to enumerate `LogRecord`s from OTLP bytes (`RawLogsData`) are themselves
documented as intentionally `!Send` (see
[Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)),
so the exporter node's per-record decode-and-publish loop is expected to stay
entirely within one core's task set.

**API uncertainty -- flag to the parallel rumqttc integration spike:**
confirm whether `rumqttc::v5::AsyncClient` and `EventLoop` are `Send` or
`!Send`. The crate's own asynchronous usage example drives the `EventLoop`
via `task::spawn(async move { ... })` around the `AsyncClient`, and drives
`eventloop.poll()` directly in the surrounding context, which is at least
consistent with (though does not by itself confirm) `Send` types -- a
different posture from `ms-mqtt-client`'s explicitly `!Send`-friendly
design. This is a genuine architectural risk for this project's `!Send`
future preference, not something to assume away: the spike must determine
whether the adapter's `rumqttc`-driving task can be kept `!Send`-compatible
(for example, by polling the `EventLoop` directly within the node's own
`!Send` future rather than spawning it onto a `Send`-requiring executor
task), or whether a `Send` boundary is unavoidable and must be isolated to
the adapter alone (with the `!Send` pdata-decoding path never crossing it),
before implementation begins. A hidden `Send` requirement crossing the
adapter boundary would be a flagged concern per the Rust review rules.

## Topic, QoS, retain, and property policy

This exporter implements the envelope contract's
[Exporter-side validation (LogRecord -> PUBLISH)](mqtt-raw-envelope-contract.md#exporter-side-validation-logrecord---publish)
table and
[Trust and security policy for exporter replay](mqtt-raw-envelope-contract.md#trust-and-security-policy-for-exporter-replay)
rules verbatim, for every individual `LogRecord` yielded by
[Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords).
This section only defines how this exporter's own configuration (above)
participates in those contract-defined decisions.

- **Topic**: read `mqtt.topic` from the record's attributes when
  `topic_policy: "attribute"` (the default). If present, it MUST pass
  `allowed_topics`/`allowed_topics_regex` when either is non-empty (contract
  trust rule 2); a disallowed topic is a permanent, per-record rejection,
  never silently rewritten to the static `topic`. If `mqtt.topic` is absent,
  fall back to the static `topic` when configured, or reject the record
  (permanent) when it is not, per the contract's "missing or empty ->
  reject" validation row. `topic_policy: "static_only"` always uses the
  static `topic` and never reads `mqtt.topic`, for pipelines where `logs`
  input never carries MQTT provenance (for example, synthetic logs from an
  unrelated receiver that an operator still wants published to one fixed
  topic).
- **QoS**: read `mqtt.qos` from the record's attributes. `0` or `1` present
  and at or below `qos_ceiling` is used directly. A value above
  `qos_ceiling` is downgraded to `qos_ceiling` or rejected per
  `qos_over_ceiling_policy` -- downgrading without an explicit operator
  opt-in is never acceptable, per the contract's trust rule 3, which is why
  `qos_over_ceiling_policy` has no implicit default that silently weakens
  delivery. `mqtt.qos = 2` is always rejected regardless of `qos_ceiling`
  (see [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)).
  An absent `mqtt.qos` uses `qos_default_when_absent` only if it is
  configured (and, per validation, it can only be configured as `0`);
  otherwise the record is rejected.
- **Retain**: read `mqtt.retain` from the record's attributes, or
  `retain_default_when_absent` if the attribute is absent. `retain_policy:
  "forbid"` (the default) downgrades a `true` value to `false` or rejects
  the record per `retain_over_policy_action`, matching the same "explicit
  opt-in, no silent downgrade" posture as QoS. `retain_policy: "allow"`
  publishes the requested value unconditionally.
- **Properties**: `mqtt.content_type` and `mqtt.message_expiry_interval`
  attributes map onto the publish-client adapter's `PublishProperties`
  request type (backed, in baseline, by `rumqttc::v5::mqttbytes::v5::PublishProperties`'s
  `content_type`/`message_expiry_interval` fields) when present;
  `default_content_type`/`default_message_expiry_seconds` apply only when
  the corresponding attribute is absent (a default, not an override).
  `mqtt.user_properties` (the array-of-pairs form defined by the contract)
  is republished verbatim as MQTT 5 User Properties, order-preserving,
  including repeated keys, per the contract's trust rule 4 ("opaque
  pass-through data... never used to affect connection or authorization
  decisions"). Baseline does not populate `response_topic`/
  `correlation_data` from anywhere but the record's own
  `mqtt.response_topic`/`mqtt.correlation_data` attributes (also
  pass-through per the contract), and never synthesizes them. **API
  uncertainty -- flag to the parallel rumqttc integration spike:** confirm
  the exact `rumqttc::v5` `AsyncClient` method or `Publish`-construction
  path needed to attach a full `PublishProperties` value (not just
  topic/QoS/retain/payload) to an outbound publish, since the crate's
  top-level usage examples only demonstrate the simpler 4-argument
  topic/QoS/retain/payload form.
- **Body**: `body` must be `BytesValue` or `StringValue` per the contract's
  body policy; any other `Value` variant, or an unset body, is a permanent,
  per-record rejection -- the exporter never coerces a numeric/bool/array/
  kvlist body to bytes.
- **Protocol-version consistency**: if `mqtt.protocol_version` is present and
  is `"3.1.1"` while an MQTT-5-only attribute (any property listed above) is
  also present, the record is rejected, per the contract's MQTT 3.1.1 vs
  MQTT 5 rule. Baseline's adapter is MQTT 5 only (it uses `rumqttc::v5`
  exclusively; see
  [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)),
  so baseline in practice always uses its own negotiated protocol version
  for the outbound PUBLISH, never a per-record value; this consistency check
  exists to catch and reject an obviously self-contradictory record rather
  than silently ignore the mismatch.
- **Trust boundary**: per the contract, `network.peer.address` and similar
  connection-scoped attributes are never used to select this exporter's
  outbound connection, and payload bytes are never parsed, executed, or
  interpreted -- only republished verbatim.

Every rejection in this section is scoped to **one** `LogRecord`/PUBLISH; it
never, by itself, decides the outcome of the whole accepted pdata batch. See
[DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction) for
how per-record outcomes roll up to the batch's single terminal Ack/Nack.

## Bounded in-flight publishing

`max_in_flight` bounds the number of concurrently outstanding **individual
PUBLISH operations**, not accepted pdata batches. This matters because a
single accepted batch may itself contain far more `LogRecord`s than
`max_in_flight` (a `processor:batch` upstream can easily produce batches of
thousands of records):

- The exporter maintains, per accepted batch, the ordered sequence of
  `LogRecordView`s from
  [Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)
  and submits records to the publish-client adapter greedily, up to
  `max_in_flight` concurrently outstanding submissions across all batches
  currently being drained, never all of one large batch at once.
- Records within one batch are submitted in their deterministic traversal
  order (see [Deterministic publish order](#deterministic-publish-order));
  batches themselves are submitted in the order the exporter accepted them
  from upstream, consistent with the pipeline's normal per-node message
  ordering.
- A bounded per-batch tracking structure (an in-progress-count, and a
  worst-case permanent/transient flag observed so far) accumulates each
  record's terminal outcome as it completes -- this is the same "fan one
  accepted request out into multiple units, track completion back to a
  single decision" shape the batch processor already implements for its own
  oversize-entry splitting (see `crates/core-nodes/src/processors/batch_processor/README.md`),
  adapted here to per-record MQTT publishes instead of per-fragment OTLP
  batches.
- When the in-flight set is at `max_in_flight`, the exporter stops
  submitting further records (from the current batch or any newly-accepted
  batch) and only drains completions, so backpressure propagates upstream
  through normal channel-full behavior rather than growing an unbounded
  internal queue -- the same principle the Kafka exporter's `max_in_flight`
  already establishes, now counting records instead of whole pdata messages.
- `max_in_flight` is also the capacity bound of the
  [PubAck correlation table](#publish-submission-and-puback-correlation)
  described below: the table can hold at most `max_in_flight` outstanding
  entries, since that is also the maximum number of records the exporter
  will ever have submitted-but-not-yet-resolved at one time.

This bound sits below `rumqttc`'s own internal request-channel capacity
(`MqttOptions`'s `request_channel_capacity`, confirmed in `v5/mod.rs`'s
`MqttOptions` struct definition; the crate's own examples construct
`AsyncClient::new(options, 10)` with a small capacity). Document the
relationship: `max_in_flight` should not be configured larger than the
adapter's own configured request-channel capacity, or submissions past that
capacity will block on the adapter's internal channel send rather than on
the exporter's own tracked set, defeating the intended single point of
backpressure. **API uncertainty -- flag to the parallel rumqttc integration
spike:** confirm whether `request_channel_capacity` (and `max_request_batch`,
also present on `MqttOptions`) should be sized to match `max_in_flight`
directly, sized larger to avoid a redundant second backpressure point, or
left at a library default with `max_in_flight` as the sole operator-facing
bound; this document does not prescribe a specific relationship, only that
one must be chosen deliberately before implementation.

## Publish submission and PUBACK correlation

`rumqttc` does **not** provide per-operation completion tokens the way
`ms-mqtt-client` does; this design does not claim otherwise anywhere in this
document. `AsyncClient::publish(...)` enqueues a publish request into an
internal channel and returns only whether that enqueue succeeded -- it gives
no visibility into whether, or when, the broker eventually acknowledges the
publish. To recover that information, this design's publish-client adapter
(baseline) builds a bounded, exporter-owned **PubAck correlation layer** on
top of the polled `v5` `EventLoop`'s event stream. This section defines that
layer's design; several exact API details it depends on are called out
explicitly as items for **the parallel rumqttc integration spike** to
confirm against the pinned crate version before implementation, not
assumptions this design makes on the spike's behalf.

### Submit

The adapter calls `AsyncClient::publish(...)` (or the richer v5 form that
also attaches `PublishProperties`; see the API uncertainty noted in
[Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy))
for one record. This call's `Result` reports only enqueue-acceptance: an
error return (the exact error type/variant is an
[open API uncertainty](#open-api-uncertainties-do-not-invent-verify-before-implementation))
means the adapter's underlying client/eventloop pair is gone or its request
channel is closed, and is treated as an immediate per-record transient
failure, conceptually analogous to (but not the same named type as)
`ms-mqtt-client`'s `DetachedError`.

### Correlate

For a QoS 1 publish, the exporter must learn the packet identifier (pkid)
`rumqttc` assigns to that publish so a later PUBACK can be matched back to
the originating record. Because `AsyncClient::publish(...)`'s own return
value does not hand back the pkid (assignment happens later, inside the
`EventLoop`, when the request is dequeued and actually sent), the
correlation layer instead observes the same polled `EventLoop`'s
`Event::Outgoing(Outgoing::Publish(pkid))` notification, which the crate
emits for each QoS 1/2 publish it sends, and binds that pkid to the oldest
still-unbound submission for that connection epoch, on the assumption that
`Outgoing::Publish` notifications are emitted in the same relative order as
the corresponding `AsyncClient::publish(...)` calls were made.

**This ordering assumption is the crux of the correlation layer and must
not be treated as verified by this document.** Flag to the parallel rumqttc
integration spike: confirm, against the pinned `rumqttc` version's actual
behavior (via source inspection and/or a targeted integration test), that
`Event::Outgoing(Outgoing::Publish(_))` notifications are strictly
FIFO-ordered with respect to submission order, including under the crate's
own internal request batching (`MqttOptions::max_request_batch`). If FIFO
ordering does not hold precisely, the correlation layer's binding step must
be redesigned (for example, by requiring the adapter to submit one publish
at a time and await its `Outgoing::Publish` notification before submitting
the next, at some throughput cost) rather than silently mis-binding a PUBACK
to the wrong record.

A QoS 0 publish has no pkid and no PUBACK; the correlation layer instead
resolves a QoS 0 record as "sent" as soon as its own `Event::Outgoing(
Outgoing::Publish(_))` notification is observed (matching the same
ordering-based binding above, since QoS 0 publishes also appear in this
notification stream), consistent with the
[QoS 0 semantics](#qos-0-semantics) section's "released for transmission,
not a delivery guarantee" wording.

### Bound and clear

- The correlation table's capacity is bounded to `max_in_flight` (see
  [Bounded in-flight publishing](#bounded-in-flight-publishing)); it cannot
  grow unboundedly even under a slow or wedged broker.
- A bounded per-record correlation timeout guards against a broker that
  accepts a publish but never sends a PUBACK on an otherwise healthy-looking
  connection; on timeout, the record is resolved transient and its table
  entry removed, freeing its `max_in_flight` slot.
- The entire table is cleared, and every entry it held resolved as
  transient, on every reconnect/new-epoch transition (see
  [Reconnect and backoff](#reconnect-and-backoff)) and on shutdown drain
  timeout (see [Cancellation](#cancellation) and
  [Lifecycle, drain, and shutdown](#lifecycle-drain-and-shutdown)), since a
  packet identifier from a prior connection epoch is never valid against a
  new one.

### Complete

When `Event::Incoming(Packet::PubAck(ack))` is observed (the v5 `PubAck`
type carries a pkid and an MQTT 5 PUBACK reason code, per
`rumqttc::v5::mqttbytes::v5`), the correlation layer resolves the matching
table entry:

- **Success** if the reason code indicates success or
  no-matching-subscribers (the same two "not actually an error" reason codes
  this project already treats as success for QoS 1 elsewhere in its MQTT
  design work).
- **Failure**, classified permanent or transient by reason code, otherwise
  (see the classification table in
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)).

**API uncertainty -- flag to the parallel rumqttc integration spike:**
enumerate the complete set of v5 PUBACK reason-code variants exposed by the
pinned `rumqttc` version's `mqttbytes::v5` module and classify each as
permanent or transient in the exporter's `error.rs`, following the same
allowlist discipline the Kafka exporter's `error.rs` already documents
("classify only a conservative allowlist of codes that can never succeed on
retry as permanent; default unknown/unclassified codes to transient"). This
document does not enumerate that table exhaustively, to avoid asserting
specifics not directly confirmed against the pinned version.

### What this replaces

An earlier draft of this document described a three-stage
submission/completion/PUBACK model borrowed directly from `ms-mqtt-client`'s
own API shape (a `Result<CompletionToken, DetachedError>` followed by an
awaitable completion). That shape does not exist in `rumqttc` and is not
reused here; the bounded correlation layer above is this design's own
composition, built specifically to compensate for the difference, and is
scoped to baseline's adapter implementation only. A future `ms-mqtt-client`-
backed adapter implementation (see
[Client strategy](#client-strategy-baseline-vs-long-term-replacement)) would
not need this correlation layer, since that crate reports completion
natively; it is not part of the `PublishClient` trait's public contract
itself, only of baseline's implementation of it.

## DFE Ack/Nack and retry interaction

### Whole-batch Ack/Nack is the only decision the engine allows

The DFE engine's `AckMsg<PData>` and `NackMsg<PData>` each carry the
**entire** accepted or refused pdata (`accepted: Box<PData>` /
`refused: Box<PData>`, per `crates/engine/src/control.rs`); there is no
engine-level mechanism to acknowledge part of an accepted pdata message and
refuse another part. This exporter fans one accepted batch out into `N`
individual PUBLISH operations, but the engine forces exactly one terminal
Ack or Nack decision for the whole batch. Concretely:

- The exporter waits for **all N** of a batch's per-record publish attempts
  to reach a terminal outcome (success or failure) before making the
  batch's Ack/Nack decision. It does not eagerly Nack the batch the moment
  the first record fails, because other records' PUBLISH operations may
  already be in flight on the wire and cannot be un-sent, and because a
  premature decision would need to be revised once the remaining records
  settle.
- **The batch is Acked if and only if all N records reached a terminal
  success** (QoS 0 "sent," or QoS 1 PUBACK resolved success per
  [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation)).
- **The batch is Nacked if any of the N records reached a terminal
  failure**, once all N have settled. The Nack's `permanent` flag reflects
  the **worst-case** per-record outcome: `true` if *any* failing record was
  individually classified permanent (see the classification table below),
  `false` only if every failing record was classified transient. This is a
  deliberate, documented imprecision forced by the single `permanent: bool`
  per whole `NackMsg`; see
  [Partial success and retry duplicates](#partial-success-and-retry-duplicates)
  for its consequence.

This whole-batch reduction is the same shape the batch processor already
implements when it fans one accepted request out into multiple downstream
fragments and tracks "ACK/NACK-sensitive request state" back to a single
decision (see
`crates/core-nodes/src/processors/batch_processor/README.md`); this design
reuses that precedent rather than inventing a new fan-out/fan-in pattern.

### Per-record outcome classification

| Correlation-layer outcome | Per-record classification | Rationale |
| --- | --- | --- |
| QoS 0 publish's `Outgoing::Publish` notification observed | Success | QoS 0 has no server acknowledgement; "released for transmission" is the strongest success signal available for QoS 0. |
| QoS 1 publish's PUBACK resolved with a success/no-matching-subscribers reason code | Success | Server-confirmed delivery. |
| Envelope-contract validation rejection (missing/empty topic, disallowed topic, `qos`/`retain` over policy with `reject` action, non-`BytesValue`/`StringValue` body, protocol-version/MQTT-5-property mismatch) | Failure, permanent | The same record will not become valid on retry without an operator or upstream-data change. |
| Adapter enqueue failure (`AsyncClient::publish(...)` returns an error; client/eventloop pair unavailable) | Failure, transient | Almost always because the exporter's own reconnect handling is between connections or shutting down. Retriable once reconnected. |
| Correlation-table entry cancelled by a reconnect/new-epoch clear or a shutdown drain timeout (see [Cancellation](#cancellation)) | Failure, transient | The record's fate is unknown; MQTT QoS 1 in-flight state should be redelivered by the broker after session resume, but the exporter cannot itself observe that redelivery, so it must not claim success. This mirrors this repository's own `mqtt-explicit-qos1-acknowledgement-drop-policy.md` principle that an unresolved/unknown outcome must never be reported as success. |
| Correlation-table entry resolved by its own per-record timeout (broker never PUBACKed on an otherwise healthy connection) | Failure, transient | May succeed on retry once the underlying cause (broker overload, network partition) clears. |
| PUBACK resolved with a reason code indicating a permanent condition (analogous to `TopicNameInvalid`, `NotAuthorized`, `PayloadFormatInvalid`, `PacketIdentifierInUse`\*) | Failure, permanent | The same request will not succeed on retry without an operator/config change. |
| PUBACK resolved with a reason code indicating a transient condition (analogous to `QuotaExceeded`, `ImplementationSpecificError`, `UnspecifiedError`) | Failure, transient | May succeed on retry (broker-side quota/backpressure). |

\* `PacketIdentifierInUse`-equivalent reason codes should not occur in
normal operation given the adapter owns packet-identifier correlation;
treat it as permanent and log it prominently as a probable adapter or
session-state bug if it appears, rather than retrying it silently forever.

**API uncertainty -- flag to the parallel rumqttc integration spike:** the
exact set and spelling of `rumqttc::v5::mqttbytes::v5` PUBACK reason-code
variants was not enumerated from the evidence reviewed for this document.
Before implementation, enumerate every variant from the pinned dependency
version and classify each as permanent or transient in `error.rs`, following
the same allowlist discipline noted in
[Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation).

Connection-level failures that are not per-record (for example, CONNECT
rejected, or the reconnect loop's own bound, if any, being exceeded) should
be surfaced as a dedicated exporter health/lifecycle event, not folded
silently into per-record outcomes, since a sustained outage affects every
in-flight and future record the same way; while such an outage lasts, every
currently in-flight record's submission or correlation will independently
surface as an enqueue failure or a cancelled/timed-out correlation entry
(transient), which is sufficient to make every affected batch Nack
transiently without any special-cased connection-level Nack path.

### Partial success and retry duplicates

Because the engine can only Ack or Nack the whole original batch, and
`processor:retry` resubmits the **unmodified, byte-identical** `refused`
pdata on a transient Nack, a batch that partially succeeded before failing
will have its already-succeeded records **republished** when the batch is
retried:

- The exporter has no persisted state correlating a retried batch with the
  prior attempt (each invocation is stateless from the exporter's point of
  view; the retried pdata is indistinguishable from a first attempt except
  for its content being identical to a previous one). It therefore cannot
  detect "this record already succeeded last time" and skip it.
- This is consistent with, not a violation of, MQTT's own delivery
  semantics: QoS 1 is "at least once," not "exactly once," and QoS 0 offers
  no delivery guarantee at all. This exporter does not claim to add
  exactly-once semantics on top of MQTT; it only guarantees that a record is
  never silently dropped without either a confirmed success or a Nack that
  makes the failure observable and retryable.
- **This must be documented prominently** in the exporter's `README.md` as a
  user-visible behavior: enabling an upstream `processor:retry` in front of
  this exporter trades "no data loss on transient failure" for "possible
  duplicate PUBLISH messages for records that had already succeeded before
  the failing record(s) were encountered." Downstream consumers that need
  deduplication must do so themselves (for example, via a content hash, an
  idempotency key carried in `mqtt.correlation_data` or `mqtt.user_properties`
  by the original producer, or a broker-specific feature), exactly as any
  other at-least-once MQTT publisher's downstream consumers must.
- The exporter emits a dedicated event (see
  [Telemetry and error categories](#telemetry-and-error-categories)) whenever
  a batch is Nacked after at least one of its records already reached a
  terminal success, so operators can see when this condition occurs, even
  though the exporter cannot prevent it.
- A future engine capability to Ack part of a batch and Nack another part
  (splitting `PData` along the lines this exporter already computes for its
  own tracking) would remove this limitation, but that capability does not
  exist today and is not invented by this document; see the corresponding
  row in [Classification of findings](#classification-of-findings).

### Deterministic publish order

The exporter MUST submit a batch's `N` records to the publish-client adapter
in one deterministic order: the traversal order defined in
[Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)
(resource index, then scope index, then log-record index within scope),
which is the same order both the OTAP and OTLP-bytes `LogsDataView`
implementations yield and the same order the original wire representation
stored the records in.

- Determinism applies to **submission order**, not completion order.
  Network and broker timing determine when each record's PUBACK arrives or
  its "sent" notification resolves; the bounded in-flight set may observe
  completions out of submission order, and that is expected and harmless,
  since only the batch's final Ack/Nack decision -- not completion order --
  is observable outside the exporter.
- Submission order is additionally load-bearing for the
  [PubAck correlation layer's](#publish-submission-and-puback-correlation)
  ordering-based pkid binding: a batch's records must be submitted to the
  adapter one deterministic order at a time so that binding remains correct.
- Because retrying a batch resubmits the exact same `PData`, and the same
  `PData` decodes to the same `LogRecordView` sequence in the same order
  every time (the view traversal is a pure function of the pdata's
  contents), a retried batch always re-attempts its records in the same
  order as the original attempt. This makes the
  [partial-success duplicate behavior](#partial-success-and-retry-duplicates)
  above reproducible rather than randomly reordered across attempts, which
  matters for any downstream consumer applying its own ordering-sensitive
  dedup logic.
- MQTT itself only guarantees ordering per publishing connection and QoS
  level, and does not guarantee ordering across different topics; this
  requirement bounds what the exporter controls (submission order to its
  own client), not broker-side or cross-topic behavior.

## QoS 0 semantics

QoS 0 has no server acknowledgement. A per-record success outcome for a QoS 0
publish means only "the adapter observed this PUBLISH's outgoing
notification" -- it is not a delivery guarantee, and it is not sufficient by
itself to Ack the batch (all N records, including any QoS 1 records in the
same batch, must still succeed; see
[Whole-batch Ack/Nack is the only decision the engine allows](#whole-batch-acknack-is-the-only-decision-the-engine-allows)).
Document this prominently in the exporter's README (mirroring how MQTT
itself documents QoS 0 as at-most-once, no acknowledgement), and telemetry
must not describe a QoS 0 per-record success as "delivered" or "confirmed";
use wording such as "sent" or "released for transmission" to avoid implying
a guarantee the protocol does not provide.

If the underlying TCP connection drops after a QoS 0 message is handed to
the OS socket but before it reaches the broker, the adapter has no way to
detect or report that loss; this is inherent to QoS 0 and is not a bug in
either the exporter or its underlying MQTT client.

## Lifecycle, drain, and shutdown

- On node start, connect (respecting `reconnect` backoff on initial connect
  failure too, not only post-connection drops) before accepting pdata, or
  admit pdata into the bounded in-flight set immediately and let the
  in-flight/backpressure mechanism naturally hold its records until the
  first connection succeeds -- pick one and document it; recommend the
  latter so a slow-to-connect broker does not block pipeline startup
  entirely, as long as the bounded in-flight set still enforces
  backpressure.
- On drain/shutdown: stop accepting new pdata, allow every already-accepted
  batch's in-flight records to reach a terminal outcome up to a bounded
  drain timeout (mirroring the Kafka exporter's flush timeout use of
  `timeout_ms`), then call the adapter's `disconnect()` (backed by
  `AsyncClient::disconnect()` in baseline) and continue driving the
  adapter's event loop until the disconnect completes.
- Any batch whose records have not all reached a terminal outcome when the
  drain timeout expires is terminally Nacked as a whole (transient, unless a
  permanent per-record failure was already observed for that batch), even if
  some of its records already succeeded -- this is the same
  [partial-success duplicate](#partial-success-and-retry-duplicates)
  behavior applied at shutdown rather than mid-run, and must be documented
  identically. This is also the shutdown-triggered path described in
  [Cancellation](#cancellation): outstanding correlation-table entries are
  cancelled and their slots freed once the drain timeout fires, not left to
  block shutdown indefinitely. This mirrors the Kafka exporter's "shutdown
  flushing failed or timed out; queued and in-flight messages were purged"
  event pattern, adapted to per-record granularity.
- Live reconfiguration: out of scope for the first implementation slice.
  If added later, it must address the same two gaps the Kafka exporter's
  live-reconfiguration section documents (in-flight data crossing
  configurations, and a blocking swap), rather than reintroducing them, and
  must additionally define what happens to a batch whose records were
  submitted under one topic/QoS/retain policy but complete under a newly
  swapped-in policy.

## Telemetry and error categories

Follow the Kafka exporter's telemetry shape (shared `exporter.exports.*`
metric set plus a component-specific detail metric set), keeping all
attributes bounded per
[`docs/telemetry/attributes-guide.md`](../telemetry/attributes-guide.md):
topic names, client IDs, and broker hostnames MUST NOT become metric
attribute values. Because this exporter fans one pdata batch out into N
publishes, telemetry is reported at **two** granularities that must not be
confused with each other:

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.exports.messages` | `{message}` | `signal`, `outcome` | Reused shared metric set, at **whole accepted pdata batch** granularity; `outcome` is `success` (all N records succeeded) or `failure` (batch Nacked per [Whole-batch Ack/Nack](#whole-batch-acknack-is-the-only-decision-the-engine-allows)). |
| `exporter.exports.duration` | `s` | `signal`, `outcome` | Reused shared metric set, same batch granularity: time from dequeuing the pdata through the batch's terminal Ack/Nack decision (i.e., until the last of its N records settles). |
| `exporter.mqtt.publishes.messages` | `{message}` | `signal`, `qos`, `outcome` | Individual PUBLISH attempts (**per LogRecord**, not per batch); `outcome` is `success` or `failure`; `qos` is `0` or `1` (bounded, never the requested-but-rejected `2`). |
| `exporter.mqtt.publishes.duration` | `s` | `signal`, `operation`, `outcome` | Time spent, per record, on `submit` (enqueue) or `puback` (correlation-layer resolution, QoS 1 only). |
| `exporter.mqtt.failures.messages` | `{message}` | `signal`, `error.type` | Failed publish attempts by actionable reason (per record). |
| `exporter.mqtt.connection.state` | n/a (event or gauge) | none | Connected/disconnected/reconnecting state transitions. |

`error.type` (bounded enum, no free-form MQTT reason strings as attribute
values) should include at least: `topic_missing`, `topic_not_allowed`,
`qos_over_ceiling`, `qos2_requested`, `retain_not_allowed`,
`body_wrong_type`, `protocol_version_mismatch`, `tls_config_rejected`
(config-time; see [No silent TLS downgrade](#no-silent-tls-downgrade)),
`enqueue_failed` (adapter could not accept the submission),
`correlation_cancelled` (reconnect/shutdown cancelled an outstanding entry),
`correlation_timeout` (no PUBACK before the per-record timeout),
`puback_permanent`, `puback_transient`, `connect_rejected`, `timeout`, and
`other`. Reason strings (broker-reported reason strings/properties) belong
in log events, not metric attribute values, per the attributes guide's
prohibition on unbounded raw error messages as metric attributes.

The exporter cannot itself detect that a given publish is a retried
duplicate of an already-succeeded prior attempt (see
[Partial success and retry duplicates](#partial-success-and-retry-duplicates));
`exporter.mqtt.publishes.messages{outcome="success"}` will legitimately
double-count such records across attempts, and no separate "duplicate"
counter is defined here because the exporter has no reliable way to compute
one. Operators wanting duplicate visibility must correlate at the broker or
consumer side.

Events (mirroring the Kafka exporter's `kafka.exporter.*` event naming under
an `mqtt.exporter.*` namespace):

| Event | Severity | Description |
| --- | --- | --- |
| `mqtt.exporter.config.tls_rejected` | `error` | Startup configuration validation rejected a TLS-shaped config (see [No silent TLS downgrade](#no-silent-tls-downgrade)); the node fails to start rather than connecting in plaintext. |
| `mqtt.exporter.connect.failed` | `warn` | CONNECT attempt failed or was rejected by the broker; includes the connect-error category but not credentials. |
| `mqtt.exporter.connect.mqtt5_required` | `warn` | The broker rejected CONNECT in a way consistent with a non-MQTT-5 endpoint (see [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)). |
| `mqtt.exporter.reconnecting` | `info` | A disconnect was observed and reconnect is in progress (whether driven entirely by the adapter's own automatic behavior or an exporter-layered supervisory bound; see [Reconnect and backoff](#reconnect-and-backoff)). |
| `mqtt.exporter.publish.rejected` | `warn` | A record's publish was rejected per an envelope-contract validation rule (see [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy)); includes `error.type`, not raw attribute values. |
| `mqtt.exporter.publish.enqueue_failed` | `warn` | The adapter could not accept a record's submission (client/eventloop unavailable). |
| `mqtt.exporter.publish.correlation_cancelled_or_timed_out` | `warn` | A record's correlation-table entry was cancelled by a reconnect/shutdown, or resolved by its own per-record timeout, without ever observing a PUBACK. |
| `mqtt.exporter.publish.puback_rejected` | `warn` | A PUBACK resolved with a failure reason code for a record; includes the reason classification (permanent/transient), not the raw broker reason string. |
| `mqtt.exporter.batch.partial_failure` | `warn` | A batch was Nacked after at least one of its records already reached a terminal success; any retry of this batch will republish those records (see [Partial success and retry duplicates](#partial-success-and-retry-duplicates)). |
| `mqtt.exporter.shutdown.drain_timed_out` | `warn` | Drain timeout expired with one or more batches' records still outstanding; each such batch was terminally Nacked as a whole. |

## Tests

Neither `rumqttc` nor `ms-mqtt-client` ships a public, in-process mock broker
analogous to `rdkafka::mocking::MockCluster` (used by this repository's
`crates/contrib-nodes/src/common/kafka/test/` suite), so the equivalent
in-process, no-Docker test tier is not available for MQTT out of the box.
Recommended test tiers:

1. **Unit tests**: config validation (TLS-shaped-config rejection at every
   combination described in
   [No silent TLS downgrade](#no-silent-tls-downgrade), QoS 2
   ceiling/attribute rejection, `client_id` requiredness, `max_in_flight`
   bounds, `topic_policy`/`allowed_topics`/`allowed_topics_regex`
   combinations), per-record outcome classification against a fixed table of
   PUBACK reason codes, and reconnect-backoff scheduling logic, all without a
   network connection. Additionally, and newly necessary given this design's
   own [PubAck correlation layer](#publish-submission-and-puback-correlation):
   unit tests for the correlation table's pkid-binding, capacity bound,
   per-record timeout, and its full-clear-on-reconnect/shutdown-cancellation
   behavior, driven against a fake/scripted stream of adapter events rather
   than a real broker, so this exporter-owned logic is verified independent
   of network timing.
2. **Pdata decoding tests** (no network needed): given a hand-built OTAP
   Arrow logs batch and an equivalent hand-built OTLP `ExportLogsRequest`
   bytes payload -- each containing the same `N` `LogRecord`s with a mix of
   present/absent `mqtt.*` attributes -- assert that both decode through
   `OtapLogsView`/`RawLogsData` into the same ordered sequence of per-record
   topic/QoS/retain/property/body decisions, exercising every row of
   [Topic, QoS, retain, and property policy](#topic-qos-retain-and-property-policy).
3. **Adapter boundary tests**: assert (via a targeted lint, module-visibility
   check, or a `grep`-based CI step) that no source file outside the
   adapter's own module imports `rumqttc` types directly, verifying the
   [publish-client adapter](#the-publish-client-adapter) boundary this design
   depends on for a low-cost stage-2 migration.
4. **Network integration tests against a real broker**: use this
   repository's `crates/validation` framework (`ContainerConfig`,
   `add_container`, `ContainerConnection`/`PipelineContainerConnection`),
   the same Docker-container-based pattern already used for other
   container-backed dependencies in that crate, running an Eclipse Mosquitto
   container (`eclipse-mosquitto`) configured for MQTT 5 over plaintext TCP
   (baseline has no TLS listener to test). Cover, all at per-record
   granularity unless noted:
   - a batch of `N` records produces exactly `N` PUBLISH packets observed by
     an independent subscriber, with topic/QoS/retain/body matching each
     record's `mqtt.*` attributes (or the configured static fallback where
     an attribute is absent);
   - the batch is Acked only once all `N` records' PUBACKs (QoS 1) or "sent"
     notifications (QoS 0) are observed, verified by holding one record's
     broker response artificially until the others complete first;
   - a batch with a mix of succeeding and permanently-failing records (for
     example, one record with a disallowed topic) results in exactly one
     batch-level Nack, classified permanent;
   - **the partial-success duplicate scenario**: a batch where some records
     succeed and a later record then hits a transient failure (for example,
     a broker disconnect mid-batch) is Nacked transiently as a whole; when
     `processor:retry` resubmits the identical batch, the previously
     succeeded records' topics receive a second, duplicate PUBLISH,
     verified by the subscriber observing each affected topic twice;
   - **deterministic order**: resubmitting the identical batch (with no
     connectivity change) produces the same per-record submission order
     both times, verified by the subscriber's observed arrival order for a
     single-topic, QoS-1, single-connection scenario where MQTT itself
     preserves publish order;
   - a broker-rejected CONNECT (bad credentials), a broker disconnect
     mid-batch verifying the correlation table is fully cleared and every
     outstanding entry resolved transient on reconnect, and drain/shutdown
     behavior with records still in flight verifying the cancellation
     behavior in [Cancellation](#cancellation);
   - a config with any TLS-shaped setting is rejected at startup and never
     attempts a connection at all, verified by asserting no TCP connection
     is opened to the Mosquitto container's TLS-disabled listener in that
     test case.
5. **Cross-client interoperability**: verify wire-level interoperability by
   subscribing with a second, independent MQTT client -- Eclipse Paho's
   Python or C client is a reasonable choice already familiar to the
   OpenTelemetry Collector community -- to confirm topic, QoS, retain, and
   PUBLISH property values observed on the wire match what the exporter
   computed from each record's attributes, independent of the adapter's own
   encode/decode correctness.
6. Document every test per this repository's convention (Scenario/Guarantees
   doc comments, per `AGENTS.md`).

## Platform, dependency, and license risks

- **License**: `rumqttc` is Apache-2.0 licensed, compatible with this
  project's own Apache-2.0 licensing for a dependency.
- **Dependency pinning**: `rumqttc` is a published, crates.io-versioned
  crate; pin it with a normal semver requirement (`"0.25.1"`), unlike
  `ms-mqtt-client`'s current unpublished-`0.1.0`/git-revision pin.
- **MSRV**: `rumqttc`'s MSRV must be confirmed compatible with this
  workspace's own MSRV policy before adding the dependency.
- **Platform coverage**: `rumqttc` with `default-features = false` is a
  pure-Rust, TCP-only client with no native TLS/certificate-store
  dependency, which should simplify (not complicate) this exporter's
  cross-platform story relative to the OpenSSL-linked alternative; confirm
  the crate's supported/tested platforms match this project's own
  supported-platform matrix regardless.

### No-crypto dependency-graph requirement

Baseline's plaintext-only requirement is a **dependency-graph** property, not
merely a runtime behavior, and it is defined precisely as: none of the
following crate names (or any other crate whose primary purpose is a TLS
transport or a cryptographic primitive) may appear anywhere in `cargo tree`
output for the `mqtt-exporter` feature, built with the workspace's normal
feature-unification rules:

`rustls`, `tokio-rustls`, `rustls-webpki`, `rustls-pemfile`,
`rustls-native-certs`, `native-tls`, `tokio-native-tls`, `openssl`,
`tokio-openssl`, `ring`, `aws-lc-rs`, `webpki`.

This is not a one-time manual review; Cargo feature unification across a
workspace can silently reintroduce a dependency (for example, if a future
edit to `Cargo.toml` adds `features = [...]` to the `rumqttc` declaration, or
if some other workspace crate's default features change in a way that
happens to pull one of these crates in transitively through a shared
dependency). This must therefore be an automated, continuously-enforced CI
gate, not just an assertion made once during initial dependency review.

Extend the existing CI gate (`.github/workflows/pr.yaml`), mirroring the
same pattern already proposed for `ms-mqtt-client` in
[`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md#ci-matrix-and-cargo-tree-assertions):

- Add a CI step that runs `cargo tree -p otel-arrow-dfe-contrib-nodes
  --no-default-features --features mqtt-exporter -e normal` (exact package
  and feature names to confirm against `crates/contrib-nodes/Cargo.toml` at
  implementation time) and asserts the output contains none of the crate
  names listed above; fail the job if any match.
- Run this assertion on every PR touching `crates/contrib-nodes/Cargo.toml`
  or the root workspace `Cargo.toml`'s `rumqttc` entry, not only once at
  initial dependency introduction, so a later, accidental reintroduction is
  caught automatically.
- This CI assertion is the primary enforcement mechanism for
  [No silent TLS downgrade](#no-silent-tls-downgrade)'s dependency-graph
  layer; keep it alongside, not instead of, the configuration-validation
  layer described there.

### Future-direction risks (ms-mqtt-client, not a baseline dependency)

The following risks apply only if/when this exporter migrates to the
long-term `ms-mqtt-client`-backed replacement (see
[Client strategy](#client-strategy-baseline-vs-long-term-replacement) and
[Replacement criteria](#replacement-criteria-stage-1-to-stage-2)); none of
them affect baseline, which does not depend on `ms-mqtt-client`:

- `ms-mqtt-client` is MIT-licensed, compatible with this project's
  Apache-2.0 licensing for a dependency.
- As of the pinned evidence commit, `ms-mqtt-client` unconditionally depends
  on `openssl`/`tokio-openssl` even with `default-features = false`; this is
  exactly the gap
  [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md)
  requests be fixed, and is the blocking condition for stage 2.
  `ms-mqtt-client` is version `0.1.0` and, per its own `README.md`, not yet
  published to crates.io; a future migration would need to re-evaluate the
  git-revision pin against whatever version ships the pluggable-TLS fix.
- `ms-mqtt-client`'s `rust-version = "1.88"` MSRV, and its documented
  platform coverage (the related upstream issue drafts in
  `docs/issue-drafts/` already call out Windows explicitly as a platform
  that must remain covered by tests), would both need re-confirming against
  this workspace's requirements at migration time.

## Docs, changelog, and acceptance criteria

- Ship a `README.md` for the new module following the same structure as
  `crates/contrib-nodes/src/exporters/kafka_exporter/README.md` (Metadata,
  Overview, Getting Started, Configuration, Telemetry, Limits), and
  prominently document both the
  [partial-success duplicate behavior](#partial-success-and-retry-duplicates)
  and baseline's plaintext-only/no-TLS limitation (see
  [Endpoints, TLS, and auth](#endpoints-tls-and-auth)) as named, expected
  limitations, not buried in a general caveats section. A `DEVELOPMENT.md`
  note is also required per the
  [Reference-Informed OTAP-Native Capability Design](../ai/reference-informed-otap-native-capability-design.md#component-development-note)
  process, capturing the findings classification in this document, the
  two-stage client strategy and its replacement criteria, and any decisions
  made during implementation.
- Add a `.chloggen` entry (`change_type: new_component`) once the
  implementation PR lands, per `AGENTS.md`'s changelog conventions; this
  design document itself is not user-facing and needs no changelog entry.

### Acceptance criteria

- `exporter:mqtt` connects over plaintext TCP to an MQTT 5 broker using
  `rumqttc` `0.25.1`'s `v5` API added with `default-features = false`, and,
  for an accepted pdata batch containing `N` `LogRecord`s (sourced from
  either OTAP Arrow records or OTLP protobuf bytes), publishes exactly `N`
  MQTT PUBLISH packets -- never one combined payload for the batch and never
  a re-split of any single record's body.
- Any TLS-shaped configuration (`tls.enabled: true`, a TLS-implying port
  such as 8883 without an explicit acknowledgement, or any populated
  `ca_file`/`cert_file`/`key_file`/`insecure` field) is rejected at
  configuration-validation time with a clear, distinct error; the exporter
  never attempts a TLS handshake and never falls back to plaintext silently
  when TLS was requested (see
  [No silent TLS downgrade](#no-silent-tls-downgrade)).
- A `cargo tree` CI assertion confirms zero TLS/cryptography crates
  (`rustls`, `native-tls`, `openssl`, `ring`, `aws-lc-rs`, and their
  companions listed in
  [No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement))
  appear anywhere in the dependency graph for the `mqtt-exporter` feature,
  and this assertion is part of the standard CI gate, not a one-off manual
  check.
- No source file outside the publish-client adapter's own module imports
  `rumqttc` types; exporter core logic (config, policy, fan-out, Ack/Nack
  reduction, telemetry) compiles against the
  [`PublishClient` adapter trait](#the-publish-client-adapter) only,
  verified by the adapter boundary test described in [Tests](#tests).
- Each PUBLISH's topic, QoS, retain flag, and properties are derived from
  that record's `mqtt.*` attributes per the envelope contract, bounded by
  this exporter's configured policy (allowlist, ceiling, forbid/allow), with
  the documented static fallback applied only when the corresponding
  attribute is absent.
- QoS 2 is rejected both at configuration time (`qos_ceiling: 2`) and at
  per-record evaluation time (`mqtt.qos = 2`), in both cases with a clear
  validation error/rejection.
- The whole accepted batch is Acked if and only if all `N` of its records'
  publishes reach a terminal success; it is Nacked, classified per the
  worst-case per-record outcome, if any record fails -- verified by tests
  that hold some records' completions back while others fail.
- A permanently rejected record (e.g. disallowed topic) causes the whole
  batch's Nack to be classified permanent and is not retried by an upstream
  `processor:retry` node; a batch whose only failures are transient (e.g.
  broker quota) is retried and eventually succeeds once the condition
  clears.
- Retrying a transiently-Nacked batch that had partially succeeded is
  demonstrated to republish the already-succeeded records' PUBLISH packets
  (documented, expected duplication, not treated as a defect), and this
  behavior is documented in the module's `README.md`.
- Resubmitting an identical batch (whether via `processor:retry` or a test
  harness) always submits its records to the publish-client adapter in the
  same deterministic order.
- The exporter reconnects with bounded backoff after a broker disconnect,
  without operator intervention, and resumes publishing; the PubAck
  correlation table is fully cleared, and every outstanding entry resolved
  transient, on every such reconnect.
- Drain/shutdown completes within the configured bound even while the broker
  is unreachable, terminally Nacking (as a whole) any batch that could not
  be confirmed, including one that had partially succeeded.
- Telemetry attributes remain bounded cardinality; no topic name, client ID,
  or raw MQTT reason string appears as a metric attribute value; batch-level
  and per-record metrics are clearly distinguished (see
  [Telemetry and error categories](#telemetry-and-error-categories)).
- Network tests exercise the behavior against a real Mosquitto broker
  (MQTT 5, plaintext), and at least one cross-client interoperability check
  uses a second MQTT client library (e.g. Paho) to independently observe
  published messages.
- The module's `README.md` and `DEVELOPMENT.md` are present and describe the
  scope, evidence, findings classification, two-stage client strategy, and
  known limitations recorded in this document, including the partial-success
  duplicate behavior and the plaintext-only baseline.

## Open API uncertainties (do not invent; verify before implementation)

All items below are explicitly flagged for **the parallel rumqttc
integration spike** to confirm against the pinned `rumqttc` `0.25.1` source
before implementation begins; this document does not assume answers to any
of them.

- Whether `rumqttc::v5::AsyncClient` and `EventLoop` are `Send` or `!Send`,
  and whether the adapter's driving task can be kept compatible with this
  project's `!Send` future preference (see [Multicore](#multicore)).
- Whether `Event::Outgoing(Outgoing::Publish(pkid))` notifications are
  strictly FIFO-ordered with respect to `AsyncClient::publish(...)`
  submission order, including under the crate's own internal request
  batching (`max_request_batch`); this ordering assumption is load-bearing
  for the [PubAck correlation layer's](#publish-submission-and-puback-correlation)
  pkid-binding design.
- The exact `rumqttc::v5` `AsyncClient` method or `Publish`-construction path
  needed to attach a full `PublishProperties` value (content_type,
  message_expiry_interval, user_properties, response_topic,
  correlation_data) to an outbound publish.
- The exact error type/variant `AsyncClient::publish(...)` returns when the
  client/eventloop pair is unavailable (client dropped, channel closed).
- The complete, exact set of v5 PUBACK reason-code variants exposed by
  `rumqttc::v5::mqttbytes::v5` and their MQTT 5 3.4.2-1 reason-code
  semantics, needed to build a complete permanent/transient classification
  table.
- Whether `rumqttc`'s internal automatic-reconnect cadence is independently
  configurable (minimum/maximum delay, jitter), and if so how, so it can
  honor this exporter's `reconnect` config block, or whether the exporter
  must layer its own outer supervisory backoff bound around the crate's
  automatic behavior instead.
- Whether `rumqttc::v5::MqttOptions` supports an MQTT 5 zero-length
  (server-assigned) Client Identifier end-to-end, given its public
  constructor appears to require a client ID string upfront.
- Whether `MqttOptions::request_channel_capacity`/`max_request_batch` should
  be sized to match `max_in_flight` directly, or configured independently.
- Whether `rumqttc`'s own v5 QoS 2 support (`PubRec`/`PubRel`/`PubComp`
  request variants) is complete and correct enough to ever revisit for a
  future revision of this design; not relied upon or verified for this
  baseline (see
  [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)).
- rumqttc's exact MSRV and its compatibility with this workspace's MSRV
  policy.
- The exact package/feature names to use for the
  [No-crypto dependency-graph requirement](#no-crypto-dependency-graph-requirement)'s
  `cargo tree` CI assertion, confirmed against
  `crates/contrib-nodes/Cargo.toml` at implementation time.
- The exact secret-reference mechanism this codebase's existing exporters use
  for credential fields, so `auth.password` can follow the same convention
  rather than requiring inline plaintext.
- Whether `RawLogsData`/`RawLogRecord`'s best-effort behavior for malformed
  nested fields can distinguish "attribute genuinely absent" from "attribute
  present but undecodable," which affects how precisely the exporter can
  apply the envelope contract's omit-don't-guess validation rules (see
  [Decoding pdata into individual LogRecords](#decoding-pdata-into-individual-logrecords)).
- The upstream state and timeline of
  [`ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md)'s
  requested change, which gates when the
  [replacement criteria](#replacement-criteria-stage-1-to-stage-2) for the
  long-term `ms-mqtt-client`-backed stage could even begin to be evaluated.
- Whether a future engine capability for partial (sub-`PData`) batch
  acknowledgement is planned; if so, this exporter's fan-out tracking
  (already computed per record) would be positioned to adopt it directly,
  removing the [partial-success duplicate](#partial-success-and-retry-duplicates)
  limitation. No such capability exists today, and this document does not
  assume one.

## Non-goals

- TLS support of any kind in baseline, including mutual TLS (deferred
  entirely to the long-term `ms-mqtt-client`-backed replacement once its
  pluggable-TLS/ambient-provider request lands; see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement)).
- Sparkplug B or any other structured payload encoding (see
  [Scope](#explicitly-out-of-scope-for-baseline)).
- Publishing a whole OTLP export request, OTAP batch, or any other
  multi-record container as a single MQTT payload (see
  [Supported input and the one-LogRecord-to-one-PUBLISH invariant](#supported-input-and-the-one-logrecord-to-one-publish-invariant)).
- `traces` and `metrics` signals (no equivalent envelope contract exists for
  them today).
- QoS 2 support (kept out of scope pending a dedicated design and test pass;
  see [MQTT5-only implications and QoS 2](#mqtt5-only-implications-and-qos-2)).
- WebSocket transport, enhanced authentication, shared subscriptions, and
  any subscribe/receive capability (this is a publish-only exporter).
- A durable, cross-restart application-level queue (compose with
  `processor:durable_buffer` instead, as with the Kafka exporter).
- Detecting or suppressing duplicate publishes caused by retrying a
  partially-succeeded batch (see
  [Partial success and retry duplicates](#partial-success-and-retry-duplicates)).
- Any engine-level partial (sub-`PData`) batch acknowledgement mechanism;
  this document works within the engine's existing whole-batch Ack/Nack
  model rather than proposing to change it.
- Live reconfiguration (deferred to a follow-on that explicitly addresses the
  same gaps documented for the Kafka exporter's live reconfiguration).
- Migrating to `ms-mqtt-client` itself; this document defines the
  [replacement criteria](#replacement-criteria-stage-1-to-stage-2) that must
  hold first, but does not schedule or perform that migration.

## Related work

- [`docs/issue-drafts/mqtt-raw-envelope-contract.md`](mqtt-raw-envelope-contract.md) --
  the normative PUBLISH-to-LogRecord mapping this exporter implements in
  reverse; see
  [Relationship to the envelope contract and the raw receiver](#relationship-to-the-envelope-contract-and-the-raw-receiver).
- [`docs/issue-drafts/mqtt-raw-receiver.md`](mqtt-raw-receiver.md) -- the
  intended (but not required) upstream producer of contract-compliant
  `LogRecord`s.
- [`docs/issue-drafts/ms-mqtt-client-pluggable-tls-crypto.md`](ms-mqtt-client-pluggable-tls-crypto.md) --
  the upstream-ready issue draft requesting a no-TLS build and pluggable,
  ambient-`CryptoProvider` `rustls` backend for `ms-mqtt-client`; landing
  this is the blocking condition for this exporter's long-term replacement
  (see [Client strategy](#client-strategy-baseline-vs-long-term-replacement)
  and [Replacement criteria](#replacement-criteria-stage-1-to-stage-2)).
- [`docs/issue-drafts/mqtt-bounded-inbound-publish-flow-control.md`](mqtt-bounded-inbound-publish-flow-control.md)
  and
  [`docs/issue-drafts/mqtt-explicit-qos1-acknowledgement-drop-policy.md`](mqtt-explicit-qos1-acknowledgement-drop-policy.md) --
  upstream `ms-mqtt-client` receiver-side gaps; not blocking for this
  publish-only exporter, but the acknowledgement-safety principle in the
  second draft directly informs
  [DFE Ack/Nack and retry interaction](#dfe-acknack-and-retry-interaction)
  above.
- [`rumqttc` (`bytebeamio/rumqtt`)](https://github.com/bytebeamio/rumqtt) --
  baseline's MQTT client; primary evidence source for
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement) and
  [Publish submission and PUBACK correlation](#publish-submission-and-puback-correlation).
- [`microsoft/rust-mqtt-client`](https://github.com/microsoft/rust-mqtt-client) --
  the long-term, preferred replacement client; evidence reviewed strictly as
  future direction (see
  [Client strategy](#client-strategy-baseline-vs-long-term-replacement)).
- `crates/pdata/src/views/` -- the backend-agnostic `LogsDataView` traits and
  `OtapLogsView`/`RawLogsData` implementations this exporter reuses to
  decode both OTAP Arrow records and OTLP protobuf bytes into individual
  `LogRecord`s.
- `crates/contrib-nodes/src/exporters/kafka_exporter/` -- the primary
  precedent for per-signal config, static-vs-dynamic topic routing with an
  operator allowlist, bounded in-flight publishing, and Ack/Nack
  classification conventions reused here.
- `crates/core-nodes/src/processors/batch_processor/` -- the primary
  precedent for fanning one accepted request out into multiple downstream
  units while tracking completion back to a single terminal decision.
- `crates/core-nodes/src/processors/retry_processor/` -- the upstream retry
  node this exporter is expected to interoperate with.
- `crates/validation/` -- the Docker-container-based test framework
  recommended for the network integration test tier.
