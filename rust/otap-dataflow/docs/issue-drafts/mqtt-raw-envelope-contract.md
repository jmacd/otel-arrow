# Raw MQTT envelope contract: PUBLISH <-> OTLP LogRecord

## Status

Draft. This document defines a shared contract, not an implementation. It is
the single source of truth for a future `receiver:mqtt` and a future
`exporter:mqtt` so both sides encode and decode the same envelope the same
way. Neither receiver nor exporter code exists yet; this document only
specifies the mapping both must implement.

## Summary

This contract defines a lossless, deterministic mapping between one MQTT
`PUBLISH` packet (MQTT 3.1.1 or MQTT 5) and one OTLP `LogRecord`, so that:

- a future raw MQTT receiver can turn an inbound `PUBLISH` into a `LogRecord`
  without losing information needed to reconstruct the original packet, and
- a future raw MQTT exporter can turn that `LogRecord` back into a `PUBLISH`
  (replay) that is wire-identical to the original where the contract promises
  round-trip, and clearly different where it does not.

The receiver and exporter are separate, independently deployable components.
Neither may assume the other is present in the same pipeline. The contract is
therefore expressed entirely in terms of the `LogRecord` wire shape (fields,
attribute keys, attribute types), not in terms of any shared Rust type.

This document does not cover MQTT client behavior (flow control, manual
acknowledgement, reconnection). Those are addressed separately in
[mqtt-bounded-inbound-publish-flow-control.md](mqtt-bounded-inbound-publish-flow-control.md)
and
[mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md).
This contract assumes a decoded `PUBLISH` (topic, payload, QoS, flags,
properties) is already available from the MQTT client crate.

This document defines two layers: a **general contract** covering every
MQTT 3.1.1 and MQTT 5 PUBLISH field, and a **v1 baseline** describing what
the first receiver implementation can actually populate given its verified
data source. See
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)
for the fields the v1 baseline cannot populate and must therefore omit
rather than guarantee.

## Scope

In scope:

- The one-PUBLISH-to-one-LogRecord mapping (this is a 1:1 mapping; no
  fan-out, no aggregation, no batching of multiple PUBLISH packets into one
  LogRecord).
- Attribute names, types, and namespace for MQTT-specific metadata.
- Which standard `messaging.*` / `network.*` / `client.*` / `server.*`
  semantic-convention attributes apply and how they are populated.
- MQTT 3.1.1 vs MQTT 5 differences (properties, reason codes, user
  properties).
- Timestamp semantics (`time_unix_nano` vs `observed_time_unix_nano`).
- Body encoding policy for binary vs UTF-8 payloads.
- Collision rules when MQTT user properties or topic segments collide with
  reserved attribute names.
- Trust and security policy for exporter-side replay of receiver-produced
  attributes.
- Validation rules a receiver and exporter must both enforce.
- Batching behavior at the OTLP/OTAP transport level (batching of
  LogRecords, not of PUBLISH packets).
- Round-trip guarantees and an explicit list of fields that are not
  preserved.
- Where MQTT envelope metadata lives: log attributes vs pdata context vs a
  future pluggable representation, and the baseline choice for the first
  implementation.
- Verified data-source limitations of the v1 baseline receiver (rumqttd
  0.20.0 internal `LinkRx`) and how they change per-field guarantees from
  "round-trips" to "unavailable, omitted".

Out of scope:

- Receiver-side or exporter-side flow control, backpressure, or
  acknowledgement timing.
- MQTT client connection management, session persistence, TLS/auth
  configuration.
- QoS 2 (not implemented by the underlying MQTT client at this time; see
  referenced issue drafts).
- Body content parsing (JSON, CBOR, Sparkplug B, protobuf, etc.). The
  contract treats the MQTT payload as an opaque byte or UTF-8 blob; a
  processor, not the receiver, is responsible for any content-aware
  decoding.
- Topic-based routing or filtering policy (subscription patterns). This
  contract only defines how one already-received PUBLISH becomes one
  LogRecord.

## Terminology

- **PUBLISH**: an MQTT PUBLISH packet, either inbound (received by the
  future receiver, whether via an embedded broker such as rumqttd or a
  client connection to an external broker) or outbound (sent by the future
  exporter to a broker).
- **Envelope**: the non-payload metadata of a PUBLISH: topic, QoS, DUP,
  RETAIN, packet identifier (QoS > 0), and MQTT 5 properties.
- **Body**: the PUBLISH application payload (the bytes after the fixed and
  variable header).
- **Round-trip**: receiver produces a LogRecord from PUBLISH A; exporter
  later produces PUBLISH B from that LogRecord. Round-trip is satisfied for a
  field when B reproduces A's value for that field exactly.
- **Replay**: the exporter's act of producing an outbound PUBLISH from a
  LogRecord that was not necessarily produced by the paired receiver (for
  example, forwarded through processors, stored, or received from an
  untrusted upstream).

## Baseline data source and field availability (rumqttd 0.20.0)

The v1 receiver ingests PUBLISH packets by embedding rumqttd 0.20.0 as an
in-process broker and reading forwarded messages from rumqttd's internal
`LinkRx` API, rather than connecting outward as a subscribing MQTT client to
an external broker. This is a verified constraint on the v1 implementation,
not a design preference, and it directly bounds which fields this contract's
v1 baseline can populate.

`LinkRx` in rumqttd 0.20.0 forwards only: **topic, payload, QoS, and the
RETAIN flag.** It does not expose the publishing client's identity, and it
does not expose most MQTT 5 PUBLISH properties. No other envelope field
(DUP, packet identifier, negotiated protocol version, or per-connection
network/peer metadata) is confirmed forwarded through `LinkRx` either; this
contract does not assume any of them are available unless independently
verified against a specific rumqttd interface.

| Envelope field | Available via `LinkRx` (rumqttd 0.20.0) | v1 baseline treatment |
| --- | --- | --- |
| Topic | Yes (verified) | Populated: `mqtt.topic` / `messaging.destination.name`. |
| Payload / body | Yes (verified) | Populated: `body`. |
| QoS | Yes (verified) | Populated: `mqtt.qos`. |
| RETAIN | Yes (verified) | Populated: `mqtt.retain`. |
| Publisher client identifier | No (verified) | `messaging.client.id` / `mqtt.client_id` omitted; never guessed or synthesized. |
| DUP flag | Not confirmed | `mqtt.dup` omitted in v1 baseline. |
| Packet identifier | Not confirmed | `mqtt.packet_id` / `messaging.message.id` omitted in v1 baseline. |
| Negotiated protocol version (3.1.1 vs 5) | Not confirmed | `mqtt.protocol_version` / `network.protocol.version` omitted in v1 baseline; see [MQTT 3.1.1 vs MQTT 5](#mqtt-311-vs-mqtt-5). |
| MQTT 5 properties (payload format indicator, message expiry interval, content type, response topic, correlation data, topic alias, subscription identifiers, user properties) | No (verified: "most MQTT 5 PUBLISH properties" are not exposed) | All omitted in v1 baseline; see [Body policy](#body-policy-binary-vs-utf-8). |
| Network/peer/client socket metadata | Not confirmed | Omitted in v1 baseline. |

Consequences that apply throughout the rest of this document:

- Every table below still defines the **general** mapping for a field, so
  that a future data source (for example, a client-mode receiver connected
  to an external broker, or a later rumqttd release/link API that exposes
  more fields) has an unambiguous target to implement against.
- Wherever a field is marked "not confirmed" or "No" above, the v1 baseline
  receiver MUST omit the corresponding attribute rather than emit a
  default, guessed, or synthesized value. An omitted attribute is the
  correct representation of "this data source did not provide it"; a
  fabricated value would misrepresent what actually happened on the wire.
  This applies in particular to [Body policy](#body-policy-binary-vs-utf-8)
  (no payload format indicator means the v1 baseline body is always
  `Value::BytesValue`) and to the [Round-trip guarantees](#round-trip-guarantees)
  section (fields unavailable to the v1 baseline cannot round-trip through
  it and are listed separately from fields the contract intentionally does
  not preserve).
- See
  [QoS 1 durability boundary](#qos-1-durability-boundary-rumqttd-0200) for
  the related, separately verified constraint that rumqttd queues PUBACK to
  the publishing client before the message is delivered to `LinkRx`.

## Design principle: mechanical projection, not semantic interpretation

Following the same division of responsibility used by the journald receiver
(see [journald-receiver.md](../journald-receiver.md#field-projection)): the
receiver performs only mechanical projection of the MQTT envelope into OTAP
log fields and attributes. It does not parse the payload, does not infer
semantic meaning from the topic, and does not drop MQTT-specific metadata
that a downstream processor might need. Semantic enrichment (for example,
extracting a device ID from a topic segment, or decoding a Sparkplug B
payload) is a processor's job, not the receiver's.

Symmetrically, the exporter performs only mechanical reconstruction. It does
not infer envelope fields from body content, and it never invents QoS,
retain, or topic values that are not explicitly present as attributes (see
[Validation](#validation) for what happens when a required field is
missing).

## Where MQTT envelope metadata lives

Three placements were considered:

1. **Log attributes** (`LogRecord.attributes`). Attributes already flow
   through every processor, exporter, and OTAP Arrow encoding path in this
   codebase without new pdata surface area. They serialize losslessly through
   OTLP and OTAP today.
2. **A new pdata context field**, analogous to how transport-header capture
   attaches request-scoped metadata to the pipeline message context (see
   [transport-headers.md](../transport-headers.md)). This would keep
   MQTT envelope data out of the exported signal entirely unless explicitly
   propagated.
3. **A future pluggable/typed representation**, for example a dedicated
   `MqttEnvelope` extension type carried alongside the LogRecord, decoded by
   name rather than by flat attribute keys.

**Baseline decision for the first implementation: log attributes (option 1),
namespaced under `mqtt.*`, plus standard `messaging.*` / `network.*` /
`client.*` attributes where they apply.**

Rationale:

- Attributes are the only one of the three options that exist in the
  pdata model today (`crates/pdata`) and are supported end-to-end by every
  exporter, including `exporter:otlp_grpc`, `exporter:otlp_http`, and
  `exporter:otap`, without new wire format or new pdata schema work.
  The transport-header context (option 2) is designed for
  receiver-to-exporter propagation of transport metadata across arbitrary
  signal types and pipelines with a single opt-in policy; it is deliberately
  generic and lossy-by-default (bounded, filterable, droppable), which is
  the wrong default for a contract that promises round-trip reconstruction
  of a specific protocol's envelope.
- A pluggable typed representation (option 3) is attractive long-term
  (strong typing, no attribute-name collisions, smaller wire size via a
  dedicated encoding) but requires new pdata surface area that does not
  exist yet and would need its own compatibility story. It is noted here as
  a future direction; see [Future work](#future-work-pluggable-envelope).
- Attributes are visible to every existing processor (filter, transform,
  attributes_processor, content_router) without new code, so operators can
  route or filter on `mqtt.topic` or `mqtt.qos` immediately.

This decision can be revisited if attribute volume or cardinality becomes a
measured problem (see [Validation](#validation) for bounds), but it is the
practical baseline compatible with the current pdata model and exporter set.

## Mapping contract: one PUBLISH to one LogRecord

Each inbound PUBLISH produces exactly one `LogRecord`. No PUBLISH is split
across multiple LogRecords, and no LogRecord aggregates more than one
PUBLISH. This is a hard invariant: it is what makes 1:1 replay possible.

### Body policy (binary vs UTF-8)

MQTT PUBLISH payloads are opaque byte sequences; MQTT 5 adds an optional
`Payload Format Indicator` (PFI) declaring the payload as UTF-8 text (`0x01`)
or unspecified bytes (`0x00`, the default). MQTT 3.1.1 has no such
indicator.

**v1 baseline note:** the rumqttd 0.20.0 `LinkRx` data source does not
expose PFI (see
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)).
The v1 baseline receiver therefore always takes the "PFI absent" row below
and emits `Value::BytesValue` unconditionally; it never emits
`Value::StringValue`. The PFI-aware rows are the general contract for a
future data source that does expose PFI.

| Condition | `LogRecord.body` |
| --- | --- |
| MQTT 5, PFI = `0x01` (UTF-8), payload is valid UTF-8 | `Value::StringValue` containing the exact decoded text |
| MQTT 5, PFI = `0x01`, payload is **not** valid UTF-8 | `Value::BytesValue` containing the raw payload (never lossy-decode; see [Validation](#validation)); `mqtt.payload_format_indicator_invalid_utf8 = true` is set |
| MQTT 5, PFI = `0x00` or absent | `Value::BytesValue` containing the raw payload, unconditionally |
| MQTT 3.1.1 (no PFI), or v1 baseline (PFI not observable) | `Value::BytesValue` containing the raw payload, unconditionally |
| Zero-length payload | `Value::BytesValue` with an empty byte vector (not `null`; a zero-length PUBLISH payload is valid and distinguishable from "no body") |

The body is never lossy-decoded (no replacement-character substitution, no
truncation). This mirrors the journald receiver's rule that "field values
must not be lossy-decoded"
(see [journald-receiver.md](../journald-receiver.md#field-projection)).

The exporter reconstructs the outbound payload as follows:

- `Value::BytesValue` -> payload is the byte vector, verbatim.
- `Value::StringValue` -> payload is the UTF-8 encoding of the string,
  verbatim; if `mqtt.protocol_version` indicates MQTT 5, the exporter also
  sets PFI = `0x01` on the outbound PUBLISH.
- Any other `Value` variant (`IntValue`, `DoubleValue`, `BoolValue`,
  `ArrayValue`, `KvlistValue`) or an unset body is a validation error (see
  [Validation](#validation)); the exporter must not silently coerce these to
  bytes.

### Standard attributes (`messaging.*`, `network.*`, `client.*`, `server.*`)

These reuse upstream OpenTelemetry semantic conventions per this project's
[attributes guide](../telemetry/attributes-guide.md) rule to "reuse existing
semantic attributes whenever possible" and not redefine them.

| Attribute | Type | Populated from | Notes |
| --- | --- | --- | --- |
| `messaging.system` | string | constant | Always `"mqtt"` (a registered `messaging.system` value). |
| `messaging.destination.name` | string | PUBLISH topic name | The full MQTT topic string, unmodified. Topic segments are not split into separate attributes (see [Non-preserved / not derived](#non-preserved-fields)). |
| `messaging.operation.type` | string | direction | `"receive"` when produced by the receiver from an inbound PUBLISH; `"send"` when about to be produced as an outbound PUBLISH by the exporter's own self-telemetry (not on the replayed LogRecord itself). |
| `messaging.message.id` | string | MQTT 5 packet identifier, if present and QoS > 0 | **Not available in the v1 baseline** (rumqttd 0.20.0 `LinkRx` does not confirm forwarding a packet identifier); omitted, never synthesized. General contract: omitted for QoS 0 (no packet identifier exists) and for MQTT 3.1.1 (see [collision rules](#collision-and-precedence-rules) for why this is not reused as a dedup key). |
| `messaging.client.id` | string | MQTT Client Identifier of the publishing client | **Not available in the v1 baseline** (rumqttd 0.20.0 `LinkRx` does not expose publisher client identity; verified). Omitted, never guessed. |
| `network.protocol.name` | string | constant | `"mqtt"`. |
| `network.protocol.version` | string | negotiated CONNACK protocol version | **Not available in the v1 baseline** (not confirmed forwarded through `LinkRx`); omitted. General contract: `"3.1.1"` or `"5"`, duplicated in `mqtt.protocol_version` (below) for consumers that prefer a closed-enum MQTT-specific key; both must always agree when populated. |
| `network.transport` | string | connection transport | `"ip_tcp"` for plain/TLS TCP, `"pipe"` for MQTT over WebSocket is intentionally **not** claimed by this contract in v1 (see [Non-goals](#non-goals)); receivers that only support TCP set `"ip_tcp"`. |
| `network.peer.address` | string | remote socket address of the connection that produced the PUBLISH | **Not available in the v1 baseline** (not confirmed forwarded through `LinkRx`); omitted. |
| `network.peer.port` | int | remote socket port of the connection that produced the PUBLISH | **Not available in the v1 baseline**; omitted. |
| `client.address` | string | local socket address of the connection that produced the PUBLISH | **Not available in the v1 baseline**; omitted. |
| `client.port` | int | local socket port of the connection that produced the PUBLISH | **Not available in the v1 baseline**; omitted. |

`messaging.message.id` deliberately does not default to a synthesized value
when no MQTT packet identifier exists (QoS 0). A missing attribute is a
correct and honest representation of "the protocol did not provide an
identifier"; synthesizing one would misrepresent broker-observable state.

### MQTT-specific attributes (`mqtt.*` namespace)

Per the [attributes guide](../telemetry/attributes-guide.md#project-defined-namespace)
rule that project-defined attributes must be namespaced to avoid upstream
collisions, and because no upstream MQTT-specific semantic convention
namespace is adopted by this project, MQTT protocol fields that have no
standard `messaging.*`/`network.*` equivalent use an `mqtt.*` namespace.

| Attribute | Type | Source | Applies to | v1 baseline (rumqttd 0.20.0 `LinkRx`) |
| --- | --- | --- | --- | --- |
| `mqtt.protocol_version` | string, closed set `{"3.1.1", "5"}` | negotiated CONNACK version | both | Not available; omitted (see [Baseline data source](#baseline-data-source-and-field-availability-rumqttd-0200)). |
| `mqtt.topic` | string | PUBLISH topic name | both (duplicate of `messaging.destination.name`; see [collision rules](#collision-and-precedence-rules) for why both exist) | Available; always populated. |
| `mqtt.qos` | int, closed set `{0, 1, 2}` | PUBLISH QoS field | both. QoS 2 may appear on an inbound PUBLISH from a broker even though this project's MQTT client does not implement QoS 2 delivery guarantees end-to-end; the receiver preserves the observed value without asserting support. | Available; always populated. |
| `mqtt.retain` | bool | PUBLISH RETAIN flag | both | Available; always populated. |
| `mqtt.dup` | bool | PUBLISH DUP flag | both. See [Round-trip guarantees](#round-trip-guarantees) - DUP is receive-time-observed, not exporter-replayed. | Not available; omitted. |
| `mqtt.packet_id` | int (uint16) | PUBLISH packet identifier | both, only when QoS > 0 | Not available; omitted. |
| `mqtt.payload_format_indicator` | int, closed set `{0, 1}` | MQTT 5 PFI property | MQTT 5 only | Not available; omitted (see [Body policy](#body-policy-binary-vs-utf-8)). |
| `mqtt.payload_format_indicator_invalid_utf8` | bool | set by receiver when PFI=1 but payload is not valid UTF-8 | MQTT 5 only, and only when true; otherwise omitted | Never set in v1 baseline (PFI is never observed as 1). |
| `mqtt.message_expiry_interval` | int (seconds) | MQTT 5 Message Expiry Interval property | MQTT 5 only, when present | Not available; omitted. |
| `mqtt.content_type` | string | MQTT 5 Content Type property | MQTT 5 only, when present | Not available; omitted. |
| `mqtt.response_topic` | string | MQTT 5 Response Topic property | MQTT 5 only, when present | Not available; omitted. |
| `mqtt.correlation_data` | bytes | MQTT 5 Correlation Data property | MQTT 5 only, when present | Not available; omitted. |
| `mqtt.topic_alias` | int | MQTT 5 Topic Alias property | MQTT 5 only, when present. See [Non-preserved fields](#non-preserved-fields) - alias resolution is receiver-local and not replayed as an alias. | Not available; omitted. |
| `mqtt.subscription_identifiers` | array of int | MQTT 5 Subscription Identifier property (0 or more) | MQTT 5 only, when present; encoded as `Value::ArrayValue` of `IntValue` in declared order | Not available; omitted. |
| `mqtt.user_properties` | kvlist | MQTT 5 User Property list (0 or more, order-preserving, keys may repeat) | MQTT 5 only, when present; see [encoding of user properties](#encoding-of-mqtt-5-user-properties) below | Not available; omitted (rumqttd 0.20.0 `LinkRx` does not expose "most MQTT 5 PUBLISH properties", verified). |
| `mqtt.client_id` | string | MQTT Client Identifier of the publishing client (or, for a client-of-external-broker receiver, the receiver's own upstream connection) | both; duplicates `messaging.client.id` (see [collision rules](#collision-and-precedence-rules)) | Not available; omitted (rumqttd 0.20.0 `LinkRx` does not expose publisher client identity, verified). |

#### Encoding of MQTT 5 User Properties

MQTT 5 User Properties are an ordered, possibly-repeating list of UTF-8
key/value string pairs -- not a map. Collapsing them into a flat map would
silently drop repeated keys and reorder them, violating losslessness.

`mqtt.user_properties` is therefore encoded as a `Value::ArrayValue` of
`Value::KvlistValue`, each holding exactly two entries with fixed keys
`"key"` and `"value"` (both `Value::StringValue`), in the exact order the
properties appeared on the wire:

```text
mqtt.user_properties = [
  { key: "device-fw", value: "1.2.3" },
  { key: "region", value: "us-west" },
  { key: "region", value: "us-west-override" }  # repeated key preserved
]
```

This is the same repeated-field problem the journald receiver solves for
duplicate field names (see
[journald-receiver.md](../journald-receiver.md#field-projection)); this
contract picks the array-of-pairs encoding as its v1 answer rather than the
journald receiver's "repeated same-key attribute" interim behavior, because a
repeated flat attribute key is not order-preserving relative to *other*
distinct keys and MQTT user property order is meaningful to some brokers'
downstream consumers.

### Timestamps

| `LogRecord` field | Value |
| --- | --- |
| `time_unix_nano` | Receiver-observed wall-clock time at which the PUBLISH was decoded from the socket, in UTC nanoseconds since epoch. MQTT has no wire timestamp field for PUBLISH (MQTT 5 Message Expiry Interval is a relative TTL in seconds, not a timestamp), so this is always locally generated, never taken from the wire. |
| `observed_time_unix_nano` | Same value as `time_unix_nano` for the receiver path (there is no separate ingestion-vs-occurrence distinction for MQTT, unlike journald's durable cursor replay). Set unconditionally so consumers that always read `observed_time_unix_nano` (per OTLP convention) get a value. |

The exporter does not attempt to reconstruct or replay these timestamps onto
the wire; MQTT PUBLISH has no field to carry them (see
[Non-preserved fields](#non-preserved-fields)).

### Client / peer metadata

`network.peer.address`, `network.peer.port`, `client.address`, `client.port`,
`messaging.client.id`, and `mqtt.client_id` describe the connection that
produced the PUBLISH: for a broker-embedded receiver (the v1 baseline
architecture), that is the publishing client's connection; for a
client-of-external-broker receiver, it is the receiver's own upstream
connection to the broker. **None of these fields are available in the v1
baseline**: rumqttd 0.20.0's `LinkRx` does not expose publisher client
identity (verified), and connection-level network/peer metadata is not
confirmed forwarded through `LinkRx` either (see
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)).
They remain part of the general contract for a data source that does expose
per-connection metadata, and are omitted, never guessed or defaulted, when
unavailable. Where available, these fields are connection-scoped, not
per-message-scoped: they would be identical across every LogRecord produced
by the same connection, and a processor may choose to hoist them to
resource or scope attributes in a later development phase (see
[Future work](#future-work-pluggable-envelope)). Where available, they are
emitted as ordinary log attributes for simplicity and to avoid introducing
new resource/scope-attribute wiring in the first implementation.

### Severity

MQTT PUBLISH carries no severity concept. `severity_number` and
`severity_text` are left unset by the receiver. A processor that parses the
payload (for example, a JSON body containing a log level) may derive and set
severity; that is out of scope for this contract, consistent with the
"receiver does mechanical projection only" principle above.

### `event_name`

Left unset by the receiver. MQTT has no analog to a structured event name,
and inventing one from the topic would be a semantic (not mechanical)
transformation.

### `trace_id` / `span_id`

Left unset by the receiver unless the payload is later parsed by a processor
that extracts W3C trace context from the body or from a `mqtt.user_properties`
entry (for example, a `traceparent` user property). MQTT 5 User Properties
are a documented mechanism some publishers use to carry trace context, but
extracting and promoting one to `trace_id`/`span_id` is payload-aware
semantic interpretation, not mechanical envelope projection, so it is a
processor's responsibility, not the receiver's.

## Collision and precedence rules

Several attributes are deliberately duplicated across the standard and
`mqtt.*` namespaces (`messaging.destination.name` / `mqtt.topic`,
`messaging.client.id` / `mqtt.client_id`, `network.protocol.version` /
`mqtt.protocol_version`). This is intentional, not an oversight:

- The standard attribute lets generic messaging-aware consumers (dashboards,
  correlation logic written against upstream semantic conventions) work
  without MQTT-specific knowledge.
- The `mqtt.*` attribute gives the exporter and any MQTT-aware processor a
  namespaced, collision-free field to read that cannot be shadowed by an
  unrelated `messaging.*`-emitting component earlier in the pipeline.

Rules:

1. **The receiver is the sole writer of both the standard and `mqtt.*` forms
   for a given field**, and always writes them with the same value in the
   same LogRecord. A receiver implementation must never write one without
   the other for fields that have both forms.
2. **The exporter reads only the `mqtt.*` form when reconstructing the
   outbound PUBLISH**, never the standard form. This makes exporter behavior
   independent of whether an intermediate processor rewrote or dropped
   `messaging.*` attributes for unrelated reasons (for example, a resource
   attribute processor scoped to `messaging.*`).
3. **If an intermediate processor sets `mqtt.*` attributes that disagree
   with the standard-namespace attributes** (for example, a transform
   processor rewrites `messaging.destination.name` but not `mqtt.topic`),
   the exporter's `mqtt.*` read wins deterministically per rule 2; this is
   not treated as an error by the exporter, but receivers and processors
   authored in this codebase must not knowingly introduce such disagreement.
4. **User-property key collisions with reserved attribute keys** (an
   upstream publisher sets a User Property literally named `mqtt.qos` or
   `messaging.system`) are not renamed or dropped. They are preserved
   verbatim inside `mqtt.user_properties` (the array-of-pairs form), which is
   a separate key from the reserved top-level attribute, so no collision is
   possible in the LogRecord's actual attribute key space. This is why User
   Properties are never flattened into top-level attribute keys.
5. **Repeated User Property keys** are preserved in order inside
   `mqtt.user_properties`; no last-write-wins or first-write-wins collapsing
   occurs at the receiver.

## MQTT 3.1.1 vs MQTT 5

**v1 baseline note:** the rumqttd 0.20.0 `LinkRx` data source does not
confirm forwarding the negotiated protocol version or "most MQTT 5 PUBLISH
properties" (verified). The v1 baseline therefore cannot distinguish MQTT
3.1.1 from MQTT 5 per message, never populates `mqtt.protocol_version`, and
never populates any MQTT-5-only attribute. The table below is the general
contract for a data source that does expose protocol version and
properties (see
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)).

| Aspect | MQTT 3.1.1 | MQTT 5 |
| --- | --- | --- |
| Properties (PFI, content type, response topic, correlation data, message expiry, topic alias, subscription identifiers, user properties, reason codes) | Do not exist on the wire | May be present; each is mapped per the tables above only when present |
| `mqtt.protocol_version` | `"3.1.1"` | `"5"` |
| Body policy | Always `BytesValue` (no PFI to consult) | `StringValue` or `BytesValue` per [Body policy](#body-policy-binary-vs-utf-8) |
| Reason codes (PUBACK/PUBREC/etc.) | Not applicable to PUBLISH itself | Not applicable to PUBLISH itself either; reason codes belong to acknowledgement packets, out of scope for this PUBLISH-to-LogRecord contract, and are addressed by [mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md) |
| Any MQTT-5-only attribute on a 3.1.1-sourced LogRecord | Must never be set | N/A |

A receiver must never emit an MQTT-5-only attribute (any `mqtt.*` attribute
in the "MQTT 5 only" rows above) when `mqtt.protocol_version = "3.1.1"`. An
exporter that receives such an inconsistent LogRecord (protocol version
3.1.1 with an MQTT-5-only attribute present) must treat it as a validation
error (see [Validation](#validation)) rather than silently ignoring the
extra attribute or upgrading the outbound protocol version.

## Round-trip guarantees

Round-trip means: PUBLISH A -> receiver -> LogRecord -> exporter -> PUBLISH
B, with no intervening processor mutation. The guarantees below are the
**general contract**; the v1 baseline can only exercise the fields it can
actually populate (see
[Unavailable in the v1 baseline](#unavailable-in-the-v1-baseline-rumqttd-0200)).

### Guaranteed to round-trip exactly

- Payload bytes (`body`), including zero-length payloads and
  non-UTF-8-under-PFI-1 payloads (preserved as bytes with the invalid-UTF-8
  marker attribute set). In the v1 baseline, `body` is always
  `Value::BytesValue` (see [Body policy](#body-policy-binary-vs-utf-8)) and
  still round-trips exactly.
- Topic name (`mqtt.topic`).
- QoS (`mqtt.qos`).
- RETAIN flag (`mqtt.retain`).

### Unavailable in the v1 baseline (rumqttd 0.20.0)

These fields are part of the general contract and round-trip when a data
source provides them, but the v1 baseline receiver cannot populate them at
all (see
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)),
so there is nothing to round-trip: the attribute is absent on the
LogRecord, and the exporter's [validation](#validation) rules for a missing
field apply.

- Packet identifier (`mqtt.packet_id`). Note for a data source that does
  provide it: the exporter reusing the exact same packet identifier value
  on a *new* connection is a protocol-level choice for the exporter to
  make (packet identifiers are connection-scoped in MQTT); the general
  contract guarantees only that the *value* survives the LogRecord
  round-trip, not that the exporter is required to reuse the same
  identifier on the wire unmodified.
- Protocol version (`mqtt.protocol_version`).
- Publisher client identity (`messaging.client.id` / `mqtt.client_id`).
- DUP flag (`mqtt.dup`) - also see
  [Non-preserved fields](#non-preserved-fields) below for the separate,
  general-contract reason this field is never exporter-replayed even when
  a data source does provide it.
- Network/peer/client socket metadata (`network.peer.address`,
  `network.peer.port`, `client.address`, `client.port`).
- All MQTT 5 properties captured in the attribute tables above
  (payload format indicator, message expiry interval, content type, response
  topic, correlation data, topic alias, subscription identifiers, user
  properties including order and repeated keys).

### Non-preserved fields

- **DUP flag is receive-observed only, never exporter-replayed as an
  outbound retransmission signal.** The exporter always sends new outbound
  PUBLISH packets with `DUP=0` (or the value it independently determines is
  correct for its own retransmission, if it retransmits). `mqtt.dup` on a
  LogRecord is diagnostic metadata about how the original packet arrived,
  not an instruction to the exporter.
- **Topic Alias is connection-local and is never replayed as an alias.**
  `mqtt.topic_alias`, if present, records that the *original* connection
  used an alias; the exporter always publishes using the full
  `mqtt.topic` string and independently decides its own alias usage (if
  any) based on its own connection's negotiated alias mappings. Reusing the
  numeric alias value from a different connection would be incorrect and
  potentially publish to the wrong topic.
- **Wire-level packet identifier reuse across connections is not
  guaranteed**, only the numeric value's presence in the LogRecord (see
  above).
- **Broker-observed receive timestamp is not part of the MQTT wire format**
  and is therefore never present to round-trip; `time_unix_nano` /
  `observed_time_unix_nano` are receiver-local clock reads, not replayed
  onto the outbound PUBLISH (MQTT PUBLISH has no timestamp field to write
  one into).
- **TCP/TLS connection-level metadata** (`network.peer.address`,
  `network.peer.port`, `client.address`, `client.port`) describes the
  *original* connection and is never used by the exporter to choose its own
  broker connection; the exporter always publishes over its own configured
  connection.
- **Topic segment decomposition** is not performed or reconstructed by this
  contract; only the whole topic string round-trips.
- **CONNECT-level session state** (clean start, session expiry, will
  message, keepalive) is not part of a PUBLISH and is out of scope.
- **Retransmission/duplicate suppression state** (which packet identifiers
  are currently in-flight) is connection state, not PUBLISH content, and is
  out of scope.

## QoS 1 durability boundary (rumqttd 0.20.0)

This is a separate, independently verified constraint from field
availability above: **rumqttd queues the PUBACK for a QoS 1 PUBLISH, and
sends it to the publishing client, before the message is delivered to
`LinkRx`.** Acknowledgement happens inside rumqttd's broker core, ahead of
and independent of whatever the receiver, any processor, or the exporter
does with the message afterward.

Consequences:

- From the original publisher's point of view, MQTT QoS 1 "at least once"
  delivery is satisfied at broker ingest. It does not wait for, and is not
  contingent on, the message ever reaching `LinkRx`, being converted to a
  LogRecord, passing through the pipeline, or being re-published by an
  exporter.
- **Round-tripping a PUBLISH through this contract's LogRecord mapping and
  back out through an exporter does not extend MQTT QoS 1 durability beyond
  what the broker already guaranteed at ingest.** If the receiver, a
  processor, or the exporter drops the message after rumqttd has already
  sent PUBACK, that loss is invisible to the original publisher and cannot
  be recovered through MQTT-level redelivery: the publisher has already
  been told the message was accepted, and no persistent-session
  redelivery mechanism is triggered by a failure that occurs after PUBACK.
- Consequently, **the delayed/manual-acknowledgement model described in
  [mqtt-explicit-qos1-acknowledgement-drop-policy.md](mqtt-explicit-qos1-acknowledgement-drop-policy.md)
  does not apply to the rumqttd-embedded-broker v1 baseline.** That
  document's model assumes the component holding the acknowledgement token
  controls PUBACK timing and can withhold it until downstream processing
  completes. In the v1 baseline, PUBACK timing is internal to rumqttd and
  is not observable or controllable at the `LinkRx` boundary; that
  document's design applies to a different, not-yet-adopted architecture
  (an MQTT client subscribing to an external broker), not to this
  contract's verified v1 receiver.
- This contract's [validation](#validation) and
  [trust and security policy](#trust-and-security-policy-for-exporter-replay)
  rules still apply for whatever portion of the pipeline runs after
  `LinkRx`, but operators must not read "the exporter successfully
  re-published this message" as evidence that the original MQTT publisher
  ever received confirmation tied to that re-publish; the publisher's
  PUBACK is already an independent, already-settled fact by that point.

## Trust and security policy for exporter replay

A LogRecord being replayed by the exporter did not necessarily come from the
paired receiver. It may have been forwarded from another pipeline, stored
and replayed later, or produced by an upstream component the operator does
not fully trust. The exporter must therefore apply the following policy
before publishing:

1. **The exporter never trusts `mqtt.*` attributes to select its own broker
   connection, credentials, or transport security settings.** Broker
   address, TLS configuration, and authentication are exporter
   configuration, never derived from LogRecord attributes. This prevents a
   malicious or buggy upstream component from redirecting outbound
   publishes to an attacker-controlled broker.
2. **Topic write access is subject to the exporter's own configured
   authorization**, not implied by the presence of `mqtt.topic` in the
   LogRecord. If the exporter is configured with a topic allowlist or topic
   prefix policy, that policy is enforced regardless of what `mqtt.topic`
   requests; a disallowed topic is a validation error (see
   [Validation](#validation)), not silently rewritten to an allowed topic.
3. **QoS and RETAIN requested via attributes are bounded by exporter
   configuration.** An operator may configure a maximum permitted QoS or
   forbid RETAIN entirely for a given exporter instance; a LogRecord
   requesting a higher QoS or RETAIN=true than configured is downgraded
   or rejected per exporter configuration (downgrade vs reject must be an
   explicit, documented exporter option; silent downgrade without
   configuration is not acceptable because it silently weakens delivery
   guarantees).
4. **`mqtt.correlation_data` and `mqtt.user_properties` are opaque
   pass-through data.** The exporter republishes them verbatim without
   interpreting or executing their contents; it never uses their values to
   affect connection or authorization decisions in this contract's baseline
   implementation.
5. **The exporter must not derive trust from `network.peer.address` /
   `network.peer.port`** (the original broker's observed address); these are
   diagnostic-only and must never be used to select an outbound destination.
6. **Payload bytes are never executed, parsed, or interpreted** by the
   exporter; they are published as opaque bytes/UTF-8 exactly as carried in
   `body`.
7. **Attribute size and count limits are enforced identically on both
   sides** (see [Validation](#validation)), so a hostile or corrupted
   LogRecord cannot cause unbounded memory allocation on replay.

## Validation

Both the receiver and the exporter validate against this contract; a
violation is always an explicit, typed error/rejection, never a silent
best-effort coercion. Rows below that reference a field unavailable in the
v1 baseline (see
[Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200))
are part of the general contract and become active once a data source
provides that field; the v1 baseline simply never observes the condition
because the field is never present.

### Receiver-side validation (PUBLISH -> LogRecord)

| Condition | Behavior |
| --- | --- |
| Topic longer than a configured `max_topic_bytes` | Reject the PUBLISH at decode time (this is normally already bounded by the MQTT client's own packet-size limits; the receiver documents its effective limit). |
| Payload longer than a configured `max_body_bytes` | Emit a LogRecord with the body truncated is **not allowed**; instead the receiver drops the message, counts it, and reports it through receiver self-telemetry (mirrors the journald receiver's `large_field_policy: drop_and_count` philosophy, but for the whole record rather than a field, because a truncated MQTT payload is not a meaningful partial value). |
| More than a configured `max_user_properties` MQTT 5 User Properties | Preserve up to the configured limit in declared order, drop the remainder, and count the drop through self-telemetry; never silently keep an unbounded number. |
| A single User Property key or value longer than a configured `max_user_property_bytes` | Drop that property and count it; do not truncate (truncation would silently corrupt data a consumer might mistake for the whole value). |
| PFI = 1 but payload is not valid UTF-8 | Do not reject; store as `BytesValue` and set `mqtt.payload_format_indicator_invalid_utf8 = true` (see [Body policy](#body-policy-binary-vs-utf-8)). |
| Protocol version reported by the client as neither 3.1.1 nor 5 | Receiver startup/connection failure; this contract does not define a mapping for other versions. |

### Exporter-side validation (LogRecord -> PUBLISH)

| Condition | Behavior |
| --- | --- |
| `mqtt.topic` missing or empty | Reject the LogRecord; do not publish. Report through exporter self-telemetry as a mapping error, not a transport error. |
| `mqtt.qos` missing | Default to QoS 0 only if the exporter is explicitly configured to allow a default; otherwise reject. The default must never silently become QoS 1 or 2 (those carry stronger delivery expectations than absence of information should imply). |
| `mqtt.qos` present but not in `{0, 1, 2}` | Reject. |
| `mqtt.qos` requests QoS 2 | Reject with a typed "QoS 2 not supported" error if the underlying MQTT client does not support QoS 2 delivery (matches the current client library's stated non-goal). |
| `body` is not `BytesValue` or `StringValue` (see [Body policy](#body-policy-binary-vs-utf-8)) | Reject the LogRecord; do not coerce numeric/bool/array/kvlist bodies to bytes. |
| `mqtt.protocol_version` is `"3.1.1"` but an MQTT-5-only attribute is present | Reject (see [MQTT 3.1.1 vs MQTT 5](#mqtt-311-vs-mqtt-5)). |
| `mqtt.protocol_version` missing | Use the exporter's own configured/negotiated protocol version; never infer it from other attributes. |
| Topic fails exporter-configured allowlist/authorization policy | Reject (see [Trust and security policy](#trust-and-security-policy-for-exporter-replay) rule 2). |
| Attribute count or aggregate attribute byte size on the LogRecord exceeds a configured exporter-side bound | Reject the whole LogRecord rather than publish a partially reconstructed PUBLISH. |
| `mqtt.user_properties` entries do not match the documented `{key, value}` KvlistValue shape | Reject; do not attempt partial recovery of malformed entries. |

Every rejection on both sides must be observable (typed error, counter, or
event) and must never include full payload contents in the observability
signal by default, consistent with
[security-privacy-guide.md](../telemetry/security-privacy-guide.md).

## Batching behavior

This contract is about the shape of one LogRecord, not about how many
LogRecords travel together on the wire between receiver and exporter (that
is an OTLP/OTAP transport concern, handled by existing batching components
such as `processor:batch`, independent of MQTT). Two rules apply
specifically because the payload is MQTT:

1. **A receiver must never merge multiple PUBLISH packets into one
   LogRecord**, even if they share the same topic, arrive back-to-back, or
   are part of the same TCP read. One PUBLISH is always exactly one
   LogRecord, preserving the 1:1 invariant this contract depends on for
   replay.
2. **An exporter must never split one LogRecord into multiple outbound
   PUBLISH packets.** If a LogRecord's body exceeds a size the exporter's
   configured MQTT client or broker will accept, that is a validation
   rejection (see [Validation](#validation)), not a silent fragmentation
   into multiple PUBLISH packets (MQTT has no standard payload
   fragmentation, and inventing one here would break the 1:1 contract for
   any consumer downstream of the exporter's broker).

Standard OTLP/OTAP batching of many LogRecords into one export request is
unaffected by this contract and follows the same rules as any other receiver
or exporter in this codebase.

## Transformations

The receiver and exporter perform only the mechanical, contract-defined
transformations described in this document (byte/UTF-8 body selection,
attribute projection per the tables above, and the validation rules above).
Any additional transformation -- topic-based routing, payload parsing,
attribute enrichment, PII redaction, sampling -- belongs in a processor
between the receiver and exporter, using the standard processor set already
available in this codebase (for example `processor:transform`,
`processor:filter`, `processor:attributes`, `processor:content_router`).
This keeps the receiver and exporter each independently testable against
this contract without coupling them to a specific pipeline topology.

## Future work: pluggable envelope

If attribute-key volume, cardinality, or typed-access ergonomics become a
measured problem for the flat-attribute baseline, a follow-up proposal may
introduce a typed, pluggable envelope representation (option 3 from
[Where MQTT envelope metadata lives](#where-mqtt-envelope-metadata-lives)),
or promotion of connection-scoped fields (client/peer metadata) to resource
or scope attributes per
[attributes-guide.md](../telemetry/attributes-guide.md#1-resource-attributes).
Any such change must preserve this document's round-trip guarantees or
explicitly revise them with a migration note, and must not be a prerequisite
for the first receiver/exporter implementation.

## Acceptance criteria

- A documented, versioned mapping table exists (this document) covering
  every MQTT PUBLISH envelope field for both MQTT 3.1.1 and MQTT 5, and both
  a future receiver and exporter implementation can be reviewed against it
  without additional design decisions.
- For every field listed under
  [Guaranteed to round-trip exactly](#guaranteed-to-round-trip-exactly), a
  round-trip test demonstrates PUBLISH -> LogRecord -> PUBLISH equality for
  that field against the v1 baseline (rumqttd 0.20.0 `LinkRx`).
- A test demonstrates that, for every field listed under
  [Unavailable in the v1 baseline](#unavailable-in-the-v1-baseline-rumqttd-0200),
  the v1 baseline receiver omits the corresponding attribute rather than
  emitting a default, guessed, or synthesized value (publisher client
  identity, DUP, packet identifier, protocol version, network/peer/client
  metadata, and all MQTT 5 properties).
- For every field listed under
  [Non-preserved fields](#non-preserved-fields),
  a test demonstrates the documented non-preserving behavior (for example,
  that `DUP` is never propagated as an outbound retransmission signal),
  using a data source or test harness that can supply the field even though
  the v1 baseline cannot.
- A test or documented explanation demonstrates that rumqttd 0.20.0 sends
  PUBACK for a QoS 1 PUBLISH before the message reaches `LinkRx`, and that
  the pipeline downstream of `LinkRx` (receiver, processors, exporter)
  cannot delay, withhold, or retroactively invalidate that PUBACK; see
  [QoS 1 durability boundary](#qos-1-durability-boundary-rumqttd-0200).
- Body policy tests cover the v1 baseline (always `Value::BytesValue`, no
  PFI observed) and, as general-contract tests against a data source that
  provides PFI: empty payload, valid-UTF-8 payload with PFI=1, non-UTF-8
  payload with PFI=1, PFI=0, and MQTT 3.1.1 (no PFI).
- General-contract MQTT 5 property tests (not exercised by the v1 baseline,
  since rumqttd 0.20.0 `LinkRx` does not expose these) cover: payload
  format indicator, message expiry interval, content type, response topic,
  correlation data, topic alias, subscription identifiers (including more
  than one), and user properties (including repeated keys and property
  order).
- A test demonstrates that an MQTT-5-only attribute present alongside
  `mqtt.protocol_version = "3.1.1"` is rejected by the exporter.
- A test demonstrates the collision/precedence rule: the exporter reads
  `mqtt.topic` even when `messaging.destination.name` has been changed or
  removed by an intervening processor.
- A test demonstrates every exporter-side validation rejection in the
  [Validation](#validation) table, and that each rejection is observable
  without exposing full payload content.
- A test demonstrates that the exporter never selects its outbound broker
  connection, credentials, topic authorization, or QoS/RETAIN ceiling from
  LogRecord attributes (see
  [Trust and security policy](#trust-and-security-policy-for-exporter-replay)).
- A test demonstrates that one PUBLISH always produces exactly one
  LogRecord and vice versa, including under batched OTLP/OTAP export of
  many LogRecords.
- A test demonstrates `mqtt.user_properties` count/size bounds are enforced
  on the receiver side (drop-and-count, not truncate) and that the drop is
  observable through self-telemetry (general contract; requires a data
  source that provides user properties).

## Non-goals

- Defining the future receiver's or exporter's configuration schema,
  connection lifecycle, flow control, or acknowledgement policy (covered
  separately).
- QoS 2 support.
- Payload content parsing or schema-aware decoding (Sparkplug B, JSON,
  protobuf, etc.).
- MQTT over WebSocket transport specifics.
- Topic-based routing, filtering, or subscription pattern semantics.
- Promoting connection-scoped attributes to resource/scope attributes (left
  to [Future work](#future-work-pluggable-envelope)).
- CONNECT/session-level state (will messages, session expiry, keepalive).
- Extending rumqttd's `LinkRx` to forward additional fields, or building a
  manual/delayed-acknowledgement mechanism on top of it. Both would require
  rumqttd changes or a different receiver architecture and are out of scope
  for this contract (see
  [QoS 1 durability boundary](#qos-1-durability-boundary-rumqttd-0200)).

## Related work

- [mqtt-bounded-inbound-publish-flow-control.md](mqtt-bounded-inbound-publish-flow-control.md)
- [mqtt-explicit-qos1-acknowledgement-drop-policy.md](
  mqtt-explicit-qos1-acknowledgement-drop-policy.md) -
  written against a client-of-external-broker architecture; its
  delayed-acknowledgement model does not apply to the rumqttd-embedded v1
  baseline (see
  [QoS 1 durability boundary](#qos-1-durability-boundary-rumqttd-0200)).
- [journald-receiver.md](../journald-receiver.md) - reference design for
  mechanical envelope-to-LogRecord projection and the drop-and-count pattern
  for oversized fields.
- [transport-headers.md](../transport-headers.md) - reference design for
  the alternative "context, not attribute" placement considered and not
  chosen as the v1 baseline.
- [telemetry/attributes-guide.md](../telemetry/attributes-guide.md) -
  attribute categorization, namespacing, and placement rules referenced
  throughout this contract.
- [telemetry/semantic-conventions-guide.md](
  ../telemetry/semantic-conventions-guide.md) - naming rules referenced for
  the `mqtt.*` namespace and reuse of upstream `messaging.*`/`network.*`
  conventions.
- rumqttd 0.20.0 - the verified v1 baseline data source; see
  [Baseline data source and field availability](#baseline-data-source-and-field-availability-rumqttd-0200)
  and [QoS 1 durability boundary](#qos-1-durability-boundary-rumqttd-0200).
- OpenTelemetry upstream semantic conventions:
  [messaging](https://opentelemetry.io/docs/specs/semconv/messaging/) and
  [general/naming](https://opentelemetry.io/docs/specs/semconv/general/naming/).
