# Two-level log sampling with feedback: prototype plan

Status: draft plan, for prototyping. Owner: @jmacd.

This document is an implementation plan for a working prototype of
**two-level (local + global) bottom-k log sampling with a feedback loop**,
built entirely from `otap-dataflow` components. It specifies the new nodes,
the shared-state and control plumbing they reuse, and the wire protocol for
the feedback return path. The return path (M2) also adds an opt-in
`sampling_feedback_channel` to the existing OTLP/HTTP receiver and exporter.

The statistical design and its evidence live outside this repo, in the
`cckr-experiment` study (Bottom-Floor sampler and the two-stage feedback
note). The experimental validation is being pursued in a separate effort;
this plan is the systems counterpart: a real prototype that exercises the
same mechanism inside one running dataflow engine.

## 1. Goal and scope

Build, inside `otap-dataflow`, a closed feedback loop in which:

- Many SDK-side pipelines each run a **local** bottom-k sampler over the
  logs they emit, then ship their sample (representatives + Horvitz-
  Thompson counts) to a collection agent via the OTLP exporter.
- A single collection-agent pipeline **passes the logs through** while a
  fixed-size, fixed-interval **global reservoir sampler** observes the
  union of all SDK samples and computes the globally high-pressure
  callsites ("heavy hitters").
- The agent **returns** the current global heavy-hitter table to each SDK
  on the OTLP response path, encoded so it fits comfortably within HTTP/2
  header limits and compresses well under HPACK.
- Each SDK **incorporates** the returned global table into its local
  admission decision at runtime, via a low-contention async pointer swap,
  closing the loop.

In scope for the prototype: logs only; a single agent; the binding-gate
admission rule (`min(tau^L * w_local, tau^G * g)`); HTTP/2 response
headers as the return channel. Out of scope for the first prototype:
multi-agent hierarchies, persistence/durability of the global table, and
metrics/traces signals.

## 2. Background: the mechanism in one paragraph

Each SDK admits records into a bottom-`(k+1)` weighted reservoir keyed by
an exponential priority `-ln(u) / w_c`. The `(k+1)`-th smallest key is the
local threshold `tau^L`; with the inverse-frequency weight
`w_local_c = 1 / nhat_c` this yields equal per-callsite coverage and an
unbiased HT count `nhat_c = m_c / (1 - exp(-tau^L * w_c))`. The agent runs
the *same* sampler one level up over the union of SDK representatives
(weighted by their `nhat_c`), producing a global count `N_c = sum_p
nhat^p_c`, a global weight `g_c = 1 / N_c`, and a global threshold
`tau^G`. The SDK then admits iff `-ln(u) < min(tau^L * w_local_c, tau^G *
g_c)`: two separately normalized inclusion pressures, the tighter binds.
`tau` is the *normalizer* (the price of a slot, `~ budget / cardinality`)
at each level; the return channel only needs to carry the small set of
globally heavy callsites and `tau^G`, since everything else rides the
most-permissive "rarest-seen" floor. See the `cckr-experiment`
`PAPER.md` (Sections 4-8, 11) and `notes/two-stage-feedback.md` (the
binding gate and the bounded return sketch) for derivations and evidence.

## 3. Architecture overview

```text
  SDK / ITS pipeline (one of many)        Collection-agent pipeline
  +-----------------------------+         +----------------------------+
  | internal logs SDK / ITS     |         | OTLP receiver (passthrough)|
  |        |                    |         |        |                   |
  |        v                    |         |        +--> downstream     |
  | local-sample processor      |         |        |    exporter/store |
  |  (bottom-(k+1), binding gate)|        |        v                   |
  |        |                    |         | global-reservoir processor |
  |        v                    |  OTLP   |  (fixed size, fixed interval|
  | OTLP exporter --------------+--------->|   weighted bottom-k)       |
  |        ^                    |  logs   |        |                   |
  |        |   global table     |         |        v                   |
  |        | (HTTP/2 resp hdrs)  |<--------+ heavy-hitter table + tau^G |
  +--------|--------------------+ response +--------|------------------+
           |  ArcSwap update                        |  ArcSwap publish
           +----------------------------------------+
```

Two pipelines run in the same engine (or two engines) for the prototype.
The feedback signal travels *backwards* along the existing OTLP request/
response exchange: the SDK's exporter already reads the OTLP response, so
the global table rides home on that same response as HTTP/2 headers. No
new connection, port, or control plane is introduced.

## 4. Components to build

Each component below names its crate location, the trait it implements,
its responsibilities, and the concrete hook points in today's code.

### C0. Registration and feature wiring (both processors)

Each new processor must be discoverable by the engine the same way the
existing contrib processors are:

- Register a `ProcessorFactory<OtapPdata>` into the
  `OTAP_PROCESSOR_FACTORIES` `linkme::distributed_slice` and give it a URN
  `name` constant (pattern in
  `crates/contrib-nodes/src/processors/condense_attributes_processor/mod.rs:18,46-47,197-202`).
  Use `urn:otel:processor:log_sampler` and
  `urn:otel:processor:global_reservoir` (snake_case component names, per
  the repo's component-naming convention).
- Add the `pub mod` behind a feature gate in
  `crates/contrib-nodes/src/processors/mod.rs` and declare the feature in
  that crate's `Cargo.toml`, mirroring `condense-attributes-processor`.
- Primary metric set names should follow the convention
  `processor.log_sampler` and `processor.global_reservoir`.

### C1. Local sampling processor (SDK side)

- Location: new processor, e.g.
  `crates/contrib-nodes/src/processors/log_sampler_processor/`
  (mirrors the existing `condense_attributes_processor` and
  `recordset_kql_processor` layout under `contrib-nodes`).
- Trait: local `Processor`
  (`crates/engine/src/local/processor.rs:64-126`,
  `async fn process(&mut self, msg, effect_handler)`), since the runtime
  is thread-per-core and the sampler state is thread-local.
- Responsibilities:
  - Maintain a bottom-`(k+1)` weighted reservoir over a window, keyed by
    `-ln(u) / w_local_c`, plus the carried weight maps (`w_seen`,
    `w_unseen`) exactly as in the Bottom-Floor design.
  - Apply the binding gate as a pre-admission skip: read the current
    global table (C4) and reject immediately if `k_G >= tau^G`, sharing
    the single `u` between the global test and the local reservoir key so
    the realized rule is exactly `min(tau^L w, tau^G g)`. The global
    weight `g_c` is looked up in the heavy-hitter table for enumerated
    callsites and defaults to the table's `g_unseen` scalar for every
    other callsite (the most-permissive global floor), so a present table
    never suppresses a non-enumerated callsite. Only a *missing* table
    means `tau^G = +inf` (pure local-only); see C6.
  - On window close (driven by `NodeControlMsg::TimerTick`,
    `crates/engine/src/control.rs:266-273`, armed via
    `start_periodic_timer`, `crates/engine/src/local/processor.rs:150-167`)
    emit the kept representatives, each annotated with its HT count
    `nhat_c` so the agent can sum them. Emission rolls the local weights
    forward (`w_next = 1/nhat_c`, `w_unseen = max w_next`).
- Output shape: representatives are ordinary log records flowing
  downstream to the OTLP exporter; the per-callsite `nhat_c` is attached
  as a log attribute (a reserved attribute key, see C5) so it survives
  OTLP transport without a schema change. The "N items skipped"
  annotation is optional for the prototype.

### C2. Global reservoir processor (agent side)

- Location: new processor, e.g.
  `crates/contrib-nodes/src/processors/global_reservoir_processor/`.
- Trait: local `Processor`, fixed-size and fixed-interval. The
  `batch_processor` is the template for an interval/size-driven node
  (`crates/core-nodes/src/processors/batch_processor/`, README
  `:13-16,:53-55,:97-103`): it shows timer-flush plus size-bounded state.
- Responsibilities:
  - **Pass through** every incoming log unchanged to its downstream
    output (the agent must not drop or reorder the data path). Observation
    is side-channel only; the `FlowMetricHook` pattern
    (`crates/engine/src/processor.rs:67-104`) is precedent for observe-
    without-mutate, but here we simply clone the identity + weight into
    the reservoir and forward the original PData.
  - Run a weighted bottom-`k` over the union of SDK representatives,
    weighting each by its carried `nhat_c`. This is the `(k+1)`-of-the-
    union reducer; it yields `N_c`, `tau^G`, `g_c = 1/N_c`, and the
    global rarest-seen floor `g_unseen = max_c g_c`.
  - Strip the reserved `nhat_c` transport attribute (C5) from each record
    on the pass-through path so the internal per-callsite weight does not
    leak to the agent's downstream exporter/store.
  - On a fixed interval (`TimerTick`), close the global window, compute
    the top-`K'` heavy hitters (smallest `g_c`, i.e. largest `N_c`),
    optionally smooth `N_c` with an EWMA for stability (the global level
    is where inertia belongs), and **publish** the new table by an atomic
    swap (C4) that the receiver reads when building responses.
  - State is `O(k)` for the reservoir and `O(K')` for the published
    table; both are fixed and bounded.

### C3. Feedback return path (OTLP response headers)

The agent must put the published table onto the OTLP response, and the
SDK exporter must read it.

- Receiver (attach headers): the OTLP/HTTP receiver builds its response in
  one place: `ok_response()` / `response_bytes()` in
  `crates/otap/src/otlp_http.rs:218-250`. Add the encoded table as
  response headers via `resp.headers_mut()` there. The server uses
  `hyper_util::server::conn::auto::Builder`
  (`crates/otap/src/otlp_http.rs:908-910`), which negotiates HTTP/2 when
  the client offers it, so the headers are HPACK-compressed on the wire.
  For the OTLP/gRPC path (`crates/otap/src/otlp_grpc.rs:83-128`) the
  equivalent is tonic response metadata (also HTTP/2 / HPACK); the
  prototype targets HTTP first.
- The receiver reads the current table from the same ArcSwap the global
  processor publishes to (C4). The receiver and the global processor are
  in the same agent pipeline and can share the `Arc<ArcSwap<Table>>` via
  the pipeline's node wiring / a shared extension handle.
- Exporter (read headers): the OTLP/HTTP exporter uses `reqwest` and
  already reads the response
  (`crates/core-nodes/src/exporters/otlp_http_exporter/mod.rs:610-625`).
  Add a hook right after `error_for_status()` (`:615-616`) to read our
  response headers before body collection, decode the table, and publish
  it to the SDK-side ArcSwap (C4) that C1 reads. The exporter does not
  inspect headers today (`:627-655`), so this is purely additive.
- HTTP/2 negotiation is a client-side prerequisite for the HPACK win:
  `reqwest` defaults to HTTP/1.1 and only speaks HTTP/2 over TLS+ALPN or
  with `http2_prior_knowledge()`. The functional round-trip works on
  HTTP/1.1 (headers are just plaintext), but the M2 HPACK-compression
  validation requires forcing the exporter onto HTTP/2. Confirm the
  prototype config negotiates h2 end-to-end before measuring header
  bytes.

### C4. Async pointer-swap shared table

- Primitive: `ArcSwap`, already the idiomatic low-contention swap in this
  codebase (`crates/otap/src/tls_utils.rs:4-31` uses
  `Arc<ArcSwap<...>>` + atomics for hot-path config reload). Readers do a
  wait-free `load()`; the publisher does a single `store()` of a freshly
  built immutable table. No locks on the per-record hot path.
- Two instances:
  - **Agent side**: global processor is the writer; OTLP receiver is the
    reader (C3).
  - **SDK side**: OTLP exporter is the writer (decodes the response
    header); local sampler is the reader on every admission decision (C1).
- This satisfies the "async variable swap to reduce contention"
  requirement on both ends without introducing channels on the hot path.
  (If we later want change-notification rather than poll-on-read, a
  `tokio::sync::watch` is the natural upgrade, but ArcSwap matches
  existing practice and is sufficient for poll-on-read.)

### C5. Callsite identity

- A "callsite" is the stable identity of a log statement. The OTLP/OTAP
  log model already surfaces `event_name`, `body`, and `attributes`
  (`crates/pdata-views/src/views/logs.rs:133-157`;
  `crates/pdata/src/proto/opentelemetry.proto.logs.v1.rs:43-56`).
- Prototype choice: use `event_name` as the callsite id when present
  (it is the OTel-model field closest to a template/statement identity),
  falling back to a hash of the (scope name + body template) otherwise.
  This is "hard-coded to the OTel model" as requested: identity is a
  first-class log field, not a configured attribute path.
- For transport between SDK and agent, the local `nhat_c` weight is
  attached under a reserved attribute key (e.g.
  `otel.sampling.nhat`); the callsite id itself is the existing
  `event_name`, so no new field is needed on the forward path.

### C6. Return wire protocol

The return payload is "mainly callsite identifiers" plus `tau^G`. Design
goals: fit within reasonable HTTP/2 header size limits and compress well
under HPACK.

- Header layout (HTTP/2, lowercase names so HPACK static/dynamic tables
  apply):
  - `otel-sample-tau-g`: the global threshold `tau^G` as a compact
    decimal/hex scalar.
  - `otel-sample-g-unseen`: the global rarest-seen floor `g_unseen`, the
    `g_c` value applied to every callsite *not* listed below. Required:
    without it the SDK cannot score non-enumerated callsites and must
    treat the whole table as absent (fail-safe below).
  - `otel-sample-heavy`: the heavy-hitter list. Each entry is a callsite
    id **and** its `g_c` (the per-callsite global weight is required, not
    optional: the binding gate computes `tau^G * g_c` per listed callsite,
    so a bare id with no weight carries no usable signal). To compress
    well:
    - Send callsite ids as fixed-width tokens (e.g. an 8-byte FNV/xxhash
      of `event_name`, base64url-encoded) so the *same* id string recurs
      identically across responses and lands in the HPACK dynamic table,
      costing ~1 byte after first use.
    - Quantize `g_c` (e.g. a fixed-point or log-domain code) so identical
      weights across rounds also recur and index well.
    - Keep `K'` small (the cost/benefit knee is `K' ~ head cardinality`),
      so the whole list is well under typical 8-16 KiB header budgets.
  - Optionally split into multiple `otel-sample-heavy` header lines to
    stay under per-field limits; HPACK indexes each repeated line.
- Versioning: a single `otel-sample-ver` header so the encoding can
  evolve. Unknown versions are ignored by the SDK (fail-safe: it just
  keeps using the local-only rule until it understands the table).
- Fail-safe semantics: a missing/malformed table (or a missing
  `g_unseen`) means "global gate slack" (`tau^G = +inf`), i.e. the SDK
  degrades to local-only sampling, never to incorrect suppression. A
  *present* table with a callsite absent from the heavy-hitter list is
  not a failure: that callsite is scored at `g_unseen`, the most-
  permissive global rate. This matches the design's cold-start behavior.

## 5. Concurrency and control model

- Hot path (per record): wait-free `ArcSwap::load()` of the global table
  (SDK) or a clone-into-reservoir (agent). No locks, no allocation beyond
  the reservoir's bounded state.
- Window/interval close: driven by the engine's periodic `TimerTick`
  control message, armed with `start_periodic_timer`
  (`crates/engine/src/local/processor.rs:150-167`;
  `crates/engine/src/control.rs:266-273`). Both the local window and the
  global window are timer-driven and independent.
- Table publish: a single `ArcSwap::store()` of a freshly built immutable
  table per interval; readers never block the writer and vice versa.
- Runtime reconfiguration of `k`, `K'`, interval can ride the existing
  `NodeControlMsg::Config` path (`crates/engine/src/control.rs:241-337`).

## 6. Pipeline wiring

Pipelines are declared in YAML/JSON via `crates/config/src/pipeline.rs`
(nodes + explicit `connections`); see `configs/otlp-otap.yaml` for a
receiver -> exporter example. The prototype adds two configs:

- `configs/sdk-local-sampler.yaml`: internal-logs/ITS source ->
  `log_sampler_processor` -> `otlp_exporter`.
- `configs/agent-global-sampler.yaml`: `otlp_receiver` ->
  `global_reservoir_processor` -> downstream exporter/store.

The shared `Arc<ArcSwap<Table>>` handles are created at pipeline build
time and injected into the two nodes that share them (receiver+global on
the agent; exporter+sampler on the SDK).

## 7. Source of the SDK logs (the "ITS pipeline")

The engine already has an internal telemetry system (ITS) that emits its
own logs, with a log tap and provider modes including `its`
(`crates/telemetry/src/lib.rs:4-26,:43-55,:139-180`;
`crates/telemetry/README.md:82-118`;
`crates/state/src/store.rs:99-155`). For the prototype the internal logs
SDK / ITS pipeline is the natural high-volume, many-callsite source to
feed C1, so we can dogfood the loop on the engine's own logs before
pointing it at external traffic.

## 8. Milestones

Implementation status (prototype): **M0-M4 implemented and unit-tested.**
The end-to-end loop is functionally complete over both the OTLP/HTTP and
OTLP/gRPC return paths. The crates are wired behind the
`log-sampler-processor` and `global-reservoir-processor` features; the C6
return protocol uses `otel-sample-*` response headers/metadata and a
process-global `Arc<ArcSwap<GlobalTable>>` registry keyed by channel name.

- **M0 - Local sampler, no feedback. [done]** Implement C1 as a standalone
  bottom-`(k+1)` processor (global gate always slack, `tau^G = +inf`).
  Validate unbiased per-callsite counts and bounded state on a synthetic
  log stream. This reproduces today's Bottom-Floor locally.
- **M1 - Global sampler, no return. [done]** Implement C2 on the agent: pass-
  through + weighted bottom-k over SDK representatives; log the computed
  heavy-hitter table, `tau^G`, and `g_unseen`. Validate `N_c` recovers
  global counts.
- **M2 - Return channel. [done]** Implement C3 + C6: agent attaches the table to
  OTLP/HTTP responses; SDK exporter decodes it and publishes to ArcSwap.
  Validate end-to-end header round-trip and HPACK compression (inspect
  on-wire header bytes across repeated responses).
- **M3 - Close the loop. [done]** Wire C4 into C1's admission (binding gate) and
  confirm the redundant-head flood drops (fleet spends less on globally
  ubiquitous callsites) while globally-unique callsites are preserved,
  matching the two-stage-feedback predictions.
- **M4 - Hardening. [done]** EWMA smoothing of `N_c`, staleness tolerance (table
  is >=1 round trip old; degrade gracefully toward local-only),
  runtime-config of `k`/`K'`/interval via `NodeControlMsg::Config`, and the
  gRPC/tonic-metadata return variant (receiver attaches the table to Logs
  export responses as `otel-sample-*` metadata; exporter decodes it on Logs
  exports), parallel to the OTLP/HTTP path.

## 9. Testing and validation

- Unit: reservoir keeps <= `k+1`; HT counts unbiased over many windows;
  binding gate equals `min` of the two scores under shared `u`; table
  encode/decode round-trips and is version-tolerant.
- Integration: two pipelines in one engine; assert the SDK's effective
  keep-rate for an enumerated heavy hitter matches `1 - exp(-tau^G g_c)`
  and that a globally-unique callsite is never throttled below local rate.
- Wire: capture HTTP/2 frames and confirm the heavy-hitter header lines
  land in the HPACK dynamic table (near-1-byte cost) on repeat responses,
  and that the table stays within the chosen header-size budget for
  `K' ~ head cardinality`.
- Cross-check against `cckr-experiment`: the prototype's over-
  representation ratio and unique-recall should track the study's
  `study_two_stage.py` results qualitatively.

## 10. Open questions and risks

- **Callsite id stability across SDKs.** `event_name` must mean the same
  thing fleet-wide for the global table to be meaningful; if SDKs differ,
  a normalized hash basis must be agreed. (Prototype: assume homogeneous
  ITS logs.)
- **Response-header reach.** Headers only return to the *immediate* OTLP
  peer. If an intermediary sits between SDK and agent, the table must be
  re-emitted hop-by-hop; the prototype assumes a direct SDK->agent link.
- **Staleness vs. stability.** The table is at least one round trip old;
  EWMA smoothing trades freshness for fleet stability. The local gate is
  the safety net (worst case = local-only), so staleness cannot cause
  incorrect over-suppression.
- **gRPC metadata limits.** If we move the primary path to OTLP/gRPC,
  confirm tonic/h2 metadata size limits accommodate the table; otherwise
  keep the feedback on the HTTP/2 path.
- **Client HTTP/2 negotiation.** The HPACK benefit only materializes if
  the SDK exporter actually speaks HTTP/2; `reqwest` defaults to
  HTTP/1.1. The functional loop is transport-agnostic, but the M2 wire
  validation must force h2 (TLS+ALPN or prior knowledge) end-to-end.
- **Reserved transport attribute hygiene.** The forward-path `nhat_c`
  attribute (C5) is internal; the agent must strip it before its
  downstream exporter/store (C2) so internal weights never leak into
  collected data.

## 11. References

- Statistical design and evidence (separate repo, `cckr-experiment`):
  `PAPER.md` (Bottom-Floor: exponential keys, `tau`, HT correction,
  mergeability - Sections 4-8, 11) and
  `notes/two-stage-feedback.md` (binding gate, bounded return sketch,
  global memory/EWMA, `K'` cost/benefit knee).
- otap-dataflow hook points:
  - Processor trait: `crates/engine/src/local/processor.rs:64-126`.
  - Control / timers: `crates/engine/src/control.rs:241-337`;
    `crates/engine/src/local/processor.rs:150-167`.
  - Observe-without-mutate precedent: `crates/engine/src/processor.rs:67-104`.
  - Interval/size processor template: `crates/core-nodes/src/processors/batch_processor/`.
  - OTLP/HTTP receiver + response construction:
    `crates/otap/src/otlp_http.rs:218-250,:908-910`.
  - OTLP/HTTP exporter + response read hook:
    `crates/core-nodes/src/exporters/otlp_http_exporter/mod.rs:610-625`.
  - ArcSwap low-contention swap idiom: `crates/otap/src/tls_utils.rs:4-31`.
  - Log identity fields: `crates/pdata-views/src/views/logs.rs:133-157`.
  - Pipeline config: `crates/config/src/pipeline.rs`; `configs/otlp-otap.yaml`.
  - Internal telemetry / ITS: `crates/telemetry/src/lib.rs`; `docs/self_tracing_architecture.md`.
