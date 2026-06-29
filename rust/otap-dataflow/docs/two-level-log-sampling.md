# Two-level log sampling with feedback: design and implementation

Status: implemented (milestones M0-M4), unit-tested. Owner: @jmacd.

This document describes the **as-built** two-level log sampling system in
`otap-dataflow`: a local SDK-side sampler, a global agent-side reservoir, and
a feedback loop that returns the agent's global view to the SDKs so the fleet
spends its log budget on diverse callsites rather than on a few chatty ones.

It complements the original prototyping plan in
[`two-level-log-sampling-feedback-plan.md`](two-level-log-sampling-feedback-plan.md),
which records the forward-looking component plan (C0-C7) and the milestone
breakdown (M0-M5). Where the plan describes intended hook points, this document
describes the code that now exists, with file and symbol citations.

This is distinct from the per-thread CCKR sampler that lives inside the
telemetry SDK itself; see
[`../crates/telemetry/src/sampler/design.md`](../crates/telemetry/src/sampler/design.md).
The system here is a pair of **pipeline processors** plus a feedback codec on
the OTLP receivers and exporters.

## 1. Overview

Internal telemetry logs follow a heavy-head / long-tail distribution: a few
callsites are extremely chatty while many are rare. Naive head-based limiting
loses the tail; naive uniform sampling drowns the tail under the head. The goal
is a fixed per-period budget that nonetheless preserves tail visibility and
yields statistically unbiased per-callsite counts.

The system has two levels:

- **Local level (SDK side).** Each pipeline that emits logs runs a
  `log_sampler` processor. Over a fixed time window it keeps a weighted
  bottom-`(k+1)` reservoir of representatives keyed by an exponential priority,
  then emits the kept records downstream, each annotated with its
  Horvitz-Thompson count so the agent can recover totals.
- **Global level (agent side).** A single collection-agent pipeline runs a
  `global_reservoir` processor. It passes every log through unchanged while, as
  a side channel, accumulating an exact global per-callsite count and running
  the *same* sampler one level up over the union of all SDK representatives.
  On each window it publishes a small **heavy-hitter table** containing the
  global threshold `tau^G`, the rarest-seen floor `g_unseen`, and the top `K'`
  globally heavy callsites.

The feedback loop returns that table to each SDK on the OTLP **response** path,
encoded as `otel-sample-*` headers (HTTP/2) or metadata (gRPC). The SDK
exporter decodes it and publishes it to a process-local shared channel; the
local `log_sampler` consults it on its next admission decision. The loop adds
no new connection, port, or control plane: the table rides home on the OTLP
exchange the SDK already performs.

## 2. The statistical mechanism

Each level admits a record into a bottom-`(k+1)` weighted reservoir keyed by an
exponential priority `-ln(u) / w_c`, where `u ~ Uniform(0, 1]` and `w_c` is the
per-callsite weight. The `(k+1)`-th smallest key is the level's threshold
`tau`. With the inverse-frequency weight `w_local_c = 1 / nhat_c`, the reservoir
yields equal per-callsite coverage and an unbiased Horvitz-Thompson count:

```text
nhat_c = m_c / (1 - exp(-tau^L * w_c))
```

where `m_c` is the number of retained representatives of callsite `c`. The
agent runs the same reservoir over the union of SDK representatives, weighting
each by its carried `nhat_c`, producing a global count `N_c = sum over SDKs of
nhat_c`, a global weight `g_c = 1 / N_c`, and a global threshold `tau^G`.

The **binding gate** combines the two levels. For each record the SDK draws a
single `u` shared between both tests and admits iff:

```text
-ln(u) < min(tau^L * w_local_c, tau^G * g_c)
```

Two separately normalized inclusion pressures; the tighter one binds. `tau` is
the *normalizer* at each level, roughly `budget / cardinality`, i.e. the price
of a reservoir slot. Because the gate is a `min`, a globally heavy callsite is
throttled fleet-wide by the global term, while a globally unique callsite,
scored at the permissive `g_unseen` floor, is never suppressed below its local
rate. The return channel therefore only needs to carry the small set of
globally heavy callsites plus `tau^G` and `g_unseen`; everything else rides the
most-permissive floor.

The Horvitz-Thompson count emitted at window close uses the **binding**
inclusion probability `1 - exp(-min(tau^L * w_c, tau^G * g_c))`, so per-callsite
estimates stay unbiased under the global gate and the agent's sum recovers the
true counts.

The statistical design and its empirical validation live outside this repo, in
the `cckr-experiment` study (`PAPER.md` and `notes/two-stage-feedback.md`).

## 3. Architecture

```text
  SDK / ITS pipeline (one of many)        Collection-agent pipeline
  +-----------------------------+         +----------------------------+
  | internal logs SDK / ITS     |         | OTLP receiver (passthrough)|
  |        |                    |         |        |                   |
  |        v                    |         |        +--> downstream     |
  | log_sampler processor       |         |        |    exporter/store |
  |  (bottom-(k+1), binding gate)|        |        v                   |
  |        |                    |  OTLP   | global_reservoir processor |
  |        v                    |  logs   |  (fixed size, fixed interval|
  | OTLP exporter --------------+--------->|   weighted bottom-k)       |
  |        ^                    |         |        |                   |
  |        |   global table     |         |        v                   |
  |        | (otel-sample-* hdrs)|<--------+ heavy-hitter table + tau^G |
  +--------|--------------------+ response +--------|------------------+
           |  ArcSwap update                        |  ArcSwap publish
           +----------------------------------------+
```

Two pipelines run in the same engine or in two engines. The feedback signal
travels backwards along the existing OTLP request/response exchange. The shared
table is handed between co-located nodes through a process-global registry
keyed by a channel name, so no engine-level wiring is required: a writer node
and a reader node configured with the same channel observe each other's
updates.

## 4. Components

### 4.1 Shared sampling state and wire codec

File: [`crates/otap/src/sampling.rs`](../crates/otap/src/sampling.rs)
(crate `otap_df_otap::sampling`).

This module is the shared vocabulary of the whole feature. It defines:

- **`GlobalTable`** (`sampling.rs:85-97`): the published table, with fields
  `version: u32`, `tau_g: f64`, `g_unseen: f64`, and `heavy: Vec<HeavyHitter>`.
  `GlobalTable::absent()` (`:99-109`) is the slack table with `version == 0`,
  `tau_g == +inf`, `g_unseen == +inf`, and an empty heavy list; `is_absent()`
  (`:111-115`) tests `version == 0`. `g_for(callsite)` (`:117-125`) returns the
  listed `g_c` for a heavy hitter and `g_unseen` otherwise.
- **`HeavyHitter`** (`:69-77`): one globally heavy callsite, `{ callsite: u64,
  g_c: f64 }`.
- **`callsite_id(scope_name, rec)`** (`:48-67`): the stable callsite identity
  (see [Section 5](#5-callsite-identity)).
- **`OTEL_SAMPLING_NHAT`** (`:34`): the reserved log attribute key
  `otel.sampling.nhat` that carries a representative's Horvitz-Thompson count
  across SDK-to-agent transport.
- **`SharedGlobalTable`** (`:128-129`): `Arc<ArcSwap<GlobalTable>>`, the
  wait-free, atomically-swappable handle.
- **`shared_global_table(channel)`** (`:137-153`): a process-global
  `LazyLock<Mutex<HashMap<String, SharedGlobalTable>>>` registry. All callers
  naming the same channel receive clones of the same `Arc`, so a writer and a
  reader configured with the same channel name share state. First use
  initializes the channel to the absent table.
- The **return wire codec** (`:155-249`): header-name constants
  `otel-sample-ver`, `otel-sample-tau-g`, `otel-sample-g-unseen`,
  `otel-sample-heavy` (`:165-175`), plus `encode_headers` (`:177-201`) and
  `decode_headers` (`:203-249`). See [Section 6](#6-return-wire-protocol).

### 4.2 Local sampler processor (SDK side)

URN: `urn:otel:processor:log_sampler`. Primary metric set:
`processor.log_sampler`. Feature: `log-sampler-processor`.

- Registration and factory:
  [`log_sampler_processor/mod.rs`](../crates/contrib-nodes/src/processors/log_sampler_processor/mod.rs)`:344-360`,
  feature-gated in
  [`processors/mod.rs`](../crates/contrib-nodes/src/processors/mod.rs)`:12-14`.
- The reservoir algorithm is a pure, engine-free module:
  [`log_sampler_processor/bottomk.rs`](../crates/contrib-nodes/src/processors/log_sampler_processor/bottomk.rs),
  `WindowSampler`. It keeps a bounded `BinaryHeap<Item>` of up to `k + 1`
  smallest-key entries (`bottomk.rs:146-157`), interns `(resource, scope)`
  groups so representatives regroup faithfully into `ResourceLogs` /
  `ScopeLogs` on emission (`:133-140`, `:278-303`), and carries per-callsite
  weights `w_seen` plus the rarest-seen floor `w_unseen` across windows.

Per-record admission, `WindowSampler::observe_logs` (`bottomk.rs:168-201`):
draw one `u`, compute `neg_ln_u = -ln(u)`, apply the global gate as a
pre-admission skip (`neg_ln_u >= tau_g * g_for(callsite)` rejects before the
reservoir), then admit with key `neg_ln_u / w`. An absent table makes the
global term infinite, so behaviour reduces to local-only.

Window close, `WindowSampler::close_window` (`bottomk.rs:212-307`): determine
`tau^L` from the spare `(k+1)`-th key, compute `m_c` per callsite, derive the
Horvitz-Thompson `nhat_c` from the **binding** inclusion probability, roll the
weights forward as `w_next = 1 / nhat_c` with `w_unseen = max w_next`, then
regroup and annotate each representative with its `nhat_c` under
`OTEL_SAMPLING_NHAT`.

Engine integration, `LogSamplerProcessor` (`mod.rs:100-327`):

- It is a **local** `Processor`, since the runtime is thread-per-core and the
  reservoir state is thread-local.
- `absorb_logs` (`:167-208`) decodes the OTLP log payload, observes it into the
  reservoir, and acknowledges the input, because sampling takes ownership of
  the data rather than forwarding it immediately.
- `close_window` (`:211-246`) is driven by `NodeControlMsg::TimerTick` and on
  `Shutdown`; it encodes the representatives and sends them downstream.
- `current_table` (`:146-163`) snapshots the shared table, tracks freshness by
  version advance, and degrades to the absent table once the table stops
  advancing for longer than `max_table_staleness`.
- Non-log signals pass through unchanged (`:298-301`).

### 4.3 Global reservoir processor (agent side)

URN: `urn:otel:processor:global_reservoir`. Primary metric set:
`processor.global_reservoir`. Feature: `global-reservoir-processor`.

- Registration and factory:
  [`global_reservoir_processor/mod.rs`](../crates/contrib-nodes/src/processors/global_reservoir_processor/mod.rs)`:316-332`,
  feature-gated in `processors/mod.rs:8-10`.
- The reducer is a pure module:
  [`global_reservoir_processor/global_window.rs`](../crates/contrib-nodes/src/processors/global_reservoir_processor/global_window.rs),
  `GlobalWindow`. It holds only callsite **keys**, never records, so the agent
  never buffers payloads.

`GlobalWindow::observe_and_strip` (`global_window.rs:143-159`): for each
representative, accumulate the exact global count `N_c += nhat_c`, push a
reservoir key `-ln(u) * weight` using the *previous* window's smoothed counts
as weights, and strip the `nhat` attribute in place. `take_nhat`
(`:252-268`) defaults a missing or malformed annotation to `1.0`, so an
unannotated record counts as one.

`GlobalWindow::close` (`global_window.rs:163-249`): pop the spare key for
`tau^G`, fold this window's exact counts into the EWMA-smoothed counts
(`smoothed = alpha * raw + (1 - alpha) * prev`), bound memory by dropping
decayed callsites below the rarest count seen, compute `g_unseen = 1 /
min_smoothed`, select the top `K'` heavy hitters by smoothed count with
`g_c = 1 / N_c`, and advance the monotonic version (never to the reserved `0`).

Engine integration, `GlobalReservoirProcessor` (`mod.rs:117-299`):

- `pass_through_logs` (`:152-195`) **forwards** the stripped payload downstream
  under the **original context**, so downstream delivery accounting flows back
  to the upstream sender. The processor does not acknowledge inputs itself.
- `close_window` (`:199-219`) publishes the new table via `self.shared.store`.
- Driven by `TimerTick` and `Shutdown`. Non-log signals pass through unchanged.

### 4.4 Feedback return path

The agent attaches the published table to OTLP **Logs** responses; the SDK
exporter decodes it and publishes it to the shared channel. The path is
Logs-only on both transports; Metrics and Traces are unaffected.

OTLP/HTTP:

- Receiver setting `sampling_feedback_channel`
  ([`otlp_http.rs:129-135`](../crates/otap/src/otlp_http.rs)). The handler
  resolves the shared table and, in `ok_response_with_feedback` (`:536-551`),
  gates on `SignalType::Logs`, loads the table, and appends the encoded
  `otel-sample-*` headers via `crate::sampling::encode_headers`. The server
  negotiates HTTP/2 when the client offers it, so the headers are
  HPACK-compressed on the wire.
- Exporter setting `sampling_feedback_channel`
  ([`otlp_http_exporter/config.rs:57-62`](../crates/core-nodes/src/exporters/otlp_http_exporter/config.rs)).
  In `query_result_to_service_response`
  ([`otlp_http_exporter/mod.rs:620-664`](../crates/core-nodes/src/exporters/otlp_http_exporter/mod.rs))
  it gates on Logs, decodes the response headers with `sampling::decode_headers`,
  and publishes with `table.store(Arc::new(decoded))`.

OTLP/gRPC (parallel path):

- Receiver setting `sampling_feedback_channel`
  ([`otap_grpc/server_settings.rs:184-190`](../crates/otap/src/otap_grpc/server_settings.rs)).
  `OtapBatchService::call`
  ([`otap_grpc/otlp/server_new.rs:378-492`](../crates/otap/src/otap_grpc/otlp/server_new.rs))
  attaches the encoded table to successful Logs responses as `otel-sample-*`
  gRPC metadata.
- Exporter setting `sampling_feedback_channel`
  ([`otlp_grpc_exporter/mod.rs:73-78`](../crates/core-nodes/src/exporters/otlp_grpc_exporter/mod.rs)).
  Only the Logs export future calls `publish_grpc_feedback` (`:786-804`,
  `:830-849`), which decodes the metadata and stores the table.

## 5. Callsite identity

A callsite is the stable identity of a log statement, computed by
`callsite_id(scope_name, rec)`
([`sampling.rs:48-67`](../crates/otap/src/sampling.rs)). It prefers
`event_name`, the OTel-model field closest to a statement identity, and falls
back to a hash of `scope_name` plus the string body when `event_name` is
absent. The result is a 64-bit FNV-1a hash, so the same callsite yields an
identical token at both sampling levels and fleet-wide, and no new field is
needed on the forward path. For the table to be meaningful across SDKs,
`event_name` must mean the same thing fleet-wide; the prototype assumes
homogeneous ITS logs.

## 6. Return wire protocol

The table is encoded as lowercase HTTP/2 headers so the HPACK static and
dynamic tables apply. The codec lives in
[`sampling.rs:155-249`](../crates/otap/src/sampling.rs):

| Header | Meaning |
| --- | --- |
| `otel-sample-ver` | Monotonic table version, a `u32` that is never `0`. Presence and parse gate the whole format. |
| `otel-sample-tau-g` | The global threshold `tau^G`. |
| `otel-sample-g-unseen` | The rarest-seen floor `g_unseen`, applied to every callsite not listed. |
| `otel-sample-heavy` | Space-separated `<16-hex-callsite>:<g_c>` entries, the top `K'` heavy hitters. |

`encode_headers` returns an empty vector for the absent table, so the agent
attaches nothing and the SDK reads a slack gate. `fmt_f64` emits `inf` / `-inf`
for infinities and a round-trippable decimal otherwise.

The format is **fail-safe**. `decode_headers` treats any missing or malformed
required field, or a `version == 0`, as `GlobalTable::absent()`, i.e. the SDK
degrades to local-only sampling rather than to incorrect suppression. A present
table with a partly malformed heavy list simply carries fewer heavy hitters;
those callsites fall back to `g_unseen`. Fixed-width hex callsite tokens and
recurring `g_c` values mean repeated header lines land in the HPACK dynamic
table at near-one-byte cost, and a small `K'` keeps the table well within
typical header-size budgets.

## 7. Concurrency, control, and staleness

- **Hot path (per record).** The SDK does a wait-free `ArcSwap::load_full` of
  the global table; the agent clones identity and weight into its bounded
  reservoir. No locks and no allocation beyond the bounded reservoir state.
- **Window close.** Both windows are driven by the engine's periodic
  `NodeControlMsg::TimerTick`, armed with `start_periodic_timer` on first
  message. The two windows are independent.
- **Publish.** A single `ArcSwap::store` of a freshly built immutable table per
  interval; readers never block the writer and vice versa.
- **Staleness.** The local sampler tracks table age by **version advance**, not
  by cross-host clocks: `last_update` is reset whenever the observed version
  changes. When `max_table_staleness` is configured and the table stops
  advancing for longer than that bound, `current_table` returns the absent
  table and increments `table_stale_degradations`. The local gate is the
  safety net, so staleness can only relax toward local-only sampling, never
  cause incorrect over-suppression.
- **Runtime reconfiguration.** Both processors honor `NodeControlMsg::Config`.
  `global_reservoir` reconfigures `k`, `k_prime`, and `count_smoothing`;
  `log_sampler` reconfigures `k`, `max_table_staleness`, and the feedback
  channel. A changed `interval` cancels and restarts the window timer. Invalid
  or malformed updates are ignored and counted under `config_errors`, so the
  node keeps running with its previous settings.

## 8. Configuration

`log_sampler` processor
([`log_sampler_processor/mod.rs:60-98`](../crates/contrib-nodes/src/processors/log_sampler_processor/mod.rs)):

| Field | Meaning | Required |
| --- | --- | --- |
| `k` | Representatives retained per window. Must be > 0. | yes |
| `interval` | Window length, e.g. `"5s"`. Must be > 0. | yes |
| `feedback_channel` | Shared-table channel to consult for the global gate. Unset means local-only. | no |
| `max_table_staleness` | Max table age before degrading to local-only. Unset trusts indefinitely. | no |

`global_reservoir` processor
([`global_reservoir_processor/mod.rs:61-115`](../crates/contrib-nodes/src/processors/global_reservoir_processor/mod.rs)):

| Field | Meaning | Required |
| --- | --- | --- |
| `k` | Global reservoir size, controls the `tau^G` estimate. Must be > 0. | yes |
| `k_prime` | Maximum heavy hitters published per window. Must be > 0. | yes |
| `interval` | Global window length. Must be > 0. | yes |
| `channel` | Shared-table channel to publish on. Must be non-empty. | yes |
| `count_smoothing` | EWMA factor in `(0, 1]`. `1.0` (default) disables smoothing. | no |

OTLP receivers and exporters add an optional `sampling_feedback_channel`. A
SDK-side pipeline names one channel on its exporter and its `log_sampler`; the
agent-side pipeline names another channel on its `global_reservoir` and its
receiver. The two channels are independent: the agent publishes on its channel
and the SDK publishes the decoded table on its own.

## 9. Metrics

`processor.log_sampler`
([metrics.rs](../crates/contrib-nodes/src/processors/log_sampler_processor/metrics.rs)):
`log_signals_consumed`, `representatives_emitted`, `windows_closed`,
`decode_errors`, `config_errors`, `globally_rejected`,
`table_stale_degradations`, `last_distinct_callsites`, `last_tau_l`,
`last_table_version`.

`processor.global_reservoir`
([metrics.rs](../crates/contrib-nodes/src/processors/global_reservoir_processor/metrics.rs)):
`representatives_consumed`, `representatives_forwarded`, `windows_closed`,
`tables_published`, `decode_errors`, `config_errors`, `last_heavy_hitters`,
`last_distinct_callsites`, `last_tau_g`, `last_g_unseen`.

A persistently high `globally_rejected` together with a small `last_tau_l`
indicates the local budget `k` is too small; a high `table_stale_degradations`
indicates the agent is not returning tables fast enough relative to
`max_table_staleness`.

## 10. Implementation status

Milestones M0-M4 are implemented and unit-tested; the end-to-end loop is
functionally complete over both the OTLP/HTTP and OTLP/gRPC return paths. See
the [plan's milestone section](two-level-log-sampling-feedback-plan.md#8-milestones)
for the original breakdown.

- **M0 - Local sampler, no feedback.** `WindowSampler` with a slack global
  gate.
- **M1 - Global sampler, no return.** `GlobalWindow` pass-through plus weighted
  bottom-k over SDK representatives.
- **M2 - Return channel.** `otel-sample-*` HTTP headers, the `sampling` codec,
  and the shared-channel registry.
- **M3 - Close the loop.** The binding gate wired into local admission via
  `feedback_channel`.
- **M4 - Hardening.** EWMA count smoothing, version-based staleness tolerance,
  runtime reconfiguration via `NodeControlMsg::Config`, and the OTLP/gRPC
  return variant.

## 11. Testing

- **Unit, local reservoir** (`bottomk.rs:310-454`): the reservoir stays within
  `k + 1`; under `k` it keeps everything with exact counts; the global gate
  suppresses a heavy callsite while preserving a globally unique one; HT counts
  are unbiased over many windows.
- **Unit, global window** (`global_window.rs` test module): count accumulation,
  EWMA smoothing, heavy-hitter selection, and version monotonicity.
- **Unit, wire codec** (`sampling.rs` test module): encode/decode round-trips
  and version/malformation tolerance.

## 12. Limitations and open questions

- **Callsite id stability across SDKs.** `event_name` must be consistent
  fleet-wide for the global table to be meaningful.
- **Response-header reach.** The table returns only to the immediate OTLP peer;
  an intermediary between SDK and agent would need to re-emit it hop-by-hop.
  The system assumes a direct SDK-to-agent link.
- **Client HTTP/2 negotiation.** The HPACK benefit only materializes when the
  exporter actually speaks HTTP/2. The functional loop is transport-agnostic,
  but the wire-compression benefit requires forcing h2 end-to-end.
- **Scope.** Logs only; a single agent; no persistence of the global table.
  Multi-agent hierarchies and metrics/traces signals are out of scope.

## 13. References

- Original plan: [`two-level-log-sampling-feedback-plan.md`](two-level-log-sampling-feedback-plan.md).
- Per-thread SDK CCKR sampler: [`../crates/telemetry/src/sampler/design.md`](../crates/telemetry/src/sampler/design.md).
- Internal telemetry pipeline: [`self_tracing_architecture.md`](self_tracing_architecture.md).
- Statistical design and evidence (separate repo, `cckr-experiment`):
  `PAPER.md` and `notes/two-stage-feedback.md`.
