# Persistent metrics transducer: implementation work list

Build a metrics-only, event-time recording engine. A rule selects metrics,
projects their identity and records their natural aggregates on aligned
boundaries. Current value, recent history and forwarding after a disconnect are
reads of the same committed results. A second output is a bounded sample of
representative inputs: users can query dimensions removed by the rule and
estimate subset totals without retaining a time series for every contributor.
This is a work list, not a description of available components or configuration.

## What we should be able to run

```text
1. StatsD -> A -> cumulative pull reader -> Prometheus /metrics

2. StatsD -> A -> delta push reader -> exporter:otap
                                   -> receiver:otap -> B (regional)
                                                    -> cumulative pull reader
                                                    -> Prometheus /metrics

Each xducer instance (A or B):
    quiver_ingest -> in-memory event-time rules -> metrics_checkpoints
       raw input                                  committed OTAP results
       + replay bookmark                          + rule recovery state
```

In the demonstration, attach both readers to A. This gives B a reference for
comparison without sending two independent copies of an untimestamped StatsD
feed. Start with one core per instance, one-minute edge evaluations and aligned
five-minute regional evaluations. Compare the same accepted input at the same
sealed boundary, not the wall-clock times of two HTTP scrapes.

## Starting point in this checkout

| Area | Reuse or gap |
| --- | --- |
| Prometheus ingress | [`receiver:prometheus`][prom-rx] already scrapes text/OpenMetrics into OTAP; [example configuration][prom-config]. Its source pipeline requires one core. Use it as an interoperability client, not as the exporter. |
| StatsD and Prometheus egress | Add a StatsD receiver and a Prometheus scrape exporter for **pipeline metric data**. The engine's admin/self-telemetry endpoint serves a different dataset. |
| Regional link | Reuse [`exporter:otap`][otap-tx] and `receiver:otap`. A gRPC stream's batch ACK correlates an attempt; it is not a durable identity for a publication retried on another stream. |
| Storage | [Quiver][quiver] has WAL recovery, immutable Arrow segments, subscriber progress and a disk budget. The [durable buffer][buffer] already adapts OTAP bundles and separates ingestion from downstream delivery. A restart-safe retention bookmark independent of delivery progress is new work. |
| Aggregation | Reuse OTAP views, builders and applicable identity logic. The [temporal reaggregation processor][temporal] is an in-memory processor with different supported types and flush semantics; keep its existing contract. |

## Work packages

### A. Specify the rule and build a reference model

- [ ] **A1. Define rule identity, time and schema.** Specify a versioned rule
  with input selector, output name, retained dimensions, evaluation period,
  allowed lateness, aggregation kind where selectable, and reader temporality.
  Separate input identity (resource/entity, scope, metric descriptor, point
  attributes and logical writer) from projected output identity. Specify
  compatible types, units, histogram layouts and schema changes. Metadata,
  reader cursors and revision IDs are not grouping dimensions. A config change
  that alters meaning must start a new generation or have an explicit migration.
- [ ] **A2. Define evaluation and finality.** Use the two existing data-point
  timestamps. Align intervals `(start, end]` to a shared UTC origin; distinguish
  evaluation cadence from data coverage and cumulative start from window start.
  Specify a monotonic *sealed-through* boundary, who advances it (including
  idle or disconnected inputs), how long state may stay open, and how to report
  arrivals for sealed intervals. Persist seal decisions with the input cut used
  to make them. Replaying tomorrow must not change yesterday's decisions.
  Start with whole, nested intervals. A delta straddling a boundary cannot be
  split exactly by rounding timestamps; specify an observable policy.
- [ ] **A3. Build a deterministic reference evaluator and fixtures.** Feed
  timestamped input, seal events and reader operations without network or
  storage. Preserve accepted input and expected output in reusable fixtures.
  Prove the direct vs. staged result for compatible projections and intervals.
  Use this oracle for the engine, crash recovery and both demos.

### B. Give Quiver a retention contract (independent of metrics)

- [ ] **B1. Add a named, durable, inclusive keep-from segment position.**
  Register a consumer before exposing its input to cleanup. Let it read ahead
  and release bundle handles without advancing the bookmark. Advance bookmarks
  monotonically; define creation on an empty store, restart, and explicit
  retirement. One bookmark per independently recoverable rule group suffices
  initially. The oldest bookmark and ordinary subscribers jointly limit GC.
- [ ] **B2. Enforce the contract on every deletion path.** Restore bookmarks
  before startup expiry, WAL replay cleanup or live maintenance. Protect pinned
  segments against age expiry and `drop_oldest`; backpressure when no permitted
  reclaim can free space. Reject incompatible retention settings if protection
  cannot be honored. A missing required segment or corrupt bookmark is an
  observable recovery error, not permission to skip ahead. The bookmark pins
  finalized segments; WAL truncation still follows segment durability.
- [ ] **B3. Prove the storage guarantee.** Read through segment 90 while holding
  42. Restart, fill the disk budget, exercise expiry, and verify 42 is still
  replayable without holding an Arrow bundle in RAM. Advance one of two
  bookmarks and verify cleanup respects the other. Interrupt bookmark updates:
  reopening may retain extra input, never less than committed recovery needs.

### C. Implement the metric algebra (A1-A3; no storage dependency)

- [ ] **C1. Resolve each contributor before projecting away its identity.**
  Consume delta intervals directly. Difference adjacent cumulative observations
  only within the same source epoch. Distinguish a known zero start, missing
  start, and an unknown-start marker (`start == end`). For a cumulative series
  first observed at T with an unknown start and value 900, record `(T, T, 900)`
  as its baseline. Reuse T as the start of that unbroken sequence; 907 next
  yields a delta of 7. A first *delta* of 7 at the regional stage counts in
  full. Keep predecessors needed at window edges and for late observations.
- [ ] **C2. Implement sums and staleness.** Both monotonic and non-monotonic
  sums are additive across sources. A non-monotonic cumulative UpDownCounter
  changing from 7 to 4 contributes -3 in the same epoch. Preserve the
  distinction between a current total and a change since a baseline. The
  aggregator owns its output epochs: it can finish one and assign a fresh
  start timestamp to the next without implying that any input reset. Rebase
  continuing cumulative contributors at that boundary rather than adding
  their lifetime totals again.
  Use `NO_RECORDED_VALUE` when the named series becomes absent. To publish a
  final numeric value, send it as a valued point; if a reader needs to know
  it is the last value, evaluate a separate opt-in final-value flag. A no-data
  point carries no usable numeric value. Neither signal erases prior history
  or closes every output to which the series contributed.
- [ ] **C3. Implement gauges and ordinary histograms.** For a spatial mean,
  select the last eligible gauge value per live contributor at the boundary;
  define deterministic ties and staleness. Carry sum and population count
  between stages, not an unweighted mean of means. Merge compatible histogram
  counts, sum and buckets; specify exact rebinning/downscaling and behavior for
  incompatible layouts. Do not infer interval min/max from cumulative extrema.
  Document handling of missing fields and unsupported metric kinds. Preserve
  input exemplars for the sampling work in G.
- [ ] **C4. Prototype gauge histograms separately.** Represent the distribution
  of *current* values: merge disjoint contributor populations across space;
  replace a contributor's prior snapshot across time. A move between buckets
  or a no-data marker may reduce a bucket's count. Cover negative-valued
  gauges. Use an opt-in intermediate schema; specify supported Prometheus
  exposition or an explicit unsupported outcome. Ordinary OTLP histograms are
  not a portable encoding of this distinction. See [model requirements][model].
- [ ] **C5. Bound state before and after projection.** Independently cap input
  series, output groups, open windows, per-series history, histogram buckets
  and staged bytes. Age state by event-time/sealing progress; enforce hard
  limits even if that progress stalls. Cold baselines can be checkpointed or
  replayed. On retirement, unlink the series from live state once its closure
  is recoverable. Keep only the history needed for accepted late updates and
  retries; expire that too at the relevant cutoff. A late point for a retired
  epoch may revise an open historical window without reviving the live series.
  Demonstrate that repeated waves of distinct, retired series leave no
  permanent per-series entries in RAM and do not change sealed totals. Define
  backpressure, safe overflow aggregation, or explicit loss for each limit.
  Never pool unrelated raw cumulative baselines.

### D. Assemble one recoverable transducer (A, B and C1-C2 first)

- [ ] **D1. Own two stores and one evaluator per instance.** Define the DFE
  node/reader wiring and stable state namespace; keep A, B and their two stores
  distinct. Reuse durable-buffer ingestion/adapters without requiring a third
  buffer. Use the [engine state directory][state-dir] contract where supported;
  enforce a single writer per namespace. Validate budgets, restart and graceful
  shutdown. Define the durability barrier for acknowledging accepted input:
  a successful buffered WAL append is not necessarily an fsync. Bound I/O
  and CPU per turn on the local runtime.
- [ ] **D2. Specify a versioned checkpoint commit.** Record rule/writer
  generation, evaluation and seal boundaries, processed-through input position,
  oldest input still needed, and committed output position. Store recorded OTAP
  points plus the minimum private state needed to resume (or references to
  retained input): contributor baselines, active windows, accepted publication
  IDs and output epoch starts/closures. Separate private recovery records from
  public OTAP results. The service total alone cannot reconstruct per-host
  baselines. Bound the total retained by both stores.
- [ ] **D3. Implement commit and recovery.** Write results and referenced state
  durably, atomically publish their commit, then advance the input bookmark.
  Readers see only published commits. On restart load the last valid commit,
  reconstruct unfinished windows from retained segments plus the recovered WAL
  tail, and resume without publishing a delta twice. Inject a crash before and
  after each durability boundary. A reader must see either the old or the new
  complete evaluation, never a partial set of OTAP payloads.
- [ ] **D4. Provide independent, MetricReader-like output readers.** A pull
  reader obtains the latest snapshot or a recent range as of commit P; a push
  reader follows commits after P with its own durable ACK cursor. At a fixed P,
  both see the same history. Collection selects representation; it does not
  shift event-time boundaries, reset another reader, or consume history on a
  scrape. Define retention for recent history and disconnected readers,
  slow-reader backpressure, and an explicit gap if a requested P has expired.

### E. Integrate the protocols

- [ ] **E1. Add `receiver:statsd` (can start after A1).** Specify a bounded
  UDP listener and a documented dialect. Start with counters and absolute
  gauges; add timers/distributions, tags, sample rates and relative gauges
  with fixtures before advertising support. Specify name/type/unit and
  source-identity mapping; an additive level and a temperature gauge cannot
  be distinguished from `|g` alone. Assign event time once at ingress for
  untimestamped observations, define their OTAP interval representation, and
  replay the assigned times. Bound datagrams, lines, tags and batching. Report
  parse errors and observable drops; UDP has no end-to-end acceptance ACK.
- [ ] **E2. Add `exporter:prometheus` (A1, D4 reader contract).** Serve a
  bounded HTTP `/metrics` scrape from one committed snapshot. Negotiate the
  supported text/OpenMetrics formats; define naming, units, label collision
  handling, staleness, and format-specific timestamp behavior. Expose monotonic
  sums as counters; represent non-monotonic totals according to the Prometheus
  mapping while preserving their additive semantics internally. Make concurrent
  scrapes read-only. Validate with the existing Prometheus receiver and an
  independent Prometheus parser or server, not just a golden text comparison.
- [ ] **E3. Use per-series first observation for unknown starts (A1, C1).**
  Preserve genuine `_created` times. For absent starts, follow the stateless
  [conversion rules][prom-spec]: leave the field unset. The stateful transducer
  establishes the [unknown-start baseline][resets] from the first observed
  sample's event timestamp for each series. Replace the receiver's current
  scraper-wide fallback for missing start columns and document migration.
  Scrape a long-lived target: series first seen at 10:03 and 10:47 must get
  distinct baselines even in the same receiver process. Cover mixed known and
  missing starts, scraper restart with a saved baseline, and a real reset.
- [ ] **E4. Define the opt-in OTAP publication contract (A2, C1-C2, D2-D4).**
  Start with sealed publications. Carry a stable writer/generation and
  publication identity across reconnect, retry and rebatching; keep it outside
  metric series identity. Couple regional durable acceptance and deduplication
  before ACK. Bound the retry/dedup history and specify stale replay behavior.
  Do not accept both representations of the same contribution as independent.
- [ ] **E5. Add progressive refinement on capable links (E4).** Prototype
  local point mask `0x0000_0002` for intentional repeat publications, including
  the first. It is not an assigned OTel bit, a revision ID, or a finality flag.
  Distinct delta publications add exactly once; a newer cumulative restatement
  replaces the older one for the same contributor and cell. Specify ordering,
  conflicting IDs, restarts, and propagation to parent windows. A parent seals
  only when its required children are final or the configured cutoff records
  an incomplete result. Emit sealed ordinary points to unaware peers. Keep
  unrepresentable corrections (e.g., negative histogram bucket deltas) explicit.

### F. Ship the examples and establish equivalence

- [ ] **F1. Local example (B, C1-C2, C5, D, E1-E2).** Ship a runnable one-core
  config, StatsD input fixture, scrape instructions, expected samples and
  restart procedure. Send `requests:3|c` and `requests:4|c` in one aligned
  interval: two scrapes at the same commit must show 7, and restarting must
  still show 7. Add an UpDownCounter and gauge case with explicit mappings.
- [ ] **F2. Regional example (F1, E4).** Ship separate edge/regional configs,
  state directories and endpoints. Project away host at the edge, retain edge
  writer identity until regional reconciliation, and aggregate by service.
  Disconnect the OTAP link while local scrapes continue; reconnect and compare
  the regional five-minute cumulative result with the direct result at the
  same sealed boundary. Lose the ACK *after* regional durable acceptance.
  Retrying must not add another 7. Repeat with two independent edge writers.
- [ ] **F3. Conformance and operations (C3-C5, D, E3-E5).** Reuse the same
  timestamped fixtures for direct, replayed and two-stage evaluation. Include
  out-of-order refinement before and after sealing; unknown start and true
  reset; signed decrease; unequal gauge populations; histogram bucket changes;
  no-data followed by late history; read-history-then-follow with no gap.
  Use controlled clocks, durable barriers and explicit ACKs, not sleeps.
  Measure resident and on-disk bytes, replay time, commit latency, bookmark and
  reader lag, series overflows, late rejections and duplicates suppressed.
  Exercise disk-full and long disconnects. Document supported limits, operator
  recovery actions, node inventory/feature gates, and user-facing changelogs.

### G. Record weighted exemplars and query subset sums

- [ ] **G1. Define the population before sampling (A1, C1-C3).** For each
  contributing input point with exemplars, use those exemplars: preserve their
  measurement, event time, filtered attributes and any real trace/span link.
  Where there are none, synthesize a candidate from the input series'
  contribution *before* projecting away its dimensions. Mark it as derived
  from an aggregate, not a traced measurement. Do not invent a trace ID or an
  individual measurement from a histogram bucket. Define which dimensions
  a rule may retain for filtering and impose a byte budget as well as a count.
  Distinguish a measurement population from a series population: to estimate
  active series cardinality, give each eligible series one contribution per
  boundary, regardless of report frequency or number of attached exemplars.
  Do not pool the two kinds of candidates into one estimate unless their
  weights refer to the same target population without double counting.
- [ ] **G2. Build a weighted bottom-k reservoir (A3, G1).** Give each eligible
  candidate stable randomness and a positive selection weight. Keep the `k`
  smallest ranks and the first excluded rank (or equivalent cutoff); if all
  fit, local retention probability is one. Use the randomness and threshold
  convention of [OTel consistent probability sampling][otel-sampling].
  Specify how the adaptive cutoff gives a per-candidate inclusion probability
  for unequal weights, including finite-precision ties. Use
  [priority sampling][priority-sampling] as a reference design. Distinguish
  selection weight from the value to be estimated: an UpDownCounter change
  may be negative.
  Carry known upstream selection probabilities through resampling, accounting
  for shared randomness. A trace link alone does not establish an exemplar's
  probability of selection from the original measurements. When it is unknown,
  keep the exemplar useful for inspection without claiming that estimate.
- [ ] **G3. Checkpoint and transport sampling state (D2-D4, E4, G2).** Specify
  an opt-in OTAP representation for provenance, sampling unit, randomness and
  estimator weights; these are not metric identity attributes. Checkpoint the
  reservoir and cutoff at the same input cut as the exact aggregate. Replay
  must not redraw, and delivery retries must not add another candidate. Merge
  compatible edge reservoirs at the regional stage using their original ranks
  and cutoffs. If a late revision removes a selected candidate, recover the
  next eligible candidate from retained inputs or sufficient checkpoint state.
  Bound total memory across reservoirs, including those for still-open windows.
- [ ] **G4. Demonstrate subset estimates (F1-F2, G3).** Drop `host` from a
  service counter; retain it on sampled contributions. Query a subset of hosts
  across the original input cardinality. For a known sampling design, sum
  `contribution / inclusion_probability` over matching retained candidates.
  Use reset-aware deltas for counters (signed for UpDownCounters); accumulate
  those estimated interval changes for a cumulative query. Do not difference
  unrelated sampled cumulative readings or count a restatement twice. For a
  gauge population estimate totals and counts at the same boundary. Report the
  result as an estimate, separately from the exact recorded service aggregate;
  no sampled matches is not proof the subset was empty. Exercise both sources
  of exemplars, mixed presence, replay, and edge-to-regional merging. Use a
  small enumerable sampling design to check expected estimates against exact
  subset sums; verify no-sampling equality and the size cap under high churn.

**Delivery order:** land A and the storage-only B independently of network
adapters. Use the sum-only evaluator and sealed publications to deliver F1,
then F2. Develop the exemplar population and sampler alongside the evaluator,
then exercise them on both pipelines. Gate progressive refinement and gauge
histograms separately; keep their conformance fixtures in the plan from the
outset. PromQL, Parquet export, multi-core routing and a third observation
timestamp are outside this first operational transducer.

[prom-rx]: ../crates/core-nodes/src/receivers/prometheus_receiver/mod.rs
[prom-config]: ../configs/prometheus-otlp.yaml
[otap-tx]: ../crates/core-nodes/src/exporters/otap_exporter/README.md
[quiver]: ../crates/quiver/README.md
[buffer]: ../crates/core-nodes/src/processors/durable_buffer_processor/
[temporal]: ../crates/core-nodes/src/processors/temporal_reaggregation_processor/README.md
[state-dir]: state-directory.md
[model]: model_requirements.md#gauge-histogram
[prom-spec]: https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/#timestamps
[resets]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
[otel-sampling]: https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/
[priority-sampling]: https://arxiv.org/abs/cs/0509026
