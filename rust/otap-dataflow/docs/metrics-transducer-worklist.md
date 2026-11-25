# Persistent metrics transducer: implementation work list

Build a metrics-only, event-time recording engine. A rule selects metrics,
projects their identity and records their natural aggregates on aligned
boundaries. Current value, recent history and forwarding after a disconnect are
reads of the same committed results. Two bounded exemplar samples serve
different queries: estimate subset totals over dimensions removed by the rule,
or find trace links likely to survive consistent trace sampling. This is a work
list, not a description of available components or configuration.

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
| StatsD over UDP | Add paired receive and export nodes using [pdata codecs][codecs] for plain StatsD and DogStatsD. The [syslog exporter proposal][syslog-tx] provides the UDP send and encoded-forwarding pattern. |
| HTTP pull | Add a codec-aware HTTP endpoint for **pipeline data**. Prometheus `/metrics` is its first use; the existing [admin endpoint][admin-metrics] serves engine self-telemetry. |
| Regional link | Reuse [`exporter:otap`][otap-tx] and `receiver:otap`. A gRPC stream's batch ACK correlates an attempt; it is not a durable identity for a publication retried on another stream. |
| Storage | [Quiver][quiver] has WAL recovery, immutable Arrow segments, subscriber progress and a disk budget. The [durable buffer][buffer] already adapts OTAP bundles and separates ingestion from downstream delivery. A restart-safe retention bookmark independent of delivery progress is new work. |
| Aggregation | Reuse OTAP views, builders and applicable identity logic. The [temporal reaggregation processor][temporal] is an in-memory processor with different supported types and flush semantics; keep its existing contract. |

## Work packages

Each linked item ID opens its design: terms, requirements, architecture and
acceptance criteria. Checkboxes track implementation, not document status.

### A. Specify the rule and build a reference model

- [ ] **[A1]. Define rule identity, time and schema.** Specify a versioned rule
  with input selector, output name, retained dimensions, evaluation period,
  allowed lateness, aggregation kind where selectable, and reader temporality.
  Separate input identity (resource/entity, scope, metric descriptor, point
  attributes and logical writer) from projected output identity. Specify
  compatible types, units, histogram layouts and schema changes. Metadata,
  reader cursors and revision IDs are not grouping dimensions. A config change
  that alters meaning must start a new generation or have an explicit migration.
- [ ] **[A2]. Define evaluation and finality.** Use the two existing data-point
  timestamps. Align intervals `(start, end]` to a shared UTC origin; distinguish
  evaluation cadence from data coverage and cumulative start from window start.
  Specify a monotonic *sealed-through* boundary, who advances it (including
  idle or disconnected inputs), how long state may stay open, and how to report
  arrivals for sealed intervals. Persist seal decisions with the input cut used
  to make them. Replaying tomorrow must not change yesterday's decisions.
  Start with whole, nested intervals. A delta straddling a boundary cannot be
  split exactly by rounding timestamps; specify an observable policy.
- [ ] **[A3]. Build a deterministic reference evaluator and fixtures.** Feed
  timestamped input, seal events and reader operations without network or
  storage. Preserve accepted input and expected output in reusable fixtures.
  Prove the direct vs. staged result for compatible projections and intervals.
  Use this oracle for the engine, crash recovery and both demos.

### B. Give Quiver a retention contract (independent of metrics)

- [ ] **[B1]. Add a named, durable, inclusive keep-from segment position.**
  Register a consumer before exposing its input to cleanup. Let it read ahead
  and release bundle handles without advancing the bookmark. Advance bookmarks
  monotonically; define creation on an empty store, restart, and explicit
  retirement. One bookmark per independently recoverable rule group suffices
  initially. The oldest bookmark and ordinary subscribers jointly limit GC.
- [ ] **[B2]. Enforce the contract on every deletion path.** Restore bookmarks
  before startup expiry, WAL replay cleanup or live maintenance. Protect pinned
  segments against age expiry and `drop_oldest`; backpressure when no permitted
  reclaim can free space. Reject incompatible retention settings if protection
  cannot be honored. A missing required segment or corrupt bookmark is an
  observable recovery error, not permission to skip ahead. The bookmark pins
  finalized segments; WAL truncation still follows segment durability.
- [ ] **[B3]. Prove the storage guarantee.** Read through segment 90 while
  holding 42. Restart, fill the disk budget, exercise expiry, and verify 42
  remains replayable without holding an Arrow bundle in RAM. Advance one of two
  bookmarks and verify cleanup respects the other. Interrupt bookmark updates:
  reopening may retain extra input, never less than committed recovery needs.

### C. Implement the metric algebra (A1-A3; no storage dependency)

- [ ] **[C1]. Resolve each contributor before projecting away its identity.**
  Consume delta intervals directly. Difference adjacent cumulative observations
  only within the same source epoch. Distinguish a known zero start, missing
  start, and an unknown-start marker (`start == end`). For a cumulative series
  first observed at T with an unknown start and value 900, record `(T, T, 900)`
  as its baseline. Reuse T as the start of that unbroken sequence; 907 next
  yields a delta of 7. A first *delta* of 7 at the regional stage counts in
  full. Keep predecessors needed at window edges and for late observations.
- [ ] **[C2]. Implement sums and staleness.** Both monotonic and non-monotonic
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
- [ ] **[C3]. Implement gauges and ordinary histograms.** For a spatial mean,
  select the last eligible gauge value per live contributor at the boundary;
  define deterministic ties and staleness. Carry sum and population count
  between stages, not an unweighted mean of means. Merge compatible histogram
  counts, sum and buckets; specify exact rebinning/downscaling and behavior for
  incompatible layouts. Do not infer interval min/max from cumulative extrema.
  Document handling of missing fields and unsupported metric kinds. Preserve
  input exemplars for the sampling work in G.
- [ ] **[C4]. Prototype gauge histograms separately.** Represent the
  distribution of *current* values: merge disjoint populations across space;
  replace a contributor's prior snapshot across time. A move between buckets
  or a no-data marker may reduce a bucket's count. Cover negative-valued
  gauges. Use an opt-in intermediate schema; specify supported Prometheus
  exposition or an explicit unsupported outcome. Ordinary OTLP histograms are
  not a portable encoding of this distinction. See [model requirements][model].
- [ ] **[C5]. Bound state before and after projection.** Independently cap input
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

- [ ] **[D1]. Own two stores and one evaluator per instance.** Define the DFE
  node/reader wiring and stable state namespace; keep A, B and their two stores
  distinct. Reuse durable-buffer ingestion/adapters without requiring a third
  buffer. Use the [engine state directory][state-dir] contract where supported;
  enforce a single writer per namespace. Validate budgets, restart and graceful
  shutdown. Define the durability barrier for acknowledging accepted input:
  a successful buffered WAL append is not necessarily an fsync. Bound I/O
  and CPU per turn on the local runtime.
- [ ] **[D2]. Specify a versioned checkpoint commit.** Record rule/writer
  generation, evaluation and seal boundaries, processed-through input position,
  oldest input still needed, and committed output position. Store recorded OTAP
  points plus the minimum private state needed to resume (or references to
  retained input): contributor baselines, active windows, accepted publication
  IDs and output epoch starts/closures. Separate private recovery records from
  public OTAP results. The service total alone cannot reconstruct per-host
  baselines. Bound the total retained by both stores.
- [ ] **[D3]. Implement commit and recovery.** Reserve space to complete a
  checkpoint even when input is pinned at its cap. Write results and referenced
  state durably, atomically publish the commit, then advance the input bookmark.
  Readers see only published commits. On restart load the last valid commit,
  reconstruct unfinished windows from retained segments plus the recovered WAL
  tail, and resume without publishing a delta twice. Inject a crash before and
  after each durability boundary. A reader must see either the old or the new
  complete evaluation, never a partial set of OTAP payloads.
- [ ] **[D4]. Provide independent, MetricReader-like output readers.** A pull
  reader obtains the latest snapshot or a recent range as of commit P; a push
  reader follows commits after P with its own durable ACK cursor. At a fixed P,
  both see the same history. Collection selects representation; it does not
  shift event-time boundaries, reset another reader, or consume history on a
  scrape. Define retention for recent history and disconnected readers,
  slow-reader backpressure, and an explicit gap if a requested P has expired.

### E. Integrate the protocols

- [ ] **[E1]. Add StatsD codecs and paired UDP nodes (can start after A1).**
  - **Codecs.** Register distinct metrics byte encodings for original StatsD
    (`statsd`) and [DogStatsD v1.1][dogstatsd-v11] (`dogstatsd-v1.1`).
    Nodes select the wire encoding; pipeline [codec bindings][codecs] supply
    its decoder and encoder. Put parsing, OTAP mappings and serialization in
    the codecs. Define which names, types, units and attributes each dialect
    can represent, and report unrepresentable output. For example, `|g` alone
    cannot distinguish an additive level from a temperature.
  - **`receiver:statsd`.** Receive UDP datagrams only; Unix sockets are out of
    scope. Emit each datagram as a **metrics bytes payload** tagged with the
    listener's configured encoding. Capture ingress time and sender identity
    once and retain them with the input so deferred decoding and replay use
    the original observation. The receiver handles socket limits and reports
    observable drops; a consumer requests decoding when it needs OTAP records.
  - **`exporter:statsd`.** Follow the [syslog exporter's UDP path][syslog-tx]
    with the metrics signal and the selected StatsD encoding. Forward matching
    encoded datagrams directly; otherwise request the wire bytes from the
    pipeline codec service. Send bounded datagrams, preserving whole metric
    lines; send only wire bytes, not local arrival metadata. A successful UDP
    send confirms local transmission only. Round-trip each dialect over UDP:
    check byte preservation on a matching relay and metric equivalence for
    representable OTAP input through encode and decode. Use `requests:3|c` for
    plain StatsD; exercise tags and packed values only in the DogStatsD codec.

  Start with counters for the first demo. Before claiming DogStatsD v1.1
  metrics compatibility, cover counts, gauges, timers, histograms,
  distributions and single-value sets. Events, service checks and later
  extensions are separate. Bound lines, packed values, tags, set cardinality
  and batching; report parse errors. For the DogStatsD codec, use the
  [Collector's proposed packed-value support][collector-packed] as a
  conformance reference. For `c`, `g`, `ms`, `h` and `d`, a colon-separated
  value list must act like separate single-value lines in wire order, sharing
  the name, type, tags and applicable sample rate. Do not split `s` values.
  Check signed relative gauge updates in order and apply the sample rate to
  each eligible value, not once per packet. Match the proposed parser's edge
  cases: skip empty entries between values; reject an all-empty list or a
  malformed/non-finite number without applying any part of that line. Keep
  the single-value fast path, reuse the datagram's sender and resolve tags
  once per packed line.
- [ ] **[E2]. Serve committed snapshots over codec-aware HTTP pull (A1, D4).**
  The endpoint receives HTTP GETs and exports pipeline data in the response;
  `receiver:prometheus` remains the scraping client. Configure each route with
  its signal and offered byte encodings. Negotiate `Accept`, ask the pipeline
  codec service for the selected representation and return its `Content-Type`.
  Support any registered encoder for the route's signal. Bound concurrency and
  response size; each request reads one committed snapshot without consuming
  it or advancing a reader cursor.

  Configure `/metrics` to offer Prometheus text and OpenMetrics. Their codecs
  own naming, units, label collisions, staleness and format-specific
  timestamps. Expose monotonic sums as counters and preserve non-monotonic
  sums' additive semantics internally. Scrape with `receiver:prometheus` and
  an independent Prometheus parser or server. Configure a second route for
  `otlp-bytes` metrics and decode its response at the same commit to prove
  the HTTP server is independent of the exposition format. An `Accept` header
  matching none of the offered types receives HTTP 406.
- [ ] **[E3]. Use per-series first observation for unknown starts (A1, C1).**
  Preserve genuine `_created` times. For absent starts, follow the stateless
  [conversion rules][prom-spec]: leave the field unset. The stateful transducer
  establishes the [unknown-start baseline][resets] from the first observed
  sample's event timestamp for each series. Replace the receiver's current
  scraper-wide fallback for missing start columns and document migration.
  Scrape a long-lived target: series first seen at 10:03 and 10:47 must get
  distinct baselines even in the same receiver process. Cover mixed known and
  missing starts, scraper restart with a saved baseline, and a real reset.
- [ ] **[E4]. Define the opt-in OTAP publication contract (A2, C1-C2, D2-D4).**
  Start with sealed publications. Carry a stable writer/generation and
  publication identity across reconnect, retry and rebatching; keep it outside
  metric series identity. Couple regional durable acceptance and deduplication
  before ACK. Bound the retry/dedup history and specify stale replay behavior.
  Do not accept both representations of the same contribution as independent.
- [ ] **[E5]. Add progressive refinement on capable links (E4).** Prototype
  local point mask `0x0000_0002` for intentional repeat publications, including
  the first. It is not an assigned OTel bit, a revision ID, or a finality flag.
  Distinct delta publications add exactly once; a newer cumulative restatement
  replaces the older one for the same contributor and cell. Specify ordering,
  conflicting IDs, restarts, and propagation to parent windows. A parent seals
  only after durable coverage of the required children's sealed publications,
  or when the configured cutoff records an incomplete result. Emit sealed
  ordinary points to unaware peers. Keep unrepresentable corrections (e.g.,
  negative histogram bucket deltas) explicit.
  The [query-defined rules RFC][rules-rfc] specifies revisions, phases,
  correction and restatement encodings, receiver rules and completeness.
- [ ] **[E6]. Accept rules written as queries (A1-A3, C1-C3; RFC).** Lower
  PromQL recording rules and OPL/KQL aggregation pipelines to one rule plan
  whose operators declare partial, merge and finalize, per the
  [query-defined rules RFC][rules-rfc]. Print each plan and, for the
  projection-and-aggregate shape, its equivalent A1 rule.
  - **[E6a]. Rule plan and decomposition classes.** Classify additive,
    idempotent, algebraic, order-dependent, composite and holistic nodes;
    reject holistic plans in v0. Use DataFusion `GroupsAccumulator` partial
    state as the checkpointed Arrow representation (D2).
  - **[E6b]. PromQL front end.** Parse with `promql-parser`; evaluate the v0
    subset in event time with exact `increase`/`rate`; map names, labels,
    types and units to OTel; accept Prometheus rule-group files.
  - **[E6c]. OPL and KQL front ends.** Lower KQL `summarize ... by bin()` and a
    new OPL `aggregate every <period> by <keys>` operator.
  - **[E6d]. Query-time executor.** Evaluate the same plan over retained input
    and committed results with DataFusion; compare it to A3's independent
    expected results and verify replay. Track prometheus-community/promql-rs
    as a differential oracle for shared semantics once published.

### F. Ship the examples and establish equivalence

- [ ] **[F1]. Local example (B, C1-C2, C5, D, E1-E2).** Ship a one-core
  config, UDP input fixture, scrape instructions, expected samples and restart
  procedure. Select the DogStatsD v1.1 codec on ingress and Prometheus text on
  the HTTP pull route. Send `requests:3:4|c|#service:api` in one interval:
  two scrapes at the same commit must show 7, and restarting must still show
  7. In a fresh run, send two single-value lines with the same tag and verify
  the same series and total. Add an UpDownCounter and gauge case with explicit
  mappings. Use this same fixture as the first end-to-end query milestone.
- [ ] **[F2]. Regional example (F1, E4).** Ship separate edge/regional configs,
  state directories and endpoints. Project away host at the edge, retain edge
  writer identity until regional reconciliation, and aggregate by service.
  Disconnect the OTAP link while local scrapes continue; reconnect and compare
  the regional five-minute cumulative result with the direct result at the
  same sealed boundary. Lose the ACK *after* regional durable acceptance.
  Retrying must not add another 7. Repeat with two independent edge writers.
- [ ] **[F3]. Conformance and operations (C3-C5, D, E3-E5).** Reuse the same
  timestamped fixtures for direct, replayed and two-stage evaluation. Include
  out-of-order refinement before and after sealing; unknown start and true
  reset; signed decrease; unequal gauge populations; histogram bucket changes;
  no-data followed by late history; read-history-then-follow with no gap.
  Use controlled clocks, durable barriers and explicit ACKs, not sleeps.
  Measure resident and on-disk bytes, replay time, commit latency, bookmark and
  reader lag, series overflows, late rejections and duplicates suppressed.
  Exercise disk-full and long disconnects. Document supported limits, operator
  recovery actions, node inventory/feature gates, and user-facing changelogs.

### G. Estimate subset sums and select trace-linked exemplars

Make these modes independently selectable per rule. **Series attribution**
serves someone asking which hosts contributed to a total after `host` was
removed. **Trace drill-down** serves someone investigating a metric who wants
a trace they can open. A rule can enable either, both with separate budgets,
or neither; the exact recorded metric is the same in all four cases.

- [ ] **[G1]. Sketch original-cardinality counter series (A3, C1-C2).** For each
  rule window, aggregate at full input identity *before* dropping dimensions
  such as `host`. Each Counter or UpDownCounter series supplies one item with
  its own aggregate value, regardless of how often it reported or whether it
  supplied trace exemplars. Use stable series randomness and a value-dependent
  weight to assign ranks. Keep the `k` smallest ranks and the first excluded
  rank (or equivalent estimator state). Retain the original value and allowed
  dimensions on the series-derived exemplar; do not invent a trace link.
  Follow [Cohen and Kaplan's bottom-k sketches][bottom-k] for subset estimates.
  For signed UpDownCounters, distinguish the signed quantity being estimated
  from the nonnegative selection weight (e.g., its magnitude). Specify the
  zero-weight case. A gauge reading is not an additive contribution: define a
  separate gauge population policy rather than weighting a gauge by its value.
- [ ] **[G2]. Select trace links by adaptive threshold (A3, C3).** From the
  supplied trace-linked exemplars, keep the largest 56-bit randomness values
  `R`, independent of the metric values. Use the least-significant 56 bits of
  a suitably random trace ID, or the explicit OTel randomness `rv` when
  available. At a fixed rejection threshold `T`, OTel keeps `R >= T` with
  probability `(2^56 - T) / 2^56`. Adapt the cutoff to the reservoir budget;
  specify the order statistic, ties, repeated trace IDs and the keep-all case.
  This favors links that survive stricter consistent trace samplers, not a
  guarantee that a backend has the trace. Preserve measurement time, filtered
  attributes and trace/span IDs. Use [Ting's adaptive-threshold paper][ting]
  for the sampling design and [OTel consistent sampling][otel-sampling] for
  the trace convention. This cutoff is not the series sketch's inclusion
  probability. With no trace-linked inputs, G1 still works and G2 is empty.
- [ ] **[G3]. Persist both summaries independently (D2-D4, E4, G1-G2).** Specify
  the OTAP representation for each candidate's provenance, rank and applicable
  estimator state outside metric identity. Checkpoint both at the same input
  cut as the exact aggregate, with separate count and byte budgets. Replay
  must not redraw and retries must not add another candidate. Merge series
  sketches across disjoint populations with unchanged weights; summing weights
  across time can change the winners. Merge trace reservoirs by trace
  randomness. If a late update changes a series weight or removes a winner,
  rebuild from retained input or sufficient state instead of keeping a stale
  rank. Bound retained attributes and total bytes across all open windows.
- [ ] **[G4]. Verify each result against its own population (F1-F2, G3).** Query
  a subset of hosts from a service counter using adjusted weights from G1.
  Use reset-aware interval contributions (signed for UpDownCounters) and add
  estimated deltas for a cumulative query. Never difference unrelated sampled
  series. Compare the estimate against exact subset sums on small enumerable
  rank fixtures; cover negative changes, skew and keep-all equality. Where
  the exact total selection weight is available, evaluate subset conditioning
  against rank conditioning. For G2, use known trace IDs: raising `T` must
  keep a nested subset, and increasing a metric value must not promote a link.
  Check direct vs. two-stage results, replay and hard caps for both paths.

**Delivery order:** land A and the storage-only B independently of network
adapters. Use the sum-only evaluator and sealed publications to deliver F1,
then F2. Develop the two exemplar samplers alongside the evaluator,
then exercise them on both pipelines. Gate progressive refinement and gauge
histograms separately; keep their conformance fixtures in the plan from the
outset. Develop the rule plan (E6a) with the sum-only evaluator so the A1 rule
is its first canonical shape; add the PromQL and OPL front ends once F1
records through the plan. Ad hoc PromQL query serving, Parquet export,
multi-core routing and a third observation timestamp are outside this first
operational transducer.

[A1]: metrics-transducer/a1.md
[A2]: metrics-transducer/a2.md
[A3]: metrics-transducer/a3.md
[B1]: metrics-transducer/b1.md
[B2]: metrics-transducer/b2.md
[B3]: metrics-transducer/b3.md
[C1]: metrics-transducer/c1.md
[C2]: metrics-transducer/c2.md
[C3]: metrics-transducer/c3.md
[C4]: metrics-transducer/c4.md
[C5]: metrics-transducer/c5.md
[D1]: metrics-transducer/d1.md
[D2]: metrics-transducer/d2.md
[D3]: metrics-transducer/d3.md
[D4]: metrics-transducer/d4.md
[E1]: metrics-transducer/e1.md
[E2]: metrics-transducer/e2.md
[E3]: metrics-transducer/e3.md
[E4]: metrics-transducer/e4.md
[E5]: metrics-transducer/e5.md
[E6]: metrics-transducer/e6.md
[E6a]: metrics-transducer/e6a.md
[E6b]: metrics-transducer/e6b.md
[E6c]: metrics-transducer/e6c.md
[E6d]: metrics-transducer/e6d.md
[F1]: metrics-transducer/f1.md
[F2]: metrics-transducer/f2.md
[F3]: metrics-transducer/f3.md
[G1]: metrics-transducer/g1.md
[G2]: metrics-transducer/g2.md
[G3]: metrics-transducer/g3.md
[G4]: metrics-transducer/g4.md
[prom-rx]: ../crates/core-nodes/src/receivers/prometheus_receiver/mod.rs
[prom-config]: ../configs/prometheus-otlp.yaml
[codecs]: codecs.md
[syslog-tx]: https://github.com/open-telemetry/otel-arrow/pull/4655
[admin-metrics]: admin/README.md
[dogstatsd-v11]: https://docs.datadoghq.com/extend/dogstatsd/datagram_shell/#dogstatsd-protocol-v11
[collector-packed]: https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/53348
[otap-tx]: ../crates/core-nodes/src/exporters/otap_exporter/README.md
[quiver]: ../crates/quiver/README.md
[buffer]: ../crates/core-nodes/src/processors/durable_buffer_processor/
[temporal]: ../crates/core-nodes/src/processors/temporal_reaggregation_processor/README.md
[state-dir]: state-directory.md
[rules-rfc]: ../rfcs/0000-query-defined-metric-rules.md
[model]: model_requirements.md#gauge-histogram
[prom-spec]: https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/#timestamps
[resets]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#cumulative-streams-handling-unknown-start-time
[otel-sampling]: https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/
[bottom-k]: https://www.vldb.org/pvldb/vol1/1453884.pdf
[ting]: https://arxiv.org/pdf/1708.04970v1
