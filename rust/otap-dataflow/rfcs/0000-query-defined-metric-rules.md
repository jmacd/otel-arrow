---
Proposal Name: query-defined-metric-rules
Start Date: 2026-11-24
RFC PR: open-telemetry/otel-arrow#0000
Tracking Issue: open-telemetry/otel-arrow#0000
---

# RFC NNNN: Query-Defined Metric Rules with Progressive Refinement

## Summary

The [persistent metrics transducer][worklist] records the natural aggregates
of selected metrics on aligned event-time windows, durably, at one or more
stages. This RFC lets a transducer rule be written as a query: a PromQL
recording rule, or an OPL/KQL pipeline ending in an aggregation stage. Every
front end lowers to one *rule plan*, a small logical algebra in which each
operator declares how it decomposes into partial state, merge and finalize.
That decomposition lets one rule run incrementally at an edge, merge again at
a regional stage, recover from a checkpoint, and publish *progressive
refinements*: provisional results before a window seals, a sealed result, and
explicit restatements for accepted late data. The fixed rule of work item A1
(selector, retained dimensions, period, aggregation) is the printed canonical
form of the simplest plan shape, so every rule can be explained in terms of
the configuration it is equivalent to.

## Motivation

**Operators already write rules as queries.** Prometheus users express
derived metrics as recording rules such as
`sum by (service) (rate(http_requests_total[5m]))`. A fixed
"retain these dimensions, sum every minute" configuration covers the first
demonstration; real rule sets add ratios, histogram quantiles, filters, unit
conversion and rules that consume other rules. Expressing these as queries
keeps the transducer useful beyond projection-and-sum without a growing set of
special-purpose configuration fields.

**The same query should mean the same thing on every replay.** Prometheus
evaluates rules at wall-clock times over whatever has been ingested; when an
input is delayed or a link disconnects, the recorded value depends on arrival
order. The transducer has accepted input in durable storage, an explicit
sealed-through boundary, and replay. Evaluating a query language over event
time gives results that are a function of the accepted input and the sealed
boundary, reproducible by any later evaluation of the same input.

**Late data should improve results without being counted twice.** The
[model requirements][model-2d] describe the two-dimensional time axis: most
data arrives within a minute, some within ten. Today a pipeline either waits
(latency) or drops (accuracy). Progressive refinement publishes early, then
restates, under a publication contract that tells a capable receiver whether
each point adds to or replaces an earlier one.

The expected outcome: a user writes a PromQL recording rule or an OPL query;
the transducer compiles it, prints its plan and canonical form, records
results equal to a direct evaluation of the same accepted input at the same
sealed boundary, and publishes provisional, sealed and restated results to
capable peers while unaware peers receive ordinary sealed points.

## Guide-level explanation

### A rule is a query inside an evaluation envelope

The query states *what* to compute. The envelope states *when*, *how late*,
and *who sees intermediate results*:

```yaml
rules:
  - name: service_requests
    generation: 1
    evaluation:
      period: 1m
      origin: 1970-01-01T00:00:00Z
      allowed_lateness: 2m
    refinement:
      provisional: every_period    # or: none
      late: reject                 # or: restate
      restatement_horizon: 10m
    promql: |
      sum by (service) (increase({"http.server.request.count"}[1m]))
    output:
      name: service.request.count
```

The envelope is the part of work item A1 that a query language cannot
express: generation, evaluation period and origin, allowed lateness, the
refinement policy, reader temporality and state budgets. The query replaces
A1's selector, retained dimensions and aggregation kind.

Prometheus rule files are accepted in their familiar shape. A group's
`interval` sets the period, its `query_offset` sets the allowed lateness, and
each `record` rule becomes one transducer rule with constant output
attributes taken from `labels`:

```yaml
prometheus_rule_groups:
  - name: http
    interval: 1m
    query_offset: 2m
    rules:
      - record: service:http_requests:increase1m
        expr: sum by (service) (increase(http_requests_total[1m]))
```

### The same rule in OPL and KQL

KQL already parses `summarize ... by bin(...)` into the shared expression
tree used by this repository's query engines. A rule can therefore be written
as (identifiers illustrative):

```text
sums
| where name == "http.server.request.count"
| summarize value = sum(value) by service = resource.attributes["service.name"],
                                  bin(time_unix_nano, 1m)
```

OPL gains a metrics-aware `aggregate` operator that applies each metric
type's natural aggregation, as OpenTelemetry SDK views do:

```text
sums
| where name == "http.server.request.count"
| aggregate every 1m by resource.attributes["service.name"]
```

OPL addresses OpenTelemetry structure directly (resource, scope, data point
attributes). PromQL addresses a flat label set and contributes vector
matching across metrics. Both lower to the same plan.

### Every rule explains itself

`explain` prints the compiled plan, its decomposition class, and, when the
plan has the projection-and-aggregate shape, the equivalent A1 rule:

```text
rule service_requests (generation 1)
  Emit      name=service.request.count type=Sum(delta, monotonic) unit=inherited
  Aggregate keys=[resource:service.name] fn=sum class=additive
  Window    period=1m range=1m origin=1970-01-01T00:00:00Z
  Resolve   kind=sum (cumulative->delta per source epoch)
  Select    metric="http.server.request.count" type=Sum
equivalent fixed rule:
  select: { metric: http.server.request.count }
  retain: [ resource.attributes["service.name"] ]
  period: 1m
  aggregation: sum
```

The PromQL, KQL and OPL rules above print the same plan. An unsupported
construct is a compile error that names the construct and its source location,
for example `topk is holistic and has no bounded partial state; rejected at
1:1`.

### Refinement, seen by a capable receiver

With `provisional: every_period` and `late: restate`, the edge publishes the
10:00-10:01 cell for `service=api` three times:

| Publication | Phase | Encoding | Value | Receiver total |
| --- | --- | --- | --- | --- |
| revision 1 at 10:01 | provisional | correction | 90 | 90 |
| revision 2 at 10:03 | sealed | correction | +8 | 98 |
| revision 3 at 10:07 | restated | correction | +2 | 100 |

A regional receiver applies each revision exactly once. A Prometheus scrape at
10:04 reads 98 in the cumulative total. An unaware OTLP peer receives one
ordinary point, the sealed value 98, at 10:03; the later restatement is
withheld from that peer and counted.

## Reference-level explanation

### Architecture

```text
PromQL rule ---promql-parser---> PromQL AST ---+
OPL / KQL   ---existing parsers-> DataExpression -+-> lower --> rule plan
fixed rule (A1) ---------------------------------+               |
                                             decompose (classes) |
                          +------------------------------------+-+
                          v                                    v
           incremental executor (hot path)      query-time executor (DataFusion)
           Select -> Resolve -> partial state    LogicalPlan over retained input
           checkpointed as Arrow (D2)            and committed results
                          |                                    |
                          +---------- equal at (cut C, seal S) +
```

The rule plan is the contract between front ends and executors. Two executors
implement it. The incremental executor runs on the transducer's local runtime
and is the system of record. The query-time executor evaluates the same plan
over retained input or committed results; it is the reference oracle for work
item A3, the verifier for replay, and the basis for later ad hoc queries.

**Equivalence invariant.** For every plan, input cut `C` and sealed-through
boundary `S`, the incremental executor's committed cells for windows ending at
or before `S` equal the query-time executor's result over the input accepted
through `C`. Conformance fixtures (A3, F3) assert this, including after crash
recovery and across two stages.

### Rule plan

| Operator | Input -> output | Responsibility |
| --- | --- | --- |
| `Select(matchers)` | OTAP metric batches -> input points with full input identity | Predicate over resource, scope, metric descriptor and point attributes; executed by the existing columnar query engine. |
| `Resolve(kind)` | input points -> per-contributor contributions | C1-C3: delta intervals per source epoch for sums and histograms; boundary samples with a staleness horizon for gauges. Stateful; owned by the transducer. |
| `Window(period, range, offset)` | contributions -> windowed contributions | Aligned `(start, end]` windows from the rule origin; `range = k * period` evaluates as `k` panes. |
| `Aggregate(keys, fn)` | windowed contributions -> one cell per output identity and window | Natural aggregate by default; partial, merge, finalize. |
| `Map(expr)` | cells -> cells | Scalar arithmetic, unit conversion, label set/rename, comparison filters. |
| `Combine(matching, op)` | two cell streams at one window -> cells | PromQL vector matching: `on`, `ignoring`, `group_left`, `group_right`, set operators. |
| `Emit(name, type, unit, labels)` | cells -> recorded OTAP points | Output identity, type, unit and reader temporality. |

A *cell* is one output series in one window of one rule generation. Every
operator above `Resolve` works on contributions or cells, so input identity
(including the logical writer) is resolved before any dimension is projected
away, as C1 requires.

Plans are DAGs. A rule group compiles into one DAG evaluated at each boundary
against one input cut, so a rule that consumes another rule's output reads that
rule's cell for the same window. Cycles are compile errors.

### Decomposition classes

Each `Aggregate`, `Map` and `Combine` node is classified. The class decides
whether the plan can run in two stages, publish provisional results, and how a
refinement is encoded.

| Class | Examples | Partial state | Two-stage | Refinement encoding |
| --- | --- | --- | --- | --- |
| Additive | `sum`, `count`, `increase`, histogram merge | sums, counts, bucket arrays | yes | correction or restatement |
| Idempotent | `min`, `max` | one value per cell | yes | restatement |
| Algebraic | `avg` (sum, count), `stddev` (sum, sum of squares, count), `rate` (increase / range) | small tuple | yes | restatement of the finalized value |
| Order-dependent | last value per contributor (gauges) | (timestamp, value) per contributor | yes, contributor retained | restatement |
| Composite | `Combine`, comparison filters, `histogram_quantile` | finalized inputs | per input | restatement; filter flips retract |
| Holistic | `topk`, `bottomk`, `quantile`, `count_values`, `quantile_over_time` | unbounded | no | rejected in v0 |

A plan's class is the least capable class among its nodes. Holistic plans are
rejected at compile time in v0; they need recomputation from retained input,
which the query-time executor can provide later as a sealed-only mode.

### PromQL in event time

The PromQL front end parses with [`promql-parser`][greptime-parser]
(GreptimeTeam, Apache-2.0, published on crates.io), lowers the AST to a rule
plan, and evaluates it over the transducer's resolved view: every counter is a
per-contributor sequence of exact delta intervals (C1), every gauge a sequence
of observations with a staleness horizon (C3).

| PromQL construct | Prometheus | Transducer |
| --- | --- | --- |
| Evaluation time `t` | wall-clock schedule with a per-group phase | aligned window end from the rule origin |
| `query_offset` | evaluate at scheduled time minus offset | allowed lateness before sealing |
| Gauge instant selector | latest sample in `(t - lookback, t]`; stale marker suppresses | last eligible observation per contributor at the boundary within the staleness horizon (default 5m); `NO_RECORDED_VALUE` suppresses |
| Counter instant selector | latest raw cumulative value | the contributor's total since the output epoch start (C2 rebase) |
| `increase(x[R])` | extrapolated from samples in the range | exact sum of resolved deltas in `(t - R, t]`; `R` a multiple of the period |
| `rate(x[R])` | extrapolated per-second rate | `increase(x[R])` divided by `R` in seconds |
| `sum/count/min/max/avg/last_over_time` | over raw samples | per-contributor algebraic partials over the window |
| `sum/count/min/max/avg by/without` | over the instant vector | `Aggregate` over projected cells |
| Binary ops, vector matching | joins on label signatures | `Combine` at one window |
| `histogram_quantile(q, ...)` | classic `le` series or native histograms | over OTel explicit or exponential histograms after merge |
| `offset d` | shifted selection | committed panes `d` earlier; `d` a multiple of the period, within history retention |
| Stale markers from recording rules | written when a series disappears | `NO_RECORDED_VALUE` (C2) |
| `@`, subqueries, `irate`, `idelta`, `changes`, `resets`, `predict_linear`, `holt_winters` | supported | rejected in v0 |

Two rows change results relative to Prometheus, deliberately:

- `increase` and `rate` are exact rather than extrapolated. The transducer
  knows each contributor's interval boundaries, so it needs no estimate.
  Greptime already applies the same rule to OTLP delta series: `increase`
  becomes the window sum and `rate` divides it by the window duration
  ([planner.rs:4317-4357][greptime-delta]).
- A counter instant selector is reset-correct: `sum by (service) (x)` is the
  re-aggregated cumulative Sum, where Prometheus would sum raw values that
  drop at each reset.

**Names.** `name_translation: none` matches selectors against OpenTelemetry
metric names using the quoted UTF-8 selector syntax shown above.
`name_translation: prometheus` matches against names translated by the
[OpenTelemetry Prometheus compatibility rules][otel-prom] (unit and `_total`
suffixes, underscore escaping), so existing rule files work unchanged. The
`record` name is used verbatim as the output metric name.

**Labels.** The label namespace is the data point attributes, `job` and
`instance` derived from resource attributes per the compatibility rules,
`otel_scope_name` and `otel_scope_version`, and the resource attributes listed
in `promote_resource_attributes`. Each label remembers its origin, so an
output attribute returns to the level it came from: grouping by a promoted
`service.name` yields a resource attribute on the recorded series. When a
point attribute and a promoted resource attribute share a label name, the
input series is excluded from that rule, counted and logged, unless the rule
declares a precedence.

**Types and units.** PromQL is untyped; OpenTelemetry metrics are typed. The
compiler records each selector's required type; binding checks it when a
series' descriptor first arrives. `increase` and `rate` require a Sum
(monotonic or not; a non-monotonic Sum yields signed results);
`*_over_time` requires a Gauge; `histogram_quantile` requires a histogram.
A mismatch excludes that series from the rule and increments a bind-error
counter with the rule and reason. Output types follow the plan:

| Plan shape | Recorded type |
| --- | --- |
| `sum/count by (...) (increase(x[P]))` with `P` equal to the period | Sum, delta; readers may present cumulative |
| `sum by (...) (x)` over a Sum | Sum, cumulative from the output epoch start |
| `sum by (...) (increase(h[P]))` over a histogram | Histogram, delta |
| `rate`, `avg`, `min`, `max`, `*_over_time`, ratios, quantiles | Gauge |

`rate` appends `/s` to the input unit. Arithmetic across metrics requires an
explicit `output.unit` unless both units are equal.

### OPL and KQL front ends

KQL `summarize` already parses into
[`SummaryDataExpression`][summary-expr] with group keys and the aggregation
functions `Sum`, `Count`, `Minimum`, `Maximum` and `Average`; the columnar
planner currently reports it as not yet supported
([planner.rs:496][planner-summary]). The transducer becomes its first
columnar consumer. Lowering recognizes `bin(time_unix_nano, P)` in the group
keys as `Window(P)` and the remaining keys as `Aggregate` keys, and requires
`P` to equal the envelope's period.

OPL adds `aggregate every <period> by <keys> [using <fn>]` to
[`opl.pest`][opl-grammar]. Without `using`, each metric type takes its natural
aggregation. Stages before `aggregate` (`where`, `set`, `rename`,
`scale_metric`, `apply data_points { ... }`) lower to `Select` and per-point
`Map` nodes and run on the existing columnar engine, which already evaluates
expressions with DataFusion across OTAP parent and child tables joined by
`parent_id` ([join.rs][query-join]). Multi-metric rules (`Combine`) are
PromQL-only in v0.

### DataFusion's role

DataFusion 55.1, already a workspace dependency, serves both executors.

**Incremental executor.** `Select` and `Map` use the columnar engine's
DataFusion expression evaluation. `Aggregate` uses DataFusion
[`GroupsAccumulator`][groups-acc]s directly and synchronously: projected
identity hashes give group indices, `update_batch` folds contributions,
`state(EmitTo)` yields partial state as Arrow arrays, `merge_batch` folds
staged inputs, refinements and recovered checkpoints, and `evaluate(EmitTo)`
finalizes a cell at publication. Built-in accumulators cover sum, count, min,
max and avg; histogram merge (including exponential rescaling) and last value
per contributor are custom accumulators. Partial state is Arrow, so a
checkpoint (D2) writes it to the `metrics_checkpoints` store as Arrow record
batches and recovery merges it back without a separate format. `Resolve` is a
custom stateful operator that owns event-time state, as the existing
transform pipeline evaluates each batch independently. The hot path calls
accumulators synchronously on the local runtime.

**Query-time executor.** Retained input (Quiver segments) and committed
results are exposed as DataFusion `TableProvider`s, and a rule plan lowers to
a `LogicalPlan` with extension nodes for `Resolve` and `Window`, the same
structure Greptime uses. This path runs off the hot path; DataFusion's
`Send + Sync` execution traits are confined to it.

### Review of existing PromQL-on-DataFusion designs

**GreptimeDB.** [`PromPlanner`][greptime-planner] lowers the PromQL AST
recursively into a DataFusion `LogicalPlan`: a selector resolves to a table
with tag, timestamp and value columns; matchers become filters, with an absent
label comparing as `""`; series are sorted and isolated (`SeriesDivide`),
normalized for offset and stale markers (`SeriesNormalize`), and sampled on the
evaluation grid (`InstantManipulate`) or folded into per-step ranges
(`RangeManipulate`) consumed by range-function UDFs; vector matching becomes
joins on evaluation time and the matching label signature, and `or` is a
custom left-preferential union (`UnionDistinctOn`). This RFC adopts the
lowering structure, the matcher semantics and the treatment of vector matching
as joins over cells at one window. It differs in three places: the transducer
records incrementally instead of scanning stored samples per query; OTAP's
resource/scope/point hierarchy replaces table-per-metric; and range state is
exact per-contributor partials, avoiding Greptime's packed `RangeArray`
dictionary encoding, which its source notes may not be a valid Arrow
dictionary ([range_array.rs][greptime-range]). The planner lives in
GreptimeDB's `query` crate, depends on its catalog and table metadata, and
builds against a DataFusion fork, so this RFC adopts its design and
implements it independently.

**prometheus-community/promql-rs.** [promql-rs][promql-rs] (Apache-2.0, on
DataFusion 55) defines a [`SeriesSource`][promql-rs-source] contract that
mirrors Prometheus's `Querier.Select`: the store returns a DataFusion
`ExecutionPlan` of series chunks, and *the store cuts time into blocks*, each
answering the steps whose window end lies in `[block_start, block_end)`.
Sealed windows map onto that contract directly, which makes promql-rs a
candidate query-time executor and an independent differential oracle for the
constructs whose semantics coincide with Prometheus's. Its crates are
unpublished and the workspace disallows git dependencies
(`deny.toml: allow-git = []`), and its own conformance report shows partial
native-histogram and subquery coverage; adoption waits for a published
release.

### Progressive refinement

This section specifies work item E5. It depends on the E4 publication
contract for writer, generation and publication identity.

**Terms.**

- *Revision*: a per-cell sequence number, dense from 1, persisted with the
  checkpoint (D2).
- *Phase*: `provisional` (the window is open), `sealed` (the revision
  published when sealed-through passes the window end), or `restated` (a
  post-seal revision for a late arrival accepted within
  `restatement_horizon`).
- *Encoding*: `correction` carries `V(r) - V(r-1)` and is applied exactly
  once; `restatement` carries `V(r)` and subsumes every lower revision.
- *Publication key*: (writer, generation, rule, cell, revision).
- *Publication metadata*: the publication key, phase, encoding, the
  producer's sealed-through boundary and, for parent cells, completeness;
  carried per E4 outside metric series identity.

**Data point mask `0x0000_0002`.** A local, unassigned OpenTelemetry flag bit
marking a point as one revision of a refining cell. The producer sets it on
every revision of a cell that may have more than one, including revision 1.
It identifies intentional repetition under the single-writer rule; phase,
encoding and revision travel in the publication metadata. Cells of a rule with
`provisional: none` and `late: reject` have exactly one revision and are
ordinary points with the bit clear.

**Encoding choice.** A cell's encoding follows its plan class and the reader's
temporality. Additive cells read with delta temporality use `correction`; all
other cells use `restatement`. An additive cell switches to `restatement` for
any revision whose correction is unrepresentable, such as a negative histogram
bucket delta after a child restates downward; the restatement records the full
value explicitly.

**Receiver rules.** A capable receiver keeps, per open cell, the highest
restatement revision `b` (initially 0, with value 0), its value, the
corrections above `b` already applied (revision and value), and a digest per
applied revision. The cell's current value is `V(b)` plus those corrections.

1. A correction with revision greater than `b` and not yet applied is added
   once. Corrections commute.
2. A restatement with revision `r > b` sets the cell's value to `V(r)` plus
   the retained corrections above `r`, discards corrections at or below `r`,
   and sets `b = r`. The result is independent of arrival order.
3. A revision at or below `b` is counted as stale; a correction already
   applied is counted as a duplicate.
4. Two publications with the same key and different content are a conflict:
   the receiver rejects the publication (Nack), logs the key, and counts it.

**Propagation to parent windows.** A regional cell merges child cells from
edge writers. For additive classes the parent applies child corrections to its
partial state directly. For a child restatement, the parent replaces that
child's prior contribution: it retains the latest value per (child writer,
child window, cell) while its own window is open, retracts the prior value
from invertible partials (sum, count, bucket counts) and re-merges retained
child values for the others (min, max, last value). The parent then publishes
its own revision under its own policy. This is the Dataflow Model's
accumulating-and-retracting mode, applied across stages.

**Completeness.** Child publications carry the child's sealed-through boundary
(E4). A parent window seals when every required child has sealed each child
window it covers, or when the parent's allowed lateness passes. In the second
case the sealed parent revision records an incomplete result in its
publication metadata with the number of missing child windows, and a counter
increments. Required children are the configured writer set, or writers seen
within a liveness horizon; a writer absent beyond the horizon is retired
explicitly and recorded.

**Restart.** A writer recovers its revisions with its checkpoint and
republishes from the committed output position (D3) under the same publication
keys, which receivers deduplicate. A writer that loses its state starts a new
generation; the parent treats generations as distinct contributors and marks
cells incomplete when the old generation never sealed them.

**Readers at the producing stage.**

- A push reader on a capable link sends every revision.
- A push reader on an unaware link sends each cell's full value as of its
  sealed revision as an ordinary point with the mask clear, and counts
  post-seal restatements it withholds.
- The pull reader (E2) exposes cumulative totals through sealed-through.
  With `expose_provisional`, it adds open-window revisions for additive,
  non-decreasing rules, which keeps Prometheus counters monotonic.

### Bounded resources

Each rule declares hard limits, enforced when event-time progress stalls (C5):
input series, output cells per window, open windows (including the
restatement horizon), revisions per cell (beyond the limit only the sealed
revision publishes), retained child contributions per parent window, and
publication-key history per open cell. Compilation bounds plan size: nodes,
`Combine` depth and nested aggregations. Each limit has a defined outcome
(backpressure, overflow cell, or counted loss) and a metric in the transducer
node's metric set.

### Errors

Compile errors carry the construct and source location from the parser. Bind
errors (type, unit, label collision) exclude the affected input series from
the rule, increment a counter labeled by rule and reason, and log at a
rate-limited error level. A rule that binds no series within its first sealed
window logs a warning naming its selectors. A conflict between publications is
a Nack. Every failure surfaces as a counter, a log record, or a Nack.

### Implementation sketch

| Piece | Location |
| --- | --- |
| Rule plan, classes, `explain`, A1 canonical form | new crate `metrics-rules` |
| PromQL front end (`promql-parser` 0.11) | `metrics-rules::promql` |
| KQL `summarize` and OPL `aggregate` lowering | `metrics-rules::opl`; grammar in `query-engine-languages` |
| Incremental executor, custom accumulators | transducer node crate (D1) |
| Query-time executor, `TableProvider`s over Quiver | `metrics-rules::reference`, used by A3 and F3 fixtures |
| Publication key and mask handling | E4 contract; `exporter:otap`, `receiver:otap` |

## Drawbacks

- Two semantics named PromQL: exact `increase`/`rate` and reset-correct
  counter selectors differ from Prometheus. Users comparing against a
  Prometheus server see different numbers for extrapolated windows.
- A new parser dependency (`promql-parser` with grmtools) and a
  grammar change to OPL.
- Refinement adds receiver state and a local flag bit that only capable
  consumers interpret; every unaware link needs a sealed-only reader.
- Two executors must stay equivalent; the conformance suite carries that
  cost.

## Rationale and alternatives

- **Fixed rule configuration only (A1).** Simplest, and kept as the canonical
  form. It covers one plan shape; ratios, quantiles and dependent rules would
  each need new fields.
- **Adopt a PromQL engine wholesale (Greptime or promql-rs).** Both evaluate
  stored samples per query. A recording engine needs incremental partial
  state, checkpointing and refinement, which sit below their planners.
  promql-rs remains the preferred query-time executor once published.
- **DataFusion streaming plans end to end.** DataFusion executes bounded plans
  over async streams; event-time state, sealing and refinement would live in
  custom operators regardless. Using accumulators directly keeps the hot path
  on the local runtime.
- **One front end.** PromQL brings an existing rule corpus and vector
  matching; OPL brings OpenTelemetry structure and the repository's own
  language. A shared plan makes the second front end cheap.
- **Without this RFC**, the transducer ships one rule shape, and refinement is
  specified without the plan classes that decide its encoding.

## Prior art

- [Prometheus recording rules][prom-rules]: groups, intervals,
  `query_offset`, stale markers; wall-clock evaluation.
- [GreptimeDB PromQL planner][greptime-planner] and
  [prometheus-community/promql-rs][promql-rs]: PromQL lowered to DataFusion.
- [VictoriaMetrics stream aggregation][vm-stream]: match, interval, `by` and
  `without` with named outputs, by default on ingestion time with in-memory
  state; the fixed-rule form of this design.
- [The Dataflow Model][dataflow] (Akidau et al., VLDB 2015): early, on-time
  and late firings with discarding, accumulating, and
  accumulating-and-retracting modes, which map to provisional, sealed and
  restated phases with correction and restatement encodings.
- [OpenTelemetry SDK views][otel-views]: attribute filtering and natural
  aggregation by instrument kind, the default for OPL `aggregate`.

## Unresolved questions

- Should the PromQL front end offer an opt-in Prometheus-extrapolation mode
  for parity checks, and can it be two-stage?
- Placement of the publication key (E4): per-batch pdata context
  ([RFC 0004][rfc-context]) requires links that never merge publications;
  per-point columns survive rebatching but change the OTAP schema.
- OPL `aggregate` syntax and whether OPL needs a multi-input form for
  `Combine`.
- Default precedence, if any, for label collisions between point and promoted
  resource attributes.
- Whether a filter flip retracts with a `NO_RECORDED_VALUE` restatement or
  with a dedicated retraction encoding.
- Memory for sliding ranges with many panes (`range = k * period`, large `k`).

## Future possibilities

- Ad hoc PromQL queries over committed results through the query-time
  executor, via promql-rs's `SeriesSource` once published.
- Alerting rules (`for`, `keep_firing_for`) evaluated in event time.
- Holistic functions in a sealed-only mode recomputed from retained input.
- OpenTelemetry specification proposals for intentional repetition and
  start-time semantics, informed by this prototype ([model requirements][model]).
- Exemplar sampling (worklist G) attached to plan cells.

[worklist]: ../docs/metrics-transducer-worklist.md
[model]: ../docs/model_requirements.md
[model-2d]: ../docs/model_requirements.md#two-dimensional-time-axis
[summary-expr]: ../../contrib/data_engine/expressions/src/summary/summary_data_expression.rs
[planner-summary]: ../crates/query-engine/src/pipeline/planner.rs
[opl-grammar]: ../crates/query-engine-languages/src/opl/opl.pest
[query-join]: ../crates/query-engine/src/pipeline/expr/join.rs
[rfc-context]: 0004-pdata-context.md
[greptime-parser]: https://github.com/GreptimeTeam/promql-parser/tree/v0.11.1
[greptime-planner]: https://github.com/GreptimeTeam/greptimedb/blob/f4ff272/src/query/src/promql/planner.rs
[greptime-delta]: https://github.com/GreptimeTeam/greptimedb/blob/f4ff272/src/query/src/promql/planner.rs#L4317-L4357
[greptime-range]: https://github.com/GreptimeTeam/greptimedb/blob/f4ff272/src/promql/src/range_array.rs#L24-L64
[promql-rs]: https://github.com/prometheus-community/promql-rs
[promql-rs-source]: https://github.com/prometheus-community/promql-rs/blob/98bc8f13d98f7b0855aa52795df5c8be43a8537f/promql-engine/src/source.rs
[groups-acc]: https://docs.rs/datafusion-expr-common/55.1.0/datafusion_expr_common/groups_accumulator/trait.GroupsAccumulator.html
[otel-prom]: https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/
[otel-views]: https://opentelemetry.io/docs/specs/otel/metrics/sdk/#view
[prom-rules]: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/
[vm-stream]: https://docs.victoriametrics.com/victoriametrics/stream-aggregation/
[dataflow]: https://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf
