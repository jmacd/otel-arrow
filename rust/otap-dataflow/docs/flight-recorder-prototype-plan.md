# OTAP Flight Recorder Prototype Plan

## Status

This document plans a focused prototype of RFC 0005. The prototype covers:

1. a one_collect-backed .NET receiver that emits OTAP Flight;
2. a NetTrace V6 backend for the file exporter that writes OTAP Flight; and
3. a demultiplexing processor that projects OTAP Flight into OTAP logs,
   traces, and metrics; followed by
4. a general-purpose one_collect Flight recorder that tests whether the model
   extends beyond .NET without introducing source-specific Arrow payloads.

OTAP Profiles is deliberately outside the prototype. The Flight schemas and
graph operations must nevertheless follow the common patterns expected by a
future Profiles implementation.

## Prototype question

The prototype should answer:

> Can OTAP Dataflow preserve a bounded, structured, mixed stream of .NET
> runtime observations in Arrow, write it as NetTrace V6, and independently
> project recognized observations into useful OTAP logs, traces, and metrics,
> while keeping the capture model general enough for other runtimes and
> instrumentation sources?

The prototype is successful only if it demonstrates all three paths:

```text
one_collect .NET callbacks
          |
          v
      OTAP Flight -----------------------> file exporter: NetTrace V6
          |
          +-> Flight demux -> OTAP logs
          |
          +-> Flight demux -> OTAP traces
          |
          +-> Flight demux -> OTAP metrics
```

The first branch tests source fidelity and recording. The other branches test
OpenTelemetry interpretation. They have different correctness contracts:

- Flight-to-V6 should preserve equivalent source observations.
- Flight-to-logs, traces, and metrics is an irreversible semantic projection.

After this vertical slice works, the same receiver and Flight builders should
be exercised with non-.NET one_collect sources:

```text
one_collect sources
  +-> EventPipe, ETW, and EventSource
  +-> perf samples and counters
  +-> tracefs and user_events
  +-> process, thread, module, stack, and symbol observations
              |
              v
          OTAP Flight
```

The general-purpose objective is not to normalize every source into an OTel
signal. It is to show that source adapters populate one static Flight graph and
that source-specific semantics remain in event schemas, values, and mapping
modules rather than creating new Arrow payload types.

## Scope

### Included

- Linux .NET collection through the one_collect path already used by the
  `user_events_receiver`.
- A small, representative subset of .NET runtime and application events.
- The complete 14-payload OTAP Flight envelope and graph contract, even when
  some prototype fixtures leave optional payloads empty.
- Bounded callback queues, Arrow builders, dictionaries, and mapper state.
- One mixed NetTrace V6 file writer owned by one exporter instance.
- Source-observation records in V6 sufficient for an OTAP Flight semantic
  round-trip test.
- Flight projections for:
  - generic structured logs;
  - one Activity start/stop span family; and
  - one EventCounter or runtime-counter metric family.
- A general-purpose one_collect capture assessment using at least one
  non-.NET perf or tracefs observation with process, thread, stack, mapping, or
  symbol context.
- Design investigations comparing the resulting model with Go runtime
  execution profiles and an instrumentation-native OTel Flight Recorder SDK.
- Explicit loss records and component loss telemetry.
- Fixtures and tests that compare semantic records rather than encoded bytes.

Windows ETW collection is a desirable follow-up if the one_collect callback
adapter remains platform-neutral, but it is not required to prove the Linux
.NET path.

### Excluded

- OTAP or OTLP Profiles implementation.
- Flight-to-Profiles projection.
- NetTrace V4/V5 ingestion.
- A production-complete NetTrace V6 receiver.
- Full coverage of .NET runtime providers and EventSource types.
- Arbitrary user-defined metric or span inference.
- Cross-core ordering or one file shared by several exporter instances.
- Production rotation, retention, checkpointing, and crash recovery.
- Byte-identical reproduction of one_collect or another V6 writer's files.

The prototype may include a small V6 decoder or test reader needed to validate
its output. That decoder is test infrastructure, not a production receiver.

## Architectural rules

### OTAP Flight is the receiver output

The one_collect receiver emits source observations as OTAP Flight. It does not
emit logs, traces, or metrics directly. This keeps capture independent from
semantic mapping and permits the same recording to be reinterpreted later.

### Projection is explicit

A dedicated processor consumes Flight and publishes zero or more normalized
signals. It is called a demultiplexer because one mixed Flight stream can
produce several signal streams, but its work is semantic projection rather
than container demultiplexing.

Unknown records remain valid Flight records and produce no normalized output
unless a configured generic-log policy applies.

### Source fidelity and normalized signals remain distinct

The V6 writer initially writes the Flight source-observation record class. It
must not encode a derived log, span, or metric as though it were an original
runtime event.

A later mixed-signal V6 backend may accept all OTAP signals and interleave
source and normalized records. Record class and derivation provenance must be
explicit before enabling that mode.

### Common OTAP conventions take precedence

Flight must use:

- the existing embedded resource and scope structs;
- `RESOURCE_ATTRS` and `SCOPE_ATTRS`;
- existing timestamp, trace ID, span ID, flags, and schema URL conventions;
- the common typed AnyValue attribute columns;
- existing parent-ID, dictionary, transport-encoding, retained-memory, and
  logical-size patterns; and
- optional common attribute-unit metadata anticipated by Profiles.

Flight-specific schemas are introduced only for ordered source observations,
source event schemas, source field definitions, decoded source values, and
execution/profile graph data that existing OTAP signals do not represent.

### Profiles compatibility is a design constraint, not a deliverable

The prototype should not add a Profiles signal or OTLP Profiles conversion.
It should establish reusable interfaces for:

- graph reachability and validation;
- filtering, splitting, concatenation, and ID remapping;
- attributes with optional units;
- sample type, sample value, stack, location, mapping, and symbol schemas; and
- resource/scope grouping.

Prototype code must not make Profiles-specific aggregation decisions in the
Flight receiver or V6 codec.

## Objective 4: General-purpose Flight recording

The final priority objective is to establish that this is a one_collect Flight
recorder with a .NET-first vertical slice, not a .NET event format embedded in
OTAP.

The shared capture boundary should accept source-neutral observations:

```text
SourceObservation
  timestamp and observation order
  process, thread, and CPU context
  source schema and typed values
  correlation
  optional stack
  optional mapping and symbol updates
  explicit loss
```

Source adapters translate one_collect callbacks into this logical boundary.
The Arrow builder then assigns pdata-local IDs, includes reachable
definitions, and emits the common 14-payload graph. Neither the logical
boundary nor the builder should depend on EventPipe metadata types, .NET
Activity types, or NetTrace IDs.

The prototype should prove this boundary with one non-.NET one_collect source,
preferably a perf sample with stack and mapping context or a tracefs event with
a typed schema. Success means that the observation uses the existing Flight
tables without a new source-specific payload and can be written by the same V6
writer.

### Open question A: Go runtime execution profiles

The Go runtime provides several overlapping sources:

- CPU and execution samples;
- goroutine stacks and lifecycle state;
- heap and allocation profiles;
- blocking and mutex profiles;
- scheduler, garbage-collection, syscall, and user-region events; and
- runtime trace events with goroutine and processor correlation.

The investigation should compare their information model with one_collect and
OTAP Flight rather than merely comparing pprof file encodings.

<!-- markdownlint-disable MD013 -->
| Question | Evidence to collect |
| --- | --- |
| Observation model | Are records periodic samples, state transitions, aggregated profile entries, or a mixture? |
| Execution identity | How do goroutine, OS thread, process, and logical processor identifiers relate? |
| Stack model | Are stack IDs stable, when are frames symbolized, and how are inline frames represented? |
| Time and ordering | Which clocks and sequence guarantees are available from runtime trace and profiles? |
| Measurements | Which values are event counts, sampled estimates, bytes, durations, or cumulative totals? |
| Loss and truncation | How are dropped trace events, truncated stacks, sampling loss, and profile aggregation reported? |
| Correlation | Can OTel trace/span context, goroutines, tasks, and user regions be connected without heuristics? |
| Capture path | Can one_collect observe the required Linux perf/runtime data externally, or is an in-process Go adapter required? |
| Flight fit | Which data maps directly to records, samples, stacks, mappings, and symbols, and which requires source event schemas? |
<!-- markdownlint-enable MD013 -->

The expected output is a mapping matrix:

```text
Go runtime observation
  -> OTAP Flight tables
  -> optional logs, traces, or metrics projection
  -> future Profiles projection
```

This work must distinguish raw runtime observations from already aggregated
pprof profiles. An aggregated Go profile may map naturally to future OTAP
Profiles but cannot reconstruct the ordered observations expected from a
Flight recorder.

### Open question B: An OTel Flight Recorder SDK

A pure OTel Flight Recorder SDK would produce Flight observations directly
from application instrumentation without requiring runtime-specific capture.
It should complement, not replace, existing OTel logs, metrics, and traces.

The investigation should define an API and data path for:

```text
OTel instrumentation
  -> bounded in-process Flight Recorder SDK
  -> OTAP Flight transport or local V6 writer
  -> optional logs, traces, and metrics projections
```

Candidate inputs include:

- existing OTel log, span, event, and metric APIs observed through SDK hooks;
- explicit structured Flight events for high-volume diagnostic data;
- context propagation for trace and span correlation;
- runtime and process samplers;
- triggered capture around errors, latency, or metric anomalies; and
- pre-trigger circular buffering with bounded post-trigger recording.

The design questions are:

1. Does the SDK observe already normalized OTel entities, expose a new Flight
   event API, or support both?
2. Which data is copied from ordinary instrumentation and which remains only
   in its original OTel signal to avoid duplication?
3. How are provider schemas declared and cached without creating dynamic Arrow
   schemas?
4. How are resource and instrumentation scope identities shared with the
   existing OTel SDK?
5. What are the allocation, synchronization, sampling, and backpressure costs
   on application threads?
6. Does the SDK export OTAP Flight directly, write V6 locally, or send a
   source-neutral callback stream to an out-of-process collector?
7. How are derivation provenance and duplicate suppression represented when
   both original OTel signals and Flight observations are recorded?
8. Which guarantees survive a crash, especially for a circular in-memory
   recorder?

The first SDK prototype, if pursued, should emit the same synthetic logical
observations used by the one_collect tests. It must not introduce a second
Flight schema or encode ordinary OTLP bytes as opaque Flight payloads.

## Representative vertical slice

The first end-to-end slice should use a deliberately small event set:

<!-- markdownlint-disable MD013 -->
| Observation | Flight representation | Projection |
| --- | --- | --- |
| Unknown application `EventSource` event | record, schema, fields, values | Structured log |
| `System.Runtime` counter payload | record, schema, fields, values | Gauge or sum selected by an explicit mapping |
| `Activity` start and stop pair | two ordered records with correlation | One completed span |
| Runtime event with stack, when available | record, stack, locations, mappings | Retained in Flight; no Profile output |
| one_collect or receiver loss | `LOSS` record and component telemetry | Optional diagnostic log, never a fabricated metric point or span |
<!-- markdownlint-enable MD013 -->

This slice exercises generic event fidelity, stateful trace assembly, metric
semantics, unknown-event handling, correlation, and loss without requiring
Profiles.

## Work plan

### Phase 0: Freeze prototype contracts and fixtures

Define the smallest normative contracts before changing pipeline enums.

Deliverables:

- canonical logical fixtures for the five observations above;
- expected Flight rows for each fixture;
- expected projected OTAP logs, traces, and metrics;
- expected V6 logical records;
- one semantic-equivalence comparator for Flight observations;
- documented limits for records, bytes, schemas, values, stack frames, and
  mapper state; and
- a provisional assignment of experimental Arrow payload enum values.

Fixtures should be producer-independent. A synthetic fixture builder must be
able to create the same observations as the live one_collect adapter so codec
and mapper work does not depend on a running .NET process.

Exit gate:

- every fixture has an explicit source-fidelity expectation and zero or more
  explicit signal-projection expectations;
- no expected metric type, unit, temporality, or span relationship is inferred
  only from a display name.

### Phase 1: Add the OTAP Flight pdata skeleton

Introduce Flight as an OTAP-only signal and implement the 14-payload container.

Primary integration points:

- `crates/config/src/lib.rs`
  - add `SignalType::FlightRecords`;
- experimental Arrow protocol definitions
  - add the 12 Flight-specific payload types;
- `crates/pdata/src/otap.rs`
  - add `OtapArrowRecords::FlightRecords` and its validated batch store;
- `crates/pdata/src/otap/raw_batch_store.rs`
  - add the raw Flight store and payload lookup;
- `crates/pdata/src/schema/payloads.rs`
  - define the provisional schemas;
- `crates/pdata/src/payload.rs`
  - support OTAP-record Flight pdata and explicitly reject unsupported OTLP
    conversion; and
- pdata builders, views, memory accounting, and test fixture helpers.

The common attribute schema gains optional `unit`. Existing signal views must
continue to behave unchanged when the column is absent or null.

Implement graph validation before graph mutation:

- unique IDs;
- all required references resolve;
- schema fields belong to the referenced event schema;
- values belong to valid record/schema fields;
- resource and scope attribute parents resolve;
- stack, location, mapping, and symbol references resolve; and
- configured cardinality and nesting limits are enforced.

Exit gate:

- all 14 payload types can be built, validated, stored, viewed, and serialized
  through Arrow IPC;
- common resource, scope, and attribute fixtures work for Flight and existing
  signals;
- unsupported Flight-to-OTLP conversion returns an explicit error.

### Phase 2: Build a synthetic Flight source

Add a test-only or feature-gated source that emits the Phase 0 fixtures as
Flight pdata. This separates Arrow and pipeline integration from one_collect
platform behavior.

The source must support:

- deterministic record order and timestamps;
- configurable batch boundaries;
- repeated schemas across batches;
- an unknown event;
- one loss record;
- an Activity start/stop pair;
- counter values including a reset; and
- an optional stack and mapping graph.

Exit gate:

- the synthetic source can drive the file exporter and projection processor in
  ordinary pipeline tests;
- changing batch boundaries does not change projected signal semantics.

### Phase 3: Prototype the Flight-to-V6 codec

Create a small stateful codec layer independent of the exporter run loop.

Suggested responsibilities:

```text
FlightV6Writer
  begin_segment(metadata)
  write_flight_pdata(records)
  finish_segment()

FlightV6TestReader
  read_source_records()
  reconstruct_logical_flight_observations()
```

The writer must:

- emit metadata before first use;
- map pdata-local IDs into V6 segment-local IDs;
- preserve provider/event schemas, typed values, timestamps, ordering,
  process/thread context, correlation, stacks, mappings, symbols, and loss;
- bound metadata, stack, label, and interning caches;
- make a pdata write transactional with respect to codec state; and
- finalize a segment into a file accepted by an independent V6 reader where
  available.

The test reader only needs enough functionality to reconstruct the prototype
fixtures. It must not be presented as the production NetTrace receiver.

Exit gate:

```text
synthetic Flight
  -> V6 bytes
  -> test reader
  -> equivalent logical Flight observations
```

The comparison ignores V6 block boundaries, dictionary order, metadata IDs,
and interning order.

### Phase 4: Add the file exporter V6 backend

Extend `exporter:file` with a `nettrace_v6` format backend.

Unlike the current JSON backend, V6 uses:

- one mixed-signal-capable writer per exporter instance;
- one output path rather than a `{signal}`-partitioned path;
- state retained across pdata messages;
- explicit finalization on shutdown; and
- an ACK only after the pdata's V6 transaction has completed according to the
  configured durability policy.

For this prototype, the backend accepts OTAP Flight only. Other signals receive
an explicit unsupported-signal NACK. This avoids prematurely defining the
normalized OTel-over-V6 profile.

Prototype configuration:

```yaml
exporters:
  recording:
    type: exporter:file
    config:
      format: nettrace_v6
      path: /tmp/runtime-{core_id}-{generation}.nettrace
      durability: write
```

Exit gate:

- synthetic Flight sent through a real pipeline produces one finalized V6
  file;
- the Phase 3 reader reconstructs equivalent observations;
- a failed write does not advance codec state or ACK the pdata;
- shutdown finalizes the stream.

### Phase 5: Implement the Flight projection processor

Add a local stateful processor with explicit output ports:

```text
flight input
  +-> logs
  +-> traces
  +-> metrics
  +-> unmatched or passthrough Flight
```

The exact engine API may require separate processor instances or routing
stages if one input message cannot publish heterogeneous outputs. Preserve the
logical contract even if the physical component decomposition differs.

The processor has two layers:

1. source-independent projection helpers that consume Flight views; and
2. .NET mappings keyed by provider, event ID, version, and field schema.

#### Log projection

The first log mapping should:

- map one unknown or generic EventSource record;
- preserve resource, scope, event time, observed time, event name, severity,
  trace ID, span ID, and flags where known;
- map only representable scalar fields into log attributes;
- retain unsupported or unmapped fields in Flight; and
- report dropped projected attributes explicitly.

#### Trace projection

The first trace mapping should:

- recognize one Activity start/stop family;
- use explicit Activity or W3C correlation rules;
- retain bounded open-span state;
- emit one OTAP span on completion;
- define timeout, duplicate-start, stop-without-start, and shutdown behavior;
  and
- preserve incomplete source observations in Flight even when no span is
  emitted.

#### Metric projection

The first metric mapping should:

- recognize one versioned counter schema;
- define metric name, description, unit, type, temporality, and monotonicity in
  mapping metadata;
- handle counter resets and missing intervals explicitly;
- use existing OTAP metric and data-point tables; and
- avoid deriving semantics from localized display names.

Exit gate:

- Phase 0 fixtures produce semantically equivalent OTAP logs, spans, and
  metrics;
- unknown events do not accidentally produce spans or metrics;
- bounded state and overflow behavior are tested;
- the original Flight pdata can be routed to recording independently of the
  projections.

### Phase 6: Connect one_collect .NET capture

Adapt the existing bounded one_collect callback pattern in
`user_events_receiver/one_collect_adapter.rs` into a Flight builder.

Separate callback-owned capture data from Arrow construction:

```text
one_collect callback
  -> copy into bounded pending observation
  -> local cooperative drain
  -> Flight batch builder
  -> pipeline publish
```

The pending observation should contain only data whose ownership and lifetime
are explicit. It must account for retained bytes, not only event count.

The adapter maps:

- provider metadata to event schemas and schema fields;
- callbacks to root records;
- decoded fields to event values;
- process/thread and timestamp data to root columns;
- Activity labels to dedicated correlation columns where valid;
- stacks, modules, and symbols to their graph tables; and
- source or pending-queue loss to `LOSS` records.

The receiver must flush on record, byte, schema, value, stack, dictionary, or
cooperative-time limits. Source-session caches may outlive a pdata, but each
pdata must contain and remap every reachable definition.

Exit gate:

- a small .NET fixture application produces the expected generic event,
  Activity span, and counter metric;
- the same capture produces a readable V6 recording;
- queue overflow and source loss are observable and do not appear as
  successful empty records;
- memory remains bounded during a sustained capture.

### Phase 7: End-to-end proof and measurements

Run the complete prototype:

```text
.NET fixture application
  -> one_collect receiver
  -> OTAP Flight fan-out
       +-> V6 file exporter
       +-> Flight projection processor
            +-> OTAP logs
            +-> OTAP traces
            +-> OTAP metrics
```

Collect:

- callback-to-Flight throughput;
- retained and logical bytes per Flight record;
- queue occupancy and drops;
- V6 bytes per source record;
- projection throughput and state cardinality;
- end-to-end latency; and
- CPU and allocation profiles for capture, Arrow construction, V6 encoding,
  and each signal mapper.

This phase is evidence gathering, not optimization without a measured
bottleneck.

Exit gate:

- one recorded source stream and its three projections agree with the fixture
  expectations;
- Flight-to-V6 semantic round-trip passes;
- batch-boundary variation does not alter log, trace, or metric semantics;
- all configured resource bounds are exercised by tests; and
- the results identify whether the architecture merits production hardening.

### Phase 8: General-purpose recorder assessment

After the .NET vertical slice passes, exercise the source-neutral capture
boundary with one non-.NET one_collect source and complete the two comparative
investigations.

Deliverables:

- one perf or tracefs fixture and live capture represented using the existing
  Flight tables;
- a source-adapter interface with no .NET or V6 types;
- a Go runtime execution-profile mapping matrix covering runtime trace,
  sampling profiles, execution identity, timing, stacks, measurements, and
  loss;
- a decision on which Go data can be collected externally through one_collect
  and which requires an in-process runtime adapter;
- an OTel Flight Recorder SDK API sketch and bounded buffering model;
- a duplication and provenance policy for SDK-originated Flight plus ordinary
  OTel signals; and
- a list of RFC 0005 schema changes, if any, supported by evidence from the
  non-.NET source.

Exit gate:

- the non-.NET source requires no new Arrow payload type;
- source-specific types are confined to an adapter and schema rows;
- the same Flight validation, V6 writer, and semantic comparator work
  unchanged;
- the Go comparison clearly separates ordered runtime observations from
  aggregated Profiles data; and
- the SDK sketch reuses standard OTel resource, scope, attributes, and context
  rather than defining parallel concepts.

## Dependency and parallelism

```text
Phase 0: contracts and fixtures
        |
        v
Phase 1: Flight pdata
        |
        +-------------------+
        |                   |
        v                   v
Phase 2: synthetic source   Phase 6 groundwork: callback model
        |
        +-------------------+
        |                   |
        v                   v
Phase 3: V6 codec       Phase 5: projections
        |
        v
Phase 4: file backend
        |
        +-------------------+
                            |
                            v
                  Phase 6: live receiver
                            |
                            v
                  Phase 7: end-to-end proof
                            |
                            v
                  Phase 8: general-purpose assessment
```

After Phase 1, codec and projection work can proceed in parallel using the
synthetic fixtures. one_collect callback investigation can proceed earlier,
but live receiver integration should not define its own intermediate model.

## Suggested ownership

<!-- markdownlint-disable MD013 -->
| Work package | Primary area | Depends on |
| --- | --- | --- |
| Flight schemas and pdata | `config`, `pdata`, Arrow protocol | Phase 0 |
| Fixture builder and semantic comparator | `pdata` testing support | Phase 0 |
| V6 codec spike | new focused codec module or crate | Phases 0-2 |
| File backend | `core-nodes/exporters/file_exporter` | Phase 3 |
| Flight views and .NET mappings | `pdata` views and `contrib-nodes` processor | Phases 1-2 |
| one_collect capture adapter | `contrib-nodes` receiver | Phase 1 and callback groundwork |
| Pipeline integration and measurements | receiver, processor, exporter test harness | Phases 4-6 |
<!-- markdownlint-enable MD013 -->

## Test matrix

<!-- markdownlint-disable MD013 -->
| Test | Flight | V6 | Logs | Traces | Metrics |
| --- | --- | --- | --- | --- | --- |
| Unknown structured event | Preserve | Round-trip | Emit | None | None |
| Activity start/stop | Preserve both | Round-trip both | Policy-dependent | One span | None |
| Counter interval | Preserve | Round-trip | Policy-dependent | None | One point |
| Counter reset | Preserve | Round-trip | None | None | Defined reset behavior |
| Stack-bearing event | Preserve graph | Round-trip graph | Optional correlation | Optional event | None |
| Loss notification | Explicit loss record | Round-trip loss | Optional diagnostic | No fabricated span | No fabricated point |
| Unknown field wire type | Raw or explicit decode error | Preserve according to fidelity policy | Do not stringify silently | None | None |
| Batch split | Self-contained graph | Equivalent records | Same semantics | Same semantics | Same semantics |
<!-- markdownlint-enable MD013 -->

Every test added under the Rust project must include the required `Scenario`
and `Guarantees` documentation immediately above its declaration.

## Prototype completion criteria

The prototype is complete when:

1. Flight is a valid OTAP-only pdata signal with the 14 payloads and common
   resource, scope, and attribute conventions.
2. A bounded one_collect .NET receiver emits representative Flight records.
3. `exporter:file` writes those records into a valid, finalized NetTrace V6
   source-observation stream.
4. The V6 stream reconstructs semantically equivalent prototype Flight
   observations through the test reader.
5. A Flight projection component emits valid OTAP logs, traces, and metrics
   for the declared mappings.
6. Unknown and incomplete records remain available as Flight without
   fabricated OTel semantics.
7. Queue, builder, codec, and mapper state are bounded and their loss behavior
   is observable.
8. The profile-aligned Flight schemas and graph utilities do not depend on
   .NET-specific or V6-specific types.
9. A non-.NET one_collect source uses the same source-neutral capture boundary,
   Flight schemas, validator, and V6 writer.
10. The Go runtime and OTel SDK investigations identify viable capture paths,
    semantic mismatches, and any evidence-based changes needed before
    production implementation.

## Decisions deferred beyond the prototype

- Final Arrow payload enum values and compatibility policy.
- Production NetTrace V6 receiver and replay behavior.
- Normalized logs, traces, metrics, and Profiles encoding in mixed V6 files.
- Derivation provenance across file segments.
- Windows ETW parity.
- Complete .NET semantic-convention coverage.
- Production file rotation, retention, durability, and recovery.
- OTAP Profiles schemas, services, builders, views, and OTLP conversion.
- Flight-to-Profiles aggregation and round-trip policy.
- Production Go runtime capture and semantic mappings.
- Implementation and standardization of an OTel Flight Recorder SDK.

The future Profiles implementation should reuse the common decisions proven
here: resource and scope hierarchy, attribute units, graph validation and
remapping, and the profile-aligned sample, stack, location, mapping, and symbol
structures.
