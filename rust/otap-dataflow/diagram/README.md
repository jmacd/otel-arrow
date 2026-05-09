# Pipeline-conversion experiments — visual design

This directory holds the slide-generating Python scripts for the
OTLP-vs-OTAP story:

| Script               | Output             | Slide                                   |
|----------------------|--------------------|-----------------------------------------|
| `gen_diagram.py`     | `experiments.svg`  | End-to-end pipeline conversion-cost experiments (TI-style timing diagram). |
| `gen_otlp_bytes.py`  | `otlp_bytes.svg`   | OTLP Logs on the wire — protobuf bytes color-coded by section, with brackets above naming each region. The "row-oriented" half of the OTLP→OTAP transformation story. |
| `gen_otap_tables.py` | `otap_tables.svg`  | OTAP Logs — the same two records as `gen_otlp_bytes.py`, rendered as the four columnar tables (`resource_attrs`, `scope_attrs`, `log_attrs`, `logs`). Colors match the OTLP slide so each byte run is visually traceable to its destination column. |
| `node_lib.py`        | (library)          | Shared SVG primitives + palette imports for every per-node slide. |
| `gen_node_retry.py`  | `node_retry.svg`   | Per-node slide for `urn:otel:processor:retry`. First instance of the per-node visual style described below. |
| `gen_node_batch.py`  | `node_batch.svg`   | Per-node slide for `urn:otel:processor:batch`. Same per-node grammar; calldata chip carries a single `slot: SlotKey` so ack/nack on a coalesced batch fans back to its contributing inputs. |
| `gen_node_transform.py` | `node_transform.svg` | Per-node slide for `urn:otel:processor:transform` (the "query" processor). Multi-language: today the `Query` config enum accepts a `kql_query` *or* an `opl_query` string (OTTL is a TODO). Both parsers produce the same `PipelineExpression` IR and feed the same OTAP/Arrow query engine, so language is a front-end choice only. Calldata chip carries `outbound: SlotKey` so Ack/Nack on each split-off batch (from a `route_to` operator) fans back to the original inbound. |
| `gen_node_attributes.py` | `node_attributes.svg` | Per-node slide for `urn:otel:processor:attribute` (`AttributesProcessor`). Applies configured insert/update/delete edits to selected OTAP attribute domains (resource / scope / record). Output is OTAP because non-noop edits force conversion of OTLP inputs to OTAP records. |
| `gen_node_content_router.py` | `node_content_router.svg` | Per-node slide for `urn:otel:processor:content_router` (`ContentRouter`). Routes each batch to one named output port keyed on a resource attribute value. Pure pass-through for both pdata formats. |
| `gen_node_debug.py` | `node_debug.svg` | Per-node slide for `urn:otel:processor:debug` (`DebugProcessor`). Forwards pdata unchanged while emitting a decoded debug rendering of selected signals. Pass-through for both formats. |
| `gen_node_delay.py` | `node_delay.svg` | Per-node slide for `urn:otel:processor:delay` (`DelayProcessor`). Sleeps for a configured duration before forwarding each pdata message — useful for back-pressure and ack-timing tests. Pass-through for both formats. |
| `gen_node_durable_buffer.py` | `node_durable_buffer.svg` | Per-node slide for `urn:otel:processor:durable_buffer` (`DurableBufferProcessor`). Persists pdata to per-core durable storage before forwarding finalized bundles downstream so acks become *durable* receipts, not in-flight promises. Forwards both formats; OTLP handling is configurable. |
| `gen_node_fanout.py` | `node_fanout.svg` | Per-node slide for `urn:otel:processor:fanout` (`FanoutProcessor`). Clones each pdata item to N configured output ports with optional ack aggregation and per-port fallbacks. Pass-through for both formats. |
| `gen_node_filter.py` | `node_filter.svg` | Per-node slide for `urn:otel:processor:filter` (`FilterProcessor`). Drops log and trace records that match configured predicates. OTAP-only output: OTLP inputs are converted to OTAP Arrow records before filtering. |
| `gen_node_log_sampling.py` | `node_log_sampling.svg` | Per-node slide for `urn:otel:processor:log_sampling` (`LogSamplingProcessor`). Reduces log volume with either a fixed sampling ratio or a per-window item budget. Forwards non-log signals unchanged, so both pdata formats appear on the output side. |
| `gen_node_signal_type_router.py` | `node_signal_type_router.svg` | Per-node slide for `urn:otel:processor:type_router` (`SignalTypeRouter`). Routes pdata to `logs` / `metrics` / `traces` named output ports based on the inbound signal type. Pass-through for both formats. |
| `gen_node_temporal_reaggregation.py` | `node_temporal_reaggregation.svg` | Per-node slide for `urn:otel:processor:temporal_reaggregation` (`TemporalReaggregationProcessor`). Reaggregates delta metrics over a configured period and flushes lower-frequency OTAP batches on a local scheduler wakeup. Non-metric signals pass through unchanged. |
| `gen_node_condense_attributes.py` | `node_condense_attributes.svg` | Per-node slide for `urn:otel:processor:condense_attributes` (contrib). Condenses selected log attributes into a single destination attribute on the OTAP output records. OTAP-only output. |
| `gen_node_recordset_kql.py` | `node_recordset_kql.svg` | Per-node slide for `urn:microsoft:processor:recordset_kql` (contrib). Runs a parsed RecordSet KQL pipeline over OTLP log records that arrive wrapped in OTAP pdata; emits OTLP bytes downstream. |
| `gen_node_resource_validator.py` | `node_resource_validator.svg` | Per-node slide for `urn:otel:processor:resource_validator` (contrib). Validates required resource attributes and permanently Nacks data that fails policy. Pass-through for both formats when validation succeeds. |
| `gen_node_otlp_receiver.py` | `node_otlp_receiver.svg` | Per-node slide for `urn:otel:receiver:otlp` (`OTLPReceiver`). First receiver slide. Listens for OTLP/gRPC and OTLP/HTTP exports and admits each request as a lazily-decoded `OtlpProtoBytes` payload; the calldata chip carries a `SlotKey` so `wait_for_result` requests are paired with their downstream Ack/Nack. |
| `gen_node_otlp_grpc_exporter.py` | `node_otlp_grpc_exporter.svg` | Per-node slide for `urn:otel:exporter:otlp_grpc` (`OTLPExporter`). First exporter slide. Encodes OTAP inputs to OTLP protobuf bytes and dispatches per-signal RPCs over a pooled tonic channel, capped by `max_in_flight`. The right-side port labels show `gRPC` / `gRPC status` because that side faces the network. |
| `gen_node_otlp_http_exporter.py` | `node_otlp_http_exporter.svg` | Per-node slide for `urn:otel:exporter:otlp_http` (`OtlpHttpExporter`). HTTP/1.1 sibling of the gRPC exporter; pools hyper clients for SO_REUSEPORT load balancing and supports per-signal endpoint overrides. Right-side port labels show `HTTP` / `HTTP status`. |
| `gen_engine_group.py` | `engine_group.svg` | Engine-architecture slide #1 — the **pipeline group**: one controller process (with its accessory thread-local tasks and shared resources), the `http-admin` thread, and the N per-core `RuntimePipeline` instances. Emphasises one OS thread per assigned core; cross-thread edges are dashed grey. |
| `gen_engine_core.py`  | `engine_core.svg`  | Engine-architecture slide #2 — one **per-core pipeline thread**: the in-thread actors (`RuntimeCtrlMsgManager`, `PipelineCompletionMsgDispatcher`), a representative receiver→processor→exporter node chain, and the per-core `TopicSet` view of the cross-core `TopicBroker`. Boundary stubs identify every channel that crosses out of the thread. |
| `gen_otap_pdata.py`   | `otap_pdata.svg`   | Anatomy of the in-flight `OtapPdata` value: one box containing a `Context` (stack of `Frame`s with `transport_headers` and `flow_compute_ns`) on the left and the `payload: OtapPayload` tagged enum on the right. Both `OTAP records` and `OTLP bytes` variants are tagged "reference-counted · zero-copy transit". A compact `Frame` anatomy strip below lists the per-frame fields. |

`gen_diagram.py` produces `experiments.svg`, a single-slide diagram that
explains the OTLP-vs-OTAP conversion-cost experiments visually. The design
borrows directly from the digital-timing diagrams in Texas Instruments
datasheets (think I²C / SPI bus traces): each experiment is a horizontal
"signal trace" reading left-to-right through the stages of an end-to-end
telemetry pipeline.

## Metaphor

A telemetry record carries a *format* as it moves through the pipeline.
We treat that format as a one-bit signal:

| Signal level | Meaning                                                 | Color                |
|--------------|---------------------------------------------------------|----------------------|
| **HIGH (1)** | OTAP pdata on a typed in-process channel                | muted cool teal      |
| **LOW (0)**  | OTLP pdata on a typed in-process channel                | muted warm red       |
| **MID**      | serialized bytes on a wire (encoding named under label) | neutral grey         |
| **UNDEF**    | pre-ITR "half-encoded" worker output (ambiguous)        | hashed grey, half-band, bottom-aligned to MID |

OTLP gets the warmer color because it is the higher-entropy on-the-wire
encoding (more CPU per byte). OTAP gets the cooler color because it is
the structurally compressed, columnar form. The wire is intentionally
neutral — what matters there is *which encoding* was chosen, called out
in the italic sublabel directly above the mid-line.

## Anatomy of a row

```
LOGGER (otap-dataflow) ────────────────────  WIRE  ──  COLLECTOR (otap-dataflow)
┌──────┬───────────┬───────┬──────────┬──────┬───────────────────────┐
│Worker│ Internal  │ Batch │ Exporter │ Wire │ Receiver              │
│      │ Telemetry │       │          │      │                       │
│      │ Receiver  │       │          │      │                       │
└──────┴───────────┴───────┴──────────┴──────┴───────────────────────┘
   ▒▒                                        ─────
   ▒▒  ────────                  ─┐    ┌────                          ← high (OTAP)
       struct/                    │    │           OTAP/OTLP bytes
       bytes                      │    │
                       ──────┐    │    │           ─────              ← mid  (wire)
                             │    │    │
                             └────┘    │                              ← low  (OTLP)
```

- The **trace** is drawn in the color of the level it is currently at
  (red for OTLP, teal for OTAP, grey for wire bytes).
- **Transitions** are sloped, not vertical — the slope angle
  (`RAMP_ANGLE_DEG`, currently 75°) signals that conversion is
  not free, it takes time. A full swing (low↔high) is twice the
  horizontal run of a half swing (low↔mid, high↔mid), so a forced
  OTAP↔OTLP conversion *looks* longer than an
  encode-to-bytes step.
- The **Worker** column uses a hashed half-height box anchored to the
  bottom of the band. The half-height is meaningful: it visually aligns
  with the wire's mid level, reminding the viewer that the worker output
  is partially serialized — somewhere between in-memory struct and
  on-the-wire bytes. The italic sublabel `struct/bytes` sits directly
  above the box.
- The **Wire** column is always at MID; its italic sublabel
  (`OTLP bytes` / `OTAP bytes`) sits directly above the mid-line in the
  same position as the worker's `struct/bytes` label, making the
  parallel obvious.
- **Stage tick lines** (faint dashed verticals) mark the boundary
  between stages without competing with the trace.

## Anatomy of the page

- **Title bar** at the top.
- **Zone header bar** spanning the track width, partitioning the
  pipeline into three colored bands: `LOGGER (otap-dataflow)` (blue
  tint), `WIRE` (grey/violet tint), `COLLECTOR (otap-dataflow)`
  (warm tint). This locates the wire visually as a thin band between
  two engines.
- **Per-row left rail** holds the scenario id (`Scenario A`…) and a
  one-line subtitle (e.g. *OTLP end-to-end*).
- **Level legend column** between the row label and the track shows
  `OTAP` (teal) at the top of the band and `OTLP` (red) at the bottom,
  reinforcing what each rail means without needing a separate key.
- Three **horizontal guide lines** (faint dashed) at the high, mid, and
  low levels span the track to anchor the eye across stages.
- A small **legend** at the bottom of the page restates the four
  signal-level colors.

## Programming model

The whole diagram is data-driven from a small Python model:

```python
@dataclass
class Segment:
    label: str          # stage name, drawn above the band
    level: Level        # "low" | "high" | "mid" | "undef"
    sublabel: str = ""  # e.g. "OTAP bytes", placed contextually
    weight: float = 1.0 # relative horizontal width

@dataclass
class Scenario:
    title: str           # left-rail title
    subtitle: str = ""   # left-rail subtitle
    segments: List[Segment]
```

Each scenario is one row of segments. Stages with `\n` in their `label`
wrap onto multiple lines stacked above the band (used by *Internal
Telemetry Receiver*).

A helper `scenario(name, subtitle, batch, expt, coll, wire_label)`
constructs the standard six-segment chain
`Worker → Internal Telemetry Receiver → Batch → Exporter → Wire → Receiver`,
parameterised by the three meaningful levels (Batch, Exporter, Receiver)
and the wire encoding name. New scenarios are one line each.

All visual constants live in a single block at the top of the script:
page size, margins, row height, band height, ramp angle, font stack,
and the color palette. Changing any of those re-flows the diagram
without touching layout code.

## Conventions worth preserving (timing diagram)

1. **Color = format.** Never use the OTLP/OTAP/wire colors for anything
   else (e.g. don't reuse them for left-rail text). The eye should be
   trained to read color as format on first glance.
2. **Slope = conversion work.** All transitions slope at the same
   angle. Don't introduce vertical edges (would suggest free conversion)
   or curved edges (no semantics).
3. **Mid-level sublabels go *above* the mid line.** Both the wire and
   the worker half-box place their explanatory text at the same vertical
   offset above their mid-line element, making the visual rhyme
   intentional.
4. **Stages are discrete.** Each segment is a flat horizontal run; the
   only motion is at the boundaries. This keeps the diagram readable as
   a state diagram, not a flow diagram.
5. **Add scenarios, not stages.** New experiments should be expressed
   as new rows with the same six segments. If the topology genuinely
   changes (e.g. a processor stage in the Collector), introduce a new
   helper rather than threading optional segments through every
   scenario.

## Regeneration

```bash
python3 gen_diagram.py            # writes experiments.svg
python3 gen_diagram.py out.svg    # custom path

python3 gen_otlp_bytes.py             # writes otlp_bytes.svg
python3 gen_otlp_bytes.py out.svg     # custom path

python3 gen_otap_tables.py            # writes otap_tables.svg
python3 gen_otap_tables.py out.svg    # custom path

python3 gen_node_retry.py             # writes node_retry.svg
python3 gen_node_retry.py out.svg     # custom path

python3 gen_node_batch.py             # writes node_batch.svg
python3 gen_node_batch.py out.svg     # custom path

python3 gen_node_transform.py             # writes node_transform.svg
python3 gen_node_transform.py out.svg     # custom path

# Remaining core processors
python3 gen_node_attributes.py            # writes node_attributes.svg
python3 gen_node_content_router.py        # writes node_content_router.svg
python3 gen_node_debug.py                 # writes node_debug.svg
python3 gen_node_delay.py                 # writes node_delay.svg
python3 gen_node_durable_buffer.py        # writes node_durable_buffer.svg
python3 gen_node_fanout.py                # writes node_fanout.svg
python3 gen_node_filter.py                # writes node_filter.svg
python3 gen_node_log_sampling.py          # writes node_log_sampling.svg
python3 gen_node_signal_type_router.py    # writes node_signal_type_router.svg
python3 gen_node_temporal_reaggregation.py  # writes node_temporal_reaggregation.svg

# Contrib processors
python3 gen_node_condense_attributes.py   # writes node_condense_attributes.svg
python3 gen_node_recordset_kql.py         # writes node_recordset_kql.svg
python3 gen_node_resource_validator.py    # writes node_resource_validator.svg

# Receivers
python3 gen_node_otlp_receiver.py         # writes node_otlp_receiver.svg

# Exporters
python3 gen_node_otlp_grpc_exporter.py    # writes node_otlp_grpc_exporter.svg
python3 gen_node_otlp_http_exporter.py    # writes node_otlp_http_exporter.svg

# Engine architecture
python3 gen_engine_group.py               # writes engine_group.svg
python3 gen_engine_core.py                # writes engine_core.svg

# OtapPdata anatomy
python3 gen_otap_pdata.py                 # writes otap_pdata.svg
```

Open in a browser (on WSL):

```bash
powershell.exe -c "Start-Process msedge '$(wslpath -w experiments.svg)'"
```

## OTLP byte-layout slide (`gen_otlp_bytes.py`)

This slide is the **row-oriented** picture: a real OTLP `LogsData`
protobuf, drawn as a horizontal array of one-byte boxes, color-coded by
the section each byte belongs to. It is the visual setup for the
upcoming OTAP slide that will show how those rows become columns and
which OTAP tables (resource attrs, scope attrs, log attrs, logs) each
section maps to.

### What's drawn

Two small `LogsData` messages (`Batch A`, `Batch B`) are encoded
exactly as `protoc` would emit them: varint tags, length-delimited
submessages, the lot. `Batch A` carries one resource, one scope, and
**two** `LogRecord`s; `Batch B` carries a different resource, one
scope (with a scope attribute), and **one** `LogRecord`. Each wire
byte is one box, labeled with two hex digits — uniform, distinct,
easy to point at.

Above each byte row, brackets describe the message structure in two
families:

- **Tier 0 (innermost)** names a contiguous logical region: *resource
  attrs*, *scope name*, *scope attrs*, *log fields*, *log attrs*.
  These are the same regions that map cleanly onto OTAP tables.
- **Tiers 1 / 2 / 3 (outer message frames)** stack upward, each tier
  enclosing the one below it:
  - Tier 1: `Resource`, `Scope`, `LogRecord`
  - Tier 2: `ScopeLogs` (visibly enclosing `Scope` + each `LogRecord`)
  - Tier 3: `ResourceLogs` (enclosing the whole record)

Tick length, stroke weight, and font size step up gently with tier so
the nesting reads at a glance.

### Color = section

| Color  | Section                                                    |
|--------|------------------------------------------------------------|
| Grey   | Outer message frame bytes (tag + length for `LogsData` /  `ResourceLogs` / `ScopeLogs` / `LogRecord` wrappers) |
| Green  | Resource attribute KVs                                     |
| Light purple | Scope name                                           |
| Deep purple  | Scope attribute KVs                                  |
| Blue   | LogRecord scalar fields (severity number, body, …)         |
| Orange | LogRecord attribute KVs                                    |

Inside a section, *all* bytes (including the inner KeyValue / AnyValue
tag and length bytes) take that section's color so each tier-0 bracket
covers a contiguous run. Only the *outer* container wrappers are grey.

### Programming model

The encoder is a tiny home-grown `Block` IR — every protobuf primitive
returns a `Block(bytes, spans)` carrying its bytes (each tagged with a
section category) plus any logical-region spans. `b_wrap(field, inner,
frame_cat, label?, tier?)` prepends the wrapper tag and varint length
(stamped with `frame_cat`), shifts the inner spans by the prefix length,
and optionally adds a new outer span with a bracket label.

That means **the wire format is the source of truth**: lengths are
computed from `len(inner.bytes)`, never written by hand, so the diagram
is guaranteed to be a valid OTLP `LogsData` message of exactly the
length displayed. Adding more attributes or records is data, not layout
work.

### Conventions worth preserving

1. **Color = section**, consistently across both batches. The same
   palette is reused on the OTAP slide so a viewer can follow each
   color from row bytes into its destination column.
2. **Frame bytes are grey.** Only the outer-container wrappers
   (`LogsData`/`ResourceLogs`/`ScopeLogs`/`LogRecord`) are grey. Inner
   wrappers (`KeyValue`, `AnyValue`, `Resource`, `Scope`) take the
   surrounding section's color so brackets stay contiguous.
3. **Brackets point down, stack up.** Tier 0 brackets sit just above
   the bytes; outer tiers stack one band higher per nesting level.
   The bar has two short downward end-ticks; the label is centered
   above.
4. **One byte = one box, two hex digits.** Don't collapse runs into a
   single wider box, and don't switch encodings (no ASCII for
   printable bytes) — uniform symbols make the box count obvious and
   keep the row visually balanced.

## OTAP table slide (`gen_otap_tables.py`)

This is the **column-oriented** companion to the OTLP byte slide. It
renders the four OTAP-Logs Arrow RecordBatches — `resource_attrs`,
`scope_attrs`, `log_attrs`, `logs` — populated with the rows that
result from transposing the same two batches used by
`gen_otlp_bytes.py`. A shrunken copy of the OTLP byte rows lives in
the bottom-left so the viewer can compare directly.

### What's drawn

Each of the four tables is a framed `Arrow RecordBatch`. Inside its
frame, every column is a **separate Arrow array** with a small gap
between arrays — this is the physical truth of an Arrow batch (a
name → array map, not a 2-D grid). Above each array the column name
is rendered as plain monospace text in the array's color, reinforcing
that the name is the map *key* and the boxes below are the *value*.

Below each array, a dashed ghost cell containing `⋮` indicates that
the array can grow downward as more rows arrive. (We deliberately
avoid arrows; the dashed cell is enough.)

Current populated state, derived from the two batches in
`gen_otlp_bytes.py`:

| Table            | Rows | Notes                                         |
|------------------|------|-----------------------------------------------|
| `resource_attrs` | 2    | `(parent_id=0, svc=A)`, `(parent_id=1, svc=B)` |
| `scope_attrs`    | 1    | only Batch B's scope carries an attribute     |
| `log_attrs`      | 4    | one for `Batch A` log 0, one for `Batch A` log 1, two for `Batch B`'s log |
| `logs`           | 3    | id 0 + id 1 from Batch A; id 2 from Batch B   |

The `logs` table includes a denormalized `scope.name` column rather
than a separate `scopes` table — this matches how OTAP-Logs is
actually laid out.

### Conventions worth preserving

1. **Same palette as the OTLP slide.** `SECTION_COLORS` is imported
   from `gen_otlp_bytes`, never redefined. A green byte run on the
   OTLP slide and the green-tinted `resource_attrs` rows here are
   exactly the same green. Touch the palette in one place and both
   slides re-flow consistently.
2. **Identifier columns are grey.** `parent_id`, `id`, `resource`,
   `scope` columns use the `struct` color (the same grey used for
   OTLP frame bytes). Data columns (`key`, `str`, `severity`, `body`,
   `scope.name`) take their section color.
3. **Each column is its own array.** Render columns with a visible
   gap between them inside the RecordBatch frame. Don't merge them
   into a contiguous grid — the gap is the point.
4. **Column names live above the array, not as a header cell.** They
   are the keys of the `name → array` map; making them filled cells
   would imply they are part of the data.
5. **Growth is downward, indicator-only.** Use a dashed ghost cell
   with `⋮` under each array. No chevrons, no arrows.
6. **Single source of records.** `RECORDS` lives in
   `gen_otlp_bytes.py` and is imported here for the mini OTLP strip;
   the OTAP table contents mirror it by hand. When you add or change
   a batch in `gen_otlp_bytes.py`, update the OTAP rows to match and
   re-run both scripts.
7. **No arrows between the mini OTLP and the tables.**
   Correspondence is conveyed entirely by color.

## Current project state

- **`gen_diagram.py` / `experiments.svg`** — finished and stable.
  Pipeline-conversion-cost timing diagram (4 scenarios A–D).
- **`gen_otlp_bytes.py` / `otlp_bytes.svg`** — finished. Two batches
  (`Batch A`: 65 bytes, 2 LogRecords; `Batch B`: 74 bytes, 1
  LogRecord) shown as labeled byte rows with three tiers of nesting
  brackets above (`Resource`/`Scope`/`LogRecord` →
  `ScopeLogs` → `ResourceLogs`). Bytes always render as two-digit
  hex; sections (resource attrs, scope name, scope attrs, log fields,
  log attrs) are color-coded.
- **`gen_otap_tables.py` / `otap_tables.svg`** — finished. Four
  Arrow RecordBatches in framed boxes, columns drawn as separate
  arrays with a downward-growth indicator under each. Mini copy of
  the OTLP batches in the bottom-left for visual cross-reference.
  Imports palette + `RECORDS` from `gen_otlp_bytes.py`; the table
  contents are mirrored by hand from those records.
- **`node_lib.py`** — layout master for per-node slides. Hosts the
  `NodeSlideSpec` dataclass, `render_node_slide()`, the locked
  geometry constants, and all SVG primitives. Per-node scripts are
  pure data and call into this module.
- **`gen_node_retry.py` / `node_retry.svg`** — first per-node slide
  built on the master.
- **`gen_node_batch.py` / `node_batch.svg`** — second per-node
  slide; calldata chip carries a single `slot: SlotKey` so ack/nack
  on a coalesced batch fans back to its contributing inputs.
- **`gen_node_transform.py` / `node_transform.svg`** — third
  per-node slide, for the multi-language query/transform
  processor (`urn:otel:processor:transform`). The SPEC lists
  `kql_query` and `opl_query` as separate `String` config rows so
  the supported languages are visible at a glance; OTTL will be
  added as a third row when implemented. The data-plane runtime
  is OTAP-only: every parser produces the same
  `data_engine_expressions::PipelineExpression` IR, the
  processor coerces inbound pdata to `OtapArrowRecords` before
  dispatching, and the engine's `execute_with_state` signature
  is `OtapArrowRecords → OtapArrowRecords`. The output chip is
  therefore `[OTAP]` only; the input chip pair is `[OTAP][OTLP]`
  because OTLP inputs are accepted and converted in-line. The
  calldata chip carries `outbound: SlotKey`, the slotmap key of
  an outbound entry in `Contexts` (`transform_processor/context.rs`):
  one `route_to` operator can split an inbound batch into N
  outbounds, each gets its own outbound slot back-pointing to a
  single inbound slot, and the inbound is only Ack'd / Nack'd
  once every outbound's Ack/Nack has decremented the refcount to
  zero (first error reason wins on Nack).
- **Core processor coverage complete.** Remaining core processors
  each have a SPEC + slide following the locked grammar:
  `gen_node_attributes.py`, `gen_node_content_router.py`,
  `gen_node_debug.py`, `gen_node_delay.py`,
  `gen_node_durable_buffer.py`, `gen_node_fanout.py`,
  `gen_node_filter.py`, `gen_node_log_sampling.py`,
  `gen_node_signal_type_router.py`,
  `gen_node_temporal_reaggregation.py`. Output-format chips
  reflect the source: pure pass-through routers print
  `[OTAP][OTLP]`, processors that normalise inputs to OTAP
  before transforming (filter, attributes) print `[OTAP]` only.
- **Contrib processor coverage complete.**
  `gen_node_condense_attributes.py`,
  `gen_node_recordset_kql.py`,
  `gen_node_resource_validator.py`. The KQL recordset
  processor's URN is `urn:microsoft:processor:recordset_kql` and
  its output chip is `[OTLP]` because it returns
  `OtlpProtoBytes` even though pdata is wrapped as OTAP.
- Receivers and exporters are now starting to be covered by per-node
  slides. `gen_node_otlp_receiver.py` is the first receiver slide;
  `gen_node_otlp_grpc_exporter.py` and `gen_node_otlp_http_exporter.py`
  are the first exporter slides. Remaining receivers (OTAP,
  internal-telemetry, syslog/CEF, traffic generator, host metrics,
  topic) and exporters (OTAP, perf, parquet, console, noop, error,
  topic, plus contrib azure_monitor / geneva) follow the same
  recipe: source-walk for config / state / effects / control-msgs /
  format chips, write a `gen_node_<name>.py` SPEC, and re-run.

The three slides are designed to be shown in sequence:

1. `experiments.svg` — *why* we care about conversion costs.
2. `otlp_bytes.svg`  — *what* the row format actually looks like on
   the wire.
3. `otap_tables.svg` — *how* OTAP rearranges those bytes into typed
   columns.

Per-node slides (`node_*.svg`) are a separate, parallel deck: one
slide per core/contrib node, each summarising the node's anatomy
using the shared visual vocabulary defined in `node_lib.py` and
described in the next section.

## Per-node slides (`node_lib.py` + `gen_node_<name>.py`)

Each core/contrib node gets a single SVG slide. The layout follows
a fixed visual grammar so that a viewer who learns to read one
slide reads them all the same way. Layout is centralised in
`node_lib.render_node_slide`; per-node scripts are pure data
(`NodeSlideSpec`). `gen_node_retry.py` is the canonical reference.

Page is `1600 × 850` with `MARGIN_X = 80`, `MARGIN_Y = 30` — these
and the rest of the slide grid live as `SLIDE_*` / `_NODE_*` /
`_CFG_*` / `_NOTES_*` constants near the top of
`node_lib.render_node_slide`'s section, and adjusting them re-flows
every per-node slide. Four distinct regions:

```text
┌──────────────────────────────────────────────────────────────────────┐
│  Title                                   ── colored rule ──    URN  │
│  italic subtitle                              cfg_field_1   Type1   │
│                                               cfg_field_2   Type2   │
│                                               cfg_field_3   Type3   │
│                                                                      │
│   PData                          node_name (bold)        ╔═══════╗  │
│   ───────────▶┌────────────────────────────────┐──────▶ [OTAP][OTLP]║ ctx ║│
│               │  state            effects       │                 ╚═══════╝│
│   Ack/Nack    │  local timer ...  notify_ack    │  ack/nack            │
│   ◀────────── │                   notify_nack   │ ◀──────────          │
│               │                   ...           │                      │
│               └────────────────────────────────┘                      │
│                ↑ ↓                  ↑ ↓                               │
│                control              ack/nack                          │
│                Variant1             Receive    ┌────── notes ───────┐ │
│                Variant2             Send       │ 1. ...             │ │
│                Variant3                        │ 2. ...             │ │
│                                                │ 3. ...             │ │
│                                                └────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

### Layout (locked)

This is the *visual contract* viewers rely on; the implementation
(coordinates, paddings, chip widths, etc.) is in
`node_lib.render_node_slide` and the `SLIDE_*` / `_NODE_*` / `_CFG_*`
/ `_NOTES_*` geometry constants above it. Per-node scripts never
hand-place anything.

Reading top-to-bottom, left-to-right:

1. **Title bar.** Large bold sans-serif title on the left (the
   node's short name); the URN in monospace on the right; a thin
   colored rule spanning the page underneath in the node's primary
   signal color (blue for OTAP, red for OTLP).
2. **Italic one-sentence subtitle**, left-justified under the title.
   One sentence: this is a tagline, not a description. The renderer
   wraps the subtitle to the **same pixel width as the notes box** in
   the lower-right corner, so a longer tagline grows downward into
   empty space rather than colliding with the upper-right config
   listing. (Width is derived from the same `_NOTES_X` constant used
   to place the notes box, so a future tweak re-flows both regions
   in lockstep.)
3. **Right-aligned config field listing**, just under the rule on
   the right margin. Two columns matching the calldata chip's
   layout below: **field name on the left** (bold mono, the
   primary identifier) and **Rust type on the right** (mono, soft
   grey, secondary). The type column is right-justified to the
   page margin; the name column is right-justified to a fixed gap
   left of the types so every type string lines up. Names are
   the exact identifiers from the node's `*Config` struct; types
   are the Rust types those fields hold (e.g. `Duration`, `f64`,
   `String`, `bool`).
4. **Node name above the box.** The node's short name (same as the
   slide title) in bold black sans, centered above the box.
5. **The node box.** Rounded rectangle, `720 × 320`, with a 6-px
   accent stripe on top in the primary signal color. Pushed down
   ~15 % of its height from where centering alone would put it.
   The interior is divided into **two columns**:
   - **Left column** — the **state / shared resources** list
     (upright sans, primary label color, top-left). One short
     phrase per holder, no parentheticals. The renderer
     automatically (a) strips any parenthetical clauses from each
     phrase and (b) sentence-cases the first letter, so SPECs can
     keep authoring lower-case bare phrases like
     `"split-off batch buffer (route_to)"` and the slide will
     render `Split-off batch buffer`. Filled in by the procedure
     documented under "State analysis" below.
   - **Right column** — the **effect-handler list**: every
     function the node calls on the `EffectHandler`, sorted
     alphabetically, in bold monospace. The cosmetic
     `_with_source_node` suffix is stripped on display.
   The two columns share the same top baseline and line height so
   the eye reads them as one structured exposition of the node's
   internals.
6. **Format chips on the port lines.** OTAP/OTLP are a property of
   the **port**, not of the box. They are drawn as filled rounded
   rectangles in the signal color (blue / red) with **inverse
   white** monospace text inside, anchored on the pdata line just
   outside the box edge:
   - **Output side (right of box):** OTAP first, then OTLP, only
     for variants the node can actually emit. Pure pass-through
     processors print both. Receivers and processors that emit but
     do not consume put their chips here.
   - **Input side (left of box):** same convention, but only the
     variants the node actually accepts. Exporters and processors
     that consume but do not emit put their chips here.
   - **Conversions** show different chips on each side (e.g. an
     OTLP→OTAP processor shows `[OTLP]` on the left and `[OTAP]`
     on the right).
   The choice of which formats to print is determined by analysing
   the source: does the node call `into_parts()` followed by
   `try_into()` on the `OtapPdata` payload to convert between
   `OtlpProto` and `OtapRecords`? Print every variant the node
   actually accepts/forwards.
7. **Four horizontal edges** on the box, all extending to the page
   margins:
   - **pdata in / pdata out** on the upper port row — thick edges,
     primary signal color, with a filled arrowhead on the
     downstream end.
   - **ack/nack in / ack/nack out** on the lower port row — thin
     grey edges with an open arrowhead.

   When a node declares **named output ports** (via the SPEC's
   `named_outputs` list — e.g. `signal_type_router` declares
   `["logs", "metrics", "traces", "default"]`), the right-side
   pdata edge fans out into N edges, centered on the canonical
   pdata-out y with a fixed 22 px step (auto-shrunk if the span
   would otherwise crowd the ack/nack rail; the cap keeps a 32 px
   gap above it). Each port name is rendered in small (10 px)
   monospace, anchored at the **box's right edge**, sitting in the
   gap **below** its line — so labels read evenly distributed
   between consecutive lines. The right-side `PData` row label
   tracks the topmost output line, so it stays above the fan-out
   band rather than landing under it. The single ack/nack rail
   and the format chips on the topmost edge stay unchanged:
   routing semantics live in the data plane, while the completion
   plane remains uniform.

   Currently five nodes use `named_outputs`:

   - `signal_type_router` — `logs`, `metrics`, `traces`, `default`
     (the three signals are fixed constants in the source; default
     is whatever the pipeline config wires as the default output).
   - `content_router` — illustrative `frontend`, `backend`,
     `default_output` (real port names are user-supplied via the
     `routes:` map plus optional `default_output`).
   - `fanout_processor` — illustrative `primary`, `secondary`,
     `fallback` (real port names come from each `destinations[]`
     entry, with optional per-port `fallback_for`).
   - `debug_processor` — illustrative `debug_a`, `debug_b` (only
     when `output_mode = Outports([...])` is configured).
   - `transform_processor` — illustrative `main`, `errors` (real
     port names come from `route_to(<port>, ...)` operators in the
     parsed query).

   Port-row labels are **role-aware** because the *non-engine* side
   of a receiver / exporter is talking to the network, not to
   another node. The renderer chooses:

   - **processor** — both rows uniform: left and right say
     `PData` / `Ack/Nack`. The whole picture is engine-internal.
   - **receiver** — *left* side is the transport (e.g.
     `gRPC / HTTP` on the upper row, `gRPC / HTTP status` on the
     lower row); *right* side is engine-facing (`PData` /
     `Ack/Nack`). The lower-left edge is therefore the response
     status the receiver writes back to the upstream client when
     `wait_for_result` is enabled, *not* an Ack/Nack to a peer
     node.
   - **exporter** — mirror image: *right* side is the transport,
     *left* side is engine-facing.

   Per-node SPECs override the transport text via
   `transport_pdata_label` and `transport_status_label`. Defaults
   are `request` / `response status` so a receiver/exporter that
   does not set them still reads correctly.
8. **Context (CallData) chip** floating above the outgoing pdata
   edge. A small dashed-bordered box listing the names and Rust
   types of the calldata fields the node attaches via
   `effect_handler.subscribe_to(...)`. The chip's right edge aligns
   with the right margin (matching the right edge of the config
   listing above). A short dotted leader drops vertically from the
   chip onto the outgoing pdata edge, with a clear gap above the
   format chips so they do not touch.
   Placement matters: a chip on the **right** = "this node
   *attaches* state going downstream"; a chip on the **left** would
   mean "this node *consumes* state arriving from upstream".
9. **Below-box: two columns of arrows + lists.**
   - **Control column (left).** A pair of thin-grey ↑/↓ arrows
     (same style as the horizontal ack/nack edges), an italic
     `control` label below the arrows, and a sorted bold-monospace
     list of the `NodeControlMsg` variant names the node handles
     **with a non-empty body**. Skip variants that fall through to
     `Ok(())` or are `unreachable!`. **Skip `Ack` and `Nack`**: the
     completion-queue / ack-nack flow is universal and automatic on
     every node and would just add noise.
     For `retry_processor` this column lists `CollectTelemetry`,
     `Config`, `DelayedData`.
   - **Ack/nack column (right).** A role-dependent set of
     thin-grey ↑/↓ arrows (same style as the control column,
     because ack/nack is also control plane), an italic `ack/nack`
     label below the arrows, and a short text list of `recv`
     and/or `send` entries. Arrows and entries encode the same
     information for redundancy:
     - **processor** — both ↑/↓ arrows; entries `recv`, `send`
     - **receiver**  — only ↑ arrow ; entry `recv` (downstream's
       responses arrive; receivers have no upstream peer to send
       ack/nack to)
     - **exporter**  — only ↓ arrow ; entry `send` (responses
       propagate to upstream; exporters have no downstream peer
       to receive ack/nack from)
     ↑ = receive (ack/nack arriving from downstream), ↓ = send
     (ack/nack propagating to upstream).

   ASCII reminder of the bottom region:

   ```text
   ┌── retry_processor ──────────────────────────────────┐
   │  STATE / SHARED RESOURCES   EFFECTS                 │
   │  local timer wheel          notify_ack              │
   │                             notify_nack             │
   │                             requeue_later           │
   │                             send_message            │
   │                             subscribe_to            │
   └─────────────────────────────────────────────────────┘
     ↑ ↓                           ↑ ↓
     control                       ack/nack
     CollectTelemetry              Receive
     Config                        Send
     DelayedData
   ```
10. **Operator notes (lower-right).** A lightly-shaded rounded box
    (`#f4f6f8` fill, fine `COLOR_CTRL_SOFT` outline, 10-px corner
    radius) holding a numbered list of operator-relevant notes
    about how the node behaves. The same style and chrome on
    every slide. Items are short factual sentences, not marketing
    copy. Naively word-wrapped.

### State analysis (where the node's state actually lives)

A node's `Processor`/`Receiver`/`Exporter` struct may have empty
operational fields and still be operationally stateful, because the
**engine holds state on the node's behalf** for some effect-handler
calls. The retry processor is the canonical example: its struct
holds only config + a precomputed delay table + a metrics handle,
but the node is *not* stateless — the engine is holding the
requeued payload in its local timer wheel.

To fill in the box's left-column state list, walk two places and
report what each contributes (one short phrase per line, **no
prefix tags, no parentheticals, omit empty categories entirely**):

1. **The node struct itself.** Inspect the node's
   `Processor`/`Receiver`/`Exporter` impl. List any fields that
   hold operational state — buffers, accumulators, queues, LRU
   caches, batch builders, in-flight maps, last-seen tables, etc.
   Do **not** list config, precomputed lookup tables derived from
   config, or telemetry/metrics handles. If there is nothing,
   contribute nothing.

2. **The engine, on the node's behalf.** Walk the effect-handler
   functions the node calls (the same list shown in the right
   column of the box). The relevant ones:
   - `requeue_later(when, data)` → the engine's **local timer
     wheel** (per-node, not the controller) holds the payload
     until the scheduled `Control::DelayedData` re-injection.
     Write `local timer wheel`.
   - Other effect handlers with engine-held resources should be
     added here as we encounter them.

Note that **`subscribe_to(...)` is not engine state**: the
calldata and any retained payload travel with the request through
the pipeline as part of the PData context, not as state stored by
the engine on the node's behalf. The right-side calldata chip
shows what the node attaches; it should not also appear in the
state column.

If both categories yield nothing, the left column is empty — that
is the node's correct characterization.

Local-vs-controller distinction for timers: the timer **wheel**
used by `requeue_later` is engine state local to the node task and
appears in the box's state list as `local timer wheel`. The
**periodic timer** delivered as `Control::TimerTick` is owned by
the pipeline controller. If a node subscribes to it, `TimerTick`
shows up in the **control column** below the box (alongside
`Config`, `Shutdown`, etc.).

For `retry_processor`, the analysis yields a single line:

```text
local timer wheel
```

Render this with `state_hint(x, y, lines)` from `node_lib.py`.

### Conventions worth preserving (per-node)

1. **Color = signal.** Same OTLP-red / OTAP-blue palette as the
   other slides; imported from `gen_diagram.py` via `node_lib.py`,
   never re-defined.
2. **OTAP/OTLP are port properties.** They never sit on the box
   itself; they always sit on a pdata line as inverse-white chips.
   OTAP comes first, OTLP second.
3. **Names, not prose.** Config fields, calldata fields, and effect
   functions are listed by their exact source-code identifiers in
   monospace. The viewer is expected to recognise them. Notes box
   is the only place for prose, and it stays short.
4. **Edges go to the page margin.** The horizontal pdata and
   ack/nack edges run from `MARGIN_X` to `PAGE_W - MARGIN_X`.
   Stubs that stop short imply something the slide does not mean.
5. **Right-side context = outgoing; left-side context = incoming.**
   If a future node both consumes and produces calldata, draw two
   chips, one on each side.
6. **One column for state, one column for effects.** Inside the
   box, state goes left and effects go right. Do not split the
   effects list by direction or interleave glyphs with names.
7. **State-list phrases are clean.** No parentheticals, no
   descriptions; the renderer strips any parentheticals
   defensively and sentence-cases the first letter. The
   procedure under "State analysis" produces exactly the phrases
   that appear in the left column. Empty categories are omitted
   (no `node: none` placeholder). An empty struct does not mean
   a stateless node — call out engine-held state explicitly when
   it exists.
8. **Notes box is the same on every slide.** Position (lower-right
   corner with comfortable margin from the node box), shading,
   outline, corner radius, numbering style — all locked. Only the
   item text varies per node.
9. **Globals belong in `node_lib.py`.** Anything that should look
   the same on every slide (port-row labels, notes-box chrome,
   state hint, format chips, controller-interaction list, …) is a
   `node_lib` helper. Per-node scripts pass data, not styling.
10. **Skip what is universal.** The completion queue and
    `Ack`/`Nack` control flow are present on every node; do not
    list them in the control column. Variants whose handler is
    `Ok(())` or `unreachable!` are also skipped — only
    `NodeControlMsg` variants the node handles with a meaningful
    non-empty body appear in the control list.

### Library (`node_lib.py`)

`node_lib.py` is the **layout master** for every per-node slide. A
per-node generator declares a `NodeSlideSpec` (pure data) and calls
`render_node_slide(spec)`; layout, geometry, and chrome all live in
the library. Visual tweaks happen in **one place** (the geometry
constants and `render_node_slide` near the bottom of `node_lib.py`)
and re-flow every slide.

Exports:

- **Slide master** — `NodeSlideSpec` (dataclass) and
  `render_node_slide(spec) -> str`. The single entry point per-node
  scripts should use. Geometry constants prefixed `SLIDE_` /
  `_NODE_*` / `_CFG_*` / `_NOTES_*` are the design-system knobs.
- **Palette** — `COLOR_OTAP`, `COLOR_OTLP`, `COLOR_CTRL`,
  `COLOR_CTRL_SOFT`, `COLOR_CTX`, `COLOR_OK`, `COLOR_FAIL`, plus
  typography constants (`FS_TITLE`, `FS_SUBTITLE`, `FS_NODE`,
  `FS_NODE_SUB`, `FS_LABEL`, `FS_TINY`). OTAP/OTLP colors are
  imported from `gen_diagram.py` so the whole deck re-flows from a
  single palette change.
- **Page chrome** — `page_open`, `page_close`, `title_bar`,
  `arrow_marker_defs`.
- **Primitives** (used internally by `render_node_slide`, also
  available for one-off panels) — `node_box`, `pdata_edge`,
  `ctrl_edge`, `signal_chip`, `node_name_label`, `state_hint`,
  `effect_list`, `controller_list`, `mono_list`, `control_column`,
  `ack_column`, `context_chip`, `port_row_labels`, `notes_box`.
- **Misc / leftover** — `curvy_loop`, `puck`, `callout`,
  `mini_panel`, `glyph_check`, `glyph_cross`, `glyph_hourglass`
  remain available for future per-node panels but are not part of
  the locked grammar.

A per-node script is a small data file:

```python
from node_lib import NodeSlideSpec, render_node_slide

SPEC = NodeSlideSpec(
    name="my_processor",
    urn="urn:otel:processor:my",
    subtitle="One-line description.",
    config_fields=[("field", "Type"), ...],
    state=["..."],
    effects=["..."],          # sorted alphabetically
    role="processor",         # | "receiver" | "exporter"
    output_formats=["OTAP", "OTLP"],
    calldata=[("name", "Type"), ...],   # optional
    control_msgs=["..."],     # NodeControlMsg variants with body
    notes=["..."],            # operator-relevant facts
    named_outputs=["..."],    # optional; right-side fan-out per port
)

if __name__ == "__main__":
    open("node_my.svg", "w").write(render_node_slide(SPEC))
```

`gen_node_retry.py`, `gen_node_batch.py`, and
`gen_node_transform.py` are the canonical references.

## Engine architecture slides (`gen_engine_group.py`, `gen_engine_core.py`)

These two slides describe the *runtime* — where threads live, which
tasks run on which thread, and which channels cross thread
boundaries. They are a parallel deck to the per-node slides and reuse
`node_lib`'s palette, title bar, notes box, and arrow markers so they
read like part of the same family.

The pair is meant to be shown in zoom-in order:

1. `engine_group.svg` — *one pipeline group at runtime*: the
   controller process (with its accessory `spawn_thread_local_task`
   tiles — `process-memory-limiter`, `metrics-aggregator`,
   `metrics-dispatcher`, `observed-state-store`, `engine-metrics` —
   plus shared resources `DeclaredTopics(TopicBroker)` and the
   `memory_pressure` watch sender), the separate `http-admin` thread
   reachable via `Arc<dyn ControlPlane>`, and the N per-core
   `RuntimePipeline` instance tiles. The blue banner across the top
   of the right column states the invariant out loud:
   *"one OS thread → one tokio current_thread runtime → one core"*.
2. `engine_core.svg` — *zoom in to one core*: a single big
   blue-bordered box represents the OS thread + its
   `tokio current_thread` + `LocalSet` (`!Send` node tasks). Inside
   the box: `RuntimeCtrlMsgManager` and
   `PipelineCompletionMsgDispatcher` on top, a representative
   receiver→processor→exporter chain in the middle, and the per-core
   `TopicSet` view at the bottom. Boundary stubs identify every
   channel that crosses out of the thread.

### Conventions specific to these slides

1. **Color extends to the runtime plane.** OTAP-blue is reused as
   the accent for "this is a pdata-carrying pipeline thread" — both
   the per-core instance tiles on the group slide and the outer
   thread frame on the per-core slide use it. Neutral grey is the
   accent for the controller process; the warm context-chip color
   is the accent for the admin thread (a non-pdata accessory
   process).
2. **Edge style = scope.** Thick colored = pdata; thin solid grey =
   in-thread control / ack-nack; thin **dashed** grey = cross-thread
   tokio channel (e.g. `RuntimeCtrlMsgSender`, `memory_pressure`
   watch, `topics`); dotted thin = "weak handle / observation" (e.g.
   `note_instance_exit`). The dashed-vs-solid distinction is the
   only way the picture marks a thread boundary.
3. **Accessory tiles.** Inside the controller box, each
   `spawn_thread_local_task` is rendered as a compact pill with the
   task name in monospace on the left and a one-line role on the
   right. Each pill is itself an OS thread + a tokio
   `current_thread` runtime — the tile chrome is intentionally
   plain to avoid implying any are pipelines.
4. **Per-core tile shape is repeated.** The group slide draws three
   instances (Core 0, Core 1, vertical ellipsis, Core N) — the
   pattern of channels between controller and one core is drawn in
   full only against Core 0; Core 1 and Core N receive short
   unlabelled stubs to communicate "every core has the same
   relationship". Adding more cores means adding more stubs, never
   more channel types.
5. **Boundary stubs name the Rust channel type.** On the per-core
   slide, every line that crosses the outer thread frame is labelled
   with the Rust handle that owns that crossing
   (`RuntimeCtrlMsgReceiver`, `PipelineCompletionMsgSender`,
   `note_instance_exit (Weak)`, `memory_pressure (watch) rx`,
   `topics (TopicBroker)`). Anything not crossing the frame is
   in-thread and stays unlabelled at the boundary.
6. **One source-of-truth comment block** sits at the top of each
   generator listing the file:line citations the slide is built
   from. When the runtime layout changes, update those citations
   and re-run.


## OtapPdata anatomy slide (`gen_otap_pdata.py`)

A single, deliberately sparse slide that opens up the value carried on
every pdata edge in the per-node deck. Reuses `node_lib`'s palette
and chrome.

Composition:

1. **One `OtapPdata` outer box** with the OTAP-blue accent stripe,
   containing both halves side-by-side.
2. **`context: Context` column (left)** — the frame stack drawn as
   a `top` `Frame` tile, three small geometrically-centered dots
   (not the unicode `⋮`, whose vertical anchor is unreliable), and
   a `bottom` `Frame` tile. Each tile shows example `node_id` and
   `interests` values so the reader sees what a frame *contains*,
   not just that it exists. Underneath, two slim chips for
   `transport_headers: Option<TransportHeaders>` and
   `flow_compute_ns: Option<NonZeroU64>`. The top frame is painted
   in the OTAP accent color so the eye lands on it first.
3. **`payload: OtapPayload` column (right)** — two large variant
   tiles stacked vertically. The upper tile reads
   **`OTAP records`** in big bold (blue), the lower tile reads
   **`OTLP bytes`** (red). Both tiles carry an inverse-white
   `[OTAP]` / `[OTLP]` format chip in the upper-right and the
   tag-line *"reference-counted · zero-copy transit"*. Between
   them sits a small **`one of`** badge with short connecting
   leaders, making the tagged-enum invariant explicit: the
   payload field is exactly one of the two, never both.
4. **`Frame` anatomy panel (below the box)** — full source-
   faithful field listing on the left (`node_id: usize`,
   `interests: Interests`, `route.calldata: CallData =
   SmallVec<[Context8u8; 3]>`, `route.entry_time_ns: u64`,
   `route.output_port_index: u16`) and a 2 × 4 grid of all eight
   `Interests` bitflag chips on the right, each labelled with
   its identifier and bit position (`1<<0` ... `1<<7`).

### Conventions specific to this slide

1. **No notes box, no parentheticals.** Everything that needs
   saying is said by a label or a colored chip.
2. **Payload variants live inside the OtapPdata box**, not in a
   separate panel — the slide's point is that Context and Payload
   are two halves of the *same* value.
3. **Color = format on the payload side**, same as the per-node
   slides: blue = OTAP, red = OTLP. Both variants are equally
   prominent because both are common in production pipelines.
4. **Stack as top + ellipsis + bottom.** The slide does not try to
   show every frame; it shows that there is a stack with a top and
   a bottom, and which one is popped first on Ack/Nack (the top,
   in OTAP-blue).
