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
     grey edges with an open arrowhead. (Some nodes have **named
     out-ports** here, producing additional lines on the right
     side; the same vocabulary still applies.)
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
)

if __name__ == "__main__":
    open("node_my.svg", "w").write(render_node_slide(SPEC))
```

`gen_node_retry.py`, `gen_node_batch.py`, and
`gen_node_transform.py` are the canonical references.

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

The three slides are designed to be shown in sequence:

1. `experiments.svg` — *why* we care about conversion costs.
2. `otlp_bytes.svg`  — *what* the row format actually looks like on
   the wire.
3. `otap_tables.svg` — *how* OTAP rearranges those bytes into typed
   columns.
