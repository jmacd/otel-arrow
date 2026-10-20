# Batch Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:batch` (`urn:otel:processor:batch`)
- Feature gate: Always enabled
- Stability: Experimental

## Overview

The batch processor combines native OTAP records and independently decodable
encoded batches before forwarding them downstream. Codecs may provide native
merge/split support; otherwise compatible item-based settings use OTAP fallback.
The processor owns buffering, flush deadlines, backpressure, and ACK/NACK routing.

Batches are sized against the configured range of acceptable sizes,
`[min_size, max_size]`. After preparation in the plan's working representation,
inputs already in that range bypass buffering and splitting. Matching encodings keep their original
bytes; fallback or an explicit output format can still require conversion. When
partitioning is enabled, both paths project context to the selected entries.

## Flush behavior

A flush of pending items is triggered by either:

- New incoming data that pushes the total buffered amount over the `min_size`
- The current batching window expires, the duration of which is set by `max_batch_duration`

When a flush is triggered, items are split if they're over the size or concatenated
with others if they're under the size. When assigning data from input batches
to output batches, the batch processor works in the order the data was received,
so that older data are flushed first.

Note: After resizing data during a flush, there may be some leftover data that
is under the configured `min_size`. This data is retained in the pending buffer,
but is first in line for the next flush. The implication is that data from any
single batch may be buffered for at most `2*max_batch_duration`.

## Getting Started

Configure format-specific sizing and the maximum time to hold pending data:

```yaml
type: processor:batch
config:
  max_batch_duration: 500ms
  format: preserve
  otap:
    min_size: 8192
    max_size: null
    sizer: items
  otlp:
    min_size: 1048576
    max_size: null
    sizer: bytes
```

## Configuration

```yaml
type: processor:batch
config:
  # Batch sizing for OTAP records (defaults are format-specific).
  otap:
    min_size: 8192 # Lower bound of the acceptable size range; defaults to max_size when null.
    max_size: null # Optional upper bound.
    sizer: items # OTAP supports "items" only.

  # Batch sizing for OTLP bytes (defaults are format-specific).
  otlp:
    min_size: 262144
    max_size: null
    sizer: bytes # Native OTLP batching uses bytes; items selects OTAP fallback.
    max_split_fragments: 65536 # Cap on fragments per oversize entry (OTLP only).
    max_split_overhead_bytes: 8388608 # Cap on duplicated wrapper bytes per oversize entry (OTLP only).
    max_split_fragments_per_flush: 65536 # Greedy per-flush split threshold (OTLP only).

  # Maximum time before flushing pending data (default: 200ms).
  max_batch_duration: 500ms

  # Pending request tracking limits.
  inbound_request_limit: 1024
  outbound_request_limit: 512

  # Optional: include a whole composite in the partition key.
  partition_by:
    - type: composite
      name: product_user
  # Buffered keys per signal and batching plan, including missing values.
  max_active_partitions: 1024

  # Output: "otap", "otlp", a registered codec name, or "preserve" (default).
  format: preserve
```

### Codec settings and output selection

`format: preserve` keeps an encoding when its codec supports the requested
batching unit. Otherwise, item-based batching decodes to OTAP and emits OTAP.
It does not promise byte preservation for every codec or configuration. Choosing
an explicit format converts to that output representation, re-encoding after
OTAP fallback when needed. Selecting an encode-only output codec is supported;
selecting a decode-only codec as an explicit output is rejected at startup.

Use `codecs` to configure batching by registered encoding name. The `otap` and `otlp`
blocks remain compatible aliases; declaring settings for the same format twice is an error.
For example, the following is equivalent to the legacy OTLP settings:

```yaml
type: processor:batch
config:
  format: otlp
  codecs:
    otlp-bytes:
      min_size: 262144
      max_size: 1048576
      sizer: bytes
```

The available names depend on the codecs linked into the binary. Unknown names,
unsupported sizing, and missing output capabilities fail at startup. Byte
sizing measures the complete encoded request, including format wrappers and
intrinsic compression; HTTP/gRPC framing and transport compression are excluded.
OTAP fallback supports item sizing, not a prediction of final encoded byte size.
Batch size thresholds and split budgets are unsigned 64-bit values. A batcher
caps upper bounds at its internal size limit rather than truncating them; large
configured limits do not allocate buffers in advance.
Outputs remain independently decodable: native batching does not imply that any
encoding can be split at arbitrary byte boundaries or concatenated blindly.

Each format object contains:

- `min_size`: lower bound of the acceptable batch size, or `null`. When `null`
  it defaults to `max_size`. `0` is allowed and means any input up to
  `max_size` is forwarded as-is. Defaults: 8192 items (OTAP), 262144 bytes
  (OTLP).
- `max_size`: optional non-zero upper bound, or `null` for no upper bound. Must
  be `>= min_size` when both are set.
- `sizer`: the unit `min_size` and `max_size` are measured in. OTAP supports
  `items` (log records, spans, or metric data points); OTLP supports `bytes`
  (encoded request size).
- `max_split_fragments` (OTLP bytes only): non-zero cap on how many fragments a
  single oversize resource entry may split into, or `null` for unbounded
  (default 65536, a power-of-two backstop). Splitting an entry that exceeds
  `max_size` re-encodes the resource/scope
  headers around each fragment, so a tiny `max_size` relative to one indivisible
  input could fan out into very many fragments. When the projected fragment
  count exceeds this budget the entry is emitted whole (best-effort, possibly
  exceeding `max_size`) and counted by the `split.budget.fallbacks` metric.
- `max_split_overhead_bytes` (OTLP bytes only): non-zero cap on how many
  duplicated wrapper bytes a single oversize resource entry may amplify into, or
  `null` for unbounded (default 8 MiB). Because each fragment re-encodes the
  resource/scope headers, a large header split across many records can amplify
  output far beyond the input even when the fragment count stays under
  `max_split_fragments`. The amplification is measured from the _actual_ greedy
  packing (the emitted fragments' total bytes minus one whole encoding of the
  entry), not a per-record worst case, so many small records under a large
  header are not falsely collapsed. When it exceeds this budget the entry is
  emitted whole (best-effort, possibly exceeding `max_size`) and counted by the
  same `split.budget.fallbacks` metric; emission also aborts early once the
  running amplification passes the budget, bounding transient memory. This is
  _measured_ from the actual packing as the entry is split -- unlike
  `max_split_fragments`, it is not projected up front.
- `max_split_fragments_per_flush` (OTLP bytes only): non-zero greedy threshold on
  the number of output batches a single flush may build from splitting, or `null`
  for unbounded (default 65536). The two budgets above bound each entry
  individually, but a flush containing many large entries builds its entire
  output vector in memory before anything is sent, so their combined split
  fan-out could still amplify into a very large allocation. Once a flush has
  produced this many output batches, any further oversize entry is emitted whole
  (best-effort, counted by `split.budget.fallbacks`) instead of split. This is a
  simple greedy running threshold on _split fan-out_, not a strict total-output
  cap. The total output can exceed the threshold in two ways. First, the entry
  that crosses the threshold may still add its full per-entry fan-out, so split
  amplification is bounded by roughly this threshold plus one entry's
  `max_split_fragments`. Second, every remaining oversize or indivisible entry is
  still emitted whole, and each such entry contributes at least one output batch;
  the threshold bounds only the _additional_ split fan-out, not this mandatory
  output floor of (at least) one batch per remaining top-level entry. It does not
  look ahead over later entries and is independent of Ack/Nack outbound-slot
  accounting (which governs _sending_, not up-front allocation).

### Context partitioning

Omit `partition_by` (or use `[]`) to retain unpartitioned behavior. Otherwise,
configure a list of typed context selectors. Every selector contributes to
**one ordered partition key**; these are not first-match rules:

```yaml
partition_by:
  - type: named
    named: [workspace_id, "product_user:customer"]
  - type: authorized_identity
    named: [customer_id]
```

- `type: named` selects transport-header primitives by stored name, or composite
  members using `composite:member`. A qualified member's domain comes from its
  composite declaration; it may be an identity, header, constant, or randomness value.
- `type: authorized_identity` selects verified primitive claims by stored name.
  It accepts unqualified primitive names only; use `type: named` for composite members.
- `type: composite` selects every member of the composite identified by `name`.

```yaml
partition_by:
  - type: composite
    name: product_user
```

Composites must be declared under `policies.context.entries`, for example:

```yaml
policies:
  context:
    entries:
      product_user:
        - type: authorized_identity
          name: customer_id
          store_as: customer
        - type: transport_header
          name: workspace_id
          store_as: workspace
```

Primitive domains are determined by selector types, never inferred from names.
References are bound at startup; unknown composites or members, empty `named`
lists, and untyped selector shorthands are rejected. Overlapping selections
contribute in configured order, but do not duplicate context fields in outputs.
Per-partition sizing overrides are not supported.

Capture the primitive sources using the corresponding transport-header and
authorized-identity policies. A composite is present only when all its members
are available and all its conditions match. Batching reads the existing
materialized snapshot; it does not regenerate randomness or re-evaluate
conditions.

Equal ordered value tuples share a buffer. Value kinds, cardinality, and member
boundaries are significant; original transport wire names are not. Each absent
reference contributes a missing marker in its own position, distinct from a
present empty value. Thus `(customer A, missing)` and `(customer B, missing)`
remain separate keys. A qualified member is absent when its enclosing composite
is absent, even if the corresponding primitive exists. A whole-composite
selection has one atomic presence marker for all its members.

`max_active_partitions` defaults to 1024 and must be positive. It applies
independently to each signal and format and includes keys containing missing values.
Excess new buffered keys are Nacked and counted by `nacked.partition.limit`;
existing keys remain admissible. Draining a partition releases its capacity.
Completion-tracking limits remain shared across partitions within each signal
and format. The partition cap is not a byte-memory limit.

Size flushes affect only the selected key. Each signal/format uses one batching
window: its timer flushes all buffered keys, and flushing one key for size does
not postpone that window for other keys. Shutdown drains every partition.

Partitioned outputs carry only selected primitives and composite members.
Selecting a member does not retain unselected siblings; retained members keep
their snapshot values and originating declaration. Unrelated headers, claims,
composites, original wire names, and peer addresses are omitted. Missing
references do not erase other selected values; only all-missing keys produce
empty request metadata. Configure
downstream header propagation with stored names rather than `name: preserve`.
This projection also applies to retained remainders and in-range passthrough
inputs. Passthrough preserves completion routing and consumes neither a
buffered-key slot nor a completion-tracking slot.

### Validation rules

- At least one of `min_size` or `max_size` must be set.
- `max_size`, when set, must be non-zero and `>= min_size`.
- A non-zero `min_size` requires a non-zero `max_batch_duration`, otherwise
  buffered data below `min_size` could wait forever.
- With `max_batch_duration: 0s`, `max_size` must be set unless `min_size` is
  explicitly `0`.
- `min_size: 0` with no `max_size` is accepted but logs a
  `batch.config.forwards_everything` warning: every input is forwarded as-is and
  no batching is performed.

## Batching Behavior

Sizes are measured in the format's sizer unit. Signals (logs, metrics, traces)
and distinct batching plans are buffered independently. Inputs using the same
OTAP fallback plan can share a buffer. Below, `min` is the
effective lower bound (`min_size`, or `max_size` if `min_size` is unset) and
`max` is `max_size` (infinite if unset).

### When an input arrives

Preparation first applies any necessary conversion to the working representation.
Codec conversion failures permanently Nack the recovered input. The following
rules then use the working representation's size:

1. **Empty input** (size 0): acknowledged immediately and dropped.
2. **Already acceptable** (`min <= size <= max`): forwarded immediately in the
   requested output representation, with its original request context. It is not
   buffered, merged, or split, and downstream Ack/Nack go straight to the original sender without
   using the processor's request-tracking slots. Any data already buffered is
   left in place.
3. **Otherwise** (smaller than `min`, or larger than `max`): appended to the
   pending buffer. If this is the first pending data, the flush timer starts.
   If the pending total reaches `min` (or `max_batch_duration` is `0s`), a size
   flush runs immediately.

### Flushing

A flush happens when pending data reaches `min` (size flush), when
`max_batch_duration` elapses after the oldest pending data arrived (timer
flush), or at shutdown.

- All pending data is merged in arrival order. If `max_size` is set,
  it is split toward `max_size`; indivisible encoded entries or split-budget
  fallbacks can exceed that best-effort bound. Items are packed to fill
  each output to exactly `max_size` when the data allows.
- If `max_size` is unset, everything pending is merged into a single output with
  no upper bound.

Note: Batch construction follows a greedy algorithm to seal batches and some
signal items with some sizers (such as metrics with an item sizer) are
non-uniform in size and either cannot be or are currently not split. `min_size`
and `max_size` should be treated as lower and upper bounds respectively.

### Example configurations

Allow a range of sizes to balance between output batch size and work done by the
batch processor.

```yaml
type: processor:batch
config:
  max_batch_duration: 200ms
  format: otap
  otap:
    min_size: 8192
    max_size: 16384
    sizer: items
```

Batches of exactly 1000 items (except timer/shutdown flushes):

```yaml
type: processor:batch
config:
  max_batch_duration: 200ms
  format: otap
  otap:
    min_size: 1000 # Note: This is the same as excluding min_size!
    max_size: 1000
    sizer: items
```

Only split oversized requests, forwarding everything else unchanged:

```yaml
type: processor:batch
config:
  max_batch_duration: 200ms
  format: otap
  otap:
    min_size: 0
    max_size: 8192
    sizer: items
```

Only enforce a minimum batch size, items already over this amount are passed
through.

```yaml
type: processor:batch
config:
  max_batch_duration: 200mss
  format: otap
  otap:
    min_size: 8192
    sizer: items
```

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

#### `otap.processor.batch`

| Metric                                               | Unit        | Description                                                                                                                                                                                            |
| ---------------------------------------------------- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `otap.processor.batch.consumed_batches_logs`         | `{item}`    | Total batches consumed for logs signal.                                                                                                                                                                |
| `otap.processor.batch.consumed_batches_metrics`      | `{item}`    | Total batches consumed for metrics signal.                                                                                                                                                             |
| `otap.processor.batch.consumed_batches_traces`       | `{item}`    | Total batches consumed for traces signal.                                                                                                                                                              |
| `otap.processor.batch.produced_batches_logs`         | `{item}`    | Total batches produced for logs signal.                                                                                                                                                                |
| `otap.processor.batch.produced_batches_metrics`      | `{item}`    | Total batches produced for metrics signal.                                                                                                                                                             |
| `otap.processor.batch.produced_batches_traces`       | `{item}`    | Total batches produced for traces signal.                                                                                                                                                              |
| `otap.processor.batch.flushes_size`                  | `{flush}`   | Number of flushes triggered by size threshold (all signals)                                                                                                                                            |
| `otap.processor.batch.flushes_timer`                 | `{flush}`   | Number of flushes triggered by timer (all signals)                                                                                                                                                     |
| `otap.processor.batch.passthrough.batches`           | `{batch}`   | Number of inputs forwarded as-is, without re-batching or completion tracking, because their size was already within `[min_size, max_size]` on arrival.                                                 |
| `otap.processor.batch.flush_pending_requests`        | `{request}` | Number of input requests pending at flush time.                                                                                                                                                        |
| `otap.processor.batch.flush_pending_bytes`           | `By`        | Number of bytes pending at flush time when byte size is known.                                                                                                                                         |
| `otap.processor.batch.flush_age_duration`            | `ns`        | Time from first pending input arrival to actual flush start.                                                                                                                                           |
| `otap.processor.batch.flush_timer_lateness_duration` | `ns`        | Delay between scheduled timer wakeup and actual timer flush start.                                                                                                                                     |
| `otap.processor.batch.flush_output_batches`          | `{batch}`   | Number of output batches emitted by each flush.                                                                                                                                                        |
| `otap.processor.batch.flush_output_bytes`            | `By`        | Number of bytes emitted by each flush when byte size is known.                                                                                                                                         |
| `otap.processor.batch.dropped_conversion`            | `{msg}`     | Number of messages dropped due to conversion failures.                                                                                                                                                 |
| `otap.processor.batch.batching_errors`               | `{error}`   | Number of batches for which errors encountered.                                                                                                                                                        |
| `otap.processor.batch.nacked_inbound_slots`          | `{msg}`     | Number of requests nacked due to inbound slot exhaustion.                                                                                                                                              |
| `otap.processor.batch.nacked_outbound_slots`         | `{msg}`     | Number of requests nacked due to outbound slot exhaustion.                                                                                                                                             |
| `otap.processor.batch.nacked.partition.limit`        | `{msg}`     | Requests Nacked because a new buffered context key exceeded the active partition limit.                                                                                                                |
| `otap.processor.batch.split_budget_fallbacks`        | `{entry}`   | Number of oversize resource entries emitted whole because splitting would have exceeded `max_split_fragments`, `max_split_overhead_bytes`, or the per-flush `max_split_fragments_per_flush` threshold. |

### Events

| Event                              | Severity | Description                                                                                                                                      |
| ---------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `batch.config.forwards_everything` | WARN     | Emitted at startup for each format configured with `min_size: 0` and no `max_size`; every input is forwarded as-is and no batching is performed. |

## Limits

- `max_size`, when set, must be non-zero. `min_size` may be `0`.
- `bytes` sizing depends on payload formats that can report encoded size.
- `max_batch_duration: 0s` disables time-based accumulation and flushes
  immediately.

## Related Docs

- [Configuration model](../../../../../docs/configuration-model.md)
- [Processor taxonomy](../../../../../docs/processors.md)
- [Core node catalog](../../../README.md)
