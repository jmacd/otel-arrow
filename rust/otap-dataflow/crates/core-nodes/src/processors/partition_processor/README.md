# Partition (Split-by-Key) Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:partition` (`urn:otel:processor:partition`)
- Feature gate: Default
- Stability: Experimental
- Signals: metrics or traces (selected by `key`); other signals pass through
  unchanged

## Overview

The partition processor is **Layer A (split-by-key)** of the partition-dispatch
design in
[`docs/durable-dispatch-topic-design.md`](../../../../../docs/durable-dispatch-topic-design.md).
For every incoming OTAP batch of the configured key's target signal it splits
the batch by key into `num_partitions` sub-batches such that **all rows sharing a
key value land in the same partition**, and emits each sub-batch tagged with its
integer partition index (carried on the request context). A downstream
partition-dispatch hop (Layer C) routes each sub-batch to its owner by that tag
without re-deriving the key -- the A->C contract (decision D25).

Splitting reuses the OTAP selection-mask cascade (`partition_otap_batch`), the
same machinery the admission processor's time-window filter uses: a partition
index is computed per *root* row (`partition = part_fn(key) & (N - 1)`), then a
cascade pass per present partition prunes the child tables (attributes, data
points, exemplars, events, links) by parent-id integrity.

The per-signal partition function follows ingest-queue decision D3:

- `metric_name`: hash the `name` column on the metrics root (names are
  low-cardinality and skewed, so hashing spreads them).
- `trace_id`: slice the low 56 bits (right-most 7 bytes) of the `trace_id`
  column on the spans root (those bits are uniform by the OpenTelemetry trace-id
  randomness definition, so no hash is needed and a whole trace stays together).

This is the in-memory core (decision D28): durability is an additive backend
behind the same dispatch interface and is added later (Layer B), so the split is
durability-independent.

## Getting Started

```yaml
type: processor:partition
config:
  key: metric_name   # or trace_id
  num_partitions: 16 # power of two
```

A shuffle-by-name pipeline (metrics):

```yaml
nodes:
  receiver:
    type: "urn:otel:receiver:otlp"
  partition:
    type: "urn:otel:processor:partition"
    config:
      key: metric_name
      num_partitions: 16
  exporter:
    type: "urn:otel:exporter:otap"
connections:
  - from: receiver
    to: partition
  - from: partition
    to: exporter
```

## Configuration

| Field            | Type                        | Default      | Description                                                 |
| ---------------- | --------------------------- | ------------ | ----------------------------------------------------------- |
| `key`            | `metric_name` \| `trace_id` | *(required)* | The key dimension to split by, and hence the target signal. |
| `num_partitions` | integer > 0                 | *(required)* | The number of partitions `N`; must be a power of two.       |

## Telemetry

Metric set `processor.partition`:

| Metric                | Unit      | Description                                                         |
| --------------------- | --------- | ------------------------------------------------------------------- |
| `batches`             | `{batch}` | Batches of the target signal that were split by key.                |
| `passthrough_batches` | `{batch}` | Batches of other signals forwarded unchanged.                       |
| `malformed_batches`   | `{batch}` | Target-signal batches that could not be split, forwarded unchanged. |
| `sub_batches`         | `{batch}` | Partition-tagged sub-batches emitted across all split batches.      |

## Notes and limitations (in-memory core)

- **Acknowledgement semantics.** The request context (its ack/nack subscribers)
  rides the **first** emitted partition only; the remaining partitions carry a
  detached context that still propagates transport metadata (tenant headers,
  peer address) but no ack subscription. Full per-partition ack/nack fan-in --
  acking the input once *all* partitions resolve -- is deferred to the
  dispatch/slot integration (decision D22), matching the loss-tolerant default
  of the in-memory profile (ingest-queue D6).
- **Routing is not yet performed here.** This node only splits and tags; the
  partition-dispatch delivery mode that pins each partition to an owner is Layer
  C, built next.
- **Standalone logs** (no `trace_id`) are not split; only metrics (`metric_name`)
  and traces (`trace_id`) are supported. The standalone-logs projection is
  decided (D26) but deferred until logs are addressed.
- **Hash stability.** The metric-name hash is deterministic within a process,
  which is all the in-memory core requires; cross-process/cross-version
  stability becomes relevant only with durable placement (Layer B).
