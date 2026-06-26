# Metrics Admission Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:metrics_admission` (`urn:otel:processor:metrics_admission`)
- Feature gate: Default
- Stability: Experimental
- Signals: metrics (logs and traces pass through unchanged)

## Overview

The metrics admission processor is the admission front-end (phase 0) of the
vertically-integrated ingest queue described in
[`docs/ingest-queue-design.md`](../../../../../docs/ingest-queue-design.md). For
every incoming OTAP metrics batch it:

1. **Records type identity.** Each metric's canonical *type descriptor*
   (instrument type, monotonicity, unit) is recorded in a per-core registry (the
   "manifest"). The first descriptor seen for a name becomes its primary; a
   later, differing descriptor is a **name conflict**. By default both streams
   are admitted and a `name_conflict` warning is emitted once per conflicting
   name -- matching the OpenTelemetry metrics SDK's instrument-conflict
   handling. An optional strict mode rejects the conflicting points instead.

2. **Applies the admission time window.** Data points whose event time falls
   outside `[now - max_lag, now + max_skew]` are dropped and counted. This
   crude, protective bound caps backfill into durable storage (too-old points)
   and rejects bad future clocks (too-future points). The downstream event-time
   windowing layer performs the fine-grained completeness estimation within this
   envelope.

Temporality and value type (int vs double) are **non-manifesting**: a difference
in either is never treated as a conflict. Admitted data is forwarded downstream
unchanged in representation (OTAP in, OTAP out).

This phase-0 implementation is single-core: the registry lives in memory, one
per processor replica. The thread-per-core model and (phase 1) shuffle by metric
name make each core the sole authority for its names, so no cross-core
coordination is required.

## Getting Started

```yaml
type: processor:metrics_admission
config:
  max_lag: 1h        # reject points older than now - max_lag
  max_skew: 5m       # reject points further ahead than now + max_skew
  strict_conflicts: false
```

A minimal admit-and-forward pipeline:

```yaml
nodes:
  receiver:
    type: "urn:otel:receiver:otlp"
  admission:
    type: "urn:otel:processor:metrics_admission"
    config:
      max_lag: 1h
      max_skew: 5m
  exporter:
    type: "urn:otel:exporter:otap"
connections:
  - from: receiver
    to: admission
  - from: admission
    to: exporter
```

## Configuration

| Field              | Type     | Default | Description                                                                 |
| ------------------ | -------- | ------- | --------------------------------------------------------------------------- |
| `max_lag`          | duration | `1h`    | Reject data points whose event time is older than `now - max_lag`.          |
| `max_skew`         | duration | `5m`    | Reject data points whose event time is further ahead than `now + max_skew`. |
| `strict_conflicts` | bool     | `false` | Reject the points of metrics that conflict with the recorded primary.       |

## Telemetry

Metric set `processor.metrics_admission`:

| Metric                         | Unit        | Description                                                |
| ------------------------------ | ----------- | --------------------------------------------------------- |
| `batches`                      | `{batch}`   | Metrics batches processed.                                |
| `batches_malformed`            | `{batch}`   | Batches forwarded unchanged because they could not be parsed/filtered. |
| `points_admitted`              | `{point}`   | Data points admitted.                                     |
| `points_rejected_too_old`      | `{point}`   | Data points rejected as older than `now - max_lag`.       |
| `points_rejected_too_future`   | `{point}`   | Data points rejected as further than `now + max_skew`.    |
| `points_rejected_malformed`    | `{point}`   | Data points rejected for other reasons (e.g. null time).  |
| `points_rejected_conflict`     | `{point}`   | Data points rejected by strict conflict mode.             |
| `names_registered`             | `{metric}`  | Distinct metric names recorded for the first time.        |
| `name_conflicts`               | `{conflict}`| Distinct metric names newly observed to conflict.         |

## Notes and limitations (phase 0)

- The admission window uses wall-clock time (`SystemTime::now`).
- Strict conflict mode drops points by metric **name**; in the rare case that a
  single batch carries both the primary and a conflicting descriptor for one
  name, both are dropped. Across batches (the common conflict case) only the
  conflicting descriptor's points are dropped.
- Tenancy is a single constant tenant (ingest-queue D9 default); the per-tenant
  projection arrives with later phases.
