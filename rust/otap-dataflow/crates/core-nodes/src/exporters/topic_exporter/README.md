# Topic Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:topic` (`urn:otel:exporter:topic`)
- Feature gate: `topic`
- Stability: Experimental

## Overview

The topic exporter publishes pdata to a named in-process topic declared in the
runtime configuration. It can bridge end-to-end ACK/NACK outcomes when the
topic declaration enables ack propagation.

## Getting Started

Publish to a topic by naming the topic in the node-level config:

```yaml
type: exporter:topic
config:
  topic: raw_signals
  queue_on_full: drop_newest
```

The `raw_signals` topic must be declared in the surrounding runtime
configuration.

## Configuration

```yaml
type: exporter:topic
config:
  # Topic name to publish to (required).
  topic: raw_signals

  # Optional local full-queue behavior. When omitted, the topic policy applies.
  # Supported values include "block" and "drop_newest".
  queue_on_full: drop_newest
```

## Context transfer

Without a topic `context.transfer` contract, topic boundaries intersect declared
context on both sides. Captured/declared headers and verified identity fields
must be visible to both pipelines. Composite snapshots survive only when their
declarations, including scope, match. Undeclared metadata and the socket peer
address do not cross implicitly. Compatible metadata shares immutable backing
storage without serializing or rebuilding values; the projection still checks
the actual entries against the compiled visibility.

For an explicit boundary, declare a flat, typed contract on the topic:

```yaml
topics:
  handoff:
    context:
      transfer:
        tenant_id:
          type: authorized_identity
        randomness:
          type: randomness
        workspace:
          type: transport_header
```

Each exporter controls required versus optional values independently:

```yaml
type: exporter:topic
config:
  topic: handoff
  context:
    export:
      tenant_id:
        required: true
        selector: {type: authorized_identity, named: [customer_id]}
      randomness:
        selector: {type: named, named: ["delivery:id"]}
      workspace:
        selector: {type: named, named: ["product_user:workspace"]}
```

`required` defaults to false. Missing required values permanently NACK the input
before publication; absent optional values remain absent. Ordered selector names
are fallback candidates: the first present source wins. A qualified member
retains its source composite's presence/provenance gate. Whole composites cannot
be assigned to a flat field.

The contract supports `transport_header`, `authorized_identity`, `constant`,
`randomness`, and `network_info`. Authority must match, except that any kind may
be downgraded to `transport_header`. Select the original socket address with
`{type: network_info, named: [peer_socket_addr]}`. `all_captured` exports a header
collection into a transport-header field, retaining individual stored/wire names
and binary values. Collection fields are imported as headers, not scalar
composite members.

Omitting `context.export` uses optional, same-name primitive sources for the
contract. An explicit empty export map exports no values. Explicit mappings
require a topic contract. ACK/NACK frames and source metrics never enter the
transfer; a blocked publish retains the same exported snapshot.

The shared config/engine/pdata adapter also provides a versioned flat storage
encoding bounded to 1 MiB serialized bytes and 1,024 fields plus values.
This spike does not yet write context into Quiver or change durable-buffer
recovery. The storage representation contains names, kinds, cardinality, and
values, not process-local layout IDs.

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

#### `exporter.attempted`

The shared exporter boundary records one attempt from the start of topic
admission through acceptance or refusal. Optional duration and item accounting
follow the node telemetry policy. Downstream tracked Ack/Nack latency is not
included in the attempt.

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.attempted.messages` | `{message}` | `signal`, `outcome` | Number of topic admission attempts. |
| `exporter.attempted.duration` | `s` | `signal`, `outcome` | Time spent attempting topic admission, including queue wait under the block policy. |
| `exporter.attempted.items` | `{item}` | `signal`, `outcome` | Signal items belonging to the attempted publish. |

#### `exporter.topic.rejections`

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.topic.rejections.messages` | `{message}` | `signal`, `reason` | Publishes refused because the queue was full, outcome capacity was exhausted, or shutdown interrupted admission. |

The bounded `reason` values are `queue_full`, `outcome_capacity`, and
`shutdown`.

#### `exporter.topic.tracked`

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `exporter.topic.tracked.messages` | `{message}` | `signal`, `result` | Admitted tracked publishes grouped by their downstream terminal result. |

The bounded `result` values are `ack`, `nack`, `timeout`, `topic_closed`, and
`shutdown`.

#### `exporter.topic`

| Metric | Unit | Description |
| --- | --- | --- |
| `exporter.topic.tracked_in_flight` | `{message}` | Current number of admitted tracked publishes waiting for a terminal result. |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `topic_exporter.start` | `info` | Exporter startup with topic name and effective publish policy. |
| `topic_exporter.drop_newest` | `warn` | A publish was dropped because the topic queue was full and policy dropped newest. |
| `topic_exporter.outcome_capacity_full` | `warn` | A publish requiring end-to-end outcome tracking was rejected because tracking capacity was exhausted. |
| `topic_exporter.context_transfer_failed` | `warn` | A required field or transfer contract could not be satisfied; the input is NACKed. |

## Limits

- The named topic must be declared and visible to the pipeline.
- Queue capacity and ack propagation limits are configured on the topic
  declaration, not on this exporter.
- Broadcast topic ack propagation currently resolves on the first subscriber
  outcome, as described in the configuration model.

## Related Docs

- [Configuration model topics](../../../../../docs/configuration-model.md#topic-declarations)
- [Core node catalog](../../../README.md)
