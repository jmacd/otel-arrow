# Topic Receiver

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `receiver:topic` (`urn:otel:receiver:topic`)
- Feature gate: `topic`
- Stability: Experimental

## Overview

The topic receiver subscribes to a named in-process topic and forwards received
pdata into its pipeline. It supports broadcast subscriptions and balanced
consumer groups.

## Getting Started

Subscribe to a declared topic with the default broadcast mode:

```yaml
type: receiver:topic
config:
  topic: raw_signals
  subscription:
    mode: broadcast
```

The `raw_signals` topic must be declared in the surrounding runtime
configuration.

## Configuration

```yaml
type: receiver:topic
config:
  # Topic name to subscribe to (required).
  topic: raw_signals

  # Subscription mode and options (default: broadcast).
  subscription:
    # "broadcast" gives each subscriber every message.
    mode: broadcast

    # Use "balanced" with a group to share messages across subscribers in the
    # same group.
    # mode: balanced
    # group: workers
```

Subscription modes:

- `broadcast`: each subscriber receives each message.
- `balanced`: subscribers in the same `group` share the stream.

## Context import and subscription conditions

Without an explicit topic transfer contract, context is projected to the
destination's compiled declarations. Private or differently scoped composites,
undeclared headers/claims, and socket peer metadata are removed. Compatible
snapshots retain their existing values and share their backing storage.

With a [topic transfer contract](../../exporters/topic_exporter/README.md#context-transfer),
the receiver can name the imported record as a local composite and subscribe
only to a context subset:

```yaml
type: receiver:topic
config:
  topic: handoff
  subscription:
    mode: balanced
    group: tenant-1
  context:
    conditions:
      tenant_id: "1"
    store_composite: topic_exp
```

Conditions are conjunctive exact UTF-8 value matches over transfer keys; a
missing key does not match, and a repeated field matches if any text value
matches. Conditions compile to transfer slots, not runtime name resolution.
Three pipelines can use `tenant_id: "1"`, `"2"`, and `"3"` with distinct groups.
Workers sharing a balanced group must have identical conditions.
Those conditions are fixed for the group's lifetime; use a new group name or
restart the topic runtime when changing them.

The broker applies balanced conditions before queue admission, so unrelated
tenants do not consume that group's capacity or reach its consumers.
Broadcast filtering happens before delivery ownership; `all` ACK consensus
includes only matching subscribers. Broadcast storage remains a shared ring,
so its topic-wide capacity and lag policy still apply. No matches retain the
existing no-subscriber publish/outcome behavior.

`store_composite` enters the receiver pipeline's composite namespace and rejects
collisions with visible declarations or other receiver composites. Downstream
consumers can select `topic_exp:tenant_id` or `topic_exp:randomness`. Values keep
their transfer authority: randomness is not regenerated, and identity does not
become an untrusted header. Transfer composites are present after a matching
import even when optional keys are absent; absent members remain absent.
This differs intentionally from existing atomic grouping composites.
Other receivers sharing the pipeline cannot implicitly generate this composite.

An optional `context.import` map selects and renames members:

```yaml
context:
  conditions: {tenant_id: "1"}
  store_composite: topic_exp
  import:
    tenant_id:
    randomness:
      store_as: request_id
```

Omitting `import` selects all transfer keys. Without `store_composite`, selected
fields are imported as primitives instead. An explicit empty import map selects
nothing and cannot define a composite. Contract violations are logged and
tracked deliveries are NACKed before forwarding. Routing/metrics state is
always fresh on the receiving side.

## Examples

Balanced subscription:

```yaml
type: receiver:topic
config:
  topic: raw_signals
  subscription:
    mode: balanced
    group: workers
```

## Telemetry

These tables list telemetry emitted directly by this node. Common engine
runtime metric sets may also be attached by the pipeline telemetry policy.

### Metric Sets

#### `receiver.topic`

| Metric | Unit | Attributes | Description |
| --- | --- | --- | --- |
| `receiver.received.messages` | `{message}` | `signal`, `outcome` | Number of topic deliveries processed at the receiver boundary, including context-import refusals; downstream forwarding is reported separately. |
| `receiver.processing.duration` | `s` | `signal` | Optional receiver-local topic delivery processing duration. Enabled by the node-local duration telemetry policy and excludes downstream channel waiting. |
| `receiver.topic.lag.events` | `{event}` | `event.type` | Number of lag events emitted by broadcast subscriptions, dimensionalized by `event.type` (`notification`, `disconnect`). |
| `receiver.topic.bridge.controls` | `{control}` | `control`, `result` | Number of downstream ACK/NACK bridge controls, dimensionalized by `control` (`ack`, `nack`) and `result` (`success`, `ignored_propagation_disabled`, `missing_calldata`, `invalid_or_untracked_id`, `runtime_failure`). |
| `receiver.topic.lagged.messages` | `{message}` | | Total messages missed across lag notifications. |
| `receiver.topic.downstream.backpressure.events` | `{event}` | | Number of downstream backpressure events (>= 500ms blocked). |
| `receiver.topic.downstream.blocked.duration` | `s` | | Distribution of per-message downstream channel waits, using the configured duration tier. |

All receiver metrics include the configured `topic` entity attribute. The
shared boundary does not report payload size because the topic transports
internal pdata rather than an encoded application payload.

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `topic_receiver.start` | `info` | Receiver startup with topic, subscription, and ACK propagation mode. |
| `topic_receiver.context_transfer_failed` | `warn` | A transfer contract or import condition could not be satisfied. |
| `topic_receiver.drain_ingress_pending_forward_nack_failed` | `warn` | A pending forward could not be NACKed during ingress drain. |
| `topic_receiver.drain_ingress_drop_pending_forward` | `warn` | A pending forward was dropped during ingress drain. |
| `topic_receiver.drain_ingress.timeout` | `warn` | Ingress drain timed out. |
| `topic_receiver.drain_ingress_force_nack_failed` | `warn` | A forced NACK failed during ingress drain. |
| `topic_receiver.bridge_ack_untracked_or_invalid_id` | `warn` | A downstream ACK referenced an invalid or untracked topic message id. |
| `topic_receiver.bridge_ack_failed` | `warn` | A downstream ACK failed to bridge to the topic runtime. |
| `topic_receiver.bridge_ack_missing_calldata` | `warn` | A downstream ACK did not include the required bridged message id calldata. |
| `topic_receiver.bridge_nack_untracked_or_invalid_id` | `warn` | A downstream NACK referenced an invalid or untracked topic message id. |
| `topic_receiver.bridge_nack_failed` | `warn` | A downstream NACK failed to bridge to the topic runtime. |
| `topic_receiver.bridge_nack_missing_calldata` | `warn` | A downstream NACK did not include the required bridged message id calldata. |
| `topic_receiver.downstream_backpressure` | `warn` | Forwarding to downstream was blocked long enough to count as backpressure. |
| `topic_receiver.forward_failed` | `warn` | Forwarding a topic message to downstream failed. |
| `topic_receiver.lag_disconnect` | `warn` | The receiver disconnected from the broadcast topic because it lagged too far behind. |
| `topic_receiver.lagged` | `warn` | The receiver missed one or more topic messages due to broadcast lag. |

## Limits

- The named topic must be declared and visible to the pipeline.
- Topic wiring must remain acyclic across topic hops.
- Queue capacity, lag policy, and ack propagation limits are configured on the
  topic declaration.

## Related Docs

- [Configuration model topics](../../../../../docs/configuration-model.md#topic-declarations)
- [Core node catalog](../../../README.md)
