# MQTT Exporter

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `exporter:mqtt` (`urn:otel:exporter:mqtt`)
- Feature gate: `mqtt-exporter` (also enabled by `contrib-exporters`)
- Stability: Experimental

## Overview

The MQTT exporter consumes OTLP log records and republishes each record body
through a bound `mqtt_egress` capability, usually provided by
`extension:mqtt_client`.

This milestone is intentionally simple:

- every output record publishes to one fixed configured topic;
- only the logs signal is supported;
- string bodies publish directly, while other OTLP body types fall back to a
  best-effort string representation.

## Configuration

```yaml
type: exporter:mqtt
capabilities:
  mqtt_egress: mqtt_client
config:
  topic: "otap/logs"
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `topic` | string | **required** | Fixed topic used for every outbound MQTT PUBLISH. Must not be empty. |

## Example

See [`../../receivers/mqtt_receiver/mqtt-client-to-file.yaml`](../../receivers/mqtt_receiver/mqtt-client-to-file.yaml)
for a capability-bound MQTT pipeline example.
