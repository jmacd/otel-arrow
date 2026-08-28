# MQTT Receiver

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `receiver:mqtt` (`urn:otel:receiver:mqtt`)
- Feature gate: `mqtt-receiver` (also enabled by `contrib-receivers`)
- Stability: Experimental

## Overview

The MQTT receiver consumes inbound MQTT PUBLISH messages through a bound
`mqtt_ingress` capability, usually provided by `extension:mqtt_client`. Each
PUBLISH becomes one OTLP log record whose body is the payload text and whose
`mqtt.topic` attribute preserves the source topic.

This milestone is intentionally lossy: payload bytes are decoded with
`String::from_utf8_lossy`, and the receiver emits standard OTLP logs rather
than a round-trippable MQTT envelope representation.

## Configuration

```yaml
type: receiver:mqtt
capabilities:
  mqtt_ingress: mqtt_client
config:
  topic_filter: "sensors/#"
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `topic_filter` | string | **required** | MQTT topic filter passed to the bound `mqtt_ingress` capability. Must not be empty. |

## Example

See [`mqtt-client-to-file.yaml`](mqtt-client-to-file.yaml) for a small pipeline
that binds `extension:mqtt_client` to `receiver:mqtt` and writes OTLP JSON to a
file exporter.
