# MQTT Sparkplug Extension

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `extension:mqtt_sparkplug` (`urn:microsoft:extension:mqtt_sparkplug`)
- Feature gate: `mqtt-sparkplug-extension` (also enabled by `contrib-extensions`)
- Stability: Experimental

## Overview

`extension:mqtt_sparkplug` is a self-hosted Sparkplug datalogger scaffold. It
embeds an MQTT v5 listener, tracks Sparkplug session state with
`otel-arrow-dfe-sparkplug`, relays valid Sparkplug device publishes through the
existing `mqtt_ingress` capability, and fans `mqtt_egress` publications back
out to matching connected clients.

Phase-1 scope intentionally matches RFC 0003's standalone milestone:

- single core, single listener;
- no TLS;
- retained delivery only for the extension's own Primary Host `STATE` topic;
- graceful `OFFLINE` publish instead of a crash-safe MQTT Will; and
- raw MQTT relay only, with no OTAP materialization yet.

## Configuration

```yaml
type: extension:mqtt_sparkplug
config:
  bind_host: "0.0.0.0"
  bind_port: 1883
  host_id: "otap-datalogger-01"
  state_profile: sparkplug_3
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bind_host` | string | `"0.0.0.0"` | Host or IP address for the embedded MQTT listener. |
| `bind_port` | integer | `1883` | Plaintext MQTT port. Use `0` in tests to request an ephemeral port. |
| `host_id` | string | **required** | Primary Host identifier used in the Sparkplug `STATE` topic. Must not be empty. |
| `state_profile` | enum | `sparkplug_3` | `sparkplug_2_2` uses `STATE/{host_id}` with `ONLINE`/`OFFLINE`; `sparkplug_3` uses `spBv1.0/STATE/{host_id}` with timestamped JSON payloads. |

## Example

This is the RFC 0003 standalone Sparkplug datalogger example, expressed with
the scaffold's current `bind_host` and `bind_port` fields:

```yaml
version: otel_dataflow/v1
engine: {}
groups:
  default:
    pipelines:
      main:
        policies:
          resources:
            core_allocation:
              type: core_count
              count: 1
        extensions:
          sparkplug:
            type: urn:microsoft:extension:mqtt_sparkplug
            config:
              bind_host: "0.0.0.0"
              bind_port: 1883
              host_id: "otap-datalogger-01"
              state_profile: sparkplug_3
        nodes:
          mqtt:
            type: receiver:mqtt
            capabilities:
              mqtt_ingress: sparkplug
            config:
              topic_filter: "spBv1.0/#"
```
