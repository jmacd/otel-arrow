# Tenant Router Processor

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `processor:tenant_router` (`urn:otel:processor:tenant_router`)
- Feature gate: Default
- Stability: Experimental

## Overview

The tenant router sends each message to a named output port by evaluating the
request's tenant context against an ordered list of conditions, first match
wins.

The decision reads no telemetry. A receiver resolves the request's tenant
tokens once into a packed context addressed by position, and this node probes
that context: one bit test and one hash lookup per bound (token, signature)
pair. The cost is therefore independent of how many routes are configured, how
many entries each route tests, and how large the batch is.

That is the difference from [`processor:content_router`](../content_router/README.md),
which routes on a resource attribute and must decode the payload and walk every
resource to do it. Route on the tenant context when the routing dimension is
*who sent the request*; route on content when it is *what the data says*.

## Getting started

Declare the tenant tokens at engine scope, then name the port each tenant's
data should take:

```yaml
engine:
  tenant_tokens:
    edge:
      extractors:
        - key: tenant_id
          transport_header: x-tenant-id
```

```yaml
type: processor:tenant_router
outputs: [acme, globex, unmatched]
config:
  tenant_routing:
    routes:
      - entries: [{ key: tenant_id, value: acme }]
        output: acme
      - entries: [{ key: tenant_id, value: globex }]
        output: globex
    default_output: unmatched
```

A key used only for matching costs no bytes on the request: `retain: true` is
needed only when a value must also travel, for an exporter to re-emit.

## Configuration

| Field | Description |
| --- | --- |
| `tenant_routing.routes` | Routes evaluated first-match-wins. Each has `entries` and an `output` port. |
| `tenant_routing.tenant_tokens` | Tenant tokens this router binds. Empty binds every declared token. |
| `tenant_routing.default_output` | Port for messages matching no route. Without it, unmatched messages are NACKed. |
| `admission_policy.on_full` | `reject_immediately` (default) or `backpressure`. |

An entry with a `value` requires that exact value; an entry without one is a
wildcard requiring only that the key be present.

Conditions are compiled by the engine before any pipeline starts. The `routes`
block must therefore live under the `tenant_routing` key, which is where the
controller looks for every node's conditions, and a route naming a key no token
declares fails at startup rather than silently matching nothing.

## Matching

Values are compared by exact bytes. Hashing selects a candidate and the
candidate is then verified against the literal the registry owns, so a hash
collision cannot route one tenant's data to another tenant's destination.

A request whose tenant value is not one the configuration declares collapses to
a single "unknown" symbol, which no condition can name. Such a request takes
`default_output`, never another tenant's port.

## Failure modes

The router fails to start, rather than mis-routing at runtime, when:

- the engine declares no `tenant_tokens`
- a route names an output port the node does not declare
- a condition tests a key that none of the bound tokens declares
- a condition tests a value that was never declared to the registry

At runtime, a message is NACKed when no condition matches and no
`default_output` is configured, and when the selected port is full under
`reject_immediately` or is closed. Unmatched data is never delivered to an
arbitrary port.

## Telemetry

### Metric Sets

#### `processor.tenant_router`

| Metric | Unit | Description |
| --- | --- | --- |
| `processor.tenant_router.signals_received` | "{msg}" | Messages received by the router |
| `processor.tenant_router.signals_routed` | "{msg}" | Messages routed by a matching tenant condition |
| `processor.tenant_router.signals_routed_default` | "{msg}" | Messages routed via the default output port |
| `processor.tenant_router.signals_without_tenant_context` | "{msg}" | Messages that carried no tenant context |
| `processor.tenant_router.signals_unmatched` | "{msg}" | Messages whose tenant context matched no route |
| `processor.tenant_router.signals_nacked` | "{msg}" | Messages NACKed by the router |
| `processor.tenant_router.signals_rejected_route_full` | "{msg}" | Messages rejected because the selected route was full |
| `processor.tenant_router.signals_rejected_route_closed` | "{msg}" | Messages rejected because the selected route was closed |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| *None* | N/A | No node-specific events are emitted. |

## Limits

- Routing is exclusive: each message selects at most one output port.
- A message is routed as a whole. The router does not split a batch that mixes
  tenants; use `processor:batch` partitioning to keep batches single-tenant.
