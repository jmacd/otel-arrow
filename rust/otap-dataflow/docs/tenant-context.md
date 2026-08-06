# Tenant Context

**Status:** Draft

## Overview

This document defines **tenant context** as the defining property of
the OTAP DFE request carrying request-specific metadata including:

- Peer network address
- Transport headers
- Authorized identity information
- Idempotency keys

Tenant contexts are effiently encoded and carried by reference-counted
bytes.  Multitenant features are provided for producers and consumers
of tenant context:

- Producers use standard engine methods to enter metadata associated
  with a new request, yielding a new tenant context.
- Consumers use standard engine methods to match by or retrieve tenant
  variables.

Multitenant features are implemented by a **tenant compiler** which is
computed from the whole engine configuration. The tenant compiler
internalizes strings and match conditions and computes hash codes for
distinct token signatures, enabling `O(1)` match operations.

## Configuration model

Taken alongside a DFE pipeline configuration, the tenant context
defines a parallel metadata pipeline where extraction and application
of key metadata are configured controlled by the user. Metadata
variables are each an aspect of the tenant context belonging to one or
more tenant tokens that are used for matching and propagating metadata
in the pipeline.

A **tenant key** is one key and value belonging a tenant context.

An **extractor** produces one tenant key. Extractors are conditional, they
can fail to match.

A **tenant token** is one set of tenant keys, defined when a list of
extractors all match.

### Producers

Receivers and processor nodes that create new contexts will use engine
methods configured through `tenant` policies, listing tokens and
extractors, defining the keys:

```yaml
policies:
  tenant:
    tokens:
      edge:
        extractors:
          - key: tenant_id
            transport_header: x-tenant-id
          - key: project_id
            transport_header: x-project-id

```

We emphasize `transport_header` in this design because at the time of
writing, transport headers are encoded using
`Option<Arc<Vec<TransportHeader>>>`. This design will replace the
implementation of transport headers with tenant context, a
reference-counted `Bytes`. Then, uusing scratch space to construct
tenant tokens, we will reduce the number of allocations to one per
tenant context.

Receivers evaluate their required and optional tenant tokens, optional
when they may be present and required when they must be present.

```yaml
nodes:
  ingest:
    type: receiver:otlp_grpc
    policies:
      ingress:
        optional_tokens: [edge]
    config:
      ...
```

### Consumers

Consumers of the tenant context fall into two categories:

- Carriers: Consumers have the general ability to extract values from
  tenant context by key.
- Matchers: Consumers have the general ability to form conditions on
  tenant context, either in configuration or in runtime data structures.

As an example of the carrier pattern, the gRPC OTLP exporter can be
configured to export a specific tenant key:

```yaml
nodes:
  backend:
    type: exporter:otlp_grpc
    config:
      grpc_endpoint: http://backend:4317
      tenant_headers:
        - key: tenant_id
          header: x-scope-orgid
```

In this example, the tenant compiler knows that tenant headers must be
carried in the tenant context, so that callers are able to reproduce
the value of `tenant_id`. In the example above, the tenant token is
not required, so the `x-scope-orgid` header will be absent when the
tenant key is undefined.

As an example of the matcher pattern, a new `tenant_router` processor
will be introduced to route by tenant context variables. The first
branch ("priority") is taken when the `tenant_id` equals "acme". The
second branch is taken when there is any `tenant_id` defined.

```yaml
nodes:
  route:
    type: processor:tenant_router
    policies:
      ingress:
        required_tokens: [edge]
    outputs:
      - priority
      - shared
    config:
      routes:
        - entries:
            - key: tenant_id
              value: acme
          port: priority
        - entries:
            - key: tenant_id
          port: shared
      ...
```

### Propagation

Tenant context propagates with each request. Like the associated
request data, tenant context can be cheaply cloned. Some nodes will
implement specific translation of tenant context. This may be done
however they see fit, for example, the batch processor can be
configured using tenant context, first by listing required tenant
tokens, then the set of partition keys.

```yaml
nodes:
  route:
    type: processor:batch
    policies:
      ingress:
        required_tokens: [edge]
    config:
      partition:
        metadata_keys: [tenant_id, project_id]
        max_cardinality: 100
      ...
```

Nodes may require tenant tokens even when they do not use them,
for example to declare that an idempotency key,

```yaml
policies:
  tenant:
    tokens:
      idempotent:
        extractors:
          - key: idem
            idempotency: uuid7
```

can be required by the recipient.

```yaml
nodes:
  route:
    type: processor:durable_buffer
    policies:
      ingress:
        required_tokens: [tenant_id, project_id, idempotent]
    config:
      ...
```

## Tenant compiler

The tenant compiler hides the details involved in evaluating and
applying tenant contexts. The engine uses the graph of nodes and
policies defined for each, then it precomputes all the necessary
information for fast evaluation:

- Compute a tenant context from the inputs
- Match a condition over tenant context variables
- Extract a value from a tenant context variable.

Tenant contexts are computed for the set of reachable nodes in a
pipeline. The compiler knows which variables are extracted and which
are only matched. Tenant variables can be "bagged" for extraction as a
list of key:values, encoding using OTLP bytes. The bagged section of
the tenant context can be borrowed as `&[u8]` for use encoding
OpenTelemetry attributes directly from the tenant context.

The topic exporter and receiver will be extended with dedicated
configuration for controlling the propagation of tenant context across
pipeline group boundaries.

Live reconfiguration of the tenant compiler will be supported. Tenant
producer and consumer configuration are paired, when either changes
both sides will be recompiled. The tenant context carries the compiled
consumer information in the form of an epoch number.

## The first cut

The diagram below shows the engine after the first few steps of the
plan, when the only extractor is `transport_header` and every declared
key is bagged. This is the smallest change that is worth shipping: it
replaces `Option<Arc<Vec<TransportHeader>>>` without adding a feature
anyone has to learn.

```text
  configuration                                        once, at startup
  ----------------------------------------------------------------------
    policies.tenant                    node policies
      tokens:                            ingest:
        edge:                              optional_tokens: [edge]
          tenant_id  <- x-tenant-id      backend:
          project_id <- x-project-id       tenant_headers:
                                             tenant_id -> x-scope-orgid
                        \                 /
                         v               v
                     +------------------------+
                     |    tenant compiler     |
                     +------------------------+
                        |                   |
          extractor plan|                   |slot numbers
                        v                   v

  one request                                        repeated, at runtime
  ----------------------------------------------------------------------

   inbound headers            +-----------+
   x-tenant-id:   acme  --->  |  ingest   |  2 of 3 headers are declared
   x-project-id:  p1    --->  | receiver  |  1 allocation, not 1 per header
   authorization: ...    -X   +-----------+
                                    |
                                    | tenant context: refcounted Bytes
                                    v
                +--------------------------------------+
                | epoch | tokens | slot ends           |  fixed header
                |--------------------------------------|
                | bag: an OTLP KeyValue run            |
                |   tenant_id  = "acme"                |  slot 0
                |   project_id = "p1"                  |  slot 1
                +--------------------------------------+
                                    |
                                    | clone is a refcount, not a copy
                                    v
                               +-----------+
                               | processor |  neither reads it nor loses it
                               +-----------+
                                    |
                                    v
                               +-----------+
                               |  backend  |  read slot 0
                               | exporter  |  write x-scope-orgid
                               +-----------+
                                    |
                                    v
                        outbound: x-scope-orgid: acme
```

Everything above the dividing line happens once, over the whole engine
configuration; everything below happens per request. The receiver is
the only node that sees a wire name on the way in and the exporter is
the only node that writes one on the way out, so `x-tenant-id` and
`x-scope-orgid` each appear exactly once in the configuration and
nowhere in between. An undeclared header such as `authorization` is
never copied, which is where most of the saving comes from: the
current implementation allocates per header received, whether or not
any rule wants it.

In this first cut every retained key is bagged, so the context is
little more than the fixed header plus an OTLP `KeyValue` run, and
reading a key is a slot lookup into that run. Later steps add to the
header region rather than changing the bag: match-only keys, which are
tested but never stored, arrive with the first matcher, and the
compiler's condition tables arrive with it. Because the bag is already
a valid OTLP repeated field, instrumentation can borrow it as `&[u8]`
and append the request's tenant identity to scope attributes without
re-encoding anything.

## PR series

Nine changes, each one reviewable on its own. The first four are
purely additive: if the design is wrong they are deleted and the
engine is exactly as it is today. Only PR5 removes an existing
feature, and only after PR2 through PR4 have shown the replacement
running end to end. PR6 onward are independent of each other and can
land in any order.

| PR | Lands | Size | What the reviewer is asked to judge |
| -- | ----- | ---- | ----------------------------------- |
| 1 | **Compiler.** `policies.tenant`, the registry, the packed layout. Resolution defined against a source trait, so no protocol crate is involved and nothing in the pipeline reads it. | ~1.1k, no deletions | Is the vocabulary right: keys, extractors, all-or-nothing tokens, engine scope only? |
| 2 | **Transport header extractors.** Node `policies.ingress` with `required_tokens` and `optional_tokens`; the OTLP gRPC and HTTP receivers become producers. Nothing consumes the result yet. | ~0.8k | Does a receiver resolve the same context a reviewer would predict from the config, including the `-bin` and repeated-header cases? |
| 3 | **Propagators.** The context rides on the request and survives every node that only forwards. Processors need no change; cloning is a refcount. | ~0.6k | Can a context be silently dropped or replaced anywhere on the path? |
| 4 | **Carriers.** Exporters read by key: `tenant_headers` on OTLP gRPC, then OTLP HTTP. The feature is now end to end, with one runnable config as proof. | ~0.7k | Static config wins on collision, so tenant material can never shadow a backend credential. Is that airtight? |
| 5 | **Remove transport headers.** Delete `header_capture`, `header_propagation`, the policy, the three resolution scopes and the docs, with migration notes. | ~2.5k removed | Is every old capability either replaced or deliberately dropped, and is each drop written down? |
| 6 | **Matchers.** Conditions in the compiler -- interned literals, packed signatures, `O(1)` probe -- and `processor:tenant_router` as the first matcher. | ~1.2k | Exact matching and fail-closed on undeclared input: can a collision or a gap route one tenant's data to another tenant's destination? |
| 7 | **Boundaries.** Topic exporter and receiver gain explicit control over what crosses a pipeline group boundary in each direction. The far side re-derives its own tokens rather than adopting the inbound context. | ~0.8k | Is the default closed, and is a hop across a boundary a place where tenant material can leak? |
| 8 | **Partitioning.** Batch processor partitions on tenant keys with a cardinality bound, so a merged batch never mixes tenants. | ~0.5k | What happens at the cardinality limit, and can a merge produce a batch whose context is a lie? |
| 9 | **Minting and durability.** An `idempotency` extractor, and `required_tokens` on `processor:durable_buffer` so a persisted request carries an identity that outlives the process. | ~0.6k | A replay site may restore a stored key but must never mint a fresh one. Is that enforced? |

Live reconfiguration is deliberately not a PR of its own. The epoch is
in the layout from PR1 and each step maintains it, so recompiling both
sides of a producer/consumer pair is a property the series carries
rather than a feature bolted on at the end.
