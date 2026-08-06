# Tenant Context

Status: design, being implemented in small steps. This document is the single
entry point. It states the whole idea in one page and then names the exact
sequence of changes that implement it.

## Why this document exists

Three earlier efforts described this feature and then grew past what could be
reviewed:

- [#3583][pr3583] -- the design, covering tenant identity, CPU/memory limits
  and limiter extensions together.
- [#3635][pr3635] -- a working compiler prototype, 4,600 lines.
- [#3636][pr3636] -- the conversion of every call site, 7,900 lines added and
  5,700 removed.

The prototypes established that the mechanism works and what it costs. They
were not reviewable, because each one arrived with its motivation, its
mechanism, and all of its consumers at once. This document keeps the
conclusions and discards the packaging. Nothing here is new; it is the same
design cut into pieces a reviewer can hold in their head.

[pr3583]: https://github.com/open-telemetry/otel-arrow/pull/3583
[pr3635]: https://github.com/open-telemetry/otel-arrow/pull/3635
[pr3636]: https://github.com/open-telemetry/otel-arrow/pull/3636

## The idea in one paragraph

An operator declares, once for the engine, which request-scoped facts the
pipeline is allowed to know about: a header, a peer address, an authenticated
subject, a locally minted identifier. The engine compiles those declarations at
startup into a registry that has already done all the string work. At request
time a receiver fills the declared facts into one small, positionally addressed
allocation -- the **tenant context** -- which travels with the request. Every
downstream decision that depends on who the request belongs to reads that
context by slot number, never by name, and never by looking at the wire again.

## Three nouns, and no others

**Key.** A named request-scoped dimension. `tenant_id`, `project_id`, `idem`.
A key is engine-global vocabulary: `tenant_id` means the same thing in every
pipeline, so a value resolved at one hop is readable by the same name at the
next.

**Extractor.** How a key gets a value on a given request. An extractor names
the key it fills and the source it reads. The extractor is also the key's
declaration: a key exists because some extractor fills it.

**Token.** A named set of extractors -- the identity bundle. A token resolves
**all or nothing**: it is present on a request only if every one of its
extractors resolved. That is what makes it an identity rather than a partially
populated map. Facts that should resolve independently belong in separate
tokens.

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

`edge` resolves only when both headers are present. A request carrying only
`x-tenant-id` resolves nothing, and nothing downstream sees a half-identity.

### Naming note, stated once

"Tenant" names the motivating case, not the limit of the mechanism. A tenant
context is a bundle of declared request-scoped dimensions. Multitenancy is one
use; carrying an authenticated subject is another; an idempotency key used to
name output deterministically across a crash-replay is a third. The design does
not change to accommodate them, which is the point -- they arrive as
configuration plus, at most, one new extractor kind.

## Where the shape comes from

This is Envoy's rate limit descriptor design, applied to a different decision.

| Envoy | Here |
| ----- | ---- |
| `rate_limits.actions` (`request_headers`, `remote_address`, `generic_key`) | extractors |
| the descriptor those actions produce | the tenant context |
| one descriptor entry | one `key: value` |
| the rate limit service's descriptor tree | the compiled condition table |

Envoy's insight is the one worth borrowing: the set of dimensions a request
will be judged on is fixed by configuration, so it can be computed once, in a
fixed layout, before anything asks a question about it.

## Matching is a hash join

Consumers do not interpret configuration at request time. They probe a table
that was built at startup, exactly as a database evaluates a hash join.

At startup, every literal that any condition tests is interned into a per-key
symbol table. A condition becomes a fixed-width tuple of symbols. At request
time, each resolved value is looked up in its key's table to get a symbol, the
symbols are packed into a signature, and the signature is probed.

Two properties follow:

- **Matching is exact.** The symbol lookup is a hash map over the literal
  bytes, so a match is confirmed by byte equality, the same way a hash join
  verifies equality after the hash narrows the candidates. A hash collision
  cannot route one tenant's data to another tenant's destination.
- **Undeclared input fails closed.** A value no condition mentions has no
  symbol, so it resolves to a reserved unknown symbol and matches nothing. It
  cannot fall through into another tenant's rule.

Cost per request is a fixed number of probes, set by the number of distinct
key-sets declared, not by the number of conditions.

## Producers and consumers

The context has two kinds of participant. Keeping them separate is what makes
the configuration standard across nodes.

A **producer** is a node that constructs a request rather than forwarding one:
a receiver, a traffic generator, the far side of a topic boundary. A producer
resolves tokens and is the only place a wire name is read.

A **consumer** is a node that decides something from a resolved context: an
exporter emitting an outbound header, a router selecting a port, a partitioner
naming a partition. A consumer reads by key and declares its own conditions.

A consumer configures itself in its own node config. It names a key; it never
names a header, because by the time the request reaches it the header is gone.
An exporter that writes tenant identity to a backend looks like this (step 3):

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

A consumer that chooses a destination declares conditions instead. Conditions
are ordered and the first match wins. Every entry in a condition must match. An
entry with no `value` matches any value, and requires only that the key is
present (step 4):

```yaml
nodes:
  route:
    type: processor:tenant_router
    config:
      routes:
        - entries:
            - key: tenant_id
              value: acme
            - key: project_id
          port: priority
        - entries:
            - key: tenant_id
              value: acme
          port: bulk
      default_port: shared
```

Requests from `acme` that also carry a project go to `priority`. Other `acme`
requests go to `bulk`. Everything else goes to `shared`.

### One complete example

The producer declaration and one consumer, which is what step 3 makes
runnable. `x-tenant-id` appears once, in the engine block. `x-scope-orgid`
appears once, on the exporter. Nothing in between mentions either name:

```yaml
policies:
  tenant:
    tokens:
      edge:
        extractors:
          - key: tenant_id
            transport_header: x-tenant-id

groups:
  default:
    pipelines:
      main:
        nodes:
          ingress:
            type: receiver:otlp
            config:
              protocols:
                grpc:
                  listening_addr: 0.0.0.0:4317
          backend:
            type: exporter:otlp_grpc
            config:
              grpc_endpoint: http://backend:4317
              tenant_headers:
                - key: tenant_id
                  header: x-scope-orgid
        connections:
          - from: ingress
            to: backend
```

An inbound `x-tenant-id: acme` is read once at `ingress`, carried through the
pipeline as a slot number, and written to the backend as
`x-scope-orgid: acme`. Any other inbound header is dropped, because no
extractor declared it.

A routing consumer selects a named output port, so its connections name that
port:

```yaml
        connections:
          - from: ingress
            to: route
          - from: route["priority"]
            to: priority_backend
          - from: route["shared"]
            to: shared_backend
```

The rule behind this, and the one that caused most of the confusion in the
earlier efforts: **a token never names an egress header.** A wire name belongs
to the backend being written to, not to the tenant, so the same key leaves one
exporter as `x-scope-orgid` and another as `x-acme-customer`. The token is the
portable identity; naming happens at the node that touches the wire. Between
the two there is only a key id and a slot number.

## Configuration follows the Policy framework

Tenant declarations are a policy, declared as `policies.tenant` and carried
through `Policies` like every other policy in the engine.

They are honored **at engine scope only**. Scope precedence is deliberately not
used here: a key is shared vocabulary, and a group that could redefine
`tenant_id` would defeat the reason the vocabulary exists. Declaring
`policies.tenant` at group, pipeline or node scope is a configuration error
reported at startup, not a silently ignored field.

Consumers configure themselves in their own node config, the way node config
normally works. Producers need no configuration beyond the engine-scoped
declaration.

## What is deliberately not in the first version

Listed here so their absence reads as a decision rather than an oversight.
Each is a later step in the plan below.

- Extractor kinds other than `transport_header`.
- The attribute bag: carrying key *names* with the request so instrumentation
  can copy tenant identity into span or log attributes without re-encoding.
- Cross-boundary policy: what a topic hop admits in each direction.
- Retiring the existing transport header policy. It stays until the replacement
  is proven end to end.

## Delivery plan

Each step is a separate pull request. Steps 1 through 3 are purely additive: if
the design turns out to be wrong, they are deleted without touching anything
that works today. Step 5 is the only one that removes an existing feature, and
it happens only after step 3 has demonstrated the replacement running.

| # | Change | Shape |
| - | ------ | ----- |
| 1 | **The compiler.** `policies.tenant` config types, validation, and the compiled `TenantRegistry`. Resolution is defined against a `RequestSource` trait, so this step depends on no protocol crate and is exercised entirely by unit tests. Nothing in the pipeline reads it. | additive, self-contained |
| 2 | **Carrying the context.** The registry reaches nodes through the pipeline context; a resolved context rides on the request. The OTLP gRPC receiver becomes the first producer. Nothing consumes it yet. | additive |
| 3 | **The first consumer.** The OTLP gRPC exporter maps a key to an outbound header name. The feature is now end to end, with one runnable config as the proof. | additive |
| 4 | **Conditions.** Condition config, the compiled table and the probe, plus one routing consumer. | additive |
| 5 | **Retire transport headers.** Delete `header_capture`, `header_propagation`, the policy and its docs, with migration notes. | removal only |
| 6+ | **Growth, one PR each.** Further extractor kinds (`generic_key`, `remote_address`, `imported_key`, `idempotency_key`), the attribute bag, boundary policy at topic hops, and the remaining consumers: Kafka receiver and exporter, partition processor, traffic generator. | additive |

Steps 6 and beyond are independent of each other. Nothing in steps 1 through 5
is designed around them, and none of them requires revisiting an earlier step.

## Limits

The registry trades breadth for density, and enforces the budget at startup by
failing the configuration rather than truncating at request time. The concrete
numbers belong with the implementation and are documented in step 1; the shape
of the trade is that more declared keys means fewer distinct values per key,
because both are packed into a fixed-width signature.

Header names are matched exactly and case-insensitively. Glob and regex
matching are not supported, because a compiled table cannot intern the literals
a pattern would admit.
