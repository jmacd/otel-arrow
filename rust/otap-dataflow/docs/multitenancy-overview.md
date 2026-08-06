# Multitenancy Overview

**Status:** Draft

## Background

Two other open-source systems influence this design:

- [Kubernetes multitenant concepts](https://kubernetes.io/docs/concepts/security/multitenancy/)
- [Envoy rate limit configuration](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting.html)

Both will be familiar to many users and we aim to keep our concepts close to
theirs.

## Definitions

The dataflow engine is deployed across a wide range of scenarios, so there is
no single definition of or data model for a tenant. Multitenancy describes a
set of features for managing tenancy requirements, not one specific aspect of
the engine. What those requirements are depends on what is being shared, what
must be isolated, and how much operational complexity is acceptable. Common
cases include:

- **Multiple teams** sharing an administrative boundary. Usually small in
  number, cooperative, and under shared administrative control.
- **Multiple customers** of a SaaS sharing a service endpoint. They have a
  contractual relationship, compete for shared resources, and may be many.
- The **self-observability** pipeline, treated as a special tenant.
- **Multiple producers** of telemetry in different namespaces, processed
  separately.

More than one concept of tenancy is often in use at a time, such as a SaaS
customer account and a signed-in user, and multitenancy is often applied at
several levels at once.

## Coarse multitenancy through configuration

Tenant tokens are sufficient to implement multitenant controls without any
per-tenant limiter. Resolve the tenant identity once at the receiver, route on
it -- to another node within a pipeline, or across a topic boundary into
another pipeline group -- and each tenant's data lands in a pipeline of its
own. That pipeline declares its own resource policies, which the operating
system enforces through control groups or Job Objects.

Isolation becomes a property of the deployment rather than of a shared limiter
table:

- **CPU**: a tenant's pipeline is capped absolutely or weighted against its
  siblings.
- **Memory**: a tenant's pipeline group is given an absolute or relative share
  of container memory.
- **Failure**: one tenant's pipeline can stall, backpressure or restart
  without touching another's.
- **Observability**: engine telemetry is already labeled by pipeline and node,
  so per-tenant accounting follows from the deployment.

This is macro-scale tenancy: coarse, statically configured, and appropriate
whenever tenants are few enough to name in configuration. It does not provide
fairness among tenants that share one pipeline, nor per-request rate limits,
and it scales only as far as tenants can be given pipelines of their own.
Those requirements are addressed by the finer-grained limits below.

Tenant tokens and the conditions evaluated over them are [detailed in a
separate document](./multitenancy-tenant.md). They are resolved once and
compiled, so every downstream decision is a fixed-cost lookup. Routing,
batching, egress headers and trust-boundary policy are all consumers of the
same identity, which is why a new per-tenant behavior needs a new consumer
rather than a new matching mechanism.

## Resource policies

Resource policies are hierarchical: a limit declared at the top level is
overridden by pipeline-group policies and then by pipeline policies. The
resolution rules are defined in
[configuration-model.md](./configuration-model.md). CPU and memory share one
naming convention, giving either an absolute limit or a relative weight taken
as a ratio against siblings.

### CPU

CPU limits are provided through built-in integration with the operating
system, using mechanisms such as Linux control groups and Windows Job Objects.

- `cpu_limiter.cpu_limit` is absolute, in milli-CPUs, so `100m` is 10% of one
  CPU.
- `cpu_limiter.cpu_weight` is relative to the sum across siblings.

### Memory

Enforcement is asymmetric, which is why memory needs separate attention. When
CPU demand exceeds a limit, the operating system throttles the process by
making it wait. When memory exceeds its limit, the operating system kills the
process. The engine must therefore stay clear of its own limit rather than
discover it.

- `memory_limiter.soft_limit` and `memory_limiter.hard_limit` are coarse
  thresholds defined against the operating system's memory accounting, used as
  a load signal and to stop admitting new requests.
- `memory_limiter.memory_limit` and `memory_limiter.memory_weight` size a
  pipeline group absolutely or relatively.

Memory is accounted at group level and shared by the group's pipelines, so a
tenant needing a memory guarantee is given a group of its own.

### Example

```yaml
policies:
  resources:
    cpu_limiter:
      cpu_limit: 100m     # whole engine: 10% of one CPU
    memory_limiter:
      # In a 100MiB container, signal overload at 85% and treat 90% as fatal.
      soft_limit: 85MiB
      hard_limit: 90MiB

engine:
  observability:
    policies:
      resources:
        cpu_limiter:
          cpu_limit: 10m  # self-observability: 1% of one CPU
        memory_limiter:
          memory_weight: 10
    pipeline: { ... }

groups:
  acme:                   # one tenant, reached by tenant-token routing
    policies:
      resources:
        cpu_limiter:
          cpu_weight: 80
        memory_limiter:
          memory_weight: 90
    pipelines: { ... }
```

## Finer-grained limits (future work)

Where tenants must share a pipeline, coarse configuration is not enough and
the engine needs limits that select a bucket per tenant. Such limits are a
**consumer of tenant tokens**, not a separate mechanism: a limiter names the
tokens it binds and selects its bucket with the same first-match-wins
conditions used for routing, plus a cardinality limit bounding how many
buckets may exist.

Limits will be declared in named units, so that a policy can restrict requests
per second, network bytes per second, items in flight or storage operations,
using unit names such as `request_bytes`, `network_bytes`, `request_count`,
`request_items`, `storage_bytes` and `storage_ops`. They fall into two
families:

- **Rate limits** count a resource consumed as a function of time. It is not
  returned, and a caller that cannot proceed may wait a definite amount of
  time. The built-in implementation is a token bucket.
- **Resource limits** count a resource held by ongoing work in queues, batches
  and topics. A caller may wait indefinitely for it to be returned. The
  built-in implementation is a semaphore.

The built-in limiters are thread-local, so they resolve at pipeline scope and
are therefore per-tenant and per-core. Limits shared across threads or CPUs
are served by policy extensions, which may delegate to systems such as the
[Envoy gRPC rate limit service](https://github.com/envoyproxy/ratelimit) or
[Gubernator](https://github.com/gubernator-io/gubernator). A shared limiter
must separate its hot and cold paths to avoid interfering with the engine.
The alternative is the coarse pattern above: route by tenant token to a
pipeline of its own, where the built-in thread-local limiter suffices.

A related direction is budgeting the memory retained by work in flight, so
that queues, batches, retry buffers and pending exporter requests are charged
to the tenant that caused them, with ownership moving as data crosses queues
and topics. Detailed limiter configuration, fairness modes, cardinality
failure modes, observability and retained-work memory budgeting are deferred
until the tenant token mechanism has landed.
