# Multitenancy: Tenant Tokens

**Status:** Draft

A **tenant token** names the identity a request belongs to: a small set of
`key: value` identifiers, resolved once at the receiver from request-scoped
material, and carried with the request for the rest of its life in the engine.

Tokens are the substrate for per-tenant behavior in the dataflow engine.
Routing, batching, egress headers, trust-boundary policy and eventually limits
all read the same resolved identity, evaluate the same condition vocabulary,
and share one compiled representation. Adding a per-tenant behavior means
adding a consumer, not a second matching mechanism.

This document describes the mechanism as built in the tenant token prototype
([otel-arrow#3635](https://github.com/open-telemetry/otel-arrow/pull/3635),
[otel-arrow#3636](https://github.com/open-telemetry/otel-arrow/pull/3636)).

## Tenant tokens

Tokens are declared once, at engine scope:

```yaml
engine:
  tenant_tokens:
    edge_tenant:
      extractors:
      - key: tenant_id
        transport_header: x-tenant-id
        retain: true
      - key: project_id
        transport_header: x-project-id
```

There is no per-group, per-pipeline or per-node override. A token is a shared
vocabulary; scoping it would let two pipelines disagree about what `tenant_id`
means, and a value resolved in one pipeline must be readable by the same key
in the next hop.

A token is a list of extractors and **resolves only when every extractor
resolves**. `edge_tenant` above requires both headers; a request carrying only
`x-tenant-id` resolves nothing. Keys that should resolve independently are
declared as separate tokens. Several tokens may resolve on one request, which
is how independent forms of tenancy coexist: an end-user identity alongside an
acting-on-behalf-of identity, or a modern header convention alongside a legacy
one.

## Extractors

| Extractor | Source of the value |
| --- | --- |
| `transport_header: <name>` | An inbound header, matched case-insensitively |
| `generic_key: <value>` | A static value the pipeline mints for itself |
| `remote_address: true` | The network peer's address |
| `imported_key: <name>` | A key admitted at an upstream topic boundary |

The term _transport header_ is generic. Receivers for non-HTTP protocols
derive headers according to the protocol in use: the Kafka receiver resolves
message headers exactly as the gRPC receiver resolves metadata, so a Kafka hop
is not a hole in the identity chain. Authorization extensions are the source
of trusted material; token definitions are not required to use secure fields,
so pipeline operators remain responsible for secure configurations.

### Retaining values

By default an extracted value is available for **matching only**: conditions
test it, but it costs no bytes on the request and cannot be read back.

- `retain: true` keeps the value, which is what lets an exporter re-emit it or
  a processor name a partition from it.
- `bag: true` implies `retain` and additionally carries the key's _name_. This
  is the only way a name travels with a request; every other use of a key name
  is compiled out at startup. Bagged values are encoded as a complete OTLP
  `repeated KeyValue` field, so instrumentation can append a request's tenant
  identity to span or log attributes by copying bytes.

Retention is what allows tenant tokens to **subsume the general-purpose
transport header map**. The engine previously copied every captured header
into an owned name/value pair on every request. It now copies only the values
the operator declared, packed into one allocation with the key names compiled
out, and the separate transport-header capture policy is retired.

## Conditions: the shared vocabulary

Every node that makes a per-tenant decision evaluates an ordered list of
**conditions**, first match wins. A condition is a set of **entries** that
must all match. An entry with a `value` requires that exact value; an entry
without one is a wildcard requiring only that the key be present.

```yaml
routes:
- entries:
  - key: tenant_id
    value: acme
  - key: tier          # no value: present with any value
  to: acme_priority
- entries:
  - key: tenant_id
    value: acme
  to: acme_bulk
```

`Entry` and `Condition` are engine-level configuration types rather than
node-local ones, and every consumer reuses them:

| Consumer | Configuration type | Decision |
| --- | --- | --- |
| `processor:tenant_router` | `TenantRouting` | Output port |
| `exporter:topic` | `TenantRouting` | Topic |
| `exporter:kafka` | `TenantRouting` | Topic, partition key, record headers |
| `processor:batch` | `TenantPartitioning` | Partition, so a merged batch never mixes tenants |
| `exporter:topic`, `receiver:topic` | `TenantContextRules` | Keys admitted across a trust boundary |
| Limiter policies (future) | `Condition` | Limit bucket |

Keeping one route type means both kinds of routing are declared, collected and
validated by the same code, and a new consumer inherits the entire compiler
without contributing any matching logic of its own.

### Where conditions are declared

Nodes place their route table under the well-known configuration key
`tenant_routing`, and their boundary policy under `tenant_context`. The
controller collects conditions by walking every node config in a pipeline
group **without knowing which node types exist**, builds the registry, and
publishes it through the pipeline context before any node starts.

That is the hinge of the design: because declarations are discoverable from
configuration alone, the compiler sees the whole engine's demand up front. A
condition testing a key that no bound token declares, or a value never
declared to the registry, fails the configuration at startup rather than
silently matching nothing at runtime.

## Egress: naming the wire

A token says nothing about the header name its retained value is re-emitted
under. The token is the portable identity; the wire name is a site-specific
decision belonging to whichever node does the emitting.

```yaml
exporter:
  type: exporter:otlp_grpc
  config:
    grpc_endpoint: http://backend:4317
    tenant_headers:
    - key: tenant_id
      header: x-acme-customer
    - key: trace_state
      header: x-trace-state
      binary: true
```

- The same token leaves one exporter as `x-acme-customer` and another as
  `x-customer-id`. Neither is "the" name, and a token carries no assumptions
  about any backend.
- `binary: true` selects gRPC binary metadata. The `-bin` suffix gRPC requires
  is appended at startup, and raw bytes are emitted rather than a base64 form,
  so a value cannot be double-encoded by a second hop.
- **Static configuration wins on collision.** A tenant header configured under
  a name the exporter also sets statically, typically `authorization`, is
  dropped. Tenant material can never shadow a backend credential.
- A key that no token retains is reported and skipped at startup.

## Trust boundaries

A boundary between pipeline groups is where tenant material could leak between
tenants, so each side names the keys it admits and everything unnamed is
dropped. One type serves both directions:

```yaml
tenant_context:
  export_keys: [tenant_id]     # read by the egress side
  import_keys: [tenant_id]     # read by the ingress side
  tenant_tokens: [edge_tenant] # resolved after import
```

The inbound context is never adopted as-is. The receiving side admits the keys
it names, then resolves its own tokens over the admitted values plus any
locally minted ones, so the downstream pipeline evaluates conditions against
identities it declared itself. An absent policy admits nothing, the
fail-closed answer for a policy nobody wrote.

Store-and-forward within one pipeline, such as `processor:durable_buffer`, is
not a trust boundary: the context leaves and returns against one registry, so
it means what it always meant and there is nothing to re-derive.

## Compiler mechanics

Conditions are compiled, not interpreted. All string comparison happens once,
at startup, and each request costs a fixed number of lookups no matter how
many conditions are declared. The shape is a compile-time hash join.

![Tenant token lifecycle: the registry compiled from configuration, the packed
context constructed at the receiver, and the downstream consumers that probe
it](./multitenancy-tenant-diagram.svg)

### Build phase (startup)

- Key names are interned to a key id, token names to a token index.
- Each token gets a `key_mask`, one bit per extractor.
- Header extractors are indexed by lowercased header name; static and imported
  extractors have their own indexes.
- Conditions are grouped by **signature**: the pair of sorted fixed keys and
  sorted wildcard keys they test. Conditions sharing a signature share a hash
  table, because they hash the same terms in the same order.
- A signature applies to a token only when its required keys are a subset of
  that token's keys. Each applicable (token, signature) pair is assigned a
  dense slot.

### Resolve phase (per request)

1. Reset a reusable, receiver-owned scratch buffer. Steady state allocates
   nothing here.
2. Run the static extractors, then make **one pass over the request headers**,
   clearing `key_mask` bits as extractors are satisfied.
3. A token whose mask reaches zero is resolved. If no token resolves, the
   request carries no tenant context at all and the whole feature costs one
   branch.
4. Resolve each key's value to a **symbol** by exact dictionary lookup, then
   pack one word per allocated pair slot, projecting symbols onto the
   signature's keys in signature order.

Token resolution being all-or-nothing yields a useful simplification: if a
signature applies to a resolved token, every wildcard key it requires is
necessarily present, so no per-request presence mask is needed.

### Probe phase

Evaluating a node's conditions costs one bit test plus one integer lookup per
bound (token, signature) pair, keeping the lowest matching condition index so
that first-match-wins holds across signatures. It allocates nothing, and the
cost is independent of how many routes are configured, how many entries each
route tests, and how large the batch is.

### The packed request context

The per-request result is a single allocation carried with the request data.
Nothing in the layout is self-describing: every key is registered, so a value
is addressed by the **value slot** its key occupies in the registry, and no
name, key id or descriptor travels with it.

![What one request carries: the arriving request and the engine declaration,
the counts in w0, and then three columns -- packed symbols, value offsets and
the values -- each showing the configuration that creates it, the bytes it
holds, and the node that reads it](./multitenancy-context-diagram.svg)

The figure above works one request all the way through, and pairs each region
with the configuration that causes it. Only the first two words sit at fixed
positions; the two counts they carry locate everything after them, so a reader
computes the start of the symbol words, the offset array and the blob with two
shifts and an add. The blob's split is what makes the bag copyable whole: its
leading `bag_len` bytes are already the bytes a `LogRecord.attributes` or
`Span.attributes` field wants, tagged with the consumer's own field number at
compile time. A slot addresses its value directly in either region, so a reader
never needs to know which region a key landed in, and an absent key is one
compare against an empty offset.

Reading the three regions together shows why the layout is shaped this way.
Conditions produce the packed symbol words, and each node resolves at startup
which word its own (token, signature) pair occupies, so at runtime it reads one
known word and looks it up in its own compiled table. The `retain` and `bag`
modifiers produce the offsets and the values, which are read by index rather
than matched. Nothing is searched and nothing is decoded.

Two details in the figure are worth naming, because they are what keep the
representation honest. A key that is matched but never retained, like `tier`,
appears in a packed symbol word but owns no value slot and therefore costs no
bytes. And a value no condition ever named, like `gold`, packs the reserved
`unknown` symbol rather than a symbol of its own, so a condition naming that
key fails to match instead of matching by accident: in the figure that is
exactly why the batch partition misses and the request takes the catch-all.

Because the layout is positional, two requests with equal tenant values
produce byte-equal contexts regardless of header ordering, which is what makes
partition names derived from the context stable across producers.

### Matching is exact

The integer probed is a packed tuple of dictionary symbols, not a digest.
Hashing selects a candidate and the candidate is verified against the literal
the registry owns, exactly as a database hash join verifies equality after the
hash narrows the candidates. **A hash collision cannot route one tenant's data
to another tenant's destination.**

A value that no condition mentions resolves to a reserved "unknown" symbol, so
it matches nothing rather than colliding with something. Such a request takes
the configured default, never another tenant's port.

### Budgets

Symbol width trades breadth for density. The compiler enforces the budget at
startup and fails the configuration rather than truncating.

| Declared keys | Distinct values per key |
| --- | --- |
| up to 4 | about 65,000 |
| up to 8 | 254 |
| up to 16 | 14 |

Values are counted per key across all conditions that mention them, not per
request, and a key that is retained but never matched consumes no symbol
space. Retained values are separately budgeted at 65,535 bytes per request
after encoding; a value that does not fit is dropped whole and reads as absent
downstream, so nothing is truncated and the attribute bag stays a well-formed
OTLP field.

## Routing, batching and load balancing

`processor:tenant_router` sends each message to a named output port. The
decision reads no telemetry, which is the difference from
`processor:content_router`: route on the tenant context when the routing
dimension is _who sent the request_, and on content when it is _what the data
says_. Routing is exclusive and a message is routed whole.

```yaml
type: processor:tenant_router
outputs: [acme, premium, unmatched]
config:
  tenant_routing:
    routes:
    - entries: [{ key: tenant_id, value: acme }]
      to: acme
    - entries: [{ key: tier, value: premium }]
      to: premium
    default_to: unmatched
```

Without `default_to`, an unmatched message is NACKed; unmatched data is never
delivered to an arbitrary port. `exporter:topic` and the Kafka exporter use
the same route table to select a topic, with one deliberate asymmetry: an
unresolvable topic-routing key fails at startup, because falling back to the
static topic would deliver one tenant's data to another tenant's topic, while
an unresolvable header key only drops decoration and is reported and skipped.

Routing across a topic boundary is what turns tenant identity into resource
isolation: a tenant's data is delivered into a pipeline group of its own,
which carries its own CPU and memory policies enforced by the operating
system. No per-tenant limiter is involved, because the isolation is a property
of the deployment. See [coarse multitenancy through
configuration](./multitenancy-overview.md#coarse-multitenancy-through-configuration).

Batching reuses conditions as partitions, so every output batch carries a
single tenant context and retained values survive the merge intact. This
serves the same purpose as the OpenTelemetry Collector's
[`sending_queue::batch::partition::metadata_keys`](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md#sending-queue-batch-settings).

Static routes isolate a small number of destinations. Engines running on many
cores also need to spread a large number of tenants, which is organized
through the topic broker: a hash over the tenant context selects one of N
topic receivers. The design of load balancing is out of scope for this
document.

## Future work

- **Limits.** Rate and resource limiters select their bucket with the same
  conditions, giving per-tenant limits with no second matching mechanism. See
  [the overview](./multitenancy-overview.md).
- **Resource-attribute tenants.** In single-resource requests, resource
  attributes can serve as extractor input, for example `resource_attribute:
  service.name`. Behavior in multi-resource contexts remains unresolved.
- **Reconfiguration.** Extractors and conditions are coupled and must be
  reconfigured atomically; the context header carries an `epoch` naming the
  layout generation for this purpose.
- **Coverage.** OTLP gRPC, OTLP HTTP and Kafka receivers resolve tokens today;
  the OTAP receiver does not yet.

## See also

- [Multitenancy overview](./multitenancy-overview.md)
- [Configuration model](./configuration-model.md)
