# Idempotency keys as tenant token dimensions

Design notes from a study session on open-telemetry/otel-arrow PR #3588,
in the context of PRs #3583, #3635 and #3636.

Intended to become an example explaining how to implement idempotency keys
in Quiver.

Status markers used below:

- **[verified]** read directly from code, config, or the GitHub API during
  the session.
- **[inferred]** reasoned, not confirmed. Treat as a claim to test.

## 1. Starting point

PR #3588 (seddonm1) assigns a UUIDv7 to each `OtapPdata` at `Context`
creation and preserves it through the Quiver WAL and qseg replay, so an
exporter retrying after a crash can name output deterministically in a
shared destination such as object storage.

Its shape **[verified]**:

- `Context` gains a non-optional `Uuid`; `Default` is hand-written and calls
  `Uuid::now_v7()`.
- Quiver gains `pub type IdempotencyKey = [u8; 16]` and
  `RecordBundle::idempotency_key() -> Option<IdempotencyKey>`, defaulted to
  `None`, so Quiver never interprets the value.
- qseg advances v1 -> v2 with a nullable `FixedSizeBinary(16)` manifest
  column; WAL gains entry type `1` carrying 16 bytes.
- New readers accept v1; old readers reject v2 rather than silently drop
  identity.

The layering (Quiver stays UUID-agnostic, UUID semantics live in OTAP) is
right and should be preserved by anything that replaces it.

## 2. Prior art in OpenTelemetry

Gathered for the SIG action item on issue #3587. All **[verified]** via the
GitHub API.

| Ref | What | Outcome |
| --- | --- | --- |
| proto#218 | "Add an idempotency key to ExportMetricsServiceRequest", jmacd, 2020-09-02 | Closed by author 2021-02-23: use resource identity, no key needed |
| proto#219 | Companion PR, `optional bytes idempotency_key` | Closed unmerged 2020-12-23 |
| spec#2862 | "Unique request id and other packet metadata in OTLP", 2022 | Still open |
| contrib#40751 | `awss3exporter` UUIDv7 object keys | Merged v0.131.0, 2025-07-16 |
| collector#15384 | Queue-oriented storage interface for persistent queues | Open |
| collector#11488 | "OTEL at-least-once e2e guarantee" | Open since 2024, zero replies |

Two objections killed proto#219: request-level keys do not survive Collector
intermediaries or load balancers, and the proposal did not distinguish
"fewer duplicates" from "exactly-once".

Neither objection applies to a key scoped to one pipeline with a declared
policy at every boundary. That is the framing to use on #3587.

The strongest precedent *for* the mechanism is contrib#40751: the collector
already mints UUIDv7 object names, but does so inside `Upload()` at PUT
time, so a retry or crash-replay writes a duplicate object under a new name.
That is the exact hole this work closes.

Collector's persistent queue keys items by a process-local monotonic `u64`
(`persistent_queue.go`, `getItemKey`), invisible to exporters and unstable
across a queue rebuild -- the same weakness as a local `segment_seq`.

## 3. The reframing

An idempotency key is not a new mechanism. It is one **extractor kind** in
the tenant token system, and its behavior is configured the same way every
other request-context dimension is configured.

The general rule the tenant token work establishes: **a node that constructs
an `OtapPdata` rather than forwarding one is a new-context creator, and
declares a projection** -- which dimensions to carry forward, which to mint
locally, which to drop. Everything not named is dropped.

Under that rule the idempotency key needs no special site, no special
lifetime, and no special persistence path.

Proposed extractor, with variants that make the historical positions
configurable rather than mutually exclusive:

```yaml
- key: idem
  idempotency_key: uuid_v7                # mint locally      (= PR #3588)
- key: idem
  idempotency_key: { transport_header: x-idempotency-key }
                                          # client-supplied   (= proto#218/#219)
- key: idem
  idempotency_key: derived                # content hash      (= spec#2862)
```

Benefits over a dedicated `Context` field:

- Costs nothing when unused. #3588 puts `Uuid::now_v7()` in
  `Context::default()`, which runs at roughly 84 call sites **[verified]**,
  many of them tests or identity-irrelevant. It also makes `Default`
  non-deterministic and changes `PartialEq`, so two default contexts are no
  longer equal.
- Makes totality a configuration property rather than an architectural one.
  Tenant tokens deliberately fail closed and resolve to nothing when
  unmatched; durability identity must be total. A pipeline that needs a key
  declares a token whose only extractor mints it, so that token always
  resolves for that pipeline and nobody else pays.
- Removes the need for a qseg format fork dedicated to one field. If the key
  is just a retained dimension, Quiver persists the whole packed context
  once, for every dimension.

## 4. Topology facts

These settle several questions that were otherwise guesswork.

**`durable_buffer` is store-and-forward, not pass-through** **[verified]**:
module docs say data is written "to a write-ahead log and segment storage
before forwarding downstream", and `TimerTick` polls storage for bundles to
send downstream. There is no bypass.

Consequence: the happy path and the replay path are the **same path**. The
exporter always sees a key read back from storage, never one held in memory,
so first delivery and post-crash replay cannot disagree. The idempotency
property falls out of the topology rather than needing to be maintained.

**No batcher appears in any durable-buffer config** **[verified]**, all four
are receiver -> durable_buffer -> exporter (or with a debug processor
between). So a persisted bundle is one receiver-scoped pdata carrying
exactly one key. #3588's `Option<[u8; 16]>` per `RecordBundle` is the
correct cardinality.

**Collector's exporterhelper is queue-before-batch** **[verified]**:
`queuebatch/queue_batch.go` constructs the queue with the batcher as its
consumer callback, `queue.NewQueue(..., b.Consume)`. Requests are persisted
individually and merged only after dequeue. Same ordering.

**Collector already has an N-to-1 request context merge, and its rule is
span links** **[verified]**: `queuebatch.Settings.MergeCtx`, implemented in
`batch_context.go` as `contextWithMergedLinks`, appends the parents of both
contexts into a link list. Direct precedent for a batcher assembling a
series of keys, and independent confirmation that span context under
batching is many-valued and belongs in links.

Corollary on span data: **span ids cannot serve as idempotency keys.** Every
retry gets a new span by definition, and every retry must reuse the same
key. An inbound trace_id is client-controlled and unreliable across retries.
Carry both, with different merge rules: span context is provenance
(many-valued, collected into links), the key is output identity.

Columnar density is not an argument for batching before the queue here.
Quiver's `OpenSegment` aggregates many bundles into per-slot streams within
one segment, and issue #3587 states the density step explicitly ("before
Parquet compaction"). So otel-arrow recovers density at compaction rather
than at a pipeline batcher, and can adopt queue-before-batch without the
tradeoff -- better positioned than the collector, whose queue stores opaque
blobs that gain nothing from aggregation. **[inferred]**

## 5. durable_buffer as a boundary

`durable_buffer` is a boundary with two sides, like a topic exporter and
topic receiver pair, except it crosses **time** rather than a pipeline
group.

- **Ingress (send side)** behaves like a receiver in the full sense: it
  declares its own tokens and runs extractors over the inbound context. This
  is where the key is minted, and where a projection decides which
  dimensions get persisted.
- **Egress (replay side)** is a new-context creator that **imports** and
  must never mint. `convert_bundle_to_pdata` is that site; in #3588 its
  `context_from_bundle` falls back to `Context::default()` **[verified]**,
  which mints a fresh UUID on the read path and destroys the property being
  bought.

The existing config shapes already fit: `TenantBoundaryPolicy { allow_keys }`
for the export side, `TenantContextRules { import, tenant_tokens }` for the
import side **[verified]**. One contract, three implementations: topic
boundary, storage boundary, any future store.

`export_boundary(scratch, view, allow: &[KeyId])` is already a projection
**[verified]** -- it rebuilds the blob rather than masking it, "because the
packed buffer is shared and any byte left in it stays readable". And
`partition_processor` already derives fresh contexts with `set_tenant(derived)`
and `fork_request_scoped` **[verified]**, so the pattern is implemented, not
hypothetical.

`batch_processor` is the one unconverted site: line 1040 constructs
`OtapPdata::new(Context::default(), ...)` and the file contains no tenant
handling at all **[verified]**. When converted it becomes the first N-to-1
projection, needing a per-key merge rule -- `require_equal` as a fail-closed
default (which would enforce the single-tenant-per-batch invariant that the
`attribute_field::SCOPE` comment already assumes), and `collect` where a
series is wanted. `collect` needs `AnyValue.array_value`, the same encoding
addition that `ClaimValue::Many` requires.

## 6. Reconfiguration

Planned direction: a `tenant_epoch` generation number in the request context
identifying the consumer tenant state each request matches, hard-failing
requests with an unknown epoch, retiring generations on a configurable
timeout.

The code is pre-wired: `build(self, generation: u16)` takes the generation
and word 0 carries `epoch:16` **[verified]**, so multiple live generations
need no layout change.

**Hard-fail is right at ingress and wrong at replay.** At a receiver or
topic boundary an unknown epoch means the request has not yet been accepted;
failing it is honest, and the client retries onto the current generation. At
`durable_buffer` egress an unknown epoch means data **already acknowledged
upstream**, with nobody left to retry it -- hard-failing there turns a config
change into loss of acked data.

The timeout does not bridge this, because context lifetimes differ by orders
of magnitude: a request context lives milliseconds to seconds, a persisted
context lives as long as retention, and unboundedly if downstream is wedged
and bundles are deferring with backoff. That is the precise sense in which
`durable_buffer` is the restricted case: it is the one boundary that cannot
outlive the problem.

### Partial recovery through the bag

The resolution does not require a new region. When `bag: true`, `pack_words`
writes each bagged key as `<tag(bag_field)><len><KeyValue{key, value}>`
contiguously at the front of the blob, and `attributes()` returns
`blob()[..bag_len]` as a complete OTLP repeated field **[verified]**. Names
and values, parseable **without the registry**.

So the recoverability axis is:

| | addressing | recoverable at unknown epoch |
| --- | --- | --- |
| `bag: true` | key name, self-describing OTLP | yes |
| `retain: true` only | registry value slot | no |

`bag: true` costs the key name bytes per request and buys survivability
across reconfiguration. Users choose per key. Splitting dimensions across
separate token definitions gives the granularity, since recovery goes
through names rather than slot positions: changing token A does not prevent
recovering token B.

A useful property: the bag is already valid OTLP, so one encoding serves
both "recover tenant context after a config change" and "project tenant
context into exported telemetry attributes". The durability representation
and the output representation are the same bytes.

Wraparound invariant to write down: 65536 generations is ample *given*
timeout-based retirement, but the safety argument is specifically that a
retired epoch must not be reused while any context carrying it still exists.
A stale context matching a recycled epoch would be read as valid rather than
rejected, which is the one failure mode the epoch exists to prevent.

## 7. Changes required

Ordered roughly by dependency.

1. **Produce `ValueKind::Binary`.** `TokenScratch::store` and
   `export_boundary` both hardcode `ValueKind::Text` **[verified]**, so
   `Binary` is defined and encoded by `put_any_value` but never produced. A
   decoded gRPC `-bin` header, or a 16-byte UUID, is currently written as
   `AnyValue.string_value` -- invalid OTLP for non-UTF-8 bytes. This is a
   live bug independent of idempotency, and a prerequisite here because the
   bag becomes the recovery representation.
2. **Add the minting extractor kind.** `resolve`'s static-extractor loop
   already branches on `extractor.value: Option<_>` (`Some` = literal,
   `None` = peer address) **[verified]**; minting becomes a third case.
3. **Fall back rather than drop on epoch mismatch.** `resolve_imported`
   early-returns `None` at the top on mismatch **[verified]**. Partial
   recovery needs a second path that extracts by name over the bag.
4. **Record `bag_field` with a persisted context.** The run is pre-tagged
   with the registry's attributes field number (`RESOURCE=1`, `SCOPE=3`,
   `LOG_RECORD=6`, `EXEMPLAR=7`, `SPAN=9`) **[verified]**. A recovery parser
   reading bytes from a differently-configured generation needs that number.
5. **Add a format version byte beside the generation.** The generation
   digests registry contents within one binary and says nothing about a
   layout change across builds. Harmless while contexts never outlive a
   process; not harmless once they are persisted.
6. **Convert `batch_processor`** to a projection site with per-key merge
   rules, and add `AnyValue.array_value` support for `collect` and for
   `ClaimValue::Many`.

### Validation rules

- A pipeline containing `processor:durable_buffer` must declare a retained
  idempotency dimension; fail at startup rather than silently persisting
  unidentified bundles.
- A replay projection may import that dimension but may not mint it.
- A minted or derived key is non-matchable: a condition testing it is a
  compile error, not a silent never-match. The registry interns condition
  literals at compile time and undeclared values fall to a reserved unknown
  symbol, so such a config would otherwise be accepted and never fire.

## 8. What Quiver itself needs

- Persist the packed context as an **opaque variable-length blob**, not a
  typed `FixedSizeBinary(16)` column. Quiver stays agnostic, exactly as
  #3588's `[u8; 16]` intends, but generalized to every dimension. An
  object-store exporter naming `tenant/project/uuid.parquet` needs the
  tenant dimensions too, not the key alone.
- Store `(format_version, epoch/generation, bag_field, words)`.
- Consider **dictionary encoding** on the manifest column. Segments are
  tenant-partitioned, so contexts repeat heavily within one segment and an
  Arrow `DictionaryArray` should collapse the column almost entirely.
  **[inferred]**
- Keep the `RecordBundle` trait shape from #3588: an optional accessor with
  a defaulted `None`, so bundle implementations without durable identity
  are unaffected.

## 9. Open questions

- Set-valued matching. A condition over a multi-valued claim is a
  **membership** test, but the hash join computes one fingerprint per
  (token, signature) with exact byte verification. This is the riskiest
  piece and should be prototyped before commitment.
- Compiling `AuthorizedIdentity` into the bundle. Roughly 20 allocations per
  JWT request today (principal, scheme, BTreeMap nodes, a String per claim
  name and value, a Vec per `Many`); its own comment says an interned
  representation "belongs with the shared tenant-token work" **[verified]**.
  Compiling claim names into declared extractor keys collapses that, but
  changes behavior: only declared claims survive, which is better for policy
  matching and lossy for audit.
- Whether a fixed-width directly-addressed region is still worth adding.
  The bag removes the *durability* argument for it, leaving only read cost
  (`slot_value` decodes two varints to reach a value whose width is known at
  compile time). Note `HEADER_WORDS` is 2 and word 0 is fully packed
  (`n_fp:16 | n_slots:16 | epoch:16 | bag_len:16`) **[verified]**, so a
  third region needs a header word or must take its extent from the
  registry.
- Operator story for changing tenant token configuration with data still
  buffered. Draining before reconfiguration is the simple answer; it should
  be stated policy rather than emergent behavior.

## 10. Corrections made during the session

Recorded so they are not re-litigated.

- **"The key is re-minted at every re-batching boundary, so it identifies a
  batch occurrence rather than source data."** Wrong in its significance.
  Batching was an unprompted line of inquiry, and no durable-buffer config
  contains a batcher. A persisted bundle is one receiver-scoped pdata.
- **"The batcher is a structural lifetime mismatch requiring a repack at
  durable-buffer ingress."** Wrong. `partition_processor` already
  implements context-creating projection; `batch_processor` is simply
  unconverted. Minting at context creation is self-consistent and needs no
  repack.
- **"The batcher already retains all N contexts for ack routing."** Asserted
  from a field name; a grep for `MultiContext`'s definition returned
  nothing. Unverified, withdrawn.
- **"Durability-critical dimensions need a format-defined fixed region to
  survive a generation change."** Superseded. `bag: true` already provides
  name-addressed, registry-free recovery.
