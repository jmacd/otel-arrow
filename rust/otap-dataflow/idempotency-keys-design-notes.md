# Idempotency keys as tenant token dimensions

Design notes from a study session on open-telemetry/otel-arrow PR #3588,
in the context of PRs #3583, #3635 and #3636.

Intended to become an example explaining how to implement idempotency keys
in Quiver.

Status markers used below:

- **[verified]** read directly from code, config, or the GitHub API during
  the session.
- **[inferred]** reasoned, not confirmed. Treat as a claim to test.

## 0. The claim

Idempotency is not a feature the engine has to grow. It is something an
operator implements **by configuration**, out of machinery that already
exists for another purpose.

That is the interesting part, and it is a claim about the tenant token
feature rather than about idempotency. Tenant tokens are named for their
motivating case, but what they actually provide is general: a declared set
of request-scoped dimensions, extracted once at a named site, carried in one
fixed per-request encoding, and projected explicitly at every boundary that
data crosses. Tenancy is one use of that. Output identity is another.

An idempotency key differs from a tenant id in three ways, none of which
needs new mechanism:

| | tenant id | idempotency key |
| --- | --- | --- |
| source | read from the request | minted locally |
| use | routing and matching | output naming |
| totality | may be absent | must always resolve |

Minting is an extractor kind. Naming is what a retained value is for.
Totality is a property of a token whose only extractor always succeeds, so
it is a configuration choice rather than an architectural one.

The test of the claim is that a second, unrelated use of the feature lands
as YAML plus one extractor, and does not perturb the design of anything
else. Section 7 is the honest accounting of how close that is.

The complete shape, for reference before the argument:

```yaml
engine:
  tenant_tokens:
    durable:
      extractors:
        - key: idem
          idempotency_key: uuid_v7   # the only new mechanism
          retain: true               # carry it with the request
          bag: true                  # and carry its name, so it survives
                                     # a configuration change
```

Nothing else in the pipeline is configured for it. `durable_buffer`
persists whatever context a request carries and hands the same context back
on replay; an exporter names its output from the key by reading it like any
other retained value.

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

This is the concrete form of the claim in section 0. The dimension is not a
tenant, the use is not routing, and the value is minted rather than read --
and none of that requires the feature to change shape. If the reframing
holds for a dimension this unlike a tenant id, the mechanism is more general
than its name.

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

## 5. durable_buffer is a pause, not a boundary

An earlier draft of these notes treated `durable_buffer` as a boundary with
two sides, symmetrical with a topic exporter and topic receiver pair. That
was wrong, and correcting it removes most of the work the draft proposed.

A **topic** joins two pipeline groups. They may compile against different
registries, they may belong to different operators, and the packed buffer
handed across stays readable by whoever receives it. So each side names an
allowlist, and the receiving side never adopts the inbound context: it
admits the keys it named and re-resolves its own tokens over them, so it
evaluates conditions against identities it declared itself. That machinery
exists and works **[verified]**.

`durable_buffer` writes and reads in **one pipeline**, against **one**
engine-scoped registry, separated only by time. The context that comes back
is the context that went in, and it means exactly what it meant. There is
nothing to re-derive and no second party to withhold anything from.

| | topic | durable_buffer |
| --- | --- | --- |
| separates | two pipeline groups | one pipeline from itself |
| across | trust | time |
| registry | may differ | same |
| inbound context | evidence, re-derived | identity, restored |
| configuration | allowlists on both sides | none |

Consequences, all subtractive:

- **No tenant configuration on the node.** No export list, no import list,
  no token binding. There is nothing an operator could usefully say.
- **No re-resolution on replay.** The fast path is
  `set_tenant(Arc::from(stored_bytes))`: no extractors, no repack, no
  allocation beyond the restore itself. Replay is *cheaper* than a topic
  hop, not comparable to it.
- **No "must declare a retained idempotency dimension" validation rule.**
  That rule existed only to stop a projection from silently dropping the
  key. With no projection there is nothing to drop.

What does still need care is `convert_bundle_to_pdata`, which builds the
replayed pdata. It calls `OtapPdata::new(Context::default(), payload)` at
two sites **[verified]**, so today a replayed message has no tenant context
at all. It must restore the stored bytes instead. This is the one place
where getting it wrong quietly destroys the property being bought, and it
is the same site PR #3588 got wrong in the other direction by minting a
fresh UUID on the read path **[verified]**.

The general rule from section 3 still holds, and this is a case of it: a
node that constructs an `OtapPdata` rather than forwarding one is a
new-context creator. The projection such a node declares just happens, for
a store-and-forward node, to be the identity projection.

`partition_processor` already implements context-creating projection with
`rewrite` and `fork_request_scoped` **[verified]**, and the topic pair
implements the two-sided kind, so both patterns have working references.
`batch_processor` remains the one unconverted site: it constructs
`OtapPdata::new(Context::default(), ...)` and contains no tenant handling at
all **[verified]**. It is not on the critical path here -- no durable-buffer
config contains a batcher -- but when converted it becomes the first N-to-1
projection, needing a per-key merge rule: `require_equal` as a fail-closed
default, and `collect` where a series is wanted. `collect` needs
`AnyValue.array_value`, the same encoding addition `ClaimValue::Many`
requires.

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

Ordered roughly by dependency. This is the honest accounting against the
claim in section 0: how much of "idempotency is configuration" is actually
configuration today.

**Done since the study session:**

1. ~~**Produce `ValueKind::Binary`.**~~ `TokenScratch::store` and
   `export_boundary` hardcoded `ValueKind::Text`, so a 16-byte UUID would
   have been written as `AnyValue.string_value` -- invalid OTLP for
   non-UTF-8 bytes. Value kind is now fixed per key at build time from the
   source the key binds to, and every staging path reads it **[verified]**.
   This was the stated prerequisite, and it is cleared: the bag can carry a
   raw UUID today.

**Still required, and the whole of what "implementing idempotency" costs:**

2. **Add the minting extractor kind.** The one genuinely new mechanism.
   Four touchpoints:
   - `Extractor` gains an untagged variant keyed on `idempotency_key`.
   - `StaticExtractor.value: Option<Box<[u8]>>` becomes three-way. Today
     `None` *means* peer address **[verified]**, so minting cannot reuse it.
   - `resolve_key_kinds` must mark a minted key `Binary`. Only `-bin`
     headers get that today **[verified]**.
   - `KeyBinding` gains a variant, so binding one key to both a mint and a
     header is caught as the configuration error it is.

**Required for durability across a configuration change, not for
idempotency itself:**

3. **Recover from the bag on epoch mismatch.** `resolve_imported`
   early-returns `None` when the epoch differs **[verified]**, which is
   right at a topic and wrong after storage. A second path is needed that
   reads the bag by name rather than by slot:

   ```rust
   pub fn resolve_recovered(
       &self, scratch: &mut TokenScratch,
       attributes: &[u8], bag_field: u32,
   ) -> Option<Arc<[u64]>>
   ```

   It walks the `<tag><len><KeyValue>` run, maps each name through
   `key_id`, and repacks. `key_id` is a linear scan, which is acceptable
   because this runs only when the epoch actually moved.

4. **Record `bag_field` beside the stored bytes.** The run is pre-tagged
   with the registry's attributes field number **[verified]**, and that
   number is registry state rather than part of the packed context, so a
   recovery parser cannot find the run without it.

5. **Add a format version byte beside the generation.** The generation
   digests registry contents within one binary and says nothing about a
   layout change across builds. Harmless while contexts never outlive a
   process; not harmless once they are persisted.

**Independent of this work:**

6. **Convert `batch_processor`** to a projection site with per-key merge
   rules, and add `AnyValue.array_value` for `collect` and for
   `ClaimValue::Many`. Needed before a batcher may precede a durable buffer;
   none does today **[verified]**.

Item 2 is the feature. Items 3 through 5 are the price of letting a
persisted context outlive the configuration that produced it, which is a
storage concern rather than an idempotency one and would be owed by any
persisted dimension.

### Validation rules

- A replay site may restore a stored key but must never mint one. Minting
  at replay produces a fresh identity for data that already has one, which
  is precisely the bug PR #3588 has at `context_from_bundle` **[verified]**.
- A minted or derived key is non-matchable: a condition testing it is a
  compile error, not a silent never-match. The registry interns condition
  literals at build time and undeclared values fall to a reserved unknown
  symbol, so such a configuration would otherwise be accepted and never
  fire.
- A key that must survive a configuration change has to be `bag: true`.
  `retain: true` alone is addressed by registry slot and is unreadable once
  the layout digest changes. Warn when a pipeline persists a key that is
  retained but not bagged, because the operator is buying a durability
  property they will not get.

Withdrawn from an earlier draft: "a pipeline containing
`processor:durable_buffer` must declare a retained idempotency dimension."
That rule guarded against a projection dropping the key at the storage
boundary. Section 5 removes the projection, so the rule has nothing to
guard. A pipeline that declares no idempotency dimension simply does not
have idempotent output, which is a choice, not an error.

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
- Epoch reuse against retention. Section 6's invariant -- a retired epoch
  must not be reused while any context carrying it still exists -- is easy
  to hold for request contexts living milliseconds and becomes a joint
  constraint between reconfiguration rate and retention period once
  contexts are persisted. 65536 generations is ample arithmetically; the
  hazard is that a recycled epoch reads as *valid* rather than being
  rejected, which is the one failure the epoch exists to prevent. This
  wants a startup check, not a comment.
- Whether the second-use test in section 0 should be made explicit in the
  tenant token documentation. If the feature is general, saying so where
  operators read about it is most of the value; if it is not, an example
  that only works for tenancy is the evidence.

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

Corrections from the follow-up session, after the topic boundary was
implemented:

- **"`durable_buffer` is a boundary with two sides, like a topic exporter
  and receiver pair, except it crosses time rather than a pipeline group."**
  Wrong, and the error was treating "crosses time" as a variety of "crosses
  trust". A topic joins two pipeline groups that may compile against
  different registries; `durable_buffer` writes and reads in one pipeline
  against one registry. The context that returns needs no re-derivation and
  no policing. Section 5 is rewritten; the export policy, the import
  policy, the token binding and one validation rule all drop out.

- **"`export_boundary` is already the projection the storage boundary
  needs."** True of the primitive, wrong as a recommendation. Projecting at
  a storage boundary costs a repack per message and buys nothing: the leak
  it prevents is a second party reading bytes left in a shared buffer, and
  storage has no second party. Its only real use there would be data
  minimization at rest, which is a different argument and not one these
  notes made.

- **`TenantBoundaryPolicy { allow_keys }`.** Renamed. The struct wrapped one
  `Vec<String>`, there is no `deny_keys` and no other relationship a
  boundary can express, and the enclosing field already named the
  direction. Now `import_keys` and `export_keys` directly. Worth recording
  why they name *keys* and not tokens: a value is addressed by its key's
  slot, so anything crossing a boundary is necessarily key-granular, while
  a token is a resolution outcome that each side recomputes. Keys and
  tokens are many-to-many **[verified]**, so neither list derives from the
  other.

- **"The idempotency key should be minted at `durable_buffer` ingress."**
  Still defensible but not obviously right, and the notes did not
  acknowledge the choice. Minting at a receiver identifies the *request*;
  minting at durable-buffer ingress identifies the *bundle*. With today's
  configurations, one pdata is one bundle and the two coincide
  **[verified]**. They diverge as soon as a batcher sits upstream, at which
  point receiver-side minting yields N keys for one bundle and needs the
  merge rule from item 6.
