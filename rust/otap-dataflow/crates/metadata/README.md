# otap-df-metadata

The **metadata compiler** turns request-metadata declarations into one immutable
artifact per compiler epoch. A producer builds a compact, reference-counted
context for each request; a consumer admits the request and evaluates only its
own descriptor map.

This crate implements the compiler described by RFC 0003, "Tenant context"
(<https://github.com/open-telemetry/otel-arrow/pull/3686>). It is deliberately a
leaf crate: it has no configuration syntax, no pipeline dependency, and no
engine dependency. The configuration crate translates policy into the
declaration API; the pipeline supplies only producer-to-consumer reachability.

## Direct translation of Envoy

| This crate | Envoy | Meaning |
|---|---|---|
| extractor | rate-limit action | produces one `key=value`, or fails |
| token | descriptor producer | an ordered extractor list; all must succeed |
| consumer token list | route `rate_limits: []` | descriptors this consumer can receive |
| condition | configured descriptor | full key sequence, with literal or wildcard per key |
| condition set | descriptor map | one consumer's ordered configured descriptors |

The binding is intentionally exact:

1. A consumer declares tokens **required** or **optional**. Both are the
   consumer's descriptor list. A condition has no token field.
2. A condition names its complete, ordered key sequence. The compiler binds it
   to every token in that consumer's list that produces exactly that sequence in
   that order. Multiple matching tokens are valid; no match is a configuration
   error.
3. At runtime, each declared token either resolves entirely or produces no
   descriptor. The consumer evaluates every resolved descriptor against its
   condition set, accumulating all matched branches.

`Optional` has Envoy semantics: a token that did not resolve contributes no
descriptor, so no condition over it matches and the consumer applies its
default. `Required` is a DFE admission extension: if it did not resolve, the
engine Nacks before constructing a `ConsumerMetadataView`, so no condition set
can be tested.

Tokens themselves have no optional members. Since a resolved token has every
one of its keys, the resolved-token bitmap answers "is this key present" without
a separate presence bit for every key.

## Carrier field identity

Conditions intentionally use bare `KeyId`: an Envoy descriptor is a sequence of
keys. Carriers are different. A value is identified by
`MetadataFieldId { token, key }`, because distinct tokens may legally produce
the same key from different sources.

`declare_read` accepts a bare key when exactly one token declared by that
consumer produces it. If more than one does, compilation fails
with an ambiguity error and the caller uses `MetadataFieldId::new(token, key)`.
This preserves the convenience of unqualified keys without ever falling back to
declaration order or allowing an untrusted header to shadow an authorized
claim.

A caller that must re-emit a value under the exact name it arrived on declares
one extractor per accepted name. The resolved token then identifies the source,
so every wire name stays compile-time state and no name travels in a context.

## What is compiled

`MetadataCompiler::compile` makes seven passes:

1. **Validate** every declaration, including full Envoy key sequences and
   consumer-to-token bindings, collecting all errors.
2. **Reachability**, globally and per producer. It drops keys, tokens, and
   extractors no reachable consumer can observe, reporting each drop for startup
   logging.
3. **Intern** wire names and value literals. A name lookup is one
   allocation-free probe. A literal becomes a dictionary `Symbol`; a value not
   declared by any condition becomes `UNKNOWN` and matches nothing.
4. **Derive signatures** by dropping wildcards from full conditions. A signature
   is compiler-only; users still declare Envoy's entire key sequence.
5. **Build PairSlots** for `(token, signature)` pairs. A slot defines the
   compact lookup word assembled from that token's extractor symbols.
6. **Build branch tables** for `(condition set, PairSlot)` pairs. A slot word
   directly indexes one selected condition entry. The compiler rejects entries
   that overlap for one token, so tables are bounded and unambiguous.
7. **Fix layout and plans.** The layout is one epoch-wide byte format; each
   producer gets only the extractor work reachable consumers need.

## The packed context

Contexts carry **matching inputs, not matching answers**:

```text
+--------+------------------+-----------------------------------------------+
| offset | region           | contents                                      |
+--------+------------------+-----------------------------------------------+
| 0      | epoch            | u32 compiler epoch                            |
| 4      | layout fingerprint | u64 byte-layout identity                    |
| 12     | token bitmap     | one bit per resolved all-or-nothing token      |
| ...    | symbol field     | bit-packed symbols for value-matched extractors|
| ...    | region index     | u16 offsets for retained values                |
| ...    | data             | retained values                               |
+--------+------------------+-----------------------------------------------+
```

There is no condition-result field. A producer dictionary-encodes each staged
value-matched extractor once and writes its symbol. When a consumer is reached,
it reads only the symbols its PairSlots need, assembles each lookup word, and
performs direct branch-table reads.

This is still `O(1)`: a consumer has a bounded descriptor list and condition
set, so it performs a bounded number of direct reads. Unlike the discarded
eager design, it does not evaluate every statically reachable consumer during
context construction.

The separation has three important properties:

- **Work follows the path.** A request reaching one router pays that router's
  table reads, not every router in the pipeline.
- **Context size follows extracted metadata.** Adding consumers and condition
  sets does not enlarge every request context.
- **Condition answers never enter the context.** Components retain the compiler
  epoch that built in-flight contexts, while a new epoch may change condition
  sets without adding result fields. Adding literals can widen their symbol
  field and requires a new layout fingerprint.

`src/layout.rs` documents each region and works through concrete bytes.

## Condition-set cardinality

A condition set is a descriptor map, not a router switch. Its result is the
bounded, zero-allocation iterator `ConditionMatches`, whose items are:

```text
ConditionMatch { token, entry }
```

Each resolved token yields zero or one match. Two tokens selecting the same
entry yield two items and therefore two limiter applications; they do not
collapse into one branch bit. Entries with different descriptor shapes naturally
classify different tokens. Entries with the same shape must be disjoint: an
entry using `tenant_id=*` overlaps one using `tenant_id=acme`, so that
configuration is rejected instead of inventing multi-dispatch.

To limit on values that arrived in separate tokens, declare a third token with
both extractors. Conditions never join across tokens.

Matches are yielded in the consumer token declaration order, the same order
Envoy uses for its descriptor vector. A duplicate consumer-token declaration is
rejected rather than silently collapsed; representing that Envoy case requires a
future consumer-token instance identifier.

## Request-time flow

```text
producer: offer inputs -> resolve tokens -> dictionary encode -> one packed context
consumer: context view -> required-token admission -> PairSlot lookup -> condition matches
```

```rust
let mut encoder = compiled.encoder(receiver, &mut scratch);
encoder.offer_transport_header("x-tenant-id", b"acme");
let context = encoder.finish()?;

let view = compiled.view(&context)?;
let consumer = view.consumer(router)?; // Missing required token -> Nack.
for matched in consumer.matches(limits) {
    apply_limit(matched.token, matched.entry);
}
```

`MetadataScratch` is reused per producer and does not allocate at steady state.
An empty context is `Bytes::new()` and allocates nothing. A nonempty context is
one `Bytes` allocation; clones are refcount increments.

## Safety and bounds

Every dimension is bounded by `Limits`: declarations, dictionaries, PairSlot
width, branch tables, scratch, values, and packed context bytes. Exceeding a
bound is an explicit error, never unbounded growth.

Hashing finds a literal candidate only; the dictionary verifies candidate bytes
before assigning a symbol. A hash collision cannot cause a cross-tenant match.
