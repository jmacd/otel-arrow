# E6a. Type the rule plan and its partial state

[Worklist](../metrics-transducer-worklist.md) | [E6](e6.md)

Compile the fixed rule and both query families to a versioned, acyclic plan.
A cell key is `(rule generation, projected identity, window)`. Evaluate
dependencies at one input cut and boundary; a dependent rule reads that
evaluation, not the last scrape. Preserve source locations for diagnostics.

## Operator contracts

- `Select` binds resource, scope, descriptor, point attributes and logical
  writer. `Resolve` takes these identified points to interval deltas or live
  gauge observations. Resolve resets, unknown starts and no-data per source
  epoch **before** projecting away a host or writer.
- `Window` assigns eligible contributions to aligned `(start, end]` panes.
  Record origin, period, range and coverage policy. `Aggregate` declares
  typed `partial(inputs) -> S`, `merge(S, S) -> S`, `finalize(S) -> V`,
  including the empty/absent case and a versioned Arrow schema for `S`.
- `Map` transforms typed cells; `Combine` matches compatible cells at one
  boundary. Declare units, matching keys, uniqueness and fan-out bounds.
  `Emit` fixes output name, kind, unit, temporality and attribute provenance.
  `explain` prints these contracts, staging eligibility and the A1 form for
  a simple projection and natural aggregate.

## When is staging valid?

Partition resolved contributions, not raw cumulative samples cut off from
their predecessors. For disjoint partitions, finalizing merged partials must
equal evaluating their union. Child keys must retain every parent key;
temporal rollups must tile the parent without gaps or overlap. Retries are
not new partitions. Detect integer overflow; specify a reproducible
reduction or comparison tolerance for floats.

- **Additive:** add sums or compatible histogram bucket counts. Merge row
  counts by summing partial counts, not by counting partial rows.
- **Idempotent:** merge minima with `min`, maxima with `max`; replacing a
  child minimum may require recomputing from remaining children.
- **Algebraic:** pass `(sum, count)` for a mean. For live gauges `[2,4]` and
  `[12]` at one boundary the parent gets `18/3 = 6`, not `(3+12)/2`.
- **Order-dependent:** select the last eligible observation per contributor
  with an event-time tie-breaker and staleness; never sum successive snapshots.
- **Composite:** merge each branch of a ratio before dividing; finalized
  child ratios alone cannot stage. **Holistic:** reject raw quantile and the
  RFC's excluded `topk` family in v0. Report the offending node and span.

## Checkpoint without consuming open groups

Fold contributions with `GroupsAccumulator::update_batch` on the local core.
The [DataFusion 55.1 API][groups] is not a peek: `state(EmitTo)` and
`evaluate(EmitTo)` remove emitted groups. At cut C, rotate the mutable
accumulator. Drain its `state(All)` once to immutable partial arrays keyed
by stable cell identity, not transient group indices. Combine these with
any previous base; post-cut updates go to a fresh accumulator. Keep the
base live and merge it into scratch state for provisional `evaluate(All)`.
Bound base, updates and scratch; backpressure before rotation if they will
not fit.

D2 commits that base with resolver baselines, schema and cut. Restore via
`merge_batch`, never by re-adding finalized values. An extraction failure
must stop the evaluator and recover from the last commit and retained input.
Accept when checkpointing and publishing an open cell twice, then adding a
late contribution, matches an uninterrupted direct fold after restart.

[groups]: https://docs.rs/datafusion-expr-common/55.1.0/datafusion_expr_common/groups_accumulator/trait.GroupsAccumulator.html
