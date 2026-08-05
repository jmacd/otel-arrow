# Algorithm K+R — K with novelty-reserve

Final design spec for the hybrid sampler that augments Algorithm K
with a bounded "reserve" channel for novel callsites. Ready to
implement.

## Motivation (brief)

Algorithm K is the better steady-state sampler — temporally
uniform, weight-unbiased, and (per the long-tail experiment)
matches or beats J on rare-callsite inclusion once `freq_prev` is
populated. K's only structural weakness is the *first window* of
a stream, where with no prior frequency table every callsite is
treated as equipriority and a callsite firing once has only
`~T/n` chance of being kept.

The novelty reserve fills this gap with zero perturbation to K's
statistics: it captures one example of each callsite that K
rejected this window and emits those examples with HT weight 0,
so they affect the output's observability surface but not its
statistical estimates.

## Data structures

K's existing state, unchanged:

- `target: usize` — sample size `T`.
- `min_period_count: u64` — Chao1 stability threshold (existing).
- `freq_prev: HashMap<C, u64>` — frozen prior-period counts.
- `freq_curr: HashMap<C, u64>` — accumulator for current period.
- `heap: BinaryHeap<HeapEntry<C, P>>` — bottom-`T+1` max-heap.
- `tau: f64` — current threshold (root of heap).
- `unseen_weight: f64` — Chao1/Good-Turing weight for callsites
  not in `freq_prev`.
- `rng: SmallRng`.

New state:

- `reserve_capacity: usize` — max reserve entries per period (`R`).
- `reserve: Vec<(C, P)>` — captured first-rejected callsites this
  period, FIFO, capped at `R`.
- `reserved_set: HashMap<C, ()>` — membership index of `reserve`;
  size at most `R`. (Or fold into a `HashSet<C>`; choose whichever
  matches the rest of the codebase's idiom. Initial sizing `R`.)

`reserve` and `reserved_set` are cleared at every flush.

## Hot-path algorithm — `admit(c)`

```text
admit(c: &C) -> Admission:
    // Always count for the next period's frozen table.
    *freq_curr.entry(c).or_insert(0) += 1

    // Standard K priority computation.
    let w_c = match freq_prev.get(c):
        Some(f) => 1.0 / (f as f64)
        None    => unseen_weight
    let u   = rng.gen::<f64>()
    let k   = u / w_c

    if k < tau:
        // K admits — reserve plays no role.
        return Admit(k)

    // K rejected. Consider the reserve.
    if reserve.len() < reserve_capacity
        && !reserved_set.contains(c):
        return Reserve

    // Rejected, not reserved (already in reserve OR reserve full).
    return Skip
```

`Admission` becomes a three-variant enum: `Admit(f64) | Reserve | Skip`.

## Hot-path algorithm — `insert(c, payload, admission)`

```text
insert(c: C, payload: P, admission: Admission):
    match admission:
        Admit(k) =>
            heap.push(HeapEntry { key: k, callsite: c, payload })
            if heap.len() > target:
                heap.pop()
                tau = heap.peek().key   // new threshold

        Reserve =>
            reserve.push((c.clone(), payload))
            reserved_set.insert(c, ())

        Skip => {}  // drop
```

## Flush — `flush_into(out: &mut Vec<(C, P, f64)>)`

```text
flush_into(out):
    // 1. Drain K's heap with standard HT weights.
    let tau_final = tau
    let mut kept_callsites: HashSet<C> = HashSet::with_capacity(heap.len())
    out.reserve(heap.len() + reserve.len())
    for entry in heap.drain():
        let w_c = match freq_prev.get(&entry.callsite):
            Some(f) => 1.0 / (f as f64)
            None    => unseen_weight
        let weight = if tau_final.is_finite():
                         (1.0 / (tau_final * w_c)).max(1.0)
                     else:
                         1.0
        kept_callsites.insert(entry.callsite.clone())
        out.push((entry.callsite, entry.payload, weight))

    // 2. Drain the reserve, deduped against K's sample,
    //    with weight 0 as the novelty signal.
    for (c, p) in reserve.drain(..):
        if !kept_callsites.contains(&c):
            out.push((c, p, 0.0))

    // 3. K's period-boundary bookkeeping (unchanged from current
    //    algo_k.rs).
    let n_curr: u64 = freq_curr.values().sum()
    if n_curr < min_period_count:
        freq_prev.clear()
        unseen_weight = 1.0
    else:
        unseen_weight = chao1_unseen_weight(freq_curr.values().copied())
        std::mem::swap(&mut freq_prev, &mut freq_curr)
    freq_curr.clear()
    reserved_set.clear()
    tau = f64::INFINITY
```

## Weight semantics

- `weight > 0` (typically ≥ 1): standard Horvitz–Thompson weight.
  The kept record represents `weight` real events from the same
  callsite. Sums and averages downstream should use `Σ w_i · x_i`
  with these weights.
- `weight == 0`: a *novelty* record. The kept payload is an
  observational example of a callsite that fired but was not
  selected into K's statistical sample. Contributes nothing to HT
  estimates by construction.

## Output shape

`Vec<(C, P, f64)>`. Same shape K already produces. No new tag is
needed; consumers distinguish reserve entries by `weight == 0.0`.

## Properties

1. **HT estimator unbiased.** Because reserve entries carry weight
   0, `E[Σŵ_c]` over a window equals K's unbiased estimate of
   `N_c` for every callsite — identical to vanilla K.
2. **No conditioning corruption.** Reserve entries are K-rejected
   events. K's RNG, τ, and heap state are computed exactly as in
   vanilla K. The reserve is a downstream consumer of K's reject
   signal; it never alters K's sampling.
3. **Coverage.** For every callsite firing ≥ 1 event in a window:
   - If K admitted any of its events, the callsite appears in the
     output with positive HT weight.
   - Else if the callsite was the (≤ R)-th distinct callsite K
     rejected this window, it appears with weight 0.
   - Else (reserve full when it was first rejected): may be absent
     from this window's output.
4. **Bounded output.** `|output| ≤ T + R` always.
5. **Bounded memory.** Reserve uses `O(R)` payload storage and
   `O(R)` HashSet entries per period.

## Configuration knobs

- `target` (`T`) — sample size. Existing K knob.
- `min_period_count` — Chao1 stability gate. Existing K knob.
- `reserve_capacity` (`R`) — **new**. Max reserve entries per
  period. Default: `R = T` (so worst-case output size ≤ `2T`).
  `R = 0` disables the reserve and yields exact vanilla K
  behavior (useful for A/B and for downstream code paths that
  cannot tolerate weight-0 entries).

## Monitoring

The sampler exposes no new metric. The reserve size is
observable as `count(records where weight == 0)` in each flushed
batch. Operators monitoring "K is missing callsites" should
alert when that count is consistently > 0 or trending toward
`R`.

Skew, variance, ESS, and other statistical-health signals are
computable downstream from the weighted records themselves.

## Testing strategy

Augment the existing `crates/telemetry/src/sampling/tests.rs`:

1. **Unbiasedness preserved** — re-run the Zipf per-callsite
   `Σŵ`-recovery test with `R > 0` and assert results are
   statistically identical to vanilla K (reserve entries
   contribute 0).
2. **Reserve population guarantee** — construct a stream where a
   specific callsite is guaranteed to be rejected by K (e.g., one
   chatty + one singleton with `R = 1`, very small `T`); assert
   the singleton appears with weight 0 in the output.
3. **Reserve cap** — flood the stream with `> R` brand-new
   callsites; assert reserve does not exceed `R` and that excess
   novel callsites are simply absent.
4. **Dedup** — construct a case where a reserved callsite later
   becomes K-admitted; assert the output contains only the
   K-admitted record (no weight-0 duplicate).
5. **Reserve reset between periods** — flush twice; assert
   reserve state is fully cleared so window 2 starts with empty
   reserve and reserved_set.
6. **`R = 0` disables reserve** — every output entry has weight
   ≥ 1; behavior is bit-for-bit vanilla K.

The existing temporal-uniformity / decile-histogram property test
should remain green — the reserve doesn't push anything into K's
heap, so the temporal distribution of K's sample is unchanged.

## What this leaves for later

- **Reserve eviction policy when full.** Current spec: stop
  reserving once `R` is reached. Alternatives (FIFO, random
  replacement, "prefer rarer-predicted callsites") add complexity
  without an obvious benefit; revisit only if operational data
  shows the cap is hit often enough to matter.
- **Per-window adaptive `R`.** Could grow `R` in response to
  observed novelty pressure. Not in this spec; static `R` first.
- **Retirement of Algorithm J.** With K+R covering the
  "first-arrival visibility" use case that was J's main
  advantage, J becomes redundant. Recommend keeping J in-tree
  through the K+R rollout for comparison, then removing once K+R
  is established.
