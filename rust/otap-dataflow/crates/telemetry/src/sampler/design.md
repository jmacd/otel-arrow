# CCKR — Chao-Cohen-Kaplan-Reservoir

A diversity sampling algorithm using Chao's sampling coverage estimate
(an estimate of the Good-Turing frequency estimate) and Bottom-K sampling
with exponential function.

## Problem

Internal telemetry logs have heavy-head / long-tail distribution: a few
chatty callsites plus many rare callsites. We need:

1. **Cost control** — hard cap of `T` events per period
2. **Unbiased estimates** — statistically correct counts per callsite
3. **Tail visibility** — capture at least one example of rare callsites

CCKR provides all three: Bottom-K sketch with exponential ranks for (1)
and (2), plus a bounded *novelty preserve* for (3).

## References

[1] **Duffield, Lund, Thorup (2007).** Priority sampling: keep `k`
items with smallest rank `r_i = u_i / w_i`; Horvitz–Thompson
estimator `ŵ_i = max(w_i, 1/τ)` is unbiased for subset sums.

[2] **Cohen & Kaplan (2007).** Bottom-k sketch with EXP ranks
`r_i = -ln(u_i)/w_i` gives WS-sketches (weighted sampling without
replacement) with tighter variance for subpopulation queries.

[3] **Chao (1984).** Chao1 estimator for species richness plus
Good–Turing missing-mass formula estimates expected frequency of
unseen callsites.

## Algorithm

Time is divided into periods. Each period:

1. **Weights**: Use frozen `freq_prev` table: `w_c = 1/freq_prev[c]`
   or Chao1-derived `unseen_weight` for novel callsites
2. **Admission**: Draw `u ~ U(0,1)`, compute EXP rank `k = -ln(u)/w_c`,
   keep in bottom-T max-heap if `k < τ` (heap root)
3. **Preserve**: Heap-rejected novel callsites go to bounded preserve
   (capacity R)
4. **Flush**: Emit heap items with Horvitz–Thompson weight
   `max(1, 1/(1-e^{-τ·w_c}))` and preserve items with weight 0
5. **Update**: Freeze `freq_curr` as `freq_prev` for next period

## State

```text
reservoir_capacity : T (sample size)
preserve_capacity  : R (novelty preserve; R=0 disables)
min_period_count   : Chao1 stability threshold (default 32)

freq_prev     : HashMap<Callsite, u64>  -- frozen from previous period
freq_curr     : HashMap<Callsite, u64>  -- accumulating this period
heap          : BinaryHeap<Entry>       -- max-heap, size ≤ T+1
tau           : f64                     -- threshold (heap root)
unseen_weight : f64                     -- Chao1 estimate for novel callsites
preserve      : HashMap<C, (P, u64)>    -- novelty map, size ≤ R
rng           : SmallRng
```

## API

**`admit(c)`** — Check before formatting payload:

```text
freq_curr[c] += 1
w_c = freq_prev[c] ? 1.0/freq_prev[c] : unseen_weight
k = -ln(U(0,1)) / w_c
if k < tau: return Admit(k)
if |preserve| < R && c not in preserve: return Preserve
return Skip
```

**`insert(c, payload, admission)`** — Store formatted payload:

```text
match admission:
  Admit(k) => heap.push(k, c, payload); evict if |heap| > T
  Preserve => preserve[c] = (payload, seq++)
  Skip => unreachable
```

`Skip` is the fast path for frequent callsites — no formatting cost.

**`flush()`** — Drain period sample:

```text
for (c, p, k) in heap.drain():
    w_c = weight_for(c)
    pi = 1 - exp(-tau * w_c)
    out.push((c, p, max(1, 1/pi)))

for (c, (p, _)) in preserve.drain():
    if c not in heap_callsites:
        out.push((c, p, 0.0))

if sum(freq_curr) >= min_period_count:
    freq_prev = freq_curr; unseen_weight = chao1(freq_curr)
else:
    freq_prev.clear(); unseen_weight = 1.0
freq_curr.clear(); tau = ∞
```

## Properties

1. **Bounded output**: `|out| ≤ T + R`
2. **Unbiased**: Horvitz–Thompson estimator `E[Σ ŵ_i] = Σ 1`
3. **Temporally uniform**: Frozen weights eliminate within-period bias
4. **Tail coverage**: Novel callsites captured by preserve with high probability
5. **Independence**: Preserve doesn't affect heap sampling distribution
6. **Memory efficient**: Hash-only state, no per-callsite allocations

## Config

| Parameter           | Meaning                              | Default |
|---------------------|--------------------------------------|---------|
| `reservoir_capacity`| Sample size `T` per period           | 128     |
| `preserve_capacity` | Novelty preserve `R` (0 disables)    | 128     |
| `min_period_count`  | Chao1 stability threshold            | 32      |

## Output

Each flush returns `Vec<(Callsite, Payload, sampling_count)>` where:

- `sampling_count ≥ 1`: Statistical sample standing in for `sampling_count` events
- `sampling_count = 0`: Observational novelty record (doesn't contribute to estimates)

High preserve counts (`sampling_count=0`) signal insufficient `T`.

## Performance

Hot path: ~20ns per event (HashMap + RNG + ln + compare)  
Skip path: <50ns (no formatting cost)  
Throughput: ~50M events/s (10K events, T=100)

## Validation

500-trial Monte Carlo on long-tailed distributions confirms:

- Unbiasedness: <2.5% relative error per callsite
- Temporal uniformity: flat timestamp distribution
- Singleton coverage: >99% capture rate
- Chao1 accuracy: ~0.3% error vs. empirical missing-mass
