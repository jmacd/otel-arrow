# BKCR — Bottom-K Chao-Reserve

A per-thread log-sampling data structure that produces, every period,
a small **bounded sample of log events** annotated with
inverse-probability weights. Designed for the OTAP Dataflow Engine
(`auto_sampler` feature) where each pipeline thread owns one
sampler instance and emits one batched flush per period to the
internal telemetry receiver.

## What problem this solves

Internal telemetry log volume is dominated by a small number of
chatty callsites and tailed by a much larger number of rare
callsites. We want:

1. **Cost control.** A hard cap of `T` events emitted per period
   per thread, regardless of arrival rate.
2. **Unbiased estimates.** Downstream counts (events/sec per
   callsite) should be statistically correct from the sample.
3. **Tail visibility.** Operators must still see at least one
   example of a rare callsite when it fires, even if statistics
   omit it.

BKCR addresses all three: a Bottom-K sketch with exponential ranks
(weighted sampling without replacement) for (1) and (2), augmented
with a bounded *novelty reserve* for (3).

## References

This algorithm is a synthesis of three pieces of prior work:

[1] **Duffield, Lund, Thorup (2007).** "Priority Sampling for
Estimation of Arbitrary Subset Sums." *Journal of the ACM*
54(6):32. The foundational priority-sampling result: from a stream
of weighted items, keep the `k` items with the smallest rank
`r_i = u_i / w_i` (`u_i ~ U(0,1)`); the Horvitz–Thompson
estimator `ŵ_i = max(w_i, 1/τ)` where `τ` is the `(k+1)`th rank
is unbiased for any subset sum, with provably near-optimal
variance.

[2] **Cohen & Kaplan (2007).** "Summarizing Data using Bottom-k
Sketches." *PODC 2007*, pp. 225–234. Generalises [1] to the
**Bottom-k sketch family**: assign each item a random rank
`r_i ~ f_{w_i}`, keep the `k` smallest. The choice `r_i = u_i/w_i`
(*priority*, or PRI ranks) is [1]; the choice `r_i = -ln(u_i)/w_i`
(*exponential*, or EXP ranks) yields **WS-sketches** (weighted
sampling without replacement) and supports tighter estimators for
subpopulation queries, with negative covariances between items.
BKCR uses EXP ranks because the downstream queries are
per-callsite counts (a subpopulation-weight aggregate).

[3] **Chao (1984).** "Nonparametric Estimation of the Number of
Classes in a Population." *Scandinavian Journal of Statistics*
11(4):265–270. The Chao1 lower-bound estimator for total species
richness from a sample, combined with the Good–Turing missing-mass
formula `f_1 / n`, yields an estimate of the *expected frequency
of an as-yet-unseen callsite*. BKCR uses this estimate as the
weight assigned to callsites that were not present in the prior
period's frozen frequency table.

## Algorithm in one paragraph

Time is divided into **periods**. At the start of each period BKCR
holds a *frozen* per-callsite frequency table `freq_prev` from the
previous period; from this it derives, for each callsite `c`, a
weight `w_c = 1 / freq_prev[c]` (or a Chao1-derived `unseen_weight`
if `c` is novel). On every arrival from callsite `c`, BKCR draws
`u ~ U(0,1)`, computes the EXP rank `k = -ln(u) / w_c`, and keeps
the event in a bottom-`T` max-heap if `k` is below the heap's root
`τ`. Heap-rejected events from callsites not yet observed by the
sketch this period are placed in a bounded *novelty reserve* of
capacity `R` (FIFO with no eviction). At flush, kept heap items
are emitted with Horvitz–Thompson weight
`max(1, 1 / (1 − e^{−τ·w_c}))`, and reserve items absent from the
heap's sample are emitted with **weight 0** as observational
records that do not influence statistical estimates. The current
period's tallies are then frozen as `freq_prev` for the next
period.

## Data structures

```text
target           : T (sample-size cap)
reserve_capacity : R (novelty-reserve cap; R = 0 disables it)
min_period_count : Chao1 stability threshold (default 32)

freq_prev        : HashMap<Callsite, u64>   -- last period
freq_curr        : HashMap<Callsite, u64>   -- this period (accumulating)
heap             : BinaryHeap<HeapEntry>    -- max-heap of size ≤ T+1
tau              : f64                      -- current threshold (heap root)
unseen_weight    : f64                      -- Chao1/Good–Turing imputation

reserve          : Vec<(Callsite, Payload)> -- novelty FIFO, size ≤ R
reserved_set     : HashSet<Callsite>        -- membership index, size ≤ R

rng              : SmallRng
```

## Hot path

`admit(c)` — the per-event entry point, called *before* the
caller has formatted the payload. Returns a three-variant
`Admission`:

```text
*freq_curr.entry(c).or_insert(0) += 1
w_c = freq_prev.get(c).map(|f| 1.0 / f as f64)
              .unwrap_or(unseen_weight)
u   = rng.gen::<f64>()
k   = -u.ln() / w_c               // EXP rank, [2]
if k < tau:           return Admit(k)
if reserve.len() < R && !reserved_set.contains(c):
                       return Reserve
                       return Skip
```

`insert(c, payload, admission)` — called only when the caller
chose to format the payload after `admit` returned `Admit` or
`Reserve`:

```text
match admission:
    Admit(k) =>
        heap.push({ key: k, callsite: c, payload })
        if heap.len() > T:
            heap.pop()
            tau = heap.peek().key
    Reserve =>
        reserve.push((c.clone(), payload))
        reserved_set.insert(c)
    Skip => unreachable
```

`Skip` short-circuits the caller before any formatting cost is
paid; this is the dominant path in steady state for chatty
callsites.

## Flush

```text
flush_into(out):
    kept = HashSet::with_capacity(heap.len())
    for entry in heap.drain():
        w_c = freq_prev.get(&entry.callsite).map(|f| 1.0/f as f64)
                       .unwrap_or(unseen_weight)
        pi  = if tau.is_finite() { 1.0 - exp(-tau * w_c) } else { 1.0 }
        weight = (1.0 / pi).max(1.0)
        kept.insert(entry.callsite.clone())
        out.push((entry.callsite, entry.payload, weight))

    for (c, p) in reserve.drain(..):
        if !kept.contains(&c):
            out.push((c, p, 0.0))

    // period boundary
    n_curr = freq_curr.values().sum()
    if n_curr < min_period_count:
        freq_prev.clear()
        unseen_weight = 1.0
    else:
        unseen_weight = chao1_unseen_weight(freq_curr.values())  // [3]
        swap(&mut freq_prev, &mut freq_curr)
    freq_curr.clear()
    reserved_set.clear()
    tau = +infinity
```

## Properties

1. **Output size is bounded.** `|out| ≤ T + R` always.
2. **Horvitz–Thompson unbiased ([1, 2]).** For any subset
   `J ⊆ callsites`, `E[Σ_{i kept, i∈J} ŵ_i] = Σ_{i∈J} 1` (total
   arrivals in `J`). Weight-0 reserve entries contribute nothing
   and so cannot bias the estimator.
3. **Temporally uniform.** Because BKCR's frozen-weight scheme
   uses `freq_prev` (constant within a period), the probability
   that an event at position `i` in the period is admitted is
   the same as for an event at any other position. There is no
   within-period bias toward early or late events.
4. **Coverage of singleton callsites in steady state.** Once
   `freq_prev` exists, a callsite `c` with `freq_prev[c] = N_c`
   that fires once in the current period is kept by the heap
   with probability `1 − exp(−τ / N_c)`; absent that, it is
   kept by the reserve with probability 1 provided fewer than
   `R` novel callsites preceded it that period.
5. **No conditioning corruption.** The reserve is a downstream
   consumer of `admit`'s reject signal; it does not feed the RNG,
   `τ`, or the heap. K's sampling distribution is identical to
   the `R = 0` baseline whatever `R` is.
6. **Hash-only state.** No per-callsite metadata is allocated
   outside the two `HashMap`s and the reserve set. Steady-state
   allocation is zero if `freq_curr` capacity has been preheated.

## Configuration

| knob               | meaning                                            | default |
|---|---|---|
| `target`           | sample size `T` per period                         | (req'd) |
| `reserve_capacity` | novelty reserve `R`; `R=0` disables               | `T`     |
| `min_period_count` | drop `freq_prev` if window arrivals < this        | `32`    |
| seed / RNG         | injectable for tests                               | OS rand |

## Output contract

`Vec<(Callsite, Payload, f64)>` per flush. The `f64` is the
Horvitz–Thompson weight:

- `weight ≥ 1`: a statistical sample. The kept record stands in
  for `weight` real events from the same callsite. Sum or
  average downstream as `Σ_i w_i · x_i`.
- `weight == 0`: a *novelty* record. Observational only;
  contributes nothing to statistical estimates by construction.
  Useful for visibility ("did callsite X fire at all this
  period?") and as a signal that `T` may be too small.

## Monitoring

BKCR exposes no new metric. The reserve size is observable as
`count(records where weight == 0)` per flush. Operators
monitoring "are rare callsites being squeezed out?" should alert
when that count is consistently trending toward `R`; in that
regime, increasing `T` is the correct response.

Skew, effective-sample-size, and other statistical-health signals
are recoverable downstream from the weighted records themselves.

## Brief experimental results

A 500-trial × 3-window × 10 000 events synthetic study (`src/sampler/bkcr.rs::criterion_bench`
and the historical `analyze_sampling` example) on four
long-tailed categorical distributions confirmed:

- **Weight unbiasedness.** Steady-state (post warmup) per-callsite
  relative error of `Σ ŵ` is ≤ 2.5 % in every distribution
  tested, for every callsite with ≥ 1 expected occurrence.
- **Temporal flatness.** Decile histograms of kept-event timestamps
  within a window are visually uniform for every callsite.
- **Singleton inclusion.** A callsite with one expected
  occurrence per window of 10 000 events is captured by BKCR
  (heap + reserve, `T = 100`, `R = 100`) in > 99 % of trials in
  steady state.
- **Chao1 accuracy.** The Chao1 / Good–Turing weight estimate
  matches empirical missing-mass within ≈ 0.3 % on every
  non-degenerate test distribution; this is the foundation that
  makes `unseen_weight` a sensible default for novel callsites
  in the first window after their introduction.

### Performance

Micro-benchmark medians on `x86_64`, single thread, release
build, for one period of 10 000 events with `T = 100`:

| workload   | wall time   | throughput        |
|---|---|---|
| uniform    | ~ 180 µs    | ~ 55 M events/s   |
| zipf-like  | ~ 200 µs    | ~ 50 M events/s   |
| bursty     | ~ 205 µs    | ~ 49 M events/s   |

Per-event hot-path cost is dominated by one `f64::ln`
(~6 ns) plus an `HashMap` insert/lookup on `freq_curr` /
`freq_prev` (~10–15 ns). On admit, an additional heap push +
optional pop (~30 ns amortised) is paid. The skip path costs one
RNG draw, one `ln`, one divide, and one compare — well under
50 ns. Formatting cost of the log record itself, which BKCR
*avoids* on the skip path, is typically 200 ns – several µs;
that gap is the entire economic justification for the sampler.

## Out of scope (deferred)

- Sliding-window / EWMA frequency tables (currently hard-edge
  period boundaries).
- Per-window adaptive `R`.
- Cross-thread merging of multiple per-thread samples (the
  dataflow-engine integration will handle this at the
  `internal_telemetry_receiver` layer; each thread emits its own
  weighted batch and the receiver concatenates).
