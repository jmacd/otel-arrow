# Long-tail sampling experiment: J vs K (steady state)

Experiment by jmacd, last updated 2026-05-22.

## What was measured

Four hard-coded categorical distributions over imaginary log
callsites. For each, the experiment drives **3 consecutive windows
of 10 000 events** through one sampler instance with target sample
size **T = 100**, repeats **500 trials**, and aggregates.

Per kept event we record its callsite, its arrival decile within
the window, and its Horvitz–Thompson sampling weight.

**This report excludes window 1 from the comparison.** Window 1 is
Algorithm K's cold-start regime: with no prior-period frequency
table, K falls back to `unseen_weight = 1.0`, treating every
callsite as equipriority. That is not a property of K's
steady-state behavior; it is the unavoidable consequence of having
no information yet. The validation of Chao1 itself (see
`chao1-validation.md`) shows the estimator is accurate to within
~10% in non-degenerate cases — window 1 simply doesn't have enough
data to compute it. All tables below use **window 3** data, which
matches window 2 within Monte-Carlo noise.

Code: `rust/otap-dataflow/crates/telemetry/examples/analyze_sampling.rs`.
Reproduce: `cargo run --release -p otap-df-telemetry --example analyze_sampling`.

## Headline findings (steady state)

In steady state J and K produce **unbiased weight sums** for every
callsite in every distribution (Σŵ matches the true arrival count
to within a few percent — Monte-Carlo noise at 500 trials). They
differ on three things:

| Property | Algorithm J | Algorithm K |
|---|---|---|
| Within-window temporal distribution (chatty callsites) | **clustered at start of window** | uniform |
| Sample budget per callsite | logarithmic in callsite rate | roughly equal across active callsites |
| Rare-callsite inclusion | per callsite: very high; total tail coverage: limited by budget split | per callsite: matches or exceeds J |

K is at least as good as J on rare-callsite inclusion in every
distribution we tested, and strictly better when the long tail is
dense (many singletons). K is also the only one of the two with
temporally uniform sampling within a window.

## Distribution 1: extreme (9999 / 1)

One chatty callsite producing ~9 999 events, one singleton.

| Callsite | True count | Algo | Inclusion | Σŵ rel err | Decile histogram |
|---|---:|---|---:|---:|---|
| c=0 (chatty)    | 9999.0 | J | 100.0% | −1.5% | `█▁▁▁▁▁▁▁▁▁` |
| c=0 (chatty)    | 9999.0 | K | 100.0% | −0.0% | `██████████` |
| c=1 (singleton) |    1.0 | J |  63.8% | +0.0% | (1 event) |
| c=1 (singleton) |    1.0 | K |  63.8% | +0.0% | (1 event) |

Both algorithms cover the singleton equally well in steady state.
The visible difference is purely temporal: J keeps all 99 of its
chatty-callsite slots from the first 1 000 events; K spreads them
across the window.

## Distribution 2: mixed long-tail (5000 / 2000 / 1000 / 20 / 10 / 1)

| Callsite | True count | Algo | Inclusion | Kept/trial | Σŵ rel err | Decile histogram |
|---|---:|---|---:|---:|---:|---|
| c=0 (5000)  | 6227 | J | 100.0% | 29.9 | −2.3% | `█▁▁▁▁▁▁▁▁▁` |
| c=0 (5000)  | 6227 | K | 100.0% | 21.6 | −0.2% | `██████████` |
| c=1 (2000)  | 2491 | J | 100.0% | 26.5 | −0.7% | `█▁▁▁▁▁▁▁▁▁` |
| c=1 (2000)  | 2491 | K | 100.0% | 21.6 | −0.5% | `██████████` |
| c=2 (1000)  | 1244 | J | 100.0% | 24.7 | +0.4% | `█▁▁▁▁▁▁▁▁▁` |
| c=2 (1000)  | 1244 | K | 100.0% | 22.1 | +2.5% | `██████▇▇▇█` |
| c=3 (20)    |   25 | J | 100.0% | 10.1 | +0.6% | `█▆▄▃▃▂▂▂▁▁` |
| c=3 (20)    |   25 | K | 100.0% | 21.3 | +0.9% | `███▇██████` |
| c=4 (10)    |   12 | J | 100.0% |  7.6 | +1.5% | `██▇▆▅▄▄▃▃▃` |
| c=4 (10)    |   12 | K | 100.0% | 12.2 | +0.1% | `▇█▇█▇▇▇█▇▇` |
| c=5 (1)     |    1 | J |  73.0% |  1.25 | −0.0% | (1 event) |
| c=5 (1)     |    1 | K |  73.0% | 1.26 | +0.0% | (1 event) |

Notable: **J spreads its 100-slot budget logarithmically across
callsites** (29.9 / 26.5 / 24.7 / 10.1 / 7.6 / 1.25 ≈ T).
**K spreads it more evenly** (21.6 / 21.6 / 22.1 / 21.3 / 12.2 /
1.26). For chatty callsites c=0..c=2, K spends fewer slots and uses
the savings on the medium-frequency callsites c=3..c=4. Rare
callsite c=5 has identical 73% inclusion under both algos.

## Distribution 3: deep tail (12 callsites, weights 5000 → 1)

| Callsite | True count | Algo | Inclusion | Kept/trial | Σŵ rel err |
|---|---:|---|---:|---:|---:|
| c=0  (5000) | 5625 | J | 100.0% | 16.2 | +0.8% |
| c=0  (5000) | 5625 | K | 100.0% | 10.0 | −0.1% |
| c=1  (2000) | 2249 | J | 100.0% | 14.4 | −0.4% |
| c=1  (2000) | 2249 | K | 100.0% | 10.2 | +2.3% |
| c=2  (1000) | 1125 | J | 100.0% | 13.1 | −3.6% |
| c=2  (1000) | 1125 | K | 100.0% |  9.9 | −1.3% |
| c=3   (500) |  564 | J | 100.0% | 12.0 | −0.5% |
| c=3   (500) |  564 | K | 100.0% | 10.3 | +3.1% |
| c=4   (200) |  226 | J | 100.0% | 10.5 | +2.4% |
| c=4   (200) |  226 | K | 100.0% | 10.1 | +0.7% |
| c=5   (100) |  113 | J | 100.0% |  9.0 | −0.4% |
| c=5   (100) |  113 | K | 100.0% | 10.2 | +1.3% |
| c=6    (50) |   56 | J | 100.0% |  7.7 | −0.4% |
| c=6    (50) |   56 | K | 100.0% | 10.4 | +1.9% |
| c=7    (20) |   23 | J | 100.0% |  6.0 | +0.8% |
| c=7    (20) |   23 | K | 100.0% | 10.3 | −0.3% |
| c=8    (10) |   11 | J | 100.0% |  4.7 | −0.2% |
| c=8    (10) |   11 | K | 100.0% |  9.6 | +0.3% |
| c=9     (5) |  5.7 | J |  99.8% |  3.5 | +1.2% |
| c=9     (5) |  5.7 | K |  99.8% |  5.7 | +0.2% |
| c=10    (2) |  2.2 | J |  90.2% |  1.9 | +1.5% |
| c=10    (2) |  2.2 | K |  90.2% |  2.2 | +0.0% |
| c=11    (1) |  1.1 | J |  66.6% |  1.0 | −2.7% |
| c=11    (1) |  1.1 | K |  66.6% |  1.1 | +0.0% |

**Inclusion rates are identical between J and K** across every
callsite in this distribution. The differences are in budget
allocation (K is flatter: 10 events per callsite, vs J's 16 → 1
fall-off) and in within-window temporal distribution.

Decile histograms:

```
c=0 (5000):  J █▁▁▁▁▁▁▁▁▁    K ▇██▇▇▇▇▇▇▇
c=1 (2000):  J █▁▁▁▁▁▁▁▁▁    K █▇▇███▇▇▇█
c=2 (1000):  J █▁▁▁▁▁▁▁▁▁    K ▇▇█▇▇█▇▇▇█
c=3 (500):   J █▁▁▁▁▁▁▁▁▁    K ▇▇█▇█▇█▇▇▇
c=4 (200):   J █▂▁▁▁▁▁▁▁▁    K ▇█▇█▇▇▇▇██
c=5 (100):   J █▂▁▁▁▁▁▁▁▁    K ▇▇▇█▇█▇█▇█
c=6 (50):    J █▃▂▁▁▁▁▁▁▁    K ██████████
c=7 (20):    J █▅▃▂▂▁▁▁▁▁    K (flat)
```

J's temporal bias gets *worse* with chattier callsites. K is
uniformly flat regardless of rate.

## Distribution 4: "many rare" (5 chatty + 10 medium + 100 singletons)

The most interesting test: 100 callsites that each fire exactly
once per window, mixed with 5 chatty callsites at 1 000 each and
10 medium callsites at 50 each.

Bulk view — how many of the 113 callsites land in each inclusion
tier in steady state:

| Algo | ~100% | 50–99% | 10–49% | <10% |
|---|---:|---:|---:|---:|
| J | 0 | **42** | **71** | 0 |
| K | 0 | **113** | 0 | 0 |

K achieves 50–99% inclusion for **all 113 callsites**; J leaves 71
of 113 callsites stuck in the 10–49% inclusion tier.

Spot-check, four representative singletons:

| Callsite | Algo | Inclusion | Kept/trial |
|---|---|---:|---:|
| c=4 (chatty, 1000) | J |  98.0% | 3.8 |
| c=4 (chatty, 1000) | K |  58.4% | 0.82 |
| c=15 (singleton)   | J |  45.0% | 0.52 |
| c=15 (singleton)   | K |  53.6% | 0.86 |
| c=99 (singleton)   | J |  49.6% | 0.56 |
| c=99 (singleton)   | K |  54.6% | 0.88 |
| c=109 (singleton)  | J |  46.2% | 0.57 |
| c=109 (singleton)  | K |  52.2% | 0.83 |

K's frozen-weight scheme makes singletons strictly more attractive
than chatty callsites in the priority order (`w_singleton = 1`,
`w_chatty = 1/1000`), so K keeps more singletons and fewer chatty
samples than J. J keeps c=4 (the chatty 1000-rate callsite) at 98%
inclusion across 3.8 events per trial, while K trades that surplus
to lift singleton inclusion across the long tail.

## Interpretation

1. **Both algorithms are weight-unbiased.** Σŵ matches true counts
   to within a few percent for every callsite in every
   distribution. Either algorithm produces correct
   Horvitz–Thompson estimates for downstream aggregation.

2. **J has a temporal bias toward the start of each window.** It
   freezes each event's priority key as `u · f_at_admit`; later
   events from a chatty callsite get mechanically larger keys and
   are crowded out of the bottom-T heap. Visualized as the
   `█▁▁▁▁▁▁▁▁▁` decile histograms above. K is temporally uniform
   by construction (all events from one callsite are i.i.d. in
   priority).

3. **K is at least as good as J on rare-callsite inclusion in
   every distribution, and strictly better when the long tail is
   dense.** In Distribution 4 K keeps every singleton in the
   50–99% inclusion band while J leaves 71 of 113 callsites at
   10–49%. K's mechanism: singletons have `w_c = 1` and chatty
   callsites have `w_c = 1/N`, so singletons dominate the bottom
   of the heap.

4. **The two algorithms differ in sample budget allocation.** J
   gives each active callsite roughly `τ · (1 + ln(N_c/τ))` slots —
   a logarithmic spread. K gives each active callsite roughly
   `min(N_c, T / S_active)` — a flatter spread that frees budget
   from the chatty top and reinvests it in the long tail.

## Recommendations

- For **steady-state log sampling** in any system that runs longer
  than ~1 window before its sampling decisions matter, **K is the
  better choice**: it matches or beats J on rare-callsite inclusion
  and is the only one that produces a temporally uniform sample.

- For **bootstrapping the first window of a stream** (or any
  scenario where a brand-new callsite must be caught on its first
  arrival), J has an inherent advantage — its `f_at_admit = 1`
  gives a fresh callsite the smallest possible key. K has no
  information about a callsite it has never seen before and falls
  back to `unseen_weight = 1.0`, which makes it a uniform-random
  sampler in window 1.

- A **hybrid scheme** worth considering: use K's frozen weights +
  heap as the steady-state engine, but treat the *first arrival*
  of any callsite within a window as a forced admit (an additive
  reserve slot, not occupying the main T-heap). That would
  inherit J's instant-coverage property for new and rare callsites
  without giving up K's temporal flatness or its steady-state
  budget efficiency.

## Reproduce & raw output

- Code: `rust/otap-dataflow/crates/telemetry/examples/analyze_sampling.rs`
- Full raw report (all 4 distributions × 3 windows × per-callsite
  tables, including window 1): `/tmp/sampling_report.md`.
- Chao1-estimator validation: `chao1-validation.md`.
- Algorithms: `crates/telemetry/src/sampling/algo_{j,k}.rs`
