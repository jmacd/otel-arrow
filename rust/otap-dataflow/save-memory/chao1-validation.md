# Chao1 / Good-Turing estimator validation

Experiment by jmacd, 2026-05-22. Code:
`rust/otap-dataflow/crates/telemetry/examples/chao1_validation.rs`.

## Why we did this

Algorithm K was missing rare callsites in the long-tail experiment
(see `logs-sampling-experiment.md`). One natural hypothesis: the
Chao1 / Good-Turing prediction baked into `chao1_unseen_weight` is
inaccurate, so the `unseen_weight` value K assigns to a fresh
callsite is wrong, making the priority either too generous or too
stingy.

This experiment checks that hypothesis by comparing the three
quantities the production helper computes — `S_unseen`,
`missing_mass`, `f_unseen` — against ground truth on five
known categorical distributions.

For each distribution and each of 500 trials, we draw a window of
10 000 events, compute the Chao1 predictions from window 1, then
draw a second independent window of 10 000 events to measure the
empirical truth.

## Headline result

**Chao1 is remarkably accurate where it matters.** For every
distribution that actually has a meaningful "unseen tail," the
predicted missing mass matches the empirical missing mass to better
than 1%, and the predicted per-unseen-category mean count
(`f_unseen`, the value K inverts to form `unseen_weight`) matches
the empirical value to within roughly 0%–11%.

| Distribution | True S | mean s_seen | Pred S_unseen / True | Pred missing_mass / Empirical | Pred f_unseen / Empirical | unseen_weight used |
|---|---:|---:|---|---|---|---:|
| uniform-1000 (all weight 1)              | 1000 | 1000.0 | 0.15 vs 0.04   *(degenerate)*  | 0.0000 vs 0.0000 (+5.8%)  | —     *(degenerate)*  | 0.77 |
| zipf-1000 (w_i = 1000/i)                 | 1000 |  867.4 | 121.8 vs 132.6 (−8.1%)         | 0.0206 vs 0.0206 (+0.2%)  | 1.71 vs 1.55 (+10.7%) | 0.59 |
| chatty + thin tail (5×1000, 1000×1)      | 1005 |  816.3 | 190.4 vs 188.7 (+0.9%)         | 0.0315 vs 0.0314 (+0.3%)  | 1.67 vs 1.66 (+0.4%)  | 0.60 |
| many ultra-rare (50×100, 5000×1)         | 5050 | 3210.9 | 1840.6 vs 1839.1 (+0.1%)       | 0.1839 vs 0.1837 (+0.1%)  | 1.00 vs 0.99 (+0.2%)  | 0.98 |
| deep-tail-only (1000×1, no chatty)       | 1000 | 1000.0 | 0.17 vs 0.05   *(degenerate)*  | 0.0000 vs 0.0001 (−13.0%) | —     *(degenerate)*  | 0.78 |

(The two "degenerate" rows are cases where almost every category
was sampled in window 1; the helper correctly produces a tiny or
NaN prediction and the fallback `unseen_weight = 1.0` would be
applied. The 250%+ relative errors on `S_unseen` there are
meaningless — both numerator and denominator are essentially 0.)

## Why this rules out "Chao1 is broken"

The number K actually uses, `unseen_weight = min(1, 1/f_unseen)`,
is derived from a quantity (`f_unseen`) the experiment shows to
be within ~10% of empirical truth in the worst non-degenerate case
and well under 1% in the rest. That precision is much tighter than
the order-of-magnitude differences in K's rare-callsite inclusion
rates we observed in the long-tail experiment.

In other words: even if we had a magically perfect
`unseen_weight`, K's rare-callsite behavior would not materially
change.

## Then why does K miss rare callsites?

The previous experiment's behavior is structural, not an estimator
bug. Two distinct mechanisms:

**1. Window-1 cold start.** With no `freq_prev` yet, K can't run
Chao1 at all. It falls back to `unseen_weight = 1.0`, giving every
callsite the same priority. The bottom-T sample becomes a uniform
random subset of all `n` events, so a callsite that fires once has
roughly `T / n` ≈ 1% chance of inclusion (we measured 0.6%–1.4%).
This is a property of "no information," not of the estimator.

**2. Steady-state competition.** Once K has prior-period data, all
"once-per-window" callsites are stored with `f_prev = 1`, so they
all carry the same `w_c = 1`. In a stream with *many* such
callsites — e.g. the "many rare" distribution with 100 singletons —
they end up competing with each other for the bottom of the
T-element heap. Each individual singleton's per-window inclusion is
≈ (its share of singleton mass) × (singleton mass / total mass)
× T, which can be tiny (~1%) even when Chao1's prediction is
perfectly accurate.

The fix for both mechanisms is the same kind of change: give a
mechanism for "first arrival of a callsite in this window" to win
the priority lottery automatically. That's exactly what
Algorithm J does via `f_at_admit = 1`. A K-style algorithm could
emulate this by treating the *first* event from any callsite seen
this window as a forced admit (an additive reserve slot), or by
combining K's frozen weights with a small J-style reservoir for
new arrivals.

## Reproduce

```
cd rust/otap-dataflow
cargo run --release -p otap-df-telemetry --example chao1_validation \
  > /tmp/chao1_report.md
```

Raw per-distribution output: `/tmp/chao1_report.md`.
