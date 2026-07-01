# Span Head-Sampling by Global Log Surprisal: Algorithm and Research Question

## Status and purpose

This document is a hand-off to the agent running the Bottom-Floor sampling
experiments. It specifies, self-contained, the algorithm that sets the head
(span-start) sampling probability for internal-telemetry spans from the global
log frequency distribution, and it states the divergence-reduction objective the
algorithm is meant to serve so a real-data study can measure its effectiveness.

The algorithm is implemented in the `otap-df-telemetry` crate; the
[Implementation](#implementation-pointers) section gives exact locations. The
broader system it sits in is described in
[`integrated-logs-traces-reservoir.md`](integrated-logs-traces-reservoir.md).
This document repeats every quantity it needs, so the reader does not have to
read that one first.

## Contents

- [Relationship to the Bottom-Floor log sampler](#relationship-to-the-bottom-floor-log-sampler)
- [Setup and notation](#setup-and-notation)
- [The value score: cross-entropy against the global log distribution](#the-value-score-cross-entropy-against-the-global-log-distribution)
- [The allocation: coverage proportional to value](#the-allocation-coverage-proportional-to-value)
- [The objective: reduce divergence](#the-objective-reduce-divergence)
- [What is and is not claimed](#what-is-and-is-not-claimed)
- [Research questions](#research-questions)
- [Suggested experimental design](#suggested-experimental-design)
- [Implementation pointers](#implementation-pointers)
- [References](#references)

## Relationship to the Bottom-Floor log sampler

The log sampler already studied, Algorithm K, is a windowed bottom-k priority
sampler with inverse-frequency weights learned from the previous window and a
rarest-seen floor for unseen callsites. It keeps a bounded sample of size `T`
per window and attaches a Horvitz-Thompson weight to every kept record so that
the sum of weights is an unbiased estimate of the true arrival count for any
callsite or group. That sampler answers the question "which individual log
records do we keep, fleet-wide, by their own callsite frequency."

The span head-sampler answers a different question one level up: "which whole
spans do we keep, by the kind of span, so the spans we keep are a
surprisal-representative sample of span kinds." It does not replace the log
sampler. It consumes the log sampler's output: the per-callsite global frequency
estimates, from which it derives the surprisals that weight span kinds. A span,
once admitted at its start, is recorded together with a reservoir-sampled trace
of the logs that fired inside it; that per-span reservoir is the same bottom-k
mechanism with a single uniform weight and is not the subject of this document.

The two samplers therefore compose: the log sampler gives the global
distribution and its surprisals, and the span head-sampler uses those surprisals
to decide where to spend a fixed span budget.

## Setup and notation

All quantities are per window unless stated otherwise. There are two
populations over one window, related by a contingency table that the algorithm
never materializes as a joint:

```text
s         a span-start callsite, the "span kind"
l         a log callsite, the unit the log sampler counts
N[s,l]    estimated count of in-span log callsite l under span kind s
N[s,.]    = sum_l N[s,l]      in-span log volume of span kind s
N[.,l]    = sum_s N[s,l]      global count of log callsite l
n         = sum_{s,l} N[s,l]  total in-span log count
N_starts[s]                   estimated span-start volume of kind s
T                             target admitted span starts per window
r                             per-span log reservoir size
```

Two derived distributions:

```text
c_l    = N[.,l] / n     global log frequency, the centroid of the profiles
f_s(l) = N[s,l] / N[s,.] the log profile of span kind s
```

The log sampler estimates `c_l` with full statistical power, because it observes
every log and every log carries its enclosing span kind `s` in context whether
or not that span was sampled. Grouping the global log sample by `s` therefore
yields a well-estimated profile `f_s` for each span kind even though individual
spans are sampled sparsely. The surprisal of a log callsite, in nats, is

```text
g_l = -ln c_l
```

capped at a floor `-ln c_min` where `c_min` is the rarest observed column mass,
so a brand-new log callsite cannot look infinitely informative. The mean
surprisal, the global log entropy, is `H = sum_l c_l * g_l`.

## The value score: cross-entropy against the global log distribution

Each span kind `s` is scored by the expected surprisal of the logs it emits:

```text
V_s = sum_l f_s(l) * g_l = sum_l f_s(l) * (-ln c_l)
```

This is exactly the **cross-entropy** `H(f_s, c)` of the span kind's log profile
against the global log distribution. It decomposes as

```text
V_s = H(f_s, c) = H(f_s) + KL(f_s || c)
```

where `H(f_s) = -sum_l f_s(l) ln f_s(l)` is the entropy of the span kind's own
log mix and `KL(f_s || c) = sum_l f_s(l) ln( f_s(l) / c_l )` is the
Kullback-Leibler divergence of that profile from the global distribution.

The decomposition is the crux of the design. A span kind earns a high value, and
therefore more budget, for either of two reasons:

- High `H(f_s)`: the span kind emits a diverse mix of logs, so each of its spans
  carries a lot of internal detail.
- High `KL(f_s || c)`: the span kind's logs are atypical for the fleet, globally
  rare relative to the head. This is the divergence term, and it is what makes
  "favor span kinds whose logs are globally rare" literal.

A span kind whose logs look just like the global head has `f_s approx c`, so its
`KL` term is near zero and its value is just the ordinary entropy `H`; it earns
no extra budget. A span kind that concentrates on globally rare logs has a large
`KL` term and is preferred.

`V_s` is linear in the profile `f_s`, so it never needs the joint table. The
processor accumulates it online: every emitted log adds `g_l` to a bucket keyed
by its span kind `s` and bumps that bucket's count `N[s,.]`. Because the
processor sees every log, in sampled and unsampled spans alike, the bucket totals
are exact window values and `V_s` carries no sampling variance of its own. The
only estimated inputs are the surprisals `g_l`, which lag by one window.

A span kind observed only a few times this window is noisy, so its score is
shrunk toward the mean surprisal `H` by `kappa` pseudo-logs at the average:

```text
V~_s = ( sum over in-span logs of s of g_l + kappa * H ) / ( N[s,.] + kappa )
```

so an under-observed span kind defaults to ordinary rather than being boosted on
the strength of one fluky rare log. When no surprisals are available yet, during
warmup, every `g_l` is zero, the score is flat, and the allocation below reduces
to plain equal coverage.

## The allocation: coverage proportional to value

The span head decision is stateless: it sets a per-kind admission probability,
not a per-arrival weight. Targeting `T` admitted starts per window with the
admitted count of each kind proportional to its value gives

```text
A_s = V~_s * T / sum_{s'} V~_{s'}            admitted starts for kind s
P_s = clamp( A_s / N_starts[s], 0, 1 )       admission probability for kind s
    = clamp( T * V~_s / ( N_starts[s] * sum_{s'} V~_{s'} ), 0, 1 )
```

The `1 / N_starts[s]` factor is the inverse-frequency term, mirroring the log
sampler: a chatty span kind is throttled and a rare one is preserved. The `V~_s`
factor tilts the budget toward kinds whose logs carry rare information. A flat
`V~_s` recovers the equal-coverage assignment exactly, so the value refinement
never differs from the baseline unless span kinds genuinely differ in the rarity
of the logs they emit.

The probability becomes an OTEP 235 consistent-probability threshold over the 56
random bits of the trace id, so the decision composes with the rest of the span
sampler and yields an unbiased per-span adjusted count:

```text
T_s = round( 2^56 * ( 1 - P_s ) )
admit a span start iff  T_s <= randomness
adjusted_count = 2^56 / ( 2^56 - T_s ) = 1 / P_s
```

A rare high-value kind can clamp to `P_s = 1`, admitting every one of its starts
and pushing the realized total above `T`. The first version accepts that
overshoot; a hard ceiling on admitted starts is a candidate refinement.

## The objective: reduce divergence

The point of admitting a span is to record an in-context trace of the logs that
fired inside it, the joint `(s, l)` structure that the global marginal `c`
cannot show. The objective is for the collection of admitted span traces to be a
**surprisal-representative** sample of span kinds: the rare, information-bearing
kinds should be present in proportion to the information they carry, not buried
under the chatty head.

The subtlety that the study must respect is this. The span-log adjusted estimate
is **already unbiased** for the global log distribution for any admission
probabilities `P_s > 0`, by the Horvitz-Thompson correction: a span of kind `s`
admitted with probability `P_s` contributes its in-span logs reweighted by
`1 / P_s`, so

```text
E[ adjusted span-log count of l ] = sum_s (1/P_s) * P_s * N[s,l] = N[.,l].
```

Bias is therefore not the target; for any allocation the expected reconstruction
equals the truth. What the allocation changes is the **finite-sample
divergence**: the variance of the reconstruction and, equivalently, the
surprisal-weighted recall of span kinds in one realized window. A kind admitted
at a low `P_s` is reweighted by a large `1 / P_s`, so its few sampled traces are
high-variance, and if it is admitted zero times this window it is not recalled at
all. Spending budget where the surprisal mass concentrates is what reduces this
finite-sample divergence.

Concretely, the span-side objective is the span-kind analogue of the log-side
surprisal-weighted recall loss. Let a span kind be **recalled** when at least one
of its starts is admitted this window. The loss is

```text
loss = ( sum over span kinds s not recalled of  N[s,.] * V_s )
       / ( sum over all span kinds s of          N[s,.] * V_s )
```

the fraction of total in-span surprisal mass belonging to span kinds that
vanished entirely, normalized to `[0, 1]`. The weighting `N[s,.] * V_s` is the
total surprisal carried by a kind's logs, so the metric emphasizes kinds that
emit a high volume of globally atypical logs, those with both large `N[s,.]` and
a large divergence term `KL(f_s || c)`. Whether to weight by this mass or by the
per-kind value `V_s` alone is one of the metric choices the study should settle.
The hypothesis is that value-weighted allocation lowers this loss, and the
variance of the per-kind reconstruction, relative to equal coverage at a matched
budget `T`. The two coincide when span profiles are homogeneous, because a flat
value recovers equal coverage exactly; the lag in `g_l` and finite-sample noise
mean the study must confirm the gain rather than assume it.

## What is and is not claimed

To keep the study honest, the following are deliberately separated.

- **Unbiasedness is proven, not assumed.** The Horvitz-Thompson correction makes
  both the per-span adjusted counts and the reconstructed log distribution
  unbiased for any `P_s > 0`. A study should still verify it empirically, as a
  correctness gate, by checking that summed adjusted counts match the truth
  within Monte-Carlo noise, exactly as the J-versus-K study checks the log
  sampler.

- **The allocation rule is a heuristic, not a proven optimum.** `A_s` is set
  proportional to `V_s`, a coverage rule that equalizes captured value per kind
  and recovers equal coverage when value is flat. It is motivated by divergence
  reduction but is not derived as the variance-minimizing allocation. Under a
  squared-error or expected-divergence objective with a budget constraint, the
  Neyman-style optimum allocates `P_s` proportional to the square root of a
  per-kind surprisal-dispersion term rather than to `V_s` directly. Whether the
  simpler proportional-to-value rule is close to that optimum on real telemetry,
  and whether a square-root or other allocation does materially better, is an
  open question this study can answer.

- **The shrinkage and floor are tunable.** `kappa` and the surprisal floor trade
  responsiveness against robustness to fluky rare logs; their best values are
  empirical.

## Research questions

1. **Effectiveness.** On real internal-telemetry traces with a natural `(s, l)`
   structure, does value-weighted span-start allocation reduce the
   surprisal-weighted span-kind recall loss defined above, and the variance of
   the reconstructed per-kind log profiles, relative to equal coverage, at a
   matched span budget `T`?

2. **Where it helps.** How does the gain scale with the heterogeneity of span
   profiles, measured by the spread of `KL(f_s || c)` across kinds? The
   prediction is no gain when profiles are homogeneous and a large gain when a
   few kinds carry the rare tail. Quantify the relationship.

3. **Allocation law.** Does the proportional-to-value rule `A_s = k*V_s` match a
   square-root or other Neyman-style allocation, and by how much on real data?
   Is the simple rule within noise of the best allocation tried?

4. **Shrinkage.** What value of `kappa` best trades protection against fluky rare
   logs against responsiveness? Does the answer depend on window length and the
   number of distinct span kinds?

5. **Lag and warmup.** What is the cost of the one-window lag in the surprisals
   `g_l`, and of the flat-value cold start, on steady-state and transient
   divergence? Mirror the J-versus-K convention of excluding the first window.

6. **Overshoot.** How often does a high-value kind clamp to `P_s = 1` and push
   the realized admitted total above `T`, and does a hard ceiling on admitted
   starts improve the budget-versus-divergence tradeoff?

## Suggested experimental design

Reuse the methodology of the existing log-sampling study: fixed synthetic
distributions, several consecutive windows per trial, many trials, the first
window excluded as cold start, and Horvitz-Thompson weight-sum checks as the
unbiasedness gate. Lift it to two levels.

- **Synthetic generators.** Build joint `(s, l)` distributions with controllable
  structure: a Zipf-like distribution over span kinds `s` and, conditional on
  `s`, a log profile `f_s` that ranges from "equal to the global head" to
  "concentrated on globally rare logs." A direct knob on the spread of
  `KL(f_s || c)` across kinds drives research question 2.

- **Metrics.** Per trial and window, report (a) the surprisal-weighted span-kind
  recall loss; (b) a reconstruction divergence between the true global log
  distribution and the one reconstructed from the span-log adjusted sample, for
  example a surprisal-weighted L1 or chi-square distance, averaged over trials;
  (c) per-kind span inclusion rate; (d) the Horvitz-Thompson unbiasedness check
  on both span and span-log adjusted counts.

- **Baselines and ablations.** Compare equal coverage, value-weighting with and
  without shrinkage, value-weighting with and without the surprisal floor, and
  one or more alternative allocation laws such as the square-root allocation, at
  several budgets `T` and reservoir sizes `r`.

- **Real data.** Replay real internal-telemetry `(s, l)` traces and compute the
  same metrics, with equal coverage as the control. The headline deliverable is
  the curve of surprisal-weighted divergence against budget `T` for value
  weighting versus equal coverage, and a characterization of which production
  span kinds drive the difference via their `KL(f_s || c)`.

## Implementation pointers

The reference implementation lives in the `otap-df-telemetry` crate under
[`src/self_tracing/sampling/`](../crates/telemetry/src/self_tracing/sampling/README.md):

- `span_start.rs` holds `build_span_start_table`, which turns the per-kind start
  counts `N_starts[s]` and a value closure `V~_s` into the threshold table
  `T_s`, with the most permissive assigned threshold as the default for unseen
  kinds.
- `processor.rs` holds `value_score`, which computes `V~_s` from the online
  surprisal accumulator, and `update_log_surprisals`, which recomputes `g_l`,
  the floor, and `H` from the log sampler's per-callsite estimates each window.
- `bottom_floor.rs` holds the windowed bottom-k sampler that produces those
  per-callsite estimates and, with a unit weight, the per-span reservoir.

The configuration knobs are `target_span_starts_per_window` (`T`),
`span_value_shrinkage` (`kappa`), `enable_span_value_weighting` (off recovers
equal coverage), and `logs_per_span_per_window` (`r`).

## References

- Duffield, Lund, Thorup. *Priority sampling for estimation of arbitrary subset
  sums*. J. ACM 54(6), 2007. The Horvitz-Thompson priority-sampling foundation.
- Ting. *Data sketches for disaggregated subset sum and frequent item
  estimation*. arXiv:1708.04970, 2017. Predictable adaptive thresholds, which
  justify learning the threshold online from the same stream.
- OTEP 235, consistent probability sampling. The 56-bit threshold encoding used
  for the span head decision.
- [`integrated-logs-traces-reservoir.md`](integrated-logs-traces-reservoir.md).
  The full integrated sampler this head-sampler is part of.
