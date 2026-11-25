# E6b. Import recording rules into an exact event-time dialect

[Worklist](../metrics-transducer-worklist.md) | [E6](e6.md)

Import [Prometheus recording-rule YAML][rules] as syntax, not as a promise of
Prometheus results. Require explicit selection of the OTel exact dialect.
Read `groups[].name`, `interval`, `rules[].record`, `expr` and constant
labels; rule labels override group constants. Resolve defaults explicitly.
Parse each expression with [GreptimeTeam's `promql-parser`][parser]. It
produces an AST, not samples. Report group, rule, offending clause and source
location. Reject alert rules, cycles, unknown fields and unsupported AST
nodes before installing anything.

```yaml
groups:
  - name: service
    interval: 1m
    rules:
      - record: service:requests:increase1m
        expr: sum by (service) (increase(requests_total[1m]))
```

## Time and value contracts

Bind `requests_total` through a declared name translation to the OTel Sum.
Evaluate at `t = origin + n * period`, using accepted input and the recorded
seal decisions. Exact `increase(x[R])` sums whole resolved contributions per
writer in `(t-R,t]`; `rate` divides that sum by `R` in seconds. A missing
start on first observation of a cumulative sum establishes a baseline, not
an increment. Prometheus instead extrapolates from counter samples in the
range. A bare counter selector in the exact dialect reads an epoch-relative
total, not necessarily the latest raw Prometheus sample.

Require positive `R` in whole periods. Reject an unaligned range or phase
at compile time. If an input delta `(10:00:50,10:01:10]` straddles a
required minute pane, reject that contribution and report incomplete
coverage, even inside a five-minute range. Do not assign by end time,
prorate or extrapolate. Overlapping ranges may not become additive deltas.

Prometheus group `query_offset` shifts the evaluation timestamp; allowed
lateness delays **sealing** without moving the window. Reject a nonzero
group offset in the first importer rather than silently translating it.
Expression `offset d` shifts only its selector: permit positive whole-pane
offsets only with the required retained history, otherwise report an error.
Reject `@`, subqueries, `irate`, `resets`, holistic functions and unimplemented
vector matching, rather than approximating their sample semantics.

## Binding and conformance

Support native or translated metric names, and label matchers with absent
labels matching as empty strings. Track point attributes, configured resource
promotions, `job`/`instance` and scope labels back to their origins. Exclude
and count ambiguous bindings, conflicting output identities or incompatible
kinds/units. The example emits a delta Sum for a non-overlapping period;
`rate` emits a Gauge with unit per second. Explain signed Sum changes as
dialect behavior, not Prometheus counter resets.

Use the [prometheus-community execution engine][engine] as a potential
differential comparator, distinct from the AST parser. Pin and qualify a
release before adoption; the [workspace policy](../../deny.toml) warns on
unknown git sources, not a blanket ban. Compare `sum by (service)` over
identical live gauge samples at identical instants and lookback, with the
same labels and no ties. Test extrapolation, resets and lateness against A3's
hand-derived exact expectations instead.

[rules]: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/
[parser]: https://github.com/GreptimeTeam/promql-parser/tree/v0.11.1
[engine]: https://github.com/prometheus-community/promql-rs/blob/98bc8f13d98f7b0855aa52795df5c8be43a8537f/promql-engine/src/source.rs
