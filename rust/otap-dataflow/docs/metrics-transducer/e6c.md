# E6c. Lower OPL and KQL without changing window membership

[Worklist](../metrics-transducer-worklist.md) | [E6](e6.md)

OPL's [grammar] has metric sources such as `sums`, filters, `set`, `rename`,
`scale_metric` and `apply`, but no `aggregate` stage. The columnar engine
already [joins OTAP expression scopes][join] by ID and parent ID. Reuse
supported per-batch predicates and transforms; these joins do not keep
writer history, schedule windows or merge between batches.

The [KQL parser][kql] already lowers `summarize` to a [summary expression]
with named grouping keys, aggregates and optional post-expressions. Its
aggregates are sum, count, minimum, maximum and average; a grouping key can
contain `bin(TimeGenerated, 1m)`. The current [columnar planner][planner]
rejects `DataExpression::Summary` as not yet supported. Parsing a summary
is therefore not evidence of executing an event-time aggregate.

## Proposed rule operators

Add a terminal OPL `aggregate every <period> by <keys> [using <fn>]` and a
rule-specific lowering of KQL summaries. For F1, the proposed spelling is:

```text
sums
| where name == "requests"
| aggregate every 1m by point.attributes["service"]
```

Here `point` is a proposed key scope of `aggregate`; it refers to the count's
data point, not metric metadata or a similarly named resource attribute.
Bind the descriptor before choosing the natural aggregate. For Sum, resolve
each writer's delta before projecting the service and adding; this must print
the fixed F1 plan and record `7`. Accept only typed, deterministic transforms
that preserve the input identity needed for resolution. Reject routing,
side effects, unsupported combinations and mismatched periods with locations.

## KQL bins have a different boundary

KQL [`bin()`][bin] rounds down: `10:01:00` belongs to `[10:01,10:02)`. As
a transducer interval end it belongs to `(10:00,10:01]`. Reject a native
`bin(EndTime, 1m)` for right-closed recording; renaming its key to a window
end or adding a minute to the label does not repair membership.

A compatible subset can use this proposed **resolved interval** view:

```kusto
ResolvedSums
| where Name == "requests"
| summarize total = sum(Delta) by Service, pane = bin(IntervalStart, 1m)
```

Bind `Service` to the tagged point attribute and `IntervalStart` to a UTC
datetime, not the start of a cumulative epoch. Require the same bin grid as
the envelope. For a nonempty interval `(s,e]`, let `b = bin(s,P)`; accept
only if `b <= s < e <= b+P`. The KQL key remains `b`; emit coverage
`(b,b+P]`. Thus `(10:00:50,10:01:00]` counts in the minute ending at 10:01,
while `(10:00:50,10:01:01]` is reported as straddling, never rounded.

Require `explain` to show the resolved fields, time convention and partial
schema. Check the boundary cases and reject unbound timestamps, dynamic bin
sizes, unsupported post-expressions and `sum` of raw cumulative snapshots.

[grammar]: ../../crates/query-engine-languages/src/opl/opl.pest
[join]: ../../crates/query-engine/src/pipeline/expr/join.rs
[kql]: ../../../contrib/data_engine/kql-parser/src/tabular_expressions.rs
[summary expression]: ../../../contrib/data_engine/expressions/src/summary/summary_data_expression.rs
[planner]: ../../crates/query-engine/src/pipeline/planner.rs
[bin]: https://learn.microsoft.com/en-us/kusto/query/bin-function
