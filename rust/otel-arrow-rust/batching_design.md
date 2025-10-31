# OTel-Arrow Rust batching design

HUMAN AUTHORED, AGENTS DO NOT MODIFY
Josh MacDonald, Oct 31, 2025

## Background

The OTAP payload format consists of N RecordBatches, most of them
optional, representing the OTel signals. Each signal has a different
N corresponding with members in a hierarchical relationship.

This effort picks up after several previous ones, see:

- ./LOGS_BATCHING_RESEARCH.md
- ./REBATCHING_PLAN.md
- ./REBATCHING_STATUS.md
- ./otel-arrow-rust/src/otap/groups.rs

With this background, we will start over.

### OTAP Logs

We will focus on the logs signal until we have it finished, then move
on to the other signals. Logs is simpler than the others because it
has a flat structure:

- Logs: a single primary table
- LogAttrs: an exclusive-child table
- ScopeAttrs: a shared-child table
- ResourceAttrs: a shared-child table

These tables are related through OTAP columns:

- `Logs.ID`: corresponds with `PARENT_ID` in the LogAttrs table (nullable for absent exclusive log attrs)
- `Logs.Scope.ID`: corresponds with `PARENT_ID` in the ScopeAttrs table (not nullable, even if no scope attrs)
- `Logs.Resource.ID`: corresponds with `PARENT_ID` in the ScopeAttrs.

Note that `Logs.Scope` and `Logs.Resource` are Arrow struct columns
embedded in the Logs table.

### Batch configuration

When we batch for any signal, we have an upper bound on size
determined by the primary table(s). For logs, with only one primary,
the payload is limited to 16 bits, the width of `PARENT_ID`. The user
passes in an Option<NonZeroUsize> for the batch-size limit.

We always measure batch size (or length) in the number of items in the
primary table(s). For logs, this is the number of log records, for
traces the number of spans, and for metrics the number of data points.

### OTAP encoding

Note that the OTAP encoding with up to N record batches (per signal)
can arrive through multiple channels, and the path will determine how
it is sorted. There is an on-the-wire encoding for `PARENT_ID` based
on delta encoding, we should not see this for this is handled
elsewhere: we expect "materialized" `ID` and `PARENT_ID` values in the
batching logic.

There is Arrow-level schema metadata indicating what we know about
sort order. We consider sort order to be one of the knobs we have for
to balance CPU time and compression performance.

After we finish work on batching logs, we will explore these other
signals which are more complex:

- Traces and Metrics: have multi-level hierarchies, tables with both children and parents
- Metrics: has multiple primary tables corresponding with data point type

The essential duty in a batching implementation is to ensure that all
of the `ID` and `PARENT_ID` columns are correct after batching; we
have to "reindex" them, each batch's identifiers are unique. It is not
necessary to shift index ranges to be consecutive from 0

### Batching ideals

There are MANY ways to complete this task.

We are interested in exploring multiple options, benchmarking them,
and allowing choices to be configured by the user.

The interface will accept `Vec<[Option<RecordBatch>; N]>`. Take the
high-level API of `groups.rs` a good start, keep this interface we
only need to change implementation details.

How we sort is an important detail affecting not only compression, it
is a key element in how we batch. The user will pass in a vector of
`J>=1` valid OTAP payloads of any size and expect to receive `K>=1`
OTAP payloads in return where `K-1` of them are full according to the
effective limit. This leaves a possible residual payload that is less
than full. To address this in our contract, the batcher must ensure
that the 0th payload of the input is fully consumed in each batching
operation so that items cannot persist across residual payloads.

We expect the user to pass in multiple output-batch-sizes of data at
once, because this presents an opportunity to rebatch efficiently, and
we expect the user to limit this sensibly, knowing we will assemble
the whole batch, sort it somehow, then rebatch it.

Sorting reduces fan-out in an aggregation pipeline, that is why it is
important to let the user set a batching policy.

#### Sort by TraceID

If we sort the Logs table by the TraceId, then logs from the same
trace will fall into the same batch; this will be instrumental in
routing to a tail sampling processor, for example.

#### Deduplicate shared table entries

Resource and Scope entries may be duplicated in the inputs. We can
group-by distinct values as we form the output.

#### Sort by Resource Attributes

Assuming we deduplicate resource attributes, we can then sort Logs
by `Logs.Resource.ID` to place logs into batches with fewer resources.

#### Multiple-dimension stable sort

It may be useful to sort the primary table in multiple ways to improve
compression downstream.

#### Sorting by event name

Place events of the same name together. 

#### Batching by time bucket, distinct value, etc. (FUTURE WORK)

We have described a number of ways to sort and deduplicate while maintaining
an fixed batch size with 1 residual.

Later, we might consider splitting the combined-and-sorted batches by
other criteria than size limit. For example, we could place events of
the same name together and then form batches having distinct event
names (span names, metric names). We could form batches by distinct
resource, scope, and so on. This will be left as future work.

## Development

We will do research in the Arrow codebase for this project. 

### Research topics

NOTE! The arrow-rs repository is checked-out in `./arrow-rs` for reference.



About dictionaries in Arrow

About schema variation: unifying and specializing

About array-type variability: when to widen

About sorting and group-by in Arrow: what data structures to build, in which order

About vectors: what do we materialize, do we use indirection?

About attribute sets: need primitives

About test harness: need assert.Equiv.

## Prompts

### About grouping in Arrow

I am doing more research on ./arrow-rs with respect to our batching
project. I would like to know about hash-based join techniques for
deduplicating and aggregating record batches. In the actual setup, we
will have several RecordBatch values input, they will each have a
subset or full set of columns for attribute key-values in OTAP, so the
structure is like [PARENT_ID, KEY(dictionary), Option<int>,
Option<f64>, Option<String(dictionary)>, ...]. We will want to
identify across batches the case where PARENT_IDs have the same exact
set of keys/values, in which case they can be deduplicated.

Parent IDs can have different cardinality, those with 1 key:value, 2
key:value, 3 ... and up.

I know that Arrow has support for group-by and can copy the
column-oriented struct into a row-oriented encoding for grouping and
related tasks. I imagine we will compute one data structure that we
can sort and compare, then build a Vec<Map<input_parent_id,
output_parent_id>>with the deduplicated mapping, a vector with index
matching the input batches, so a Map for each input to the output
parent_id: the map could be a Vec for dense mappings, likely, or a
HashMap for spare mappings.

Please summarize the tools and techniques we'll take advantage of in arrow-rs
in a new file.

#### Reply

docs/ARROW_GROUPING_DEDUPLICATION.md

### About sort-order in OTAP

Considering the sort order question, please investigate the
otel-arrow-rust/**/*.rs sources for how we encode information about
sort order, when we know it.

docs/SORT_ORDER_ENCODING.md

### About consecutive rows

Is there support in Arrow for making Eq and PartialEq-bound values
from consecutive Rows? If I have a ResourceAttrs table and sort by
PARENT_ID and KEY, then I can partition by PARENT_ID and the
concatenation of KEY, VALUE Rows i.e., the sequence of adjacent Row
values w/ the same PARENT_ID are the thing that makes resources
unique. We would want a consecutive-row type which the underlying
representation has as a contiguous [u8] underneath the abstraction.

#### Reply

Basically, no, but yielded algorithm development:

docs/ARROW_GROUPING_DEDUPLICATION.md

### About schema unification

I want to study otap-dataflow/**/*.rs and otel-arrow-rust/**/*.rs for
existing patterns about schema unification. We will have to address
the fact for example that one batch of attributes may contain only a
i64 column and another may contain only f64, while some will be mixed
and contain the other columns. We will need to construct the unified
schema as we batch.

#### Reply

docs/SCHEMA_UNIFICATION_PATTERNS.md

### About original groups.rs implementation

docs/BATCHING_COMPARISON.md

## Implementation plan

Note `otel-arrow-rust/src/otap/groups.rs` has been reduced to just the
public API for batching, `otel-arrow-rust/src/otap/groups_old.rs` is
the original implementation, and `otel-arrow-rust/src/otap/groups_v1.rs` 
was an early attempt of mine to start from scratch.

We will start with a minimum viable implementation with no
configuration parameters other than the size limit, which already
exists in the API. We will form one primary table, then sort by two
columns: Logs.Resource.ID then Logs.Scope.ID, then split into batches
and re-assemble the child tables.

For the child tables, we have Resource and Scope (shared) and LogAttrs
(exclusive). We will deduplicate the shared tables, if present. 

We will keep the `select()`, `unify()` and `reindex()` support as wel
as `IDSeq` and reusable portions described in BATCHING_COMPARISON.md



