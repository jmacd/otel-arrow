# OTel-Arrow Rust batching design

(Josh MacDonald, Oct 31, 2025)

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
we expect the user to limit this sensibly, knowing we will assemble the
whole batch, sort it somehow, then rebatch it.

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

NOTE! The arrow-rs repository is checked-out in `./arrow-rs` for reference.

About dictionaries in Arrow

About schema variation: unifying and specializing

About array-type variability: when to widen

About sorting and group-by in Arrow: what data structures to build, in which order

About vectors: what do we materialize, do we use indirection?

About attribute sets: need primitives

About test harness: need assert.Equiv.
