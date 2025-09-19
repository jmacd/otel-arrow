# OpenTelemetry OTel-Arrow Rust Tail Sampling processor

This code base is a Hackathon entry by:

Joshua MacDonald <jmacdonald@microsoft.com>
Utkarsh Pillai <Utkarsh.Pillai@microsoft.com>
Ganga Mahesh Siddem <gangams@microsoft.com>

**Project entry: [Hackathon 2025](https://innovationstudio.microsoft.com/hackathons/hackathon2025/project/104343)**

**Completed: September 19, 2025**

## Project vision statement

Microsoft is sponsoring development of the "OTel-Arrow" project in
OpenTelemetry, and we are building a Rust and Apache Arrow-based data
path for large-scale OpenTelemetry data. With its basic data path now
reaching early milestones, we can begin to use OTel-Arrow data
directly in "Parquet" file format.

We will experiment with Apache DataFusion in the context of an
open-source distributed tracing system. We expect DataFusion to plan
and execute queries that perform our sampling logic, then write the
OpenTelemetry data onward to a configured destination.

Our objective is to build a component that behaves like the
OpenTelemetry-Collector-Contrib "tailsamplingprocessor", however, our
goals are larger:

The processor should maintain correct "representivity" in the form of
sampling thresholds that yield accurate "adjusted count" information
(and use a bounded amount of memory) The processor should support an
additional endpoint to retrieve Head sampler configuration for
telemetry SDKs to query, with the intent to reduce bytes of
collected-and-dropped data in the sample (for a bounded amount of
output data).

## Project background: OpenTelemetry

_Joshua_ leads the OpenTelemetry Sampling SIG with an interest in the
present-day OpenTelemetry
[`tailsamplingprocessor`](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/tailsamplingprocessor/README.md),
a Golang component, and the [OTel-Arrow
project](https://github.com/open-telemetry/otel-arrow/blob/main/README.md)
which is [building a Rust Collector pipeline aligned with the internal
Strato
project](https://github.com/aep-health-and-standards/Telemetry-Collection-Spec/blob/main/Strato/design-goals-and-principles.md).

To begin, the OTel-Arrow project recently gained a `parquet_exporter`
component making it possible to write OTAP data to timestamp-labeled
Parquet files partitioned by a virtual column "_part_id={uuid}", yet
no receiver exists to replay the data. We had already seen interest
from [Microsoft engineer Raki Rahman in using the same data model to
query SQL Server
telemetry](https://www.rakirahman.me/otel-arrow-delta-lake/). The OTAP
representation in Parquet uses a star-schema, like (for Logs):

```
base_directory/
├── logs/
│   ├── _part_id=<uuid1>/
│   │   ├── part-<timestamp>-<uuid>.parquet
│   │   └── part-<timestamp>-<uuid>.parquet
│   └── _part_id=<uuid2>/
│       └── ...
├── log_attrs/
│   ├── _part_id=<uuid1>/
│   │   └── part-<timestamp>-<uuid>.parquet
│   └── _part_id=<uuid2>/
│       └── ...
├── resource_attrs/
│   └── ... (similar structure)
└── scope_attrs/
    └── ... (similar structure)
```

The project has a stated goal of exploring the integration of
Datafusion with OTAP data. The OTAP dataflow engine streams telemetry
payloads as Arrow record batches.  A sampling component can be
structured as a processor, like `tailsamplingprocessor`, that
accumulates data points in memory, however we chose to sample from
output of `parquet_exporter`, letting us increase the volume of data
we consider when sampling and let Datafusion implement our query plan.

To reduce project scope, our attention would be limited to the Logs
signal. 

## Project background: Mathematics

We shared resources that are important background for sampling in
telemetry systems, including:

- [Priority sampling for estimation of arbitrary subset sums](https://dl.acm.org/doi/10.1145/1314690.1314696)
- [Adaptive Threshold Sampling](https://dl.acm.org/doi/10.1145/3514221.3526122)
- [Bottom-k sketches: Bottom-k sketches: better and more efficient estimation of aggregates](https://dl.acm.org/doi/10.1145/1269899.1254926)

We discussed and skimmed over a number of documents!

- [Jake Dern's brief introduction to OTAP](https://github.com/open-telemetry/otel-arrow/blob/main/docs/otap_basics.md)
- [OpenTelemetry Probability Sampling](https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/)
  - [W3C Trace Context Level 2 random flag (56 bits)](https://www.w3.org/TR/trace-context-2/#random-trace-id-flag) 
  - [OpenTelemetry `tracestate: ot=th:_;rv:_` headers](https://opentelemetry.io/docs/specs/otel/trace/tracestate-handling/)
  - [Proof-of-concept for consistent log sampling](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/probabilisticsamplerprocessor/README.md)
- [Good-Turing frequency estimation](https://en.wikipedia.org/wiki/Good%E2%80%93Turing_frequency_estimation)
- [Anne Chao & "Seen more than once"](https://sci-hub.se/10.1111/2041-210x.12768) ways to estimate sample "coverage"

## Project deliverables

### Test setup

We learned how to export test data with `parquet_exporter`.

**Status: Functional!** [This script](test_parquet_exporter.sh)
generates OTAP data in Parquet, what we call "Barquet" for
Batch-Arrow-Records in Parquet. Hack-a-thon quality!

### Parquet receiver

As a precursor to the final objective, first we would learn how to use
the basic Parquet files and "reconstruct" them back into OTAP records
data, from where they could pass through an OTAP-dataflow pipeline and
be exported as OTAP streams or OTLP protobufs.

This component:

- Monitors the host filesystem for Parquet files using host file system
  to discover all partitions
- Scans all partitions in order with a streaming merge-join of the four
  OTAP tables for Logs (logs, log_attrs, scope_attrs, resource_attrs).
- Maps Parquet data types e.g. (Uint32) for primary/parent identifier into
  batch-independent Uint16 representation.
- Gathers the 1-to-N relations between logs, log_attrs, ... to a sequence
  of OtapPdata batches.
- Stream all the partitions it finds indefinitely, no logic to terminate
- Marks the data as "PLAIN" data, meaning not delta-encoded in the
  OTAP schema's metadata.

The [parquet_receiver test script](test_parquet_receiver.sh) generates
new partitions of data using the "fake_signal_generator" component. It
sends reconstructed OTAP batches onwards to the "debug_processor"
which prints them to the console. The test setup uses realistic data
generated from OpenTelemetry semantic conventions.

**Status: Functional!** This is a useful counterpart to the
parquet_exporter. Hack-a-thon quality!

### Sampling reciever

Modeled on the Parquet receiver, the `sampling_receiver` demonstrates
that we can use DataFusion to query OTAP data and then reassemble the
results into OTAP batches, much like we did with Parquet directly,
above.  However, in this case the DF query engine is able to apply its
own optimizations.

This component:

- Organizes the input Parquet files by time window based on their
  stated timestamp. ([Placeholder for a real solution to temporal indexing in the pipeline.](https://github.com/aep-health-and-standards/Telemetry-Collection-Spec/blob/main/ObservabilityAgents-Shared-Components/DFS/dfs.md); issues queries for full windows across all partitions after a delay.
- Assembles a DF query centered on the `log_attrs/*` table, by
  partition. Whereas parquet_receiver centers on the logs table and
  merges with the other three, this one works with logs because it
  allows the sampler to output attributes.
- We use the DataFusion ParitionedTable provider for supporting 
  the "virtual" `_part_id` column written by `parquet_exporter`
- Limits the attributes query into batches of up to 65536 identifiers
  to fit a UInt16. Uses vectorized Arrow kernels to cast and subtract
  the offset when we convert IDs. 
- Extracts the partition ID column from the resulting data.
- Assembles the distinct IDs among the matching attributes.
- Forms an Array of the distinct IDs in one batch, enter it as a `MemTable`
  in the DF session context.
- Separately queries the logs, scope_attrs, and resource_attrs tables
  using a SQL "IN" expression applied to the distinct IDs.
- Concatenates the four streams into correlated batches
- Reconstructs OTAP data the same way `parquet_receiver` does.

In its default configuration, the effective query is `select * from
log_attrs` query, i.e., "100%" of logs will be read from the Parquet
files.

**Status: Functional!** This is a useful counterpart to the
parquet_exporter. Hack-a-thon quality!

### Future plans

#### Real sampling queries

If carried out, here are how to complete the original vision of a
weighted sampler for OTAP-dataflow.  Here is a sample query that we
could register that (hypothetical: untested) collects a weighted
sample of 100 items per `service.name` value (per minute window, the
temporal default configuration).

Note that we read and write an attribute named
`sampling.adjusted_count`, which reflects the amount of sampling that
has been done to a particular item. We did not implement a
user-defined aggregate function (UDAF) in Datafusion that really
performs weighted sampling, but we understand that such a function
will output the matching log records in the form of modified
attributes. This explains why the `sampling_receiver` uses an
"attributes-first" form of query and OTAP reconstruction.

```sql
-- Step 1: Reconstruct a minimal view with fields needed for sampling and weight calculation
WITH logs_with_existing_weights AS (
    SELECT
        l.id,
        ra.str as service_name,
        -- Get existing adjusted_count if it exists, otherwise use 1.0 as default weight
        COALESCE(
            CAST(
                (SELECT la.str FROM log_attributes la 
                 WHERE la.parent_id = l.id AND la.key = 'sampling.adjusted_count'
                 LIMIT 1) AS DOUBLE
            ), 
            1.0
        ) as input_weight
    FROM
        logs l
    JOIN
        resource_attributes ra ON l.id = ra.parent_id AND ra.key = 'service.name'
)

-- Step 2: Apply the UDAF to get sampling decisions
SELECT
    service_name,
    -- The UDAF uses the input_weight for weighted sampling decisions
    weighted_reservoir_sample(
        STRUCT(
            id,
            input_weight
        ),
        100 -- The desired sample size 'k'
    ) AS sample_decisions
FROM
    logs_with_existing_weights
GROUP BY
    service_name;
```

We have transformed the process of deciding how to execute this query,
which applies to both columns from the logs table (e.g., timestamps,
event names) and fieldsfrom the log_attrs table (e.g., `service.name`,
`sampling.adjusted_count`).

Above, we imagine that the `weighted_reservoir_sample()` function
applies one of the algorithms mentioned above, such as the Bottom-K or
Adaptive Threshold algorithms.

#### Real sampling UDAF

We studied how to implement a UDAF in DataFusion. With help from
Copilot, we were able to identify many details about this code base.

- [Sample UDAF function for "Algorithm R"](./crates/otap/src/sampling_receiver/sampler_udf.rs), untested
- [Rust implementation of Bottom-K](./crates/bottom_krust/src/lib.rs)

The basic logic for Bottom-K is shown below:

```rust
    /// Ingest a weighted record with deterministic randomness
    pub fn ingest(&mut self, record: WeightedRecord<K>) {
        let heap_record = HeapRecord {
            key: record.key,
            weight: record.weight,
            randomness: record.randomness,
        };
        
        if self.heap.len() < self.k + 1 {
            // If we have space, just add it
            self.heap.push(heap_record);
        } else {
            // If heap is full, check if new record has higher randomness than the minimum
            if let Some(min_record) = self.heap.peek() {
                if heap_record.randomness > min_record.randomness {
                    // Replace the minimum with the new record
                    self.heap.pop(); // Remove minimum
                    self.heap.push(heap_record); // Add new record
                }
            }
        }
    }
```

I believe I have this right! More testing is needed.

## Use of AI tools

This project was carried out using Github Copilot, mostly using Claude
4 Sonnet.

Here are many of the machine-generated documents used to guide the effort:

- [sampling-receiver-overview](./sampling-receiver-overview.md): How to get from the finished `parquet_receiver` prototype to the Datafusion-based sampling receiver
- [sampler-implementation-plan](./sampler-implementation-plan.md): Establishes the basic form of a query for weighted sampling of OTAP
- [arrow-compute-optimization-plan](./arrow-compute-optimization-plan.md): Which Arrow vector instructions we would use while reconstructing OTAP after finishing `parquet_receiver.md`
- [streaming-separate-queries](./streaming-separate-queries.md): How to form the memory table containing the "IN" expression values for the Datafusion-based streaming merge.
- [query-first-parquet-receiver](./query-first-parquet-receiver.md): How to replace the Parquet receiver's hard-coded logic with a partitioned Datafusion `ListingTable` provider.
- [IMPLEMENTATION-JOURNAL](./IMPLEMENTATION-JOURNAL.md): A series of updates as we made progress.
- [old_docs/parquet-receiver-design](./old_docs/parquet-receiver-design.md): The first step was to read Parquet files.
- [old_docs/streaming-join-status](./old_docs/streaming-join-status.md): We had trouble at first merging OTAP data
- [old_docs/parquet-receiver-implementation-phase1](./old_docs/parquet-receiver-implementation-phase1.md): We were very optimistic at this point
- [old_docs/listingtable_ordering_analysis](./old_docs/listingtable_ordering_analysis.md): We did focused studies of Datafusion support
- [old_docs/weighted_reservoir_sampling_design](./old_docs/weighted_reservoir_sampling_design.md): We studied how to implement weighted sampling in Datafusion
- [large-output-debugging](./large-output-debugging.md): How to work with debugging data for the agent.
