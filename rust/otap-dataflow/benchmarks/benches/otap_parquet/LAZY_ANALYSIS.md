<!-- markdownlint-disable MD013 -->

# Analysis part 3: lazy versus eager Parquet, and where lazy pays off

This part tests the premise the rest of the study assumes: that because a vendor
accepts Parquet, the gateway should encode it eagerly on the write path. Eager
encoding pays `flatten + pq-write` on every file, which is the price of having
the file indexed and query-ready before anyone reads it. If the average file is
read fewer than once, it might be cheaper to write Arrow IPC to disk and convert
to Parquet lazily, on the first read, so files that are never read never pay the
conversion.

The question is quantitative: at what average read rate does eager become
cheaper than lazy? Call that break-even `R*`. The model lives in
`parquet_study::lazy` and the numbers are printed by the "Lazy vs eager Parquet"
table in the benchmark.

This part assumes **large batches**, at least ten thousand log records, so warm
Arrow IPC is larger than the equivalent Parquet file, as the streaming table in
part 1 shows. That size gap is the crux of the result.

## The model

Let `R` be the expected number of reads of a file over its lifetime, with reads
modeled as Poisson, so the probability a file is read at least once, which is
what triggers a lazy conversion, is `p = 1 - e^{-R}`.

Eager, per file: pay `flatten + pq-write` once on the write path, transport and
store the Parquet bytes, and pay `pq-read` on each of the `R` reads.

Lazy, per file: pay `ipc-encode` once on the write path, transport and store the
larger Arrow IPC bytes, and on the first read, with probability `p`, pay the
deferred conversion `ipc-decode + flatten + pq-write` and rewrite the file as
Parquet. Reads after conversion pay `pq-read`.

The cost is totalled over four priced resources, each configurable through a
`$/CPU-ms` or `$/GB` knob: CPU for every pipeline step, network bytes from the
gateway to the ingestion service, disk or object-store I/O bytes, and
storage-at-rest bytes with the retention window folded into the price.

### The per-read term cancels

Both strategies serve reads from Parquet, because lazy converts on the first
read, so the per-read cost of `pq-read` plus the read I/O is identical and drops
out of the comparison. What remains is a write-path and at-rest trade:

- Lazy replaces the eager `flatten + pq-write` on the write path with the much
  cheaper `ipc-encode`, a saving it keeps on every file whether or not the file
  is read.
- Lazy pays the conversion `ipc-decode + flatten + pq-write` only with
  probability `p`, that is only on files that are actually read.
- Because warm IPC is larger than the Parquet file, lazy pays more for transport
  and for storage-at-rest until, and unless, a file is converted.

So lazy wins when the write-path CPU it avoids on unread files outweighs the
conversion CPU on read files plus the size penalty of holding and moving the
larger IPC bytes. `R*` is where those balance.

## Results

Indicative numbers from one development machine running WSL with jemalloc, at the
three breakdown shapes. Times are milliseconds; sizes are bytes. `ipc-enc` is the
lazy write-path cost, `convert` is the lazy conversion `ipc-decode + flatten +
pq-write`, and `eager-wr` is the eager write-path `flatten + pq-write`. `R*` is
reported with each priced dimension added cumulatively, left to right.

Knobs: CPU at \$0.05 per vCPU-hour, network at \$0.08/GB, I/O at \$0.005/GB, and
storage at \$0.069/GB, which is \$0.023/GB-month over a three-month retention.

| shape  | comp | ipc-enc | convert | eager-wr |    ipc-B |    pq-B | R\*(cpu) | R\*(+store) | R\*(+net) | R\*(+io=all) |
|--------|------|--------:|--------:|---------:|---------:|--------:|---------:|------------:|----------:|-------------:|
| 10,000 | zstd |    3.29 |   29.37 |    28.25 |   80,944 |  57,023 |    1.895 |       eager |     eager |        eager |
| 10,000 | lz4  |    3.38 |   40.08 |    35.56 |   97,520 |  75,336 |    1.625 |       eager |     eager |        eager |
| 30,000 | zstd |   10.39 |   96.29 |    92.93 |  239,728 | 149,091 |    1.947 |       eager |     eager |        eager |
| 30,000 | lz4  |   11.98 |  104.93 |    86.96 |  276,208 | 201,112 |    1.254 |       eager |     eager |        eager |
| 60,000 | zstd |   22.36 |  189.33 |   181.70 |  465,968 | 285,411 |    1.843 |       eager |     eager |        eager |
| 60,000 | lz4  |   26.16 |  229.67 |   196.77 |  544,944 | 391,010 |    1.358 |       eager |     eager |        eager |

Retention sweep at the 60,000-record shape with zstd, network and I/O on, showing
that storage duration does not change the verdict once bytes are priced at all:

| retention   | R\* (all-in) |
|-------------|-------------:|
| none        |        eager |
| 0.25 months |        eager |
| 1 month     |        eager |
| 3 months    |        eager |
| 12 months   |        eager |

### Reading the result

Under **CPU-only** accounting, where the bytes are treated as free, `R*` is about
1.1 to 1.9 reads per file. This is the regime the original premise has in mind,
and it confirms the intuition: if a file is read fewer than roughly one to two
times, skipping the eager `flatten + pq-write` on the files that are never read
saves more CPU than the occasional conversion costs. The break-even follows
directly from the measured steps, since at the crossing `ipc-encode + p * convert
= eager-write`, so `p = (eager-write - ipc-encode) / convert`; for the 10,000
zstd row that is about 0.85, which is `R*` of about 1.9.

The moment any byte-priced dimension is added, the verdict flips to eager at
**every** read rate, including zero reads and zero retention. In the large-batch
regime warm IPC is about 1.3 to 1.6 times the Parquet file, and the network
egress on that size difference alone, on the order of ten dollars per million
files, already exceeds the flatten-and-write CPU it would save, on the order of
two dollars per million files. Storage-at-rest adds to that and grows with
retention, but it is not even needed to decide the outcome; transport does it on
its own. This is why the retention sweep reads "eager" at every window.

## What the exporter actually writes: multi-file standard Parquet

The model above compares a single flattened Parquet file, but the production
`parquet_exporter` in this codebase does not write one file. It writes the
normalized OTAP layout as one Parquet file per payload type: Logs,
ResourceAttrs, ScopeAttrs, and LogAttrs. Each file carries its own footer,
schema, and dictionary pages, and the files cannot share a dictionary or
co-compress. So the multi-file layout sits one notch further along the
fixed-overhead axis than the single flat file, in the same way OTAP carries more
fixed overhead than OTLP and Parquet carries more than either. The question is
how much that costs in size.

The `print_standard_multifile_table` in the benchmark writes each payload as its
own Parquet file and sums the sizes, against the study's single flattened file,
across payload sizes. Bytes, zstd:

| scenario                    | nested 1-file | multi-file | files | multi / nested |
|-----------------------------|--------------:|-----------:|------:|---------------:|
| small (100 logs)            |        19,343 |     17,177 |     4 |          0.89x |
| medium (10,000 logs)        |       663,660 |    701,291 |     4 |          1.06x |
| log-heavy (60,000 logs)     |     3,863,435 |  4,085,495 |     4 |          1.06x |
| resource-heavy (60k / 600r) |     2,750,346 |  3,086,543 |     4 |          1.12x |

The multi-file penalty is modest, about 1.05 to 1.12x at scale, and the layout is
actually smaller at tiny payloads. Two effects nearly cancel. Multi-file pays the
Parquet fixed overhead four times and carries a `parent_id` join column in each
attribute file, but the single flat file pays for the `List<Struct>` repetition
and definition levels and for denormalizing shared attributes inline.

The resource-heavy row is the instructive one. Denormalizing twenty resource
attributes across sixty thousand log rows in the single flat file sounds
wasteful, so the normalized multi-file layout should win, yet it loses by twelve
percent. This is the study's central point working here: Parquet's own dictionary
and run-length encoding compresses the repeated shared attributes in the flat
file so well that avoiding the repetition saves almost nothing, while the
multi-file `parent_id` columns and per-file overhead cost real bytes.

Two caveats make the real exporter's overhead higher than this table. It writes
each payload as more than one file, splitting by partition and by row-count and
age thresholds, so a real deployment has many more, smaller files. And it emits a
fresh fileset per flush, so the fixed overhead recurs per batch, exactly the
per-batch schema and dictionary cost that the streaming table in part 1 shows for
IPC. Small or frequent flushes are where the multi-file layout hurts most, and
large infrequent files are where its penalty shrinks toward the single-digit
percentages above.

## Bottom line

For large batches, lazy conversion is a false economy once total cost of
ownership is counted. It saves write-path CPU, but it pays for that saving by
transporting and storing Arrow IPC that is larger than the Parquet it defers, and
in this regime the size penalty dominates. Eager Parquet at the gateway is
cheaper at every read rate.

Lazy only competes when bytes are effectively free relative to CPU, for example a
purely on-premises deployment with no network egress and cheap local storage, and
even then only when files are read fewer than about one to two times. Two levers
would move the balance toward lazy: cheaper bytes, either no egress or short
retention on cheap storage, or a lazy store format that is no larger than
Parquet, so the transport and storage penalty disappears and only the CPU trade
remains, which is the CPU-only column above.

This reinforces the part 1 conclusion from the opposite direction. Part 1 found
that when one organization owns both the exporter and the store, moving the
Parquet encode to the sending gateway is attractive because ingestion can stay
close to a metadata-validated append. This part shows that the alternative of
deferring the encode does not pay for large batches, because the deferred
representation is the larger one on the wire and at rest. The eager gateway is
the right default, and the small-batch streaming regime from part 1, where IPC is
smaller than Parquet, is the separate case where keeping the client on OTAP/IPC
wins outright.
