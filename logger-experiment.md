# obsday Logger-Overhead Experiment

## Prompt (verbatim, paraphrased)

We are in an empty project with only the `otel-arrow` subdirectory (a repo I
maintain). We will develop an experiment to measure CPU overhead for the
internal logging pipeline feature of this codebase.

* First we will create a receiver for `otap-dataflow` similar in spirit to
  `fake_data_generator` but which uses the **internal logging path**; we'll
  configure the **ITS** mode for the internal logging provider, then add the
  **batch processor**, then **OTLP exporter**. As configured, the tester
  resembles a simple program logging through OTLP — much like an OTel logging
  SDK.
* We pair this with a `df_engine` configured with **OTLP receiver +
  noop_exporter**: it simply receives data and does nothing.
* We profile each for CPU usage. We are measuring instrumentation overhead for
  simple logging and for a simple logs collector.
* After this works, we will repeat the experiment using **OpenTelemetry-Go SDK
  + OpenTelemetry Collector**.

Additional requirements:

* Vary the size of the log message in a coarse way: number of attributes
  (all strings) plus a normal distribution of value sizes given by `mean` and
  `stddev`. Data must be **distinct** — no string reuse. CPU cost of generation
  is acceptable; the profile will show it.

## Where We Are (Stack A — otap-dataflow)

### Code & artefacts (committed on this branch)

| Path | Purpose |
| --- | --- |
| `rust/otap-dataflow/examples/obsday_logger.rs` | Custom binary. Embeds the engine via `Controller::run_forever` plus N std::thread workers that emit `otel_info!` records at a paced rate. Strings are generated per-worker from a seeded `StdRng`; sizes drawn from a normal distribution via Box–Muller, clamped to `[min, mean + 6σ]`. |
| `rust/otap-dataflow/configs/obsday-logger.yaml` | Engine config used by `obsday_logger`. Telemetry providers: `global=its`, `engine=its`, `internal=noop`, `admin=console_direct`. The observability pipeline is `internal_telemetry → batch (1000 items / 1 s) → otlp_grpc → http://127.0.0.1:4317`. A placeholder `groups.placeholder.pipelines: {}` is present because the schema requires `groups`. |
| `rust/otap-dataflow/configs/obsday-collector.yaml` | Stand-alone `df_engine` config: `otlp/grpc :4317 → noop`, admin on `:9877`. |
| `rust/otap-dataflow/tools/obsday/run.sh` | Profiling harness. Builds `--profile profiling`; launches the collector under `samply` (background); waits for ports `4317` and `9877`; launches the logger under `samply` (foreground); on exit POSTs a graceful shutdown to the collector admin and SIGTERMs `df_engine` (samply's child) so samply flushes and writes its profile. Output lives in `obsday-out/{logger,collector}.{profile.json.gz,log}` (gitignored). |
| `rust/otap-dataflow/Cargo.toml` (modified) | Added `[dev-dependencies]` block: `otap-df-telemetry.workspace = true`, `rand.workspace = true`. |

### CLI flags accepted by `obsday_logger`

```
--config PATH               engine config file (required)
--rate N                    target logs/sec aggregated across workers
--duration S                seconds of emission
--workers N                 std::thread workers
--attrs N                   number of attributes per record (one of 1, 2, 4, 8, 16, 32)
--attr-size-mean F          mean string length
--attr-size-stddev F        stddev of string length
--attr-size-min N           minimum string length (default 1)
--seed N                    RNG seed (workers use seed XOR worker_id)
```

The `--attrs` value must be in `{1,2,4,8,16,32}` because `otel_info!` requires
statically-known field names per call site, so we hand-wrote six emitter
functions.

### Validation status

* `cargo check` and `cargo build --profile profiling` succeed.
* End-to-end smoke run via the harness (rate=2000/s, duration=6s, workers=2,
  attrs=8, mean=24, stddev=8) produces both
  `obsday-out/logger.profile.json.gz` and `obsday-out/collector.profile.json.gz`,
  and both processes shut down cleanly.
* Data flow positively verified by temporarily inserting a `processor:debug`
  with `verbosity: basic` between receiver and exporter on the collector: a
  short run at 500/s for 3s produced `Received 1501 log records` (= 500 × 3 + 1)
  in three batches, matching the logger's reported `emitted=1501`.
* `cargo xtask quick-check` (clippy `-D warnings` across the workspace) was
  **not** completed — this dev machine ran out of memory mid-build. Needs to
  be re-run on a larger box.

### Known operational notes / gotchas

* Workspace `rand = "0.10"`. Use `RngExt` (not `Rng`) and `random_range(range)`;
  `StdRng::seed_from_u64`.
* `otel_info!` expands to `tracing::info!(name: ..., target: ...)` and bakes the
  field names at the call site. Hence the fixed set of attribute counts.
* `Controller::run_forever` does not exit on `POST /api/v1/groups/shutdown`
  alone (the admin call returns success once the pipelines stop, but the
  controller continues to hold). The harness therefore POSTs the shutdown for
  graceful pipeline drain, then SIGTERMs the `df_engine` child of `samply`;
  `samply` flushes its `.json.gz` when the child exits.
* The OTLP receiver binds via `SO_REUSEPORT`; you will see one "Starting OTLP
  gRPC receiver" log per CPU core. This is normal.
* `engine.telemetry.logs.providers.internal` cannot be `its` or
  `console_async`; `admin` cannot be `console_async`. The logger config uses
  `internal=noop` and `admin=console_direct`.
* Required pre-startup: `otap_df_otap::crypto::install_crypto_provider()`
  (rustls). Side-effect imports
  `use otap_df_contrib_nodes as _; use otap_df_core_nodes as _;` are needed so
  the `linkme` factory slices populate.
* `samply` is at `/home/jmacd/.cargo/bin/samply` (0.13.1). View profiles with
  `samply load <file.json.gz>`.

## What Is Left

### Phase 1 (Stack A) — close out

1. Run `cargo xtask quick-check` (or at minimum `cargo clippy -p otap-df-otap
   --example obsday_logger -- -D warnings` plus `cargo fmt --check`) on a
   larger machine and fix any findings.
2. Capture baseline flame graphs at one or two representative operating
   points, e.g. `--rate 10000 --duration 30 --attrs 8 --mean 24 --stddev 8`,
   and record peak/avg CPU%.
3. Decide whether to keep the `processor:debug` toggle as an opt-in flag in
   the harness for sanity checks (currently you must edit the YAML).

### Phase 2 (Stack B) — OTel-Go SDK + OpenTelemetry Collector

Mirror the experiment with the upstream Go ecosystem so we can compare
overhead. **Build outside `otel-arrow/`** to avoid polluting the repo.

1. **Go logger** (e.g. `~/src/otel/obsday/go-logger/`):
   * `go.opentelemetry.io/otel/log` + OTLP/gRPC log exporter +
     `BatchProcessor`. Mirror the CLI flags from `obsday_logger`. Generate
     distinct attribute strings the same way.
2. **Collector**:
   * `otelcol-contrib` (or a custom build) with `receivers: [otlp/grpc]`,
     `exporters: [debug verbosity=basic]` for sanity, switching to a true
     null sink for measurement.
3. **Harness**:
   * Extend `tools/obsday/run.sh` (or a sibling script outside the repo) to
     profile the Go binaries with samply, using the same operating points.
4. **Report**:
   * Compare CPU time per million logs (and per-byte cost) between Stack A
     and Stack B at matched operating points; include flame graph excerpts.

## Quick Reference

```bash
# Build + smoke profile (Stack A)
cd otel-arrow/rust/otap-dataflow
bash tools/obsday/run.sh \
    --rate 10000 --duration 30 --workers 2 \
    --attrs 8 --mean 24 --stddev 8

# Inspect
samply load obsday-out/logger.profile.json.gz
samply load obsday-out/collector.profile.json.gz
```
