// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! `obsday_logger` -- internal-logging-pipeline overhead experiment.
//!
//! This example embeds the OTAP dataflow engine as a library (mirroring
//! `examples/custom_collector.rs`) and pairs it with a fixed-rate generator
//! of `otel_info!` calls. The engine config is expected to enable the ITS
//! logging provider so those events flow through the internal-telemetry
//! receiver and out to an external collector via OTLP/gRPC.
//!
//! ```bash
//! cargo run --example obsday_logger -- \
//!     --config configs/obsday-logger.yaml \
//!     --rate 10000 --duration 30 --workers 2 \
//!     --attrs 8 --attr-size-mean 24 --attr-size-stddev 8 --seed 1
//! ```

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use otap_df_config::engine::{HttpAdminSettings, OtelDataflowSpec};
use otap_df_config::policy::{CoreAllocation, CoreRange};
// Side-effect imports: link the crates so their `linkme` distributed-slice
// registrations (component factories) are visible in `OTAP_PIPELINE_FACTORY`.
use otap_df_contrib_nodes as _;
use otap_df_controller::Controller;
use otap_df_controller::startup;
use otap_df_core_nodes as _;
use otap_df_otap::OTAP_PIPELINE_FACTORY;
use otap_df_telemetry::otel_info;
use rand::SeedableRng;
use rand::rngs::StdRng;
use weaver_common::vdir::VirtualDirectoryPath;
use weaver_forge::registry::ResolvedRegistry;
use weaver_resolver::SchemaResolver;
use weaver_semconv::attribute::{AttributeType, Examples, PrimitiveOrArrayTypeSpec};
use weaver_semconv::group::GroupType;
use weaver_semconv::registry_repo::RegistryRepo;

/// Default URL of the semantic-conventions registry to use for the
/// attribute value pool.
const DEFAULT_SEMCONV_URL: &str =
    "https://github.com/open-telemetry/semantic-conventions.git";

/// A logging-overhead experiment driver for the OTAP dataflow engine.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Path to the engine configuration file (.yaml or .json).
    #[arg(short = 'c', long, value_name = "FILE")]
    config: PathBuf,

    /// Number of cores to use (0 for all available cores).
    #[arg(long)]
    num_cores: Option<usize>,

    /// Inclusive range of CPU core IDs (e.g. "0-3").
    #[arg(long, value_name = "RANGE", value_parser = parse_core_id_allocation, conflicts_with = "num_cores")]
    core_id_range: Option<CoreAllocation>,

    /// Address to bind the HTTP admin server to.
    #[arg(long, default_value = "127.0.0.1:9876")]
    http_admin_bind: String,

    /// Validate the configuration and exit without starting the engine.
    #[arg(long)]
    validate_and_exit: bool,

    /// Target log emission rate (logs per second, summed across workers).
    #[arg(long, default_value_t = 1_000)]
    rate: u64,

    /// Duration of the steady-state emission window, in seconds.
    #[arg(long, default_value_t = 30)]
    duration: u64,

    /// Number of generator threads.
    #[arg(long, default_value_t = 1)]
    workers: usize,

    /// Number of string attributes per log record. Must be one of
    /// {1, 2, 4, 8, 16, 32}; pick the smallest supported value at or above
    /// your target.
    #[arg(long, default_value_t = 8)]
    attrs: usize,

    /// RNG seed (XOR-ed with worker id per worker).
    #[arg(long, default_value_t = 0xC0FFEE)]
    seed: u64,

    /// Seconds to wait after the engine starts before emitting logs.
    #[arg(long, default_value_t = 2)]
    warmup_secs: u64,

    /// Seconds to wait after emission ends before requesting shutdown.
    #[arg(long, default_value_t = 5)]
    cooldown_secs: u64,
}

fn parse_core_id_allocation(s: &str) -> Result<CoreAllocation, String> {
    let ranges: Result<Vec<CoreRange>, String> = s
        .split(',')
        .map(|part| {
            let part = part.trim();
            if let Ok(n) = part.parse::<usize>() {
                return Ok(CoreRange { start: n, end: n });
            }
            let normalized = part.replace("..=", "-").replace("..", "-");
            let mut split = normalized.split('-');
            let start = split
                .next()
                .ok_or_else(|| "missing start".to_string())?
                .trim()
                .parse::<usize>()
                .map_err(|_| "invalid start".to_string())?;
            let end = split
                .next()
                .ok_or_else(|| "missing end".to_string())?
                .trim()
                .parse::<usize>()
                .map_err(|_| "invalid end".to_string())?;
            Ok(CoreRange { start, end })
        })
        .collect();
    Ok(CoreAllocation::core_set(ranges?))
}

/// One realistic log-event shape harvested from the OpenTelemetry
/// semantic-conventions registry.
///
/// `name` is the event name; `slot_pools[i]` contains the example values
/// (or, when no examples are present, the attribute name itself) for the
/// i-th attribute of the originating semconv group. At emit time, slot
/// `j` of the fixed `emit_N` callsite is filled by drawing one value from
/// `slot_pools[j % slot_pools.len()]`. Drawing values from the same
/// template per emit keeps attributes correlated; the small example pools
/// give downstream OTAP dictionary encoding real dedup opportunities.
struct EventTemplate {
    name: String,
    slot_pools: Vec<Vec<String>>,
}

/// Persistent path where we cache a clone of the semconv registry across
/// runs so each `obsday_logger` invocation does not re-clone (which would
/// otherwise pollute every CPU profile with `gix`/`mkdir`/`unlinkat`).
fn semconv_cache_dir() -> PathBuf {
    let home = std::env::var_os("HOME").unwrap_or_else(|| ".".into());
    PathBuf::from(home).join(".cache/obsday/semantic-conventions")
}

/// Ensure a local clone of the OpenTelemetry semantic-conventions registry
/// exists at `semconv_cache_dir()`; clone (shallow) if not. Returns the
/// path to the cloned tree.
fn ensure_semconv_clone() -> Result<PathBuf, String> {
    let dir = semconv_cache_dir();
    let model = dir.join("model");
    if model.is_dir() {
        return Ok(dir);
    }
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {parent:?}: {e}"))?;
    }
    println!("cloning {DEFAULT_SEMCONV_URL} into {dir:?} (one-time) ...");
    let status = std::process::Command::new("git")
        .args([
            "clone",
            "--depth=1",
            "--single-branch",
            DEFAULT_SEMCONV_URL,
            dir.to_str().ok_or("invalid clone path")?,
        ])
        .status()
        .map_err(|e| format!("spawn git: {e}"))?;
    if !status.success() {
        return Err(format!("git clone failed with {status}"));
    }
    Ok(dir)
}

/// Build a list of realistic event templates from the OpenTelemetry
/// semantic-conventions registry.
///
/// One template per `event` group. For each attribute we collect either
/// the registered string examples or, if none, the attribute name as a
/// single-entry pool. Empty entries and entries longer than 256 bytes
/// are skipped. Returns an error if no usable templates were found.
fn build_event_templates() -> Result<Vec<EventTemplate>, String> {
    let clone_dir = ensure_semconv_clone()?;
    let model_dir = clone_dir.join("model");
    let path = VirtualDirectoryPath::LocalFolder {
        path: model_dir
            .to_str()
            .ok_or_else(|| format!("non-utf8 model path: {model_dir:?}"))?
            .to_string(),
    };
    let repo = RegistryRepo::try_new("main", &path).map_err(|e| e.to_string())?;
    let registry = match SchemaResolver::load_semconv_repository(repo, false) {
        weaver_common::result::WResult::Ok(r) => r,
        weaver_common::result::WResult::OkWithNFEs(r, _) => r,
        weaver_common::result::WResult::FatalErr(e) => return Err(e.to_string()),
    };
    let resolved = match SchemaResolver::resolve(registry, true) {
        weaver_common::result::WResult::Ok(r) => r,
        weaver_common::result::WResult::OkWithNFEs(r, _) => r,
        weaver_common::result::WResult::FatalErr(e) => return Err(e.to_string()),
    };
    let resolved_registry = ResolvedRegistry::try_from_resolved_registry(
        &resolved.registry,
        resolved.catalog(),
    )
    .map_err(|e| e.to_string())?;

    fn keep(s: &str) -> bool {
        !s.is_empty() && s.len() <= 256
    }

    let mut templates: Vec<EventTemplate> = Vec::new();
    for group in &resolved_registry.groups {
        if !matches!(group.r#type, GroupType::Event) {
            continue;
        }
        let event_name = group
            .name
            .clone()
            .or_else(|| Some(group.id.clone()))
            .unwrap_or_default();
        if event_name.is_empty() || group.attributes.is_empty() {
            continue;
        }
        let mut slot_pools: Vec<Vec<String>> = Vec::with_capacity(group.attributes.len());
        for attr in &group.attributes {
            let mut examples: Vec<String> = Vec::new();
            let is_stringy = matches!(
                &attr.r#type,
                AttributeType::PrimitiveOrArray(
                    PrimitiveOrArrayTypeSpec::String | PrimitiveOrArrayTypeSpec::Any,
                )
            );
            if is_stringy {
                match &attr.examples {
                    Some(Examples::String(s)) if keep(s) => examples.push(s.to_string()),
                    Some(Examples::Strings(ss)) => {
                        for s in ss {
                            if keep(s) {
                                examples.push(s.to_string());
                            }
                        }
                    }
                    _ => {}
                }
            }
            if examples.is_empty() {
                // Fall back to the attribute name so the slot still has a
                // realistic, repeating value (real apps emit attribute
                // names verbatim a lot anyway).
                examples.push(attr.name.clone());
            }
            slot_pools.push(examples);
        }
        templates.push(EventTemplate {
            name: event_name,
            slot_pools,
        });
    }
    if templates.is_empty() {
        return Err("no event templates found in semconv registry".to_string());
    }
    Ok(templates)
}

fn fill_from_template(rng: &mut StdRng, t: &EventTemplate, values: &mut [String]) {
    use rand::RngExt;
    for (j, buf) in values.iter_mut().enumerate() {
        let pool = &t.slot_pools[j % t.slot_pools.len()];
        let s = &pool[rng.random_range(0..pool.len())];
        buf.clear();
        buf.push_str(s);
    }
}

/// Emitter table: indexed by attribute count. `None` slots are unsupported.
type Emitter = fn(&str, u64, &[String]);

const fn emitter_for(attrs: usize) -> Option<Emitter> {
    match attrs {
        1 => Some(emit_1),
        2 => Some(emit_2),
        4 => Some(emit_4),
        8 => Some(emit_8),
        16 => Some(emit_16),
        32 => Some(emit_32),
        _ => None,
    }
}

// Each emitter is a distinct `tracing` callsite with a fixed set of fields.
// The tracing event name (callsite metadata) must be a string literal, so
// it is fixed at "obsday.log". The realistic semconv event name is passed
// through as the `evt` attribute value; with a small set of templates the
// `evt` strings repeat heavily and OTAP dictionary encoding deduplicates
// them along with the per-slot k0..kN values.

fn emit_1(evt: &str, seq: u64, v: &[String]) {
    otel_info!("obsday.log", seq = seq, evt = evt, k0 = v[0].as_str());
}
fn emit_2(evt: &str, seq: u64, v: &[String]) {
    otel_info!(
        "obsday.log",
        seq = seq,
        evt = evt,
        k0 = v[0].as_str(),
        k1 = v[1].as_str()
    );
}
fn emit_4(evt: &str, seq: u64, v: &[String]) {
    otel_info!(
        "obsday.log",
        seq = seq,
        evt = evt,
        k0 = v[0].as_str(),
        k1 = v[1].as_str(),
        k2 = v[2].as_str(),
        k3 = v[3].as_str()
    );
}
fn emit_8(evt: &str, seq: u64, v: &[String]) {
    otel_info!(
        "obsday.log",
        seq = seq,
        evt = evt,
        k0 = v[0].as_str(),
        k1 = v[1].as_str(),
        k2 = v[2].as_str(),
        k3 = v[3].as_str(),
        k4 = v[4].as_str(),
        k5 = v[5].as_str(),
        k6 = v[6].as_str(),
        k7 = v[7].as_str()
    );
}
fn emit_16(evt: &str, seq: u64, v: &[String]) {
    otel_info!(
        "obsday.log",
        seq = seq,
        evt = evt,
        k0 = v[0].as_str(),
        k1 = v[1].as_str(),
        k2 = v[2].as_str(),
        k3 = v[3].as_str(),
        k4 = v[4].as_str(),
        k5 = v[5].as_str(),
        k6 = v[6].as_str(),
        k7 = v[7].as_str(),
        k8 = v[8].as_str(),
        k9 = v[9].as_str(),
        k10 = v[10].as_str(),
        k11 = v[11].as_str(),
        k12 = v[12].as_str(),
        k13 = v[13].as_str(),
        k14 = v[14].as_str(),
        k15 = v[15].as_str()
    );
}
fn emit_32(evt: &str, seq: u64, v: &[String]) {
    otel_info!(
        "obsday.log",
        seq = seq,
        evt = evt,
        k0 = v[0].as_str(),
        k1 = v[1].as_str(),
        k2 = v[2].as_str(),
        k3 = v[3].as_str(),
        k4 = v[4].as_str(),
        k5 = v[5].as_str(),
        k6 = v[6].as_str(),
        k7 = v[7].as_str(),
        k8 = v[8].as_str(),
        k9 = v[9].as_str(),
        k10 = v[10].as_str(),
        k11 = v[11].as_str(),
        k12 = v[12].as_str(),
        k13 = v[13].as_str(),
        k14 = v[14].as_str(),
        k15 = v[15].as_str(),
        k16 = v[16].as_str(),
        k17 = v[17].as_str(),
        k18 = v[18].as_str(),
        k19 = v[19].as_str(),
        k20 = v[20].as_str(),
        k21 = v[21].as_str(),
        k22 = v[22].as_str(),
        k23 = v[23].as_str(),
        k24 = v[24].as_str(),
        k25 = v[25].as_str(),
        k26 = v[26].as_str(),
        k27 = v[27].as_str(),
        k28 = v[28].as_str(),
        k29 = v[29].as_str(),
        k30 = v[30].as_str(),
        k31 = v[31].as_str()
    );
}

fn worker_loop(
    worker_id: u64,
    seed: u64,
    period: Duration,
    deadline: Instant,
    attrs: usize,
    templates: Arc<Vec<EventTemplate>>,
    counter: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    use rand::RngExt;
    let emit = emitter_for(attrs).expect("validated in main");
    let mut rng = StdRng::seed_from_u64(seed ^ worker_id);
    let mut values: Vec<String> = (0..attrs).map(|_| String::with_capacity(64)).collect();
    let mut local_seq: u64 = 0;
    let start = Instant::now();
    let mut next_tick = start;
    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        // Pace the loop. If we are behind, fire immediately.
        let now = Instant::now();
        if now < next_tick {
            std::thread::sleep(next_tick - now);
        }
        next_tick += period;

        let t = &templates[rng.random_range(0..templates.len())];
        fill_from_template(&mut rng, t, &mut values);
        // Encode worker id into the sequence so values are globally unique.
        let seq = (worker_id << 48) | local_seq;
        emit(t.name.as_str(), seq, &values);
        local_seq += 1;
    }
    let _ = counter.fetch_add(local_seq, Ordering::Relaxed);
}

/// Wait until the admin HTTP server accepts TCP connections, then return.
fn wait_admin_ready(addr: &SocketAddr, timeout: Duration) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect_timeout(addr, Duration::from_millis(200)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("admin not ready within {timeout:?}"))
}

/// Issue a fire-and-blocking-wait shutdown via the admin HTTP API.
fn request_shutdown(addr: &SocketAddr, wait_secs: u64) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(addr, Duration::from_secs(2))
        .map_err(|e| format!("connect admin: {e}"))?;
    let req = format!(
        "POST /api/v1/groups/shutdown?wait=true&timeout_secs={wait_secs} HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Content-Length: 0\r\n\
         Connection: close\r\n\
         \r\n"
    );
    stream
        .write_all(req.as_bytes())
        .map_err(|e| format!("write admin: {e}"))?;
    let mut resp = Vec::new();
    let _ = stream.read_to_end(&mut resp);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otap_df_otap::crypto::install_crypto_provider()
        .map_err(|e| format!("install rustls crypto provider: {e}"))?;

    let args = Args::parse();

    if emitter_for(args.attrs).is_none() {
        return Err(format!(
            "--attrs={} not supported; pick one of {{1,2,4,8,16,32}}",
            args.attrs
        )
        .into());
    }
    if args.workers == 0 {
        return Err("--workers must be >= 1".into());
    }
    if args.rate == 0 {
        return Err("--rate must be >= 1".into());
    }

    println!("{}", startup::system_info(&OTAP_PIPELINE_FACTORY, "system"));

    let mut engine_cfg = OtelDataflowSpec::from_file(&args.config)?;
    startup::apply_cli_overrides(
        &mut engine_cfg,
        args.num_cores,
        args.core_id_range,
        Some(args.http_admin_bind.clone()),
    );
    if engine_cfg.engine.http_admin.is_none() {
        engine_cfg.engine.http_admin = Some(HttpAdminSettings {
            bind_address: args.http_admin_bind.clone(),
        });
    }

    startup::validate_engine_components(&engine_cfg, &OTAP_PIPELINE_FACTORY)?;

    if args.validate_and_exit {
        println!("Configuration '{}' is valid.", args.config.display());
        return Ok(());
    }

    // Build event templates up-front (the registry fetch is slow and does
    // not belong in the measurement window).
    println!("loading semconv registry from {DEFAULT_SEMCONV_URL} ...");
    let pool_t0 = Instant::now();
    let templates: Arc<Vec<EventTemplate>> = Arc::new(
        build_event_templates().map_err(|e| format!("event templates: {e}"))?,
    );
    let total_examples: usize = templates
        .iter()
        .flat_map(|t| t.slot_pools.iter().map(|p| p.len()))
        .sum();
    println!(
        "event templates ready: {} events, {} slot-examples in {:.2}s",
        templates.len(),
        total_examples,
        pool_t0.elapsed().as_secs_f64(),
    );

    let admin_addr: SocketAddr = args
        .http_admin_bind
        .parse()
        .map_err(|e| format!("--http-admin-bind '{}': {e}", args.http_admin_bind))?;

    // Run the engine on a dedicated thread; it sets up the global tracing
    // dispatcher (ITS provider) before returning startup completion.
    let engine_thread = thread::Builder::new()
        .name("obsday-engine".to_string())
        .spawn(move || {
            let controller = Controller::new(&OTAP_PIPELINE_FACTORY);
            controller.run_till_shutdown(engine_cfg)
        })?;

    // Wait for the admin HTTP server to bind so we know the engine is up.
    wait_admin_ready(&admin_addr, Duration::from_secs(15))?;
    // Extra warmup to let pipelines reach steady state.
    thread::sleep(Duration::from_secs(args.warmup_secs));

    println!(
        "starting emission: rate={}/s workers={} attrs={} duration={}s templates={}",
        args.rate,
        args.workers,
        args.attrs,
        args.duration,
        templates.len(),
    );

    let per_worker_rate = (args.rate as f64) / (args.workers as f64);
    let period = Duration::from_secs_f64(1.0 / per_worker_rate);
    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let stop = Arc::new(AtomicBool::new(false));
    let counter = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(args.workers);
    let wall_start = Instant::now();
    for w in 0..args.workers {
        let counter = Arc::clone(&counter);
        let stop = Arc::clone(&stop);
        let templates = Arc::clone(&templates);
        let attrs = args.attrs;
        let seed = args.seed;
        handles.push(
            thread::Builder::new()
                .name(format!("obsday-w{w}"))
                .spawn(move || {
                    worker_loop(
                        w as u64, seed, period, deadline, attrs, templates, counter, stop,
                    );
                })?,
        );
    }
    for h in handles {
        let _ = h.join();
    }
    let elapsed = wall_start.elapsed();
    let emitted = counter.load(Ordering::Relaxed);
    println!(
        "emission done: emitted={} wall={:.3}s effective_rate={:.0}/s",
        emitted,
        elapsed.as_secs_f64(),
        emitted as f64 / elapsed.as_secs_f64().max(1e-9),
    );

    // Cooldown: let batch + exporter drain before signalling shutdown.
    thread::sleep(Duration::from_secs(args.cooldown_secs));
    println!("requesting graceful shutdown");
    request_shutdown(&admin_addr, 30)?;

    match engine_thread.join() {
        Ok(Ok(())) => {
            println!("engine exited cleanly");
            Ok(())
        }
        Ok(Err(e)) => Err(format!("engine error: {e}").into()),
        Err(_) => Err("engine thread panicked".into()),
    }
}
