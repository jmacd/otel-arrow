//! Profile harness: drives one of the samplers (or a baseline) in a
//! tight loop so `samply record` can attribute time to RNG, hashmap
//! ops, sort, etc.
//!
//! Usage: `cargo run --release --example profile_sampling -- <mode> [iters]`
//! where mode ∈ {j, k, rng, hashmap, baseline}.
#![allow(missing_docs)]

use std::collections::HashMap;
use std::env;
use std::hint::black_box;
use std::time::Instant;

use otap_df_telemetry::sampling::{Admission, AlgorithmJ, AlgorithmK, LogSampler};
use rand::{RngExt, SeedableRng, rngs::SmallRng};

const SEED: u64 = 0xD15EA5E_5A112024;
const N: usize = 10_000;
const K: usize = 64;
const TARGET: usize = 100;
const J_BUFFER: usize = 1_000;

fn gen_zipf(k: usize, n: usize, seed: u64) -> Vec<u32> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let weights: Vec<f64> = (0..k).map(|i| 1.0 / (i as f64 + 1.0)).collect();
    let total: f64 = weights.iter().sum();
    let cumulative: Vec<f64> = weights
        .iter()
        .scan(0.0, |a, w| {
            *a += w / total;
            Some(*a)
        })
        .collect();
    (0..n)
        .map(|_| {
            let u: f64 = rng.random();
            cumulative.iter().position(|&c| u < c).unwrap_or(k - 1) as u32
        })
        .collect()
}

fn run_j(cs: &[u32], iters: usize) {
    for _ in 0..iters {
        let s: AlgorithmJ<u32, u64> = AlgorithmJ::with_seed(TARGET, J_BUFFER, SEED);
        for (i, c) in cs.iter().enumerate() {
            match s.admit(c) {
                Admission::Skip => {}
                Admission::Admit(t) => s.insert(*c, t, i as u64),
            }
        }
        let _ = black_box(s.flush().len());
    }
}

fn run_k(cs: &[u32], iters: usize) {
    // Warm K once so freq_prev is populated.
    let s: AlgorithmK<u32, u64> = AlgorithmK::with_options(TARGET, 32, SEED);
    for (i, c) in cs.iter().enumerate() {
        if let Admission::Admit(t) = s.admit(c) {
            s.insert(*c, t, i as u64);
        }
    }
    let _ = s.flush();
    for _ in 0..iters {
        for (i, c) in cs.iter().enumerate() {
            match s.admit(c) {
                Admission::Skip => {}
                Admission::Admit(t) => s.insert(*c, t, i as u64),
            }
        }
        let _ = black_box(s.flush().len());
    }
}

fn run_rng(cs: &[u32], iters: usize) {
    // Just draw one f64 per "event". Lower bound: pure RNG cost.
    for _ in 0..iters {
        let mut rng = SmallRng::seed_from_u64(SEED);
        let mut acc = 0.0f64;
        for _ in cs {
            let u: f64 = rng.random();
            acc += u;
        }
        let _ = black_box(acc);
    }
}

fn run_hashmap(cs: &[u32], iters: usize) {
    // RNG + per-event HashMap counter increment. Approximates the
    // admit hot-path minus the comparison + Admission match.
    for _ in 0..iters {
        let mut rng = SmallRng::seed_from_u64(SEED);
        let mut freq: HashMap<u32, u64> = HashMap::new();
        let mut acc = 0.0f64;
        for c in cs {
            let entry = freq.entry(*c).or_insert(0);
            *entry += 1;
            let f = *entry;
            let u: f64 = rng.random();
            acc += u * (f as f64);
        }
        let _ = black_box(acc);
        let _ = black_box(freq.len());
    }
}

fn run_baseline(cs: &[u32], iters: usize) {
    // Just iterate the slice. Pure loop overhead floor.
    for _ in 0..iters {
        let mut acc: u64 = 0;
        for c in cs {
            acc = acc.wrapping_add(*c as u64);
        }
        let _ = black_box(acc);
    }
}

fn main() {
    let mode = env::args().nth(1).unwrap_or_else(|| "j".into());
    let iters: usize = env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let cs = gen_zipf(K, N, SEED);
    let total_events = (cs.len() * iters) as u64;

    let start = Instant::now();
    match mode.as_str() {
        "j" => run_j(&cs, iters),
        "k" => run_k(&cs, iters),
        "rng" => run_rng(&cs, iters),
        "hashmap" => run_hashmap(&cs, iters),
        "baseline" => run_baseline(&cs, iters),
        other => panic!("unknown mode: {other}"),
    }
    let dt = start.elapsed();
    let ns_per_event = dt.as_nanos() as f64 / total_events as f64;
    let throughput = total_events as f64 / dt.as_secs_f64() / 1e6;
    println!(
        "mode={mode} iters={iters} n={N} k={K} target={TARGET} elapsed={:.2?} \
         events={total_events} ns/event={ns_per_event:.2} Mevents/s={throughput:.1}",
        dt
    );
}
