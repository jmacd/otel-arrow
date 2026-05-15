// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Head-to-head benchmark of Algorithm J vs Algorithm K on synthetic
//! log-event streams.
//!
//! For each (callsite alphabet size, stream length, target) point we
//! drive both samplers through one full period (admit + insert +
//! flush) and compare their throughput.  The payload is a `u64` so
//! the benchmark measures *sampler* overhead, not formatting.
//!
//! Workload generators:
//! - `uniform`   — every callsite equally likely (worst case for J:
//!   τ tightens slowly; best case for K once warm)
//! - `zipf_like` — `1/(i+1)` probabilities (heavy skew, the common
//!   real-world case)
//! - `bursty`    — half the stream from one callsite, half spread
//!   uniformly (regime-shift stress test)

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use otap_df_telemetry::sampling::{Admission, AlgorithmJ, AlgorithmK, LogSampler};
use rand::{RngExt, SeedableRng, rngs::SmallRng};

const SEED: u64 = 0xD15EA5E_5A112024;
const N_VALUES: &[usize] = &[10_000, 100_000];
const K_VALUES: &[usize] = &[8, 64];
const TARGET: usize = 100;
const J_BUFFER: usize = 1_000;

fn gen_uniform(k: usize, n: usize, seed: u64) -> Vec<u32> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..n)
        .map(|_| (rng.random::<u64>() % k as u64) as u32)
        .collect()
}

fn gen_zipf_like(k: usize, n: usize, seed: u64) -> Vec<u32> {
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

fn gen_bursty(k: usize, n: usize, seed: u64) -> Vec<u32> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..n)
        .map(|i| {
            if i < n / 2 {
                0u32
            } else {
                1 + (rng.random::<u64>() % (k as u64 - 1)) as u32
            }
        })
        .collect()
}

fn drive_j(sampler: &AlgorithmJ<u32, u64>, callsites: &[u32]) {
    for (i, c) in callsites.iter().enumerate() {
        match sampler.admit(c) {
            Admission::Skip => {}
            Admission::Admit(t) => sampler.insert(*c, t, i as u64),
        }
    }
    let flushed = sampler.flush();
    let _ = black_box(flushed.len());
}

fn drive_k(sampler: &AlgorithmK<u32, u64>, callsites: &[u32]) {
    for (i, c) in callsites.iter().enumerate() {
        match sampler.admit(c) {
            Admission::Skip => {}
            Admission::Admit(t) => sampler.insert(*c, t, i as u64),
        }
    }
    let flushed = sampler.flush();
    let _ = black_box(flushed.len());
}

fn bench_workload(c: &mut Criterion, workload: &str, genfn: fn(usize, usize, u64) -> Vec<u32>) {
    let mut group = c.benchmark_group(format!("sampling/{workload}"));
    for &n in N_VALUES {
        for &k in K_VALUES {
            let cs = genfn(k, n, SEED);
            let _ = group.throughput(Throughput::Elements(n as u64));

            let _ = group.bench_with_input(
                BenchmarkId::new("algo_j", format!("k={k}/n={n}")),
                &cs,
                |b, cs| {
                    b.iter_with_setup(
                        || AlgorithmJ::<u32, u64>::with_seed(TARGET, J_BUFFER, SEED),
                        |s| drive_j(&s, cs),
                    );
                },
            );

            let _ = group.bench_with_input(
                BenchmarkId::new("algo_k_warm", format!("k={k}/n={n}")),
                &cs,
                |b, cs| {
                    b.iter_with_setup(
                        || {
                            let s = AlgorithmK::<u32, u64>::with_options(TARGET, 32, SEED);
                            drive_k(&s, cs);
                            s
                        },
                        |s| drive_k(&s, cs),
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_all(c: &mut Criterion) {
    bench_workload(c, "uniform", gen_uniform);
    bench_workload(c, "zipf_like", gen_zipf_like);
    bench_workload(c, "bursty", gen_bursty);
}

criterion_group!(benches, bench_all);
criterion_main!(benches);
