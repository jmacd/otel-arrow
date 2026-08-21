// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context regression benchmarks.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use otap_df_config::transport_headers_policy::{
    CaptureDefaults, CaptureRule, CompiledHeaderCapturePolicy, HeaderCapturePolicy,
    HeaderPropagationPolicy, PropagationDefault, PropagationSelector, PropagationSelectorType,
};
use otap_df_otap::context_bytes::{HeaderInput, HeaderValueKind, PdataContextBytes};

const HEADER_COUNTS: [usize; 4] = [1, 4, 16, 32];

criterion_group!(benches, bench_context_bytes);
criterion_main!(benches);

fn bench_context_bytes(c: &mut Criterion) {
    bench_capture(c);

    let policy = capture_policy(32);
    let compiled = policy.compile().expect("valid capture policy");
    let pairs = pairs(32);
    let context = capture_context(&compiled, &pairs);
    let propagation = propagation_policy();

    let _ = c.bench_function("pdata_context/lookup/entry", |b| {
        b.iter(|| {
            black_box(&context)
                .entry(0)
                .expect("tenant entry")
                .values()
                .count()
        });
    });
    let _ = c.bench_function("pdata_context/lookup/stored_name", |b| {
        b.iter(|| black_box(&context).find_by_name("x-header-30").count());
    });
    let _ = c.bench_function("pdata_context/clone", |b| {
        b.iter(|| black_box(context.clone()));
    });
    let _ = c.bench_function("pdata_context/capture_then_clone", |b| {
        b.iter(|| {
            let captured = capture_context(&compiled, &pairs);
            black_box(captured.clone())
        });
    });
    let _ = c.bench_function("pdata_context/propagate", |b| {
        b.iter(|| {
            black_box(&context)
                .propagate(&propagation)
                .map(|header| header.header_name.len() + header.value.len())
                .sum::<usize>()
        });
    });

    let partition_values = partition_values();
    let _ = c.bench_function("pdata_context/project_partition_8", |b| {
        b.iter(|| {
            for value in &partition_values {
                let projected = black_box(&context)
                    .project()
                    .append_bag_header(HeaderInput {
                        wire_name: "partition",
                        stored_name: "partition",
                        value,
                        kind: HeaderValueKind::Text,
                        rule_id: u16::MAX,
                        entry: None,
                    })
                    .expect("context projection");
                let _ = black_box(projected);
            }
        });
    });

    let _ = c.bench_function("pdata_context/end_to_end", |b| {
        b.iter(|| {
            let captured = capture_context(&compiled, &pairs);
            let hop1 = captured.clone();
            let hop2 = hop1.clone();
            black_box(
                hop2.propagate(&propagation)
                    .map(|header| header.value.len())
                    .sum::<usize>(),
            )
        });
    });
}

fn bench_capture(c: &mut Criterion) {
    let mut group = c.benchmark_group("pdata_context/capture");
    for header_count in HEADER_COUNTS {
        let policy = capture_policy(header_count);
        let compiled = policy.compile().expect("valid capture policy");
        let pairs = pairs(header_count);
        let _ = group.bench_with_input(
            BenchmarkId::from_parameter(header_count),
            &header_count,
            |b, _| b.iter(|| black_box(capture_context(&compiled, &pairs))),
        );
    }
    group.finish();
}

fn capture_policy(header_count: usize) -> HeaderCapturePolicy {
    let mut rules = Vec::with_capacity(header_count);
    rules.push(CaptureRule {
        match_names: vec!["x-tenant".to_string()],
        store_as: Some("tenant".to_string()),
        sensitive: false,
        value_kind: None,
    });
    for index in 1..header_count {
        rules.push(CaptureRule {
            match_names: vec![format!("x-header-{index}")],
            store_as: None,
            sensitive: false,
            value_kind: None,
        });
    }
    HeaderCapturePolicy::new(CaptureDefaults::default(), rules)
}

fn propagation_policy() -> HeaderPropagationPolicy {
    HeaderPropagationPolicy::new(
        PropagationDefault {
            selector: PropagationSelector {
                selector_type: PropagationSelectorType::AllCaptured,
                named: None,
            },
            ..PropagationDefault::default()
        },
        vec![],
    )
}

fn pairs(header_count: usize) -> Vec<(String, Vec<u8>)> {
    let mut pairs = Vec::with_capacity(header_count);
    pairs.push(("x-tenant".to_string(), b"acme".to_vec()));
    for index in 1..header_count {
        pairs.push((
            format!("x-header-{index}"),
            format!("value-{index}").into_bytes(),
        ));
    }
    pairs
}

fn partition_values() -> Vec<Vec<u8>> {
    (0..8)
        .map(|index| format!("partition-{index}").into_bytes())
        .collect()
}

fn capture_context(
    policy: &CompiledHeaderCapturePolicy,
    pairs: &[(String, Vec<u8>)],
) -> PdataContextBytes {
    PdataContextBytes::capture(
        policy,
        pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice())),
    )
    .expect("context capture")
    .0
    .expect("captured headers")
}
