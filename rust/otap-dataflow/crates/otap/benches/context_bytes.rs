// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Replacement-shape benchmark: legacy headers versus staged context bytes.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use otap_df_config::transport_headers::{TransportHeader, TransportHeaders};
use otap_df_config::transport_headers_policy::{CaptureDefaults, CaptureRule, HeaderCapturePolicy};
use otap_df_otap::context_bytes::{HeaderInput, HeaderValueKind, PdataContextBytes};

const HEADER_COUNT: usize = 32;

criterion_group!(benches, bench_context_bytes);
criterion_main!(benches);

fn bench_context_bytes(c: &mut Criterion) {
    let policy = policy();
    let compiled_policy = policy.compile();
    let pairs = pairs();
    let legacy = capture_legacy(&policy, &pairs);
    let bytes = capture_bytes(&compiled_policy, &pairs);

    let _ = c.bench_function("context_headers/capture/legacy", |b| {
        b.iter(|| black_box(capture_legacy(&policy, &pairs)));
    });
    let _ = c.bench_function("context_headers/capture/bytes", |b| {
        b.iter(|| black_box(capture_bytes(&compiled_policy, &pairs)));
    });
    let _ = c.bench_function("context_headers/lookup/legacy-linear", |b| {
        b.iter(|| black_box(&legacy).find_by_name("tenant").count());
    });
    let _ = c.bench_function("context_headers/lookup/bytes-entry", |b| {
        b.iter(|| {
            black_box(&bytes)
                .entry(0)
                .expect("tenant entry")
                .values()
                .count()
        });
    });
    let _ = c.bench_function("context_headers/clone/legacy", |b| {
        b.iter(|| black_box(legacy.clone()));
    });
    let _ = c.bench_function("context_headers/clone/bytes", |b| {
        b.iter(|| black_box(bytes.clone()));
    });
    let _ = c.bench_function("context_headers/clone/bytes-raw", |b| {
        b.iter(|| black_box(bytes.bytes().clone()));
    });
    let _ = c.bench_function("context_headers/capture_then_clone/legacy", |b| {
        b.iter(|| {
            let captured = capture_legacy(&policy, &pairs);
            black_box(captured.clone())
        });
    });
    let _ = c.bench_function("context_headers/capture_then_clone/bytes", |b| {
        b.iter(|| {
            let captured = capture_bytes(&compiled_policy, &pairs);
            black_box(captured.clone())
        });
    });
    let _ = c.bench_function("context_headers/shared_mutation/legacy-cow", |b| {
        b.iter(|| {
            let mut fork = legacy.clone();
            fork.push(TransportHeader::text(
                "x-added",
                "X-Added",
                b"added".as_slice(),
            ));
            black_box(fork)
        });
    });
    let _ = c.bench_function("context_headers/partition_split_8/legacy-cow", |b| {
        b.iter_batched(
            partition_headers,
            |partition_headers| {
                for partition_header in partition_headers {
                    let mut output = legacy.clone();
                    output.push(partition_header);
                    let _ = black_box(output);
                }
            },
            BatchSize::SmallInput,
        );
    });
    let _ = c.bench_function("context_headers/partition_split_8/bytes-project", |b| {
        b.iter_batched(
            partition_headers,
            |partition_headers| {
                for partition_header in &partition_headers {
                    let output = bytes
                        .project()
                        .append_bag_header(HeaderInput {
                            wire_name: &partition_header.wire_name,
                            stored_name: &partition_header.name,
                            value: &partition_header.value,
                            kind: HeaderValueKind::Text,
                            rule_id: u16::MAX,
                            entry: None,
                        })
                        .expect("context projection");
                    let _ = black_box(output);
                }
            },
            BatchSize::SmallInput,
        );
    });
}

fn partition_headers() -> Vec<TransportHeader> {
    (0..8)
        .map(|index| {
            TransportHeader::text(
                "partition",
                "partition",
                format!("partition-{index}").into_bytes(),
            )
        })
        .collect()
}

fn policy() -> HeaderCapturePolicy {
    let mut rules = Vec::with_capacity(HEADER_COUNT);
    rules.push(CaptureRule {
        match_names: vec!["x-tenant".to_string()],
        store_as: Some("tenant".to_string()),
        sensitive: false,
        value_kind: None,
    });
    for index in 0..HEADER_COUNT - 1 {
        rules.push(CaptureRule {
            match_names: vec![format!("x-header-{index}")],
            store_as: None,
            sensitive: false,
            value_kind: None,
        });
    }
    HeaderCapturePolicy::new(CaptureDefaults::default(), rules)
}

fn pairs() -> Vec<(String, Vec<u8>)> {
    let mut pairs = vec![
        ("x-tenant".to_string(), b"acme".to_vec()),
        ("x-tenant".to_string(), b"acme-secondary".to_vec()),
    ];
    for index in 0..HEADER_COUNT - 2 {
        pairs.push((
            format!("x-header-{index}"),
            format!("value-{index}").into_bytes(),
        ));
    }
    pairs
}

fn capture_legacy(policy: &HeaderCapturePolicy, pairs: &[(String, Vec<u8>)]) -> TransportHeaders {
    let mut headers = TransportHeaders::new();
    let _ = policy.capture_from_pairs(
        pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice())),
        &mut headers,
    );
    headers
}

fn capture_bytes(
    policy: &otap_df_config::transport_headers_policy::CompiledHeaderCapturePolicy,
    pairs: &[(String, Vec<u8>)],
) -> PdataContextBytes {
    PdataContextBytes::capture(
        policy,
        pairs
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice())),
    )
    .expect("bytes capture")
    .0
    .expect("captured headers")
}
