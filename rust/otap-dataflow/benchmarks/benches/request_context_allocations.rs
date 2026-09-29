// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Counts allocations in request-context construction and partition lookup.

#![allow(clippy::print_stdout)]

use std::hint::black_box;
use std::sync::Arc;

use hashbrown::HashMap;
use otel_arrow_dfe_config::ContextEntryName;
use otel_arrow_dfe_config::authorized_identity_policy::AuthorizedIdentityPolicy;
use otel_arrow_dfe_config::context_layout::{ContextPrimitive, ContextSource};
use otel_arrow_dfe_config::context_policy::{
    ContextEntryDeclaration, ContextEntryDefinition, ContextEntryPart, ContextScope,
};
use otel_arrow_dfe_config::transport_headers::{TransportHeader, TransportHeaders};
use otel_arrow_dfe_engine::capability::auth::{AuthorizedIdentity, ClaimValue};
use otel_arrow_dfe_engine::context_declaration::{BoundContextEntry, CompiledContextLayout};
use otel_arrow_dfe_otap::packed_context::{CapturedClaim, CapturedHeader, PackedContext};
use otel_arrow_dfe_otap::pdata::{AuthorizedIdentityEntries, Context, ContextPartitionKey};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

fn name(value: &str) -> ContextEntryName {
    ContextEntryName::try_from(value).expect("valid context entry name")
}

fn measure<T>(label: &str, operation: impl FnOnce() -> T) {
    let profiler = dhat::Profiler::builder().testing().build();
    let output = black_box(operation());
    let stats = dhat::HeapStats::get();
    println!(
        "{label}: allocations={}, bytes={}",
        stats.total_blocks, stats.total_bytes
    );
    drop(output);
    drop(profiler);
}

fn main() {
    let entry_name = name("product_user");
    let layout = CompiledContextLayout::compile_with_bindings(
        vec![
            ContextPrimitive {
                source: ContextSource::AuthorizedIdentity,
                name: name("customer_id"),
            },
            ContextPrimitive {
                source: ContextSource::TransportHeader,
                name: name("workspace_id"),
            },
        ],
        &[ContextEntryDeclaration {
            scope: ContextScope::Engine,
            name: entry_name.clone(),
            definition: ContextEntryDefinition(vec![
                ContextEntryPart::AuthorizedIdentity {
                    name: name("customer_id").into(),
                    store_as: None,
                },
                ContextEntryPart::TransportHeader {
                    name: name("workspace_id").into(),
                    store_as: None,
                },
                ContextEntryPart::TransportHeaderMatch {
                    name: name("workspace_id").into(),
                    value: "workspace-abc".to_owned(),
                },
            ]),
        }],
        &[entry_name.clone().into()],
    )
    .expect("valid bound context layout");
    let binding = BoundContextEntry::new_compiled(Arc::clone(&layout), &entry_name.clone().into())
        .expect("bound composite entry");

    let mut headers = TransportHeaders::new();
    headers.push(TransportHeader::text(
        name("workspace_id"),
        b"workspace-abc".to_vec(),
    ));
    let identity_policy: AuthorizedIdentityPolicy = serde_json::from_value(serde_json::json!([
        {"claim": "sub", "store_as": "customer_id"}
    ]))
    .expect("valid identity policy");
    let identity = AuthorizedIdentity::new().with_subject("customer-123");
    let identities = AuthorizedIdentityEntries::capture_for_benchmark(&identity_policy, &identity);

    let claim_value = ClaimValue::one("customer-123");
    let captured_headers = [CapturedHeader {
        name: "workspace_id",
        original_name: None,
        kind: otel_arrow_dfe_config::transport_headers::ValueKind::Text,
        value: b"workspace-abc",
    }];
    let captured_claims = [CapturedClaim {
        name: "customer_id",
        value: &claim_value,
    }];
    measure("packed_context_construction", || {
        PackedContext::pack(
            black_box(&layout),
            black_box(&captured_headers),
            black_box(&captured_claims),
        )
        .expect("packed request context")
    });

    measure("header_only_request_context_construction", || {
        Context::from_request_context_for_benchmark(
            Arc::clone(&layout),
            headers.clone(),
            AuthorizedIdentityEntries::default(),
        )
    });
    measure("identity_only_request_context_construction", || {
        Context::from_request_context_for_benchmark(
            Arc::clone(&layout),
            TransportHeaders::default(),
            identities.clone(),
        )
    });
    measure("request_context_construction", || {
        Context::from_request_context_for_benchmark(
            Arc::clone(&layout),
            headers.clone(),
            identities.clone(),
        )
    });

    let context = Context::from_request_context_for_benchmark(
        Arc::clone(&layout),
        headers.clone(),
        identities,
    );
    let key = context.partition_selection(&binding).into_key();
    let mut partitions = HashMap::<ContextPartitionKey, usize>::with_capacity(4);
    _ = partitions.insert(key, 1);

    measure("existing_partition_lookup", || {
        let selection = black_box(&context).partition_selection(black_box(&binding));
        let hash = selection.table_hash(partitions.hasher());
        partitions
            .raw_entry()
            .from_hash(hash, |candidate| selection.matches(candidate))
            .map(|(_, pending)| *pending)
    });

    let other_identity = AuthorizedIdentity::new().with_subject("customer-456");
    let other_identities =
        AuthorizedIdentityEntries::capture_for_benchmark(&identity_policy, &other_identity);
    let other_context =
        Context::from_request_context_for_benchmark(Arc::clone(&layout), headers, other_identities);
    measure("missing_partition_lookup", || {
        let selection = black_box(&other_context).partition_selection(black_box(&binding));
        let hash = selection.table_hash(partitions.hasher());
        partitions
            .raw_entry()
            .from_hash(hash, |candidate| selection.matches(candidate))
            .is_none()
    });
    measure("new_partition_key_materialization", || {
        black_box(&other_context)
            .partition_selection(black_box(&binding))
            .into_key()
    });
    let other_key = other_context.partition_selection(&binding).into_key();
    measure("preallocated_partition_insert", || {
        partitions.insert(other_key, 2)
    });
}
