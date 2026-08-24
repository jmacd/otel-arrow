// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Partition key generation for Kafka messages.
//!
//! This module provides functions to generate deterministic partition keys for
//! Kafka messages based on transport headers. The transport headers are hashed
//! into a fixed-size, hex-encoded key that librdkafka's partitioner algorithm
//! (configured via [`PartitionerStrategy`]) then maps to a concrete partition
//! number.
//!
//! [`PartitionerStrategy`]: super::config::PartitionerStrategy

use otel_arrow_dfe_otap::context_bytes::PdataContextBytes;
use std::hash::{Hash, Hasher};
use xxhash_rust::xxh64::Xxh64;

/// Builds a deterministic partition key from the packed context bag.
#[must_use]
pub fn partition_key_from_context_bytes(context: &PdataContextBytes) -> Option<String> {
    let mut sorted: Vec<_> = context
        .items()
        .filter_map(|item| Some((item.stored_name()?, item.value()?.1)))
        .collect();
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_unstable();
    let mut hasher = Xxh64::new(0);
    for (name, value) in sorted {
        name.hash(&mut hasher);
        value.hash(&mut hasher);
    }
    Some(hex::encode(hasher.finish().to_be_bytes()))
}

/// Determine the partition key for a signal based on its per-signal config and
/// the pdata context.
#[must_use]
pub fn partition_key_for_signal(
    signal_config: &super::config::SignalConfig,
    context: &otel_arrow_dfe_otap::pdata::Context,
) -> Option<String> {
    if signal_config.partition_by_transport_headers() {
        if let Some(context_bytes) = context.pdata_context_bytes() {
            return partition_key_from_context_bytes(context_bytes);
        }
    }

    None
}

// TODO: Explore `partition_by_trace_id` -- partition traces by hex-encoded trace ID.
//   Trace IDs are 16-byte FixedSizeBinary values in the OTAP Arrow Spans schema.
//   A single OTAP batch can contain spans with different trace IDs, so implementing
//   this requires splitting the batch into sub-batches grouped by trace ID (returning
//   something like `Vec<(String, RoaringBitmap)>`) before sending each sub-batch to
//   Kafka with its own partition key. Empty/zero trace IDs should map to an empty
//   partition key (round-robin).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka::MessageFormat;
    use crate::exporters::kafka_exporter::config::SignalConfig;
    use otel_arrow_dfe_otap::context_bytes::PdataContextBytes;
    use otel_arrow_dfe_otap::pdata::Context;
    use otel_arrow_dfe_otap::testing::{TestContextHeader, test_pdata_context};

    /// Scenario: a packed context contains no header items.
    /// Guarantees: no Kafka partition key is generated for an empty context.
    #[test]
    fn empty_context_returns_none() {
        let context =
            PdataContextBytes::build(0, std::iter::empty()).expect("empty packed context");
        assert!(partition_key_from_context_bytes(&context).is_none());
    }

    /// Scenario: the same packed header set is hashed repeatedly.
    /// Guarantees: partition-key generation is deterministic.
    #[test]
    fn packed_context_key_is_deterministic() {
        let context = test_pdata_context([
            TestContextHeader::text("X-Tenant-Id", "x_tenant_id", b"tenant-123"),
            TestContextHeader::text("X-Region", "x_region", b"us-east-1"),
        ]);

        let key1 = partition_key_from_context_bytes(&context);
        let key2 = partition_key_from_context_bytes(&context);

        assert_eq!(key1, key2);
        assert!(key1.is_some());
    }

    /// Scenario: packed contexts differ by either stored name or value.
    /// Guarantees: each distinct logical header set produces a distinct key.
    #[test]
    fn distinct_packed_contexts_produce_distinct_keys() {
        let tenant_a = test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-a",
        )]);
        let tenant_b = test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-b",
        )]);
        let region =
            test_pdata_context([TestContextHeader::text("X-Region", "x_region", b"tenant-a")]);

        assert_ne!(
            partition_key_from_context_bytes(&tenant_a),
            partition_key_from_context_bytes(&tenant_b)
        );
        assert_ne!(
            partition_key_from_context_bytes(&tenant_a),
            partition_key_from_context_bytes(&region)
        );
    }

    /// Scenario: a packed context contains arbitrary binary header bytes.
    /// Guarantees: the resulting key is a fixed-size lowercase hexadecimal hash.
    #[test]
    fn binary_context_key_is_fixed_size_hex() {
        let context = test_pdata_context([TestContextHeader::binary(
            "X-Binary",
            "x_binary",
            &[0x01, 0x02, 0x03, 0xff],
        )]);

        let key = partition_key_from_context_bytes(&context).expect("partition key");

        assert_eq!(key.len(), 16);
        assert!(key.chars().all(|character| character.is_ascii_hexdigit()));
    }

    /// Scenario: equivalent packed header sets use opposite insertion orders.
    /// Guarantees: partition-key generation is independent of item order.
    #[test]
    fn packed_context_key_is_order_independent() {
        let tenant = TestContextHeader::text("X-Tenant-Id", "x_tenant_id", b"tenant-123");
        let region = TestContextHeader::text("X-Region", "x_region", b"us-east-1");
        let context_ab = test_pdata_context([tenant, region]);
        let context_ba = test_pdata_context([region, tenant]);

        assert_eq!(
            partition_key_from_context_bytes(&context_ab),
            partition_key_from_context_bytes(&context_ba)
        );
    }

    /// Scenario: a known packed header is hashed using the documented XXH64 pipeline.
    /// Guarantees: changes that would remap existing tenants to Kafka partitions are detected.
    #[test]
    fn packed_context_hash_matches_documented_pipeline() {
        let context = test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-123",
        )]);

        let key = partition_key_from_context_bytes(&context).expect("partition key");
        let mut hasher = Xxh64::new(0);
        "x_tenant_id".hash(&mut hasher);
        b"tenant-123".as_slice().hash(&mut hasher);
        let expected = hex::encode(hasher.finish().to_be_bytes());

        assert_eq!(key, expected);
    }

    /// Scenario: partitioning by transport headers is disabled.
    /// Guarantees: a context never produces a Kafka key unless explicitly configured.
    #[test]
    fn disabled_signal_partitioning_returns_none() {
        let config = SignalConfig::new("topic".into(), MessageFormat::OtlpProto);
        let mut context = Context::default();
        context.set_pdata_context_bytes(test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-123",
        )]));

        assert!(partition_key_for_signal(&config, &context).is_none());
    }

    /// Scenario: partitioning is enabled but no packed context is attached.
    /// Guarantees: Kafka receives a null key when there are no transport headers.
    #[test]
    fn enabled_signal_without_context_returns_none() {
        let config = SignalConfig::new("topic".into(), MessageFormat::OtlpProto)
            .with_partition_by_transport_headers(true);

        assert!(partition_key_for_signal(&config, &Context::default()).is_none());
    }

    /// Scenario: partitioning is enabled and a packed context is attached.
    /// Guarantees: signal routing uses the same key as direct packed-context hashing.
    #[test]
    fn enabled_signal_uses_packed_context() {
        let config = SignalConfig::new("topic".into(), MessageFormat::OtlpProto)
            .with_partition_by_transport_headers(true);
        let packed = test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-123",
        )]);
        let expected = partition_key_from_context_bytes(&packed);
        let mut context = Context::default();
        context.set_pdata_context_bytes(packed);

        assert_eq!(partition_key_for_signal(&config, &context), expected);
    }

    /// Scenario: sensitive header names and values are used for partitioning.
    /// Guarantees: the Kafka key contains only a fixed-size hash, never plaintext secrets.
    #[test]
    fn partition_key_never_contains_plaintext_header_data() {
        let secret_value = "Bearer-super-secret-token-abcdef0123456789";
        let context = test_pdata_context([TestContextHeader::text(
            "Authorization",
            "authorization",
            secret_value.as_bytes(),
        )]);

        let key = partition_key_from_context_bytes(&context).expect("partition key");

        assert!(!key.contains(secret_value));
        assert!(!key.contains("secret"));
        assert!(!key.contains("token"));
        assert!(!key.contains("authorization"));
        assert_eq!(key.len(), 16);
    }
}
