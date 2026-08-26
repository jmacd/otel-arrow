// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Partition key generation for Kafka messages.
//!
//! This module generates deterministic partition keys from configured context
//! registers. Register symbols are compiled to numeric IDs and their typed values are
//! hashed into a fixed-size key that librdkafka maps to a partition.
//!
//! [`PartitionerStrategy`]: super::config::PartitionerStrategy

use otel_arrow_dfe_otap::context_bytes::ContextRegisterSetBinding;
use otel_arrow_dfe_otap::pdata::Context;
use std::hash::Hasher;
use xxhash_rust::xxh64::Xxh64;

fn hash_bytes(hasher: &mut Xxh64, bytes: &[u8]) {
    hasher.write(&(bytes.len() as u64).to_be_bytes());
    hasher.write(bytes);
}

/// Compiled Kafka partition-key binding for an ordered set of context registers.
pub struct ContextPartitionKeyBinding {
    registers: ContextRegisterSetBinding,
}

impl ContextPartitionKeyBinding {
    /// Creates a binding from context-register symbols in configuration order.
    #[must_use]
    pub fn new<'a>(registers: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            registers: ContextRegisterSetBinding::new(registers),
        }
    }

    /// Builds a deterministic key from configured entries present in `context`.
    pub fn partition_key(&mut self, context: &Context) -> Option<String> {
        let context = context.pdata_context_bytes()?;
        let mut hasher = Xxh64::new(0);
        let visited = self.registers.visit_present(context, |ordinal, register| {
            hasher.write(&(ordinal as u64).to_be_bytes());
            for (kind, value) in register.values() {
                hasher.write(&[kind as u8]);
                hash_bytes(&mut hasher, value);
            }
        });
        (visited > 0).then(|| hex::encode(hasher.finish().to_be_bytes()))
    }
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
    use otel_arrow_dfe_otap::pdata::Context;
    use otel_arrow_dfe_otap::testing::{TestContextHeader, test_pdata_context};

    fn partition_key(
        entries: &[&str],
        context: otel_arrow_dfe_otap::context_bytes::PdataContextBytes,
    ) -> Option<String> {
        let mut pdata_context = Context::default();
        pdata_context.set_pdata_context_bytes(context);
        ContextPartitionKeyBinding::new(entries.iter().copied()).partition_key(&pdata_context)
    }

    /// Scenario: the pdata context is absent from the Context.
    /// Guarantees: no Kafka partition key is generated when there is no context.
    #[test]
    fn empty_context_returns_none() {
        let mut binding = ContextPartitionKeyBinding::new(["tenant"]);
        assert!(binding.partition_key(&Context::default()).is_none());
    }

    /// Scenario: the same packed header set is hashed repeatedly.
    /// Guarantees: partition-key generation is deterministic.
    #[test]
    fn packed_context_key_is_deterministic() {
        let context = test_pdata_context([
            TestContextHeader::text("X-Tenant-Id", "x_tenant_id", b"tenant-123"),
            TestContextHeader::text("X-Region", "x_region", b"us-east-1"),
        ]);

        let key1 = partition_key(&["x_tenant_id", "x_region"], context.clone());
        let key2 = partition_key(&["x_tenant_id", "x_region"], context);

        assert_eq!(key1, key2);
        assert!(key1.is_some());
    }

    /// Scenario: packed contexts differ by value or by compile-time register symbol.
    /// Guarantees: values affect keys while symbols are compiled out of runtime hashing.
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
            partition_key(&["x_tenant_id"], tenant_a.clone()),
            partition_key(&["x_tenant_id"], tenant_b)
        );
        assert_eq!(
            partition_key(&["x_tenant_id"], tenant_a),
            partition_key(&["x_region"], region)
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

        let key = partition_key(&["x_binary"], context).expect("partition key");

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
            partition_key(&["x_tenant_id", "x_region"], context_ab),
            partition_key(&["x_tenant_id", "x_region"], context_ba)
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

        let key = partition_key(&["x_tenant_id"], context).expect("partition key");
        assert_eq!(key, "32a1d962c6357ee7");
    }

    /// Scenario: a context-entry binding has no packed context to evaluate.
    /// Guarantees: Kafka receives a null key when none of the configured entries are present.
    #[test]
    fn enabled_signal_without_context_returns_none() {
        let mut binding = ContextPartitionKeyBinding::new(["x_tenant_id"]);
        assert!(binding.partition_key(&Context::default()).is_none());
    }

    /// Scenario: a configured context entry is present in a packed context.
    /// Guarantees: repeated bindings derive the same key from that logical entry.
    #[test]
    fn enabled_signal_uses_packed_context() {
        let packed = test_pdata_context([TestContextHeader::text(
            "X-Tenant-Id",
            "x_tenant_id",
            b"tenant-123",
        )]);
        let expected = partition_key(&["x_tenant_id"], packed.clone());
        let mut context = Context::default();
        context.set_pdata_context_bytes(packed);
        let mut binding = ContextPartitionKeyBinding::new(["x_tenant_id"]);

        assert_eq!(binding.partition_key(&context), expected);
    }

    /// Scenario: sensitive header names and values are used for partitioning.
    /// Guarantees: the Kafka key contains only a fixed-size hash, never plaintext secrets.
    #[test]
    fn partition_key_never_contains_plaintext_header_data() {
        let secret_value = "Bearer-super-secret-token-abcdef0123456789";
        let context = test_pdata_context([TestContextHeader::text(
            "Authorization",
            "auth_partition",
            secret_value.as_bytes(),
        )]);

        let key = partition_key(&["auth_partition"], context).expect("partition key");

        assert!(!key.contains(secret_value));
        assert!(!key.contains("secret"));
        assert!(!key.contains("token"));
        assert!(!key.contains("authorization"));
        assert_eq!(key.len(), 16);
    }
}
