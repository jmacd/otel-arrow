// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context transfer delegates metadata handling to the engine and resets delivery state.

use super::{Context, OtapPdata};
use otel_arrow_dfe_engine::pdata_context::{
    CompiledTransferConditions, ContextExportBinding, ContextImportBinding, Metadata,
};

impl OtapPdata {
    /// Reads request metadata for a compiled context-transfer boundary.
    #[must_use]
    pub const fn context(&self) -> &Context {
        &self.context
    }
}

impl Context {
    /// Evaluates startup-bound routing conditions before topic admission/delivery.
    #[must_use]
    pub fn matches_transfer(&self, conditions: &CompiledTransferConditions) -> bool {
        self.metadata().matches_transfer(conditions)
    }

    /// Exports selected metadata with fresh routing and flow metric state.
    pub fn export_transfer(&self, binding: &ContextExportBinding) -> Result<Self, String> {
        self.metadata()
            .export_transfer(binding)
            .map(|metadata| Self::default().with_metadata(metadata))
    }

    /// Imports selected metadata with fresh routing and flow metric state.
    pub fn import_transfer(&self, binding: &ContextImportBinding) -> Result<Self, String> {
        self.metadata()
            .import_transfer(binding)
            .map(|metadata| Self::default().with_metadata(metadata))
    }

    /// Encodes a previously exported flat snapshot for storage.
    pub fn encode_transfer(&self) -> Result<Vec<u8>, String> {
        self.metadata().encode_transfer()
    }

    /// Decodes a flat snapshot without restoring routing or flow metric state.
    pub fn decode_transfer(bytes: &[u8]) -> Result<Self, String> {
        Metadata::decode_transfer(bytes).map(|metadata| Self::default().with_metadata(metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::SignalType;
    use otel_arrow_dfe_config::context::transfer::ContextTransferSchema;
    use otel_arrow_dfe_engine::Interests;
    use otel_arrow_dfe_engine::pdata_context::ContextVisibility;
    use std::sync::Arc;

    /// Scenario: contexts with delivery state cross visible and serialized flat transfer boundaries.
    /// Guarantees: exports, imports, and decoding create fresh delivery state without mutating the source.
    #[test]
    fn context_transfer_resets_delivery_state() {
        let mut source = Context::default();
        source.update_send_context(1, Interests::ACKS | Interests::NACKS);
        source.flow_compute_ns = std::num::NonZeroU64::new(123);
        source.capture_signal(SignalType::Logs);
        assert!(source.has_ack_or_nack_subscribers());

        let visibility = Arc::new(ContextVisibility::default());
        let exported = source
            .export_transfer(&ContextExportBinding::Visible(visibility.clone()))
            .expect("visible export");
        let imported = source
            .import_transfer(&ContextImportBinding::Visible(visibility))
            .expect("visible import");
        let schema = Arc::new(ContextTransferSchema::default());
        let flat = source
            .export_transfer(&ContextExportBinding::Flat {
                schema: schema.clone(),
                sources: vec![],
                required: vec![],
            })
            .expect("flat export");
        let decoded =
            Context::decode_transfer(&flat.encode_transfer().expect("encode")).expect("decode");
        let flat_imported = decoded
            .import_transfer(&ContextImportBinding::Flat {
                schema: schema.clone(),
                destinations: vec![],
                composite: None,
                conditions: Arc::new(
                    CompiledTransferConditions::compile(&schema, &Default::default())
                        .expect("conditions"),
                ),
            })
            .expect("flat import");
        for transferred in [exported, imported, flat, decoded, flat_imported] {
            assert!(!transferred.has_ack_or_nack_subscribers());
            assert_eq!(transferred.source_node(), None);
            assert_eq!(transferred.flow_compute_ns, None);
            assert_eq!(transferred.signal(), None);
        }
        assert!(source.has_ack_or_nack_subscribers());
        assert_eq!(source.flow_compute_ns, std::num::NonZeroU64::new(123));
        assert_eq!(source.signal(), Some(SignalType::Logs));
    }
}
