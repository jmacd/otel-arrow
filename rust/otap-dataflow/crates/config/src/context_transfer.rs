// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flat, authority-preserving context transfer contracts shared by topic and storage boundaries.

use crate::context::{ContextEntryName, ContextSelector};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Bound on flat fields plus values in one transfer.
pub const MAX_TRANSFER_ENTRIES: usize = 1024;
/// Bound on serialized context bytes.
pub const MAX_TRANSFER_BYTES: usize = 1024 * 1024;

/// Authority of a flat transferred value. Only transport headers accept downgrades.
#[derive(
    Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(rename_all = "snake_case")]
pub enum TransferKind {
    /// Captured, untrusted transport metadata.
    TransportHeader,
    /// Verified authorization claims.
    AuthorizedIdentity,
    /// Configured constant.
    Constant,
    /// Generated value retained from its original materialization.
    Randomness,
    /// Receiver-observed network information.
    NetworkInfo,
}

/// One field in a flat transfer contract.
#[derive(
    Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(deny_unknown_fields)]
pub struct TransferField {
    /// Required source authority, or transport_header to permit downgrade.
    #[serde(rename = "type")]
    pub kind: TransferKind,
}

/// Shared topic/storage transfer contract. An absent contract uses declaration-aware projection.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq, PartialOrd, Ord, Hash,
)]
#[serde(deny_unknown_fields)]
pub struct ContextTransferSchema {
    /// Flat names in canonical order; composites cannot be transfer fields.
    pub transfer: BTreeMap<ContextEntryName, TransferField>,
}

impl ContextTransferSchema {
    /// Checks fixed transfer bounds before allocating runtime bindings.
    pub fn validate(&self) -> Result<(), String> {
        if self.transfer.len() > MAX_TRANSFER_ENTRIES
            || self.transfer.keys().map(|name| name.len()).sum::<usize>() > MAX_TRANSFER_BYTES
        {
            return Err("context transfer schema exceeds entry or byte limits".into());
        }
        Ok(())
    }
}

/// Source candidates for one transfer field, tried in configured order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ContextExportField {
    /// Reject a message when this exporter cannot supply the field.
    #[serde(default)]
    pub required: bool,
    /// Selects primitive values or qualified composite members, never whole composites.
    pub selector: ContextSelector,
}

/// Export-side mapping. Omission uses same-name primitive sources from the transfer contract.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ContextExportConfig {
    /// Explicit source mappings. An empty map exports no fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub export: Option<BTreeMap<ContextEntryName, ContextExportField>>,
}

/// Import destination for a flat field.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ContextImportField {
    /// Destination name; omission retains the transfer name and authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_as: Option<ContextEntryName>,
}

/// Import-side mapping. Omission imports all contract fields under their transfer names.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(deny_unknown_fields)]
pub struct ContextImportConfig {
    /// Conjunctive exact text matches over transfer keys, used for topic routing.
    #[serde(default)]
    pub conditions: BTreeMap<ContextEntryName, String>,
    /// Register the matching transfer as a receiver-local composite with optional members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_composite: Option<ContextEntryName>,
    /// Explicit selection; null entries retain their names. An empty map imports nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import: Option<BTreeMap<ContextEntryName, Option<ContextImportField>>>,
}
