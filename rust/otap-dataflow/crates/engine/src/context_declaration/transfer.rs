// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Startup-bound context transfer and a bounded, versioned storage representation.
//!
//! Topic and persistence adapters share these contracts. No routing frames or
//! declaration-local IDs are serialized.

use super::*;
use otel_arrow_dfe_config::context::{ContextSelector, ContextSelectorType};
use otel_arrow_dfe_config::context_transfer::{
    ContextExportConfig, ContextImportConfig, ContextTransferSchema, TransferKind,
};
use serde::{Deserialize, Serialize};

pub use otel_arrow_dfe_config::context_transfer::{MAX_TRANSFER_BYTES, MAX_TRANSFER_ENTRIES};

/// Names and definitions visible to one pipeline, resolved during compilation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextVisibility {
    /// Case-normalized captured/declared header names.
    pub headers: BTreeSet<ContextEntryName>,
    /// Case-sensitive verified identity names.
    pub identities: BTreeSet<ContextEntryName>,
    /// Visible composite definitions, including their scope.
    pub composites: BTreeMap<ContextEntryName, ConfigContextEntryDeclaration>,
}

impl ContextVisibility {
    pub(super) fn compile(nodes: &HashMap<ConfigNodeId, PreparedNodeContextDeclarations>) -> Self {
        let mut result = Self::default();
        let mut field = |domain, name: &ContextEntryName| match domain {
            ContextDomain::TransportHeader => {
                _ = result.headers.insert(
                    original_name_key(name)
                        .as_ref()
                        .try_into()
                        .expect("valid name"),
                );
            }
            ContextDomain::AuthorizedIdentity => {
                _ = result.identities.insert(name.clone());
            }
        };
        for node in nodes.values() {
            for composite in &node.composites {
                _ = result
                    .composites
                    .insert(composite.name.clone(), composite.clone());
            }
            for declaration in node.declarations.iter() {
                match declaration {
                    ContextDeclaration::Produces { domain, entry } => field(*domain, entry),
                    ContextDeclaration::Consumes {
                        selector: ContextConsumerSelector::Entries { entries },
                    } => {
                        for entry in entries {
                            if let ContextEntryTarget::Primitive { domain, name } = &entry.target {
                                field(*domain, name);
                            }
                        }
                    }
                    ContextDeclaration::HeaderCapture { policy } => {
                        for name in policy.stored_names() {
                            field(ContextDomain::TransportHeader, &name);
                        }
                    }
                    ContextDeclaration::AuthorizedIdentityCapture { policy } => {
                        for claim in policy.iter() {
                            field(ContextDomain::AuthorizedIdentity, &claim.store_as);
                        }
                    }
                    _ => {}
                }
            }
        }
        result
    }

    /// Whether a captured header belongs to this pipeline's declarations.
    #[must_use]
    pub fn contains_header(&self, name: &str) -> bool {
        self.headers
            .iter()
            .any(|allowed| allowed.as_str().eq_ignore_ascii_case(name))
    }
}

/// One startup-resolved export source.
#[derive(Clone, Debug)]
pub enum TransferSource {
    /// One primitive with an explicit authority.
    Primitive {
        /// Authority.
        kind: TransferKind,
        /// Stored source name.
        name: ContextEntryName,
    },
    /// Existing composite snapshot member, including its presence/provenance gate.
    Member {
        /// Shared source pipeline materialization plan.
        plan: ContextMaterializationPlan,
        /// Pre-resolved projection, never a runtime name lookup.
        projection: ContextProjection,
    },
    /// Header collection retaining individual stored and wire names.
    AllCaptured,
}

/// Export binding for a topic or storage boundary.
#[derive(Clone, Debug)]
pub enum ContextExportBinding {
    /// Safe projection of declared metadata; compatible backing stores can be shared.
    Visible(Arc<ContextVisibility>),
    /// Flat contract and ordered candidates for each field.
    Flat {
        /// Canonical field ordering used by both sides.
        schema: Arc<ContextTransferSchema>,
        /// Candidate sources, indexed by schema field.
        sources: Vec<Vec<TransferSource>>,
        /// Export-side presence requirements, aligned with the source slots.
        required: Vec<bool>,
    },
}

/// Import binding for a topic or storage boundary.
#[derive(Clone, Debug)]
pub enum ContextImportBinding {
    /// Intersect incoming metadata with destination visibility and provenance.
    Visible(Arc<ContextVisibility>),
    /// Reconstruct selected flat fields without changing authority.
    Flat {
        /// Expected contract, independent of process-local IDs.
        schema: Arc<ContextTransferSchema>,
        /// Source slot, authority, and destination name, resolved once at construction.
        destinations: Vec<(usize, TransferKind, ContextEntryName)>,
        /// Receiver-local composite retaining available fields after its conditions match.
        composite: Option<ContextMaterializationPlan>,
        /// Startup-bound routing predicate over flat slots.
        conditions: Arc<CompiledTransferConditions>,
    },
}

/// Immutable predicate usable by the topic broker before queue admission.
#[derive(Clone, Debug)]
pub struct CompiledTransferConditions {
    schema: Arc<ContextTransferSchema>,
    matches: Vec<(usize, Box<[u8]>)>,
    identity: Vec<u8>,
}

impl CompiledTransferConditions {
    /// Resolves condition keys to flat slots and validates the transfer contract.
    pub fn compile(
        schema: &ContextTransferSchema,
        conditions: &BTreeMap<ContextEntryName, String>,
    ) -> Result<Self, Error> {
        schema.validate().map_err(invalid_context)?;
        if schema.transfer.len() > MAX_TRANSFER_ENTRIES
            || conditions.values().map(String::len).sum::<usize>() > MAX_TRANSFER_BYTES
        {
            return Err(invalid_context("context transfer predicate exceeds limits"));
        }
        let matches = conditions
            .iter()
            .map(|(name, value)| {
                let index = schema
                    .transfer
                    .keys()
                    .position(|candidate| candidate == name)
                    .ok_or_else(|| {
                        invalid_context(format!(
                            "condition references unknown transfer field `{name}`"
                        ))
                    })?;
                Ok((index, value.as_bytes().into()))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self {
            schema: Arc::new(schema.clone()),
            matches,
            identity: serde_json::to_vec(&(schema, conditions))
                .map_err(|_| invalid_context("cannot encode transfer predicate identity"))?,
        })
    }

    /// Whether this receiver has no context conditions.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }

    /// Stable identity used to reject inconsistent predicates within one balanced group.
    #[must_use]
    pub fn identity(&self) -> Vec<u8> {
        self.identity.clone()
    }

    /// Matches only flat snapshots from the expected topic contract.
    #[must_use]
    pub fn accepts(&self, transfer: &TransferredContext) -> bool {
        transfer.schema() == self.schema.as_ref()
            && self
                .matches
                .iter()
                .all(|(index, expected)| match &transfer.fields()[*index] {
                    Some(TransferValue::Values(values)) => values.values().iter().any(|v| {
                        v.kind() == ContextValueKind::Text && v.bytes() == expected.as_ref()
                    }),
                    _ => false,
                })
    }
}

impl ContextImportBinding {
    /// Returns explicit subscription conditions; default visibility has no predicate.
    #[must_use]
    pub fn conditions(&self) -> Option<Arc<CompiledTransferConditions>> {
        match self {
            Self::Flat { conditions, .. } if !conditions.is_empty() => Some(Arc::clone(conditions)),
            _ => None,
        }
    }
}

/// One captured header in a transferable all-captured collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferHeader {
    /// Stored name.
    pub name: ContextEntryName,
    /// Kind, bytes, and original wire name.
    pub value: MaterializedContextValue,
}

/// A flat value retains cardinality and authority separately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "representation", content = "data", rename_all = "snake_case")]
pub enum TransferValue {
    /// Scalar or repeated primitive values, including present empty collections.
    Values(MaterializedContextValues),
    /// All captured headers, retaining independent names.
    Headers(Vec<TransferHeader>),
}

/// Versioned, flat context suitable for a Quiver sidecar; it contains no composite entries.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransferredContext {
    version: u8,
    schema: ContextTransferSchema,
    fields: Vec<Option<TransferValue>>,
}

impl<'de> Deserialize<'de> for TransferredContext {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u8,
            schema: ContextTransferSchema,
            fields: Vec<Option<TransferValue>>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let context = Self {
            version: wire.version,
            schema: wire.schema,
            fields: wire.fields,
        };
        context.validate().map_err(serde::de::Error::custom)?;
        Ok(context)
    }
}

impl std::fmt::Debug for TransferredContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferredContext")
            .field("field_count", &self.fields.len())
            .finish()
    }
}

impl TransferredContext {
    /// Reads one typed imported primitive without degrading its authority.
    #[must_use]
    pub fn value(
        &self,
        kind: TransferKind,
        name: &ContextEntryName,
    ) -> Option<&MaterializedContextValues> {
        self.schema
            .transfer
            .iter()
            .zip(&self.fields)
            .find_map(|((field_name, field), value)| {
                if field_name == name && field.kind == kind {
                    match value {
                        Some(TransferValue::Values(values)) => Some(values),
                        _ => None,
                    }
                } else {
                    None
                }
            })
    }
    /// Builds and validates a flat snapshot.
    pub fn new(
        schema: ContextTransferSchema,
        fields: Vec<Option<TransferValue>>,
    ) -> Result<Self, String> {
        let result = Self {
            version: 1,
            schema,
            fields,
        };
        result.validate()?;
        Ok(result)
    }

    /// Returns the flat contract.
    #[must_use]
    pub fn schema(&self) -> &ContextTransferSchema {
        &self.schema
    }

    /// Returns slots in canonical schema order.
    #[must_use]
    pub fn fields(&self) -> &[Option<TransferValue>] {
        &self.fields
    }

    /// Serializes through a bounded writer, without an unbounded intermediate buffer.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        struct Bounded(Vec<u8>);
        impl std::io::Write for Bounded {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                if data.len() > MAX_TRANSFER_BYTES.saturating_sub(self.0.len()) {
                    return Err(std::io::Error::other(
                        "context transfer byte limit exceeded",
                    ));
                }
                self.0.extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut output = Bounded(Vec::new());
        serde_json::to_writer(&mut output, self)
            .map_err(|_| "context transfer encoding failed or byte limit exceeded".to_owned())?;
        Ok(output.0)
    }

    /// Decodes untrusted storage bytes and checks version, shape, types, and limits.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > MAX_TRANSFER_BYTES {
            return Err("context transfer byte limit exceeded".into());
        }
        let result: Self = serde_json::from_slice(bytes)
            .map_err(|_| "invalid context transfer encoding".to_owned())?;
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || self.fields.len() != self.schema.transfer.len()
            || self.fields.len() > MAX_TRANSFER_ENTRIES
        {
            return Err("unsupported context transfer version or shape".into());
        }
        let mut count = self.fields.len();
        let mut bytes = 0usize;
        for ((name, field), value) in self.schema.transfer.iter().zip(&self.fields) {
            bytes = bytes.saturating_add(name.len());
            let values: Vec<&MaterializedContextValue> = match value {
                None => Vec::new(),
                Some(TransferValue::Values(values)) => {
                    if values.cardinality() == ContextValueCardinality::One
                        && values.values().len() != 1
                    {
                        return Err("invalid scalar transfer cardinality".into());
                    }
                    values.values().iter().collect()
                }
                Some(TransferValue::Headers(headers)) => {
                    if field.kind != TransferKind::TransportHeader {
                        return Err("header collections cannot carry trusted authority".into());
                    }
                    for header in headers {
                        bytes = bytes.saturating_add(header.name.len());
                    }
                    headers.iter().map(|h| &h.value).collect()
                }
            };
            count = count.saturating_add(values.len());
            for value in values {
                bytes = bytes
                    .saturating_add(value.bytes().len())
                    .saturating_add(value.original_name().map_or(0, str::len));
                if value.kind() == ContextValueKind::Text && value.as_str().is_none() {
                    return Err("invalid UTF-8 context transfer value".into());
                }
                if field.kind != TransferKind::TransportHeader
                    && (value.kind() != ContextValueKind::Text || value.original_name().is_some())
                {
                    return Err("trusted transfer values must be text without wire names".into());
                }
            }
        }
        if count > MAX_TRANSFER_ENTRIES || bytes > MAX_TRANSFER_BYTES {
            return Err("context transfer entry or byte limit exceeded".into());
        }
        Ok(())
    }
}

impl CompiledContextBindings {
    /// Binds a declared export against a topic/storage contract before processing any data.
    pub fn bind_context_export(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
        config: &ContextExportConfig,
        schema: Option<&ContextTransferSchema>,
    ) -> Result<ContextExportBinding, Error> {
        if self.by_pipeline.is_empty() && schema.is_none() && config.export.is_none() {
            return Ok(ContextExportBinding::Visible(Arc::default()));
        }
        let expected = ContextDeclaration::TransferExport {
            config: config.clone(),
        };
        self.validate_transfer_declaration(pipeline, node, &expected)?;
        let Some(schema) = schema else {
            if config.export.is_some() {
                return Err(invalid_context(
                    "context.export requires a transfer contract",
                ));
            }
            return Ok(ContextExportBinding::Visible(self.visibility(pipeline)));
        };
        schema.validate().map_err(invalid_context)?;
        if let Some(mapping) = &config.export {
            for name in mapping.keys() {
                if !schema.transfer.contains_key(name) {
                    return Err(invalid_context(format!("unknown transfer field `{name}`")));
                }
            }
        }
        let plan = self
            .context_materialization_plan(pipeline)
            .cloned()
            .unwrap_or_default();
        let sources = schema
            .transfer
            .iter()
            .map(|(name, field)| {
                let selected = match &config.export {
                    None => vec![TransferSource::Primitive {
                        kind: field.kind,
                        name: name.clone(),
                    }],
                    Some(mapping) => match mapping.get(name) {
                        None => Vec::new(),
                        Some(export) => compile_sources(&export.selector, field.kind, &plan)?,
                    },
                };
                if config
                    .export
                    .as_ref()
                    .and_then(|mapping| mapping.get(name))
                    .is_some_and(|field| field.required)
                    && selected.is_empty()
                {
                    return Err(invalid_context(format!(
                        "required transfer field `{name}` has no export mapping"
                    )));
                }
                Ok(selected)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(ContextExportBinding::Flat {
            schema: Arc::new(schema.clone()),
            sources,
            required: schema
                .transfer
                .keys()
                .map(|name| {
                    config
                        .export
                        .as_ref()
                        .and_then(|mapping| mapping.get(name))
                        .is_some_and(|field| field.required)
                })
                .collect(),
        })
    }

    /// Binds import destinations by slot; runtime never resolves schema names.
    pub fn bind_context_import(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
        config: &ContextImportConfig,
        schema: Option<&ContextTransferSchema>,
    ) -> Result<ContextImportBinding, Error> {
        if self.by_pipeline.is_empty()
            && schema.is_none()
            && config == &ContextImportConfig::default()
        {
            return Ok(ContextImportBinding::Visible(Arc::default()));
        }
        let expected = ContextDeclaration::TransferImport {
            config: config.clone(),
            topic: None,
        };
        self.validate_transfer_declaration(pipeline, node, &expected)?;
        let Some(schema) = schema else {
            if config.import.is_some()
                || config.store_composite.is_some()
                || !config.conditions.is_empty()
            {
                return Err(invalid_context(
                    "context.import/store_composite requires a transfer contract",
                ));
            }
            return Ok(ContextImportBinding::Visible(self.visibility(pipeline)));
        };
        let mut destinations = Vec::new();
        let mut seen = BTreeSet::new();
        let mappings = config
            .import
            .clone()
            .unwrap_or_else(|| schema.transfer.keys().map(|n| (n.clone(), None)).collect());
        for (name, mapping) in mappings {
            let (index, (_, field)) = schema
                .transfer
                .iter()
                .enumerate()
                .find(|(_, (n, _))| **n == name)
                .ok_or_else(|| invalid_context(format!("unknown transfer field `{name}`")))?;
            let target = mapping.and_then(|m| m.store_as).unwrap_or(name);
            let key = if field.kind == TransferKind::TransportHeader {
                target.as_str().to_ascii_lowercase()
            } else {
                target.to_string()
            };
            let namespace = match field.kind {
                TransferKind::Constant | TransferKind::Randomness | TransferKind::NetworkInfo => {
                    TransferKind::Constant
                }
                kind => kind,
            };
            if !seen.insert((namespace, key)) {
                return Err(invalid_context("duplicate context import destination"));
            }
            destinations.push((index, field.kind, target));
        }
        let composite = config
            .store_composite
            .as_ref()
            .map(|name| {
                let visibility = self.visibility(pipeline);
                let definition = visibility.composites.get(name).ok_or_else(|| {
                    invalid_context("implicit transfer composite was not declared")
                })?;
                let expected =
                    implicit_composite(config, schema, name.clone(), definition.scope.clone())?;
                let mut actual = definition.clone();
                actual.definition.0.sort_unstable();
                let mut expected = expected;
                expected.definition.0.sort_unstable();
                if actual != expected {
                    return Err(invalid_context(
                        "implicit transfer composite contract changed after compilation",
                    ));
                }
                ContextMaterializationPlan::compile(&[expected])
            })
            .transpose()?;
        Ok(ContextImportBinding::Flat {
            schema: Arc::new(schema.clone()),
            destinations,
            composite,
            conditions: Arc::new(CompiledTransferConditions::compile(
                schema,
                &config.conditions,
            )?),
        })
    }

    fn visibility(&self, pipeline: &PipelineKey) -> Arc<ContextVisibility> {
        self.visibility_by_pipeline
            .get(pipeline)
            .cloned()
            .unwrap_or_default()
    }

    fn validate_transfer_declaration(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
        expected: &ContextDeclaration,
    ) -> Result<(), Error> {
        if self.by_pipeline.get(pipeline).and_then(|nodes| nodes.get(node))
            .is_some_and(|bindings| bindings.component_declarations.iter().any(|d| d == expected || matches!((d, expected),
                (ContextDeclaration::TransferImport { config: actual, .. }, ContextDeclaration::TransferImport { config: expected, .. }) if actual == expected))) {
            Ok(())
        } else { Err(Error::UnrecognizedContextDeclaration {}) }
    }
}

fn compile_sources(
    selector: &ContextSelector,
    target_kind: TransferKind,
    plan: &ContextMaterializationPlan,
) -> Result<Vec<TransferSource>, Error> {
    selector.validate().map_err(invalid_context)?;
    if selector.selector_type == ContextSelectorType::AllCaptured {
        if target_kind != TransferKind::TransportHeader {
            return Err(invalid_context("all_captured cannot upgrade authority"));
        }
        return Ok(vec![TransferSource::AllCaptured]);
    }
    if selector.selector_type == ContextSelectorType::None {
        return Ok(Vec::new());
    }
    if selector.selector_type == ContextSelectorType::Composite {
        return Err(invalid_context(
            "composites are not transferable; select individual members",
        ));
    }
    selector
        .named
        .as_ref()
        .expect("validated names")
        .iter()
        .map(|reference| {
            let (source, kind) = if let Some(composite) = reference.scope() {
                let projection = plan.layout().resolve_member(composite, reference.name())?;
                let member = &projection.members().expect("member projection")[0];
                let kind = match &member.source {
                    ContextMemberSource::Field(id) => {
                        match plan.layout().fields()[id.index()].domain {
                            ContextDomain::TransportHeader => TransferKind::TransportHeader,
                            ContextDomain::AuthorizedIdentity => TransferKind::AuthorizedIdentity,
                        }
                    }
                    ContextMemberSource::Constant(_) => TransferKind::Constant,
                    ContextMemberSource::Randomness(_) => TransferKind::Randomness,
                    ContextMemberSource::Transferred { kind, .. } => *kind,
                };
                (
                    TransferSource::Member {
                        plan: plan.clone(),
                        projection,
                    },
                    kind,
                )
            } else {
                let kind = match selector.selector_type {
                    ContextSelectorType::Named => TransferKind::TransportHeader,
                    ContextSelectorType::AuthorizedIdentity => TransferKind::AuthorizedIdentity,
                    ContextSelectorType::Constant => TransferKind::Constant,
                    ContextSelectorType::Randomness => TransferKind::Randomness,
                    ContextSelectorType::NetworkInfo => TransferKind::NetworkInfo,
                    _ => unreachable!("handled non-primitive selector"),
                };
                (
                    TransferSource::Primitive {
                        kind,
                        name: reference.name().clone(),
                    },
                    kind,
                )
            };
            if kind != target_kind && target_kind != TransferKind::TransportHeader {
                return Err(invalid_context(
                    "context transfer cannot upgrade or change trusted authority",
                ));
            }
            Ok(source)
        })
        .collect()
}

pub(super) fn implicit_composite(
    config: &ContextImportConfig,
    schema: &ContextTransferSchema,
    name: ContextEntryName,
    scope: otel_arrow_dfe_config::context_policy::ContextScope,
) -> Result<ConfigContextEntryDeclaration, Error> {
    let _ = CompiledTransferConditions::compile(schema, &config.conditions)?;
    let mappings = config.import.clone().unwrap_or_else(|| {
        schema
            .transfer
            .keys()
            .map(|name| (name.clone(), None))
            .collect()
    });
    let conditions = Arc::new(config.conditions.clone());
    let parts = mappings
        .into_iter()
        .map(|(source, mapping)| {
            let field = schema
                .transfer
                .get(&source)
                .ok_or_else(|| invalid_context(format!("unknown transfer field `{source}`")))?;
            let store_as = mapping
                .and_then(|m| m.store_as)
                .unwrap_or_else(|| source.clone());
            Ok(ContextEntryPart::Transferred {
                name: source,
                kind: field.kind,
                store_as,
                conditions: Arc::clone(&conditions),
            })
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let result = ConfigContextEntryDeclaration {
        scope,
        name,
        definition: otel_arrow_dfe_config::context_policy::ContextEntryDefinition(parts),
    };
    validate_definition(&result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn field(name: &str, kind: TransferKind) -> ContextTransferSchema {
        ContextTransferSchema {
            transfer: BTreeMap::from([(
                name.try_into().unwrap(),
                otel_arrow_dfe_config::context_transfer::TransferField { kind },
            )]),
        }
    }

    /// Scenario: stored context contains missing fields, binary headers, and empty repeated identity.
    /// Guarantees: versioned serialization preserves presence, cardinality, kind, bytes, and wire names.
    #[test]
    fn transfer_storage_round_trip_and_validation() {
        let values = MaterializedContextValues::new(ContextValueCardinality::Many, vec![]).unwrap();
        let original = TransferredContext::new(
            field("roles", TransferKind::AuthorizedIdentity),
            vec![Some(TransferValue::Values(values))],
        )
        .unwrap();
        assert_eq!(
            TransferredContext::decode(&original.encode().unwrap()).unwrap(),
            original
        );
        let binary = MaterializedContextValues::new(
            ContextValueCardinality::One,
            vec![MaterializedContextValue::new(
                Some("X-Binary".into()),
                ContextValueKind::Binary,
                vec![0, 255],
            )],
        )
        .unwrap();
        let original = TransferredContext::new(
            field("binary", TransferKind::TransportHeader),
            vec![Some(TransferValue::Values(binary))],
        )
        .unwrap();
        assert_eq!(
            TransferredContext::decode(&original.encode().unwrap()).unwrap(),
            original
        );
        let absent =
            TransferredContext::new(field("optional", TransferKind::TransportHeader), vec![None])
                .unwrap();
        assert_eq!(
            TransferredContext::decode(&absent.encode().unwrap()).unwrap(),
            absent
        );
        for changed in [
            json!({"version": 2, "schema": {"transfer": {}}, "fields": []}),
            json!({"version": 1, "schema": {"transfer": {}}, "fields": [null]}),
            json!({"version": 1, "schema": {"transfer": {"id": {"type": "authorized_identity"}}}, "fields": [
                {"representation": "values", "data": {"cardinality": "One", "values": []}}
            ]}),
            json!({"version": 1, "schema": {"transfer": {"id": {"type": "authorized_identity"}}}, "fields": [
                {"representation": "values", "data": {"cardinality": "One", "values": [{"original_name": null, "kind": "Text", "bytes": [255]}]}}
            ]}),
        ] {
            assert!(TransferredContext::decode(&serde_json::to_vec(&changed).unwrap()).is_err());
        }
        assert!(TransferredContext::decode(&vec![b' '; MAX_TRANSFER_BYTES + 1]).is_err());
        let large = MaterializedContextValues::new(
            ContextValueCardinality::One,
            vec![MaterializedContextValue::new(
                None,
                ContextValueKind::Binary,
                vec![255; MAX_TRANSFER_BYTES / 2],
            )],
        )
        .unwrap();
        let transfer = TransferredContext::new(
            field("large", TransferKind::TransportHeader),
            vec![Some(TransferValue::Values(large))],
        )
        .unwrap();
        assert!(
            transfer.encode().is_err(),
            "serialized size, not raw byte size, is bounded"
        );
        let oversized = MaterializedContextValues::new(
            ContextValueCardinality::Many,
            (0..MAX_TRANSFER_ENTRIES)
                .map(|_| MaterializedContextValue::new(None, ContextValueKind::Text, Vec::new()))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert!(
            TransferredContext::new(
                field("large", TransferKind::AuthorizedIdentity),
                vec![Some(TransferValue::Values(oversized))]
            )
            .is_err()
        );
    }

    /// Scenario: a transfer selects primitive authority or a member of an atomically gated composite.
    /// Guarantees: binding resolves members once and permits only exact authority or transport downgrade.
    #[test]
    fn transfer_binding_rejects_authority_upgrade_and_whole_composites() {
        let declaration = ConfigContextEntryDeclaration {
            scope: otel_arrow_dfe_config::context_policy::ContextScope::Engine,
            name: "request".try_into().unwrap(),
            definition: serde_json::from_value(json!([
                {"type":"randomness","name":"id","value":"uuid7"},
                {"type":"transport_header","name":"tenant"}
            ]))
            .unwrap(),
        };
        let plan = ContextMaterializationPlan::compile(&[declaration]).unwrap();
        for (selection, target, accepted) in [
            (
                json!({"type":"named","named":["tenant"]}),
                TransferKind::AuthorizedIdentity,
                false,
            ),
            (
                json!({"type":"named","named":["request:id"]}),
                TransferKind::Randomness,
                true,
            ),
            (
                json!({"type":"named","named":["request:id"]}),
                TransferKind::TransportHeader,
                true,
            ),
            (
                json!({"type":"named","named":["request:id"]}),
                TransferKind::AuthorizedIdentity,
                false,
            ),
            (
                json!({"type":"composite","named":["request"]}),
                TransferKind::TransportHeader,
                false,
            ),
            (
                json!({"type":"all_captured"}),
                TransferKind::AuthorizedIdentity,
                false,
            ),
            (
                json!({"type":"all_captured"}),
                TransferKind::TransportHeader,
                true,
            ),
        ] {
            let selector = serde_json::from_value(selection).unwrap();
            assert_eq!(compile_sources(&selector, target, &plan).is_ok(), accepted);
        }
    }
}
