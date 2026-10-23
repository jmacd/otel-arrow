// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Context-only adapters for topics and persistent queues. Routing stays local.

use super::*;
use otel_arrow_dfe_config::context_transfer::{ContextTransferSchema, TransferField, TransferKind};
use otel_arrow_dfe_config::transport_headers::{TransportHeader, ValueKind};
use otel_arrow_dfe_engine::context_declaration::{
    ContextExportBinding, ContextImportBinding, ContextVisibility, MAX_TRANSFER_BYTES,
    MAX_TRANSFER_ENTRIES, TransferHeader, TransferSource, TransferValue, TransferredContext,
};

#[derive(Default)]
struct TransferBudget {
    entries: usize,
    bytes: usize,
}

impl TransferBudget {
    fn add(&mut self, entries: usize, bytes: usize) -> Result<(), String> {
        self.entries = self.entries.saturating_add(entries);
        self.bytes = self.bytes.saturating_add(bytes);
        if self.entries > MAX_TRANSFER_ENTRIES || self.bytes > MAX_TRANSFER_BYTES {
            return Err("context transfer entry or byte limit exceeded".into());
        }
        Ok(())
    }

    fn values(&mut self, values: &MaterializedContextValues) -> Result<(), String> {
        for value in values.values() {
            self.add(
                1,
                value
                    .bytes()
                    .len()
                    .saturating_add(value.original_name().map_or(0, str::len)),
            )?;
        }
        Ok(())
    }
}

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
    pub fn matches_transfer(
        &self,
        conditions: &otel_arrow_dfe_engine::context_declaration::CompiledTransferConditions,
    ) -> bool {
        self.materialized
            .transferred()
            .is_some_and(|transfer| conditions.accepts(transfer))
    }

    /// Exports only selected metadata, leaving all source routing and metrics untouched.
    pub fn export_transfer(&self, binding: &ContextExportBinding) -> Result<Self, String> {
        match binding {
            ContextExportBinding::Visible(visibility) => self.visible_context(visibility),
            ContextExportBinding::Flat {
                schema,
                sources,
                required,
            } => {
                let mut fields = Vec::with_capacity(sources.len());
                let mut budget = TransferBudget::default();
                budget.add(
                    sources.len(),
                    schema.transfer.keys().map(|name| name.len()).sum(),
                )?;
                for (index, candidates) in sources.iter().enumerate() {
                    let mut selected = None;
                    for source in candidates {
                        if let Some(value) = self.transfer_source(source)? {
                            selected = Some(value);
                            break;
                        }
                    }
                    if required[index] && selected.is_none() {
                        return Err(format!(
                            "required export field `{}` is absent",
                            schema.transfer.keys().nth(index).expect("bound field")
                        ));
                    }
                    match &selected {
                        Some(TransferValue::Values(values)) => budget.values(values)?,
                        Some(TransferValue::Headers(headers)) => {
                            for header in headers {
                                budget.add(
                                    1,
                                    header
                                        .name
                                        .len()
                                        .saturating_add(header.value.bytes().len())
                                        .saturating_add(
                                            header.value.original_name().map_or(0, str::len),
                                        ),
                                )?;
                            }
                        }
                        None => {}
                    }
                    fields.push(selected);
                }
                let transfer = TransferredContext::new(schema.as_ref().clone(), fields)?;
                let mut output = Self::default();
                output.materialized.set_transferred(Arc::new(transfer));
                Ok(output)
            }
        }
    }

    /// Imports a flat contract or applies the destination's declaration-aware intersection.
    pub fn import_transfer(&self, binding: &ContextImportBinding) -> Result<Self, String> {
        match binding {
            ContextImportBinding::Visible(visibility) => self.visible_context(visibility),
            ContextImportBinding::Flat {
                schema,
                destinations,
                composite,
                conditions,
            } => {
                let transfer = self
                    .materialized
                    .transferred()
                    .ok_or("topic message is missing its declared context transfer")?;
                if transfer.schema() != schema.as_ref() {
                    return Err(
                        "context transfer contract does not match the import binding".into(),
                    );
                }
                if !conditions.accepts(transfer) {
                    return Err("context transfer does not satisfy receiver conditions".into());
                }
                if let Some(plan) = composite {
                    if destinations.iter().any(|(index, _, _)| {
                        matches!(transfer.fields()[*index], Some(TransferValue::Headers(_)))
                    }) {
                        return Err(
                            "header collections cannot be stored as scalar composite members"
                                .into(),
                        );
                    }
                    let mut output = Self::default();
                    output.materialized.materialize_transfer(plan, transfer);
                    return Ok(output);
                }
                let mut output = Self::default();
                let mut headers = TransportHeaders::new();
                let mut identities = Vec::new();
                let mut imported = std::collections::BTreeMap::new();
                for (index, kind, name) in destinations {
                    let Some(value) = &transfer.fields()[*index] else {
                        continue;
                    };
                    match (*kind, value) {
                        (TransferKind::TransportHeader, TransferValue::Headers(collection)) => {
                            for header in collection {
                                headers.push(header_from_value(header.name.clone(), &header.value));
                            }
                        }
                        (TransferKind::TransportHeader, TransferValue::Values(values)) => {
                            for value in values.values() {
                                headers.push(header_from_value(name.clone(), value));
                            }
                        }
                        (TransferKind::AuthorizedIdentity, TransferValue::Values(values)) => {
                            identities.push((name.clone(), values.clone()));
                        }
                        (_, TransferValue::Values(values)) => {
                            if imported
                                .insert(name.clone(), (*kind, values.clone()))
                                .is_some()
                            {
                                return Err("duplicate imported primitive name".into());
                            }
                        }
                        _ => return Err("invalid trusted transfer collection".into()),
                    }
                }
                output.set_transport_headers(headers);
                output.authorized_identity =
                    AuthorizedIdentityEntries::from_transferred(&identities)?;
                if !imported.is_empty() {
                    let schema = ContextTransferSchema {
                        transfer: imported
                            .iter()
                            .map(|(n, (kind, _))| (n.clone(), TransferField { kind: *kind }))
                            .collect(),
                    };
                    let fields = imported
                        .into_values()
                        .map(|(_, v)| Some(TransferValue::Values(v)))
                        .collect();
                    output
                        .materialized
                        .set_transferred(Arc::new(TransferredContext::new(schema, fields)?));
                }
                Ok(output)
            }
        }
    }

    /// Encodes the flat snapshot for a storage adapter. Ordinary context must first be exported.
    pub fn encode_transfer(&self) -> Result<Vec<u8>, String> {
        self.materialized
            .transferred()
            .ok_or("context has not been flattened for storage")?
            .encode()
    }

    /// Recovers a bounded flat snapshot; importing still requires a compiled contract.
    pub fn decode_transfer(bytes: &[u8]) -> Result<Self, String> {
        let mut result = Self::default();
        result
            .materialized
            .set_transferred(Arc::new(TransferredContext::decode(bytes)?));
        Ok(result)
    }

    fn visible_context(&self, visibility: &ContextVisibility) -> Result<Self, String> {
        let identity = if self
            .authorized_identity
            .iter()
            .all(|e| visibility.identities.iter().any(|n| n.as_str() == e.name()))
        {
            self.authorized_identity.clone()
        } else {
            let source = PdataContextValueSource {
                authorized_identity: &self.authorized_identity,
            };
            let entries = visibility
                .identities
                .iter()
                .filter_map(|name| {
                    source
                        .values(&ContextFieldLayout {
                            domain: ContextDomain::AuthorizedIdentity,
                            name: name.clone(),
                        })
                        .map(|values| (name.clone(), values))
                })
                .collect::<Vec<_>>();
            AuthorizedIdentityEntries::from_transferred(&entries)?
        };
        Ok(Self {
            authorized_identity: identity,
            materialized: self.materialized.project_visible(visibility),
            ..Self::default()
        })
    }

    fn transfer_source(&self, source: &TransferSource) -> Result<Option<TransferValue>, String> {
        let values = match source {
            TransferSource::AllCaptured => {
                let mut headers = Vec::new();
                let mut budget = TransferBudget::default();
                for header in self.transport_headers().into_iter().flat_map(|h| h.iter()) {
                    budget.add(
                        1,
                        header
                            .name
                            .as_str()
                            .len()
                            .saturating_add(header.value.bytes.len())
                            .saturating_add(header.wire_name().len()),
                    )?;
                    headers.push(TransferHeader {
                        name: header.name.as_str().try_into().expect("valid stored name"),
                        value: MaterializedContextValue::new(
                            Some(header.wire_name().into()),
                            context_kind(header.value.value_kind),
                            header.value.bytes,
                        ),
                    });
                }
                return Ok(Some(TransferValue::Headers(headers)));
            }
            TransferSource::Member { plan, projection } => {
                let mut value = Ok(None);
                if let Some(selected) = self.materialized.resolve(plan, projection) {
                    selected.visit_members(|member| {
                        value = TransferBudget::default()
                            .values(member.values())
                            .map(|()| Some(member.values().clone()));
                    });
                }
                value?
            }
            TransferSource::Primitive {
                kind: TransferKind::TransportHeader,
                name,
            } => {
                let mut budget = TransferBudget::default();
                for header in self
                    .transport_headers()
                    .into_iter()
                    .flat_map(|h| h.iter())
                    .filter(|h| h.name.as_str().eq_ignore_ascii_case(name.as_str()))
                {
                    budget.add(
                        1,
                        header
                            .value
                            .bytes
                            .len()
                            .saturating_add(header.wire_name().len()),
                    )?;
                }
                self.materialized.transport_values(&ContextFieldLayout {
                    domain: ContextDomain::TransportHeader,
                    name: name.clone(),
                })
            }
            TransferSource::Primitive {
                kind: TransferKind::AuthorizedIdentity,
                name,
            } => {
                if let Some(entry) = self.authorized_identity.get(name.as_str()) {
                    let mut budget = TransferBudget::default();
                    for value in entry.value().values() {
                        budget.add(1, value.len())?;
                    }
                }
                PdataContextValueSource {
                    authorized_identity: &self.authorized_identity,
                }
                .values(&ContextFieldLayout {
                    domain: ContextDomain::AuthorizedIdentity,
                    name: name.clone(),
                })
            }
            TransferSource::Primitive {
                kind: TransferKind::NetworkInfo,
                name,
            } if name.as_str() == "peer_socket_addr" && self.peer_addr.is_some() => {
                MaterializedContextValues::new(
                    ContextValueCardinality::One,
                    vec![MaterializedContextValue::new(
                        None,
                        ContextValueKind::Text,
                        self.peer_addr
                            .expect("checked peer")
                            .to_string()
                            .into_bytes(),
                    )],
                )
            }
            TransferSource::Primitive { kind, name } => {
                self.materialized.transferred().and_then(|transfer| {
                    transfer
                        .schema()
                        .transfer
                        .iter()
                        .zip(transfer.fields())
                        .find_map(|((n, field), value)| {
                            if n == name && field.kind == *kind {
                                match value {
                                    Some(TransferValue::Values(v)) => Some(v.clone()),
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        })
                })
            }
        };
        Ok(values.map(TransferValue::Values))
    }
}

fn context_kind(kind: ValueKind) -> ContextValueKind {
    match kind {
        ValueKind::Text => ContextValueKind::Text,
        ValueKind::Binary => ContextValueKind::Binary,
    }
}

fn header_from_value(
    name: otel_arrow_dfe_config::ContextEntryName,
    value: &MaterializedContextValue,
) -> TransportHeader {
    let wire_name = value.original_name().unwrap_or(name.as_str());
    TransportHeader::captured(
        name.clone(),
        wire_name,
        true,
        match value.kind() {
            ContextValueKind::Text => ValueKind::Text,
            ContextValueKind::Binary => ValueKind::Binary,
        },
        value.bytes(),
    )
}

impl AuthorizedIdentityEntries {
    fn from_transferred(
        entries: &[(
            otel_arrow_dfe_config::ContextEntryName,
            MaterializedContextValues,
        )],
    ) -> Result<Self, String> {
        if entries.is_empty() {
            return Ok(Self::default());
        }
        let value_count = entries.iter().map(|(_, v)| v.values().len()).sum::<usize>();
        let entry_bytes = entries.len() * AUTHORIZED_ENTRY_LEN;
        let descriptor_len = entry_bytes + value_count * AUTHORIZED_VALUE_LEN;
        let blob_len = entries
            .iter()
            .map(|(n, v)| n.len() + v.values().iter().map(|v| v.bytes().len()).sum::<usize>())
            .sum::<usize>();
        let mut bytes = vec![0; descriptor_len + blob_len];
        let mut blob_at = descriptor_len;
        let mut value_index = 0;
        for (index, (name, values)) in entries.iter().enumerate() {
            let entry_at = index * AUTHORIZED_ENTRY_LEN;
            let range = write_context_blob(&mut bytes, &mut blob_at, name.as_bytes())
                .ok_or("identity transfer name overflow")?;
            write_context_range(&mut bytes, entry_at, range)
                .ok_or("identity transfer range overflow")?;
            write_context_u32(&mut bytes, entry_at + 8, value_index)
                .ok_or("identity transfer index overflow")?;
            write_context_u32(&mut bytes, entry_at + 12, values.values().len())
                .ok_or("identity transfer count overflow")?;
            bytes[entry_at + 16] = u8::from(values.cardinality() == ContextValueCardinality::Many);
            for value in values.values() {
                if value.kind() != ContextValueKind::Text || value.as_str().is_none() {
                    return Err("authorized identity transfer requires valid UTF-8 text".into());
                }
                let range = write_context_blob(&mut bytes, &mut blob_at, value.bytes())
                    .ok_or("identity transfer value overflow")?;
                write_context_range(
                    &mut bytes,
                    entry_bytes + value_index * AUTHORIZED_VALUE_LEN,
                    range,
                )
                .ok_or("identity transfer descriptor overflow")?;
                value_index += 1;
            }
        }
        Ok(Self {
            packed: Some(Arc::new(PackedAuthorizedIdentity {
                bytes: bytes.into_boxed_slice(),
                entry_count: entries.len(),
                value_count,
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::context_policy::{
        ContextEntryDeclaration, ContextEntryDefinition, ContextEntryPart, ContextScope,
    };
    use otel_arrow_dfe_engine::context_declaration::ContextMaterializationPlan;
    use std::collections::BTreeMap;

    /// Scenario: two exporters use the same flat schema but differ in required-field policy.
    /// Guarantees: only the required export refuses missing data, while import conditions
    /// reject mismatches and ordinary receiver materialization cannot fabricate transfer composites.
    #[test]
    fn context_transfer_required_export_and_conditions_are_independent() {
        use otel_arrow_dfe_engine::context_declaration::CompiledTransferConditions;
        let schema = Arc::new(ContextTransferSchema {
            transfer: BTreeMap::from([(
                name("tenant_id"),
                TransferField {
                    kind: TransferKind::TransportHeader,
                },
            )]),
        });
        let source = Context::default();
        for required in [false, true] {
            let binding = ContextExportBinding::Flat {
                schema: schema.clone(),
                required: vec![required],
                sources: vec![vec![TransferSource::Primitive {
                    kind: TransferKind::TransportHeader,
                    name: name("tenant"),
                }]],
            };
            assert_eq!(source.export_transfer(&binding).is_err(), required);
        }
        let conditions = BTreeMap::from([(name("tenant_id"), "1".to_owned())]);
        let predicate =
            Arc::new(CompiledTransferConditions::compile(&schema, &conditions).unwrap());
        let plan = ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
            scope: ContextScope::Pipeline("group".into(), "receiver".into()),
            name: name("topic_exp"),
            definition: ContextEntryDefinition(vec![ContextEntryPart::Transferred {
                name: name("tenant_id"),
                kind: TransferKind::TransportHeader,
                store_as: name("tenant_id"),
                conditions: Arc::new(conditions),
            }]),
        }])
        .unwrap();
        let binding = ContextImportBinding::Flat {
            schema: schema.clone(),
            destinations: vec![(0, TransferKind::TransportHeader, name("tenant_id"))],
            composite: Some(plan.clone()),
            conditions: predicate.clone(),
        };
        for tenant in ["1", "2"] {
            let data = TransferredContext::new(
                schema.as_ref().clone(),
                vec![Some(TransferValue::Values(values(
                    ContextValueKind::Text,
                    ContextValueCardinality::One,
                    &[tenant.as_bytes()],
                )))],
            )
            .unwrap();
            let mut context = Context::decode_transfer(&data.encode().unwrap()).unwrap();
            context.materialize_context(&plan);
            assert!(
                context.materialized_context().is_none(),
                "another receiver cannot instantiate the implicit composite"
            );
            assert_eq!(context.matches_transfer(&predicate), tenant == "1");
            assert_eq!(context.import_transfer(&binding).is_ok(), tenant == "1");
        }
    }

    fn name(value: &str) -> otel_arrow_dfe_config::ContextEntryName {
        value.try_into().unwrap()
    }

    fn values(
        kind: ContextValueKind,
        cardinality: ContextValueCardinality,
        data: &[&[u8]],
    ) -> MaterializedContextValues {
        MaterializedContextValues::new(
            cardinality,
            data.iter()
                .map(|bytes| MaterializedContextValue::new(None, kind, bytes.to_vec()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    /// Scenario: all five transfer authorities are decoded from storage into a receiver-local composite.
    /// Guarantees: values are not regenerated, no primitive/routing state leaks, and absent optional fields do not erase the matching composite.
    #[test]
    fn context_transfer_composite_storage_round_trip_preserves_optional_fields() {
        let fields = [
            (
                "customer",
                TransferKind::AuthorizedIdentity,
                values(ContextValueKind::Text, ContextValueCardinality::Many, &[]),
            ),
            (
                "peer",
                TransferKind::NetworkInfo,
                values(
                    ContextValueKind::Text,
                    ContextValueCardinality::One,
                    &[b"127.0.0.1:1234"],
                ),
            ),
            (
                "randomness",
                TransferKind::Randomness,
                values(
                    ContextValueKind::Text,
                    ContextValueCardinality::One,
                    &[b"unchanged-token"],
                ),
            ),
            (
                "route",
                TransferKind::Constant,
                values(
                    ContextValueKind::Text,
                    ContextValueCardinality::One,
                    &[b"primary"],
                ),
            ),
            (
                "workspace",
                TransferKind::TransportHeader,
                values(
                    ContextValueKind::Binary,
                    ContextValueCardinality::Many,
                    &[b"a", b"\xff"],
                ),
            ),
        ];
        let schema = Arc::new(ContextTransferSchema {
            transfer: fields
                .iter()
                .map(|(n, k, _)| (name(n), TransferField { kind: *k }))
                .collect(),
        });
        let declaration = ContextEntryDeclaration {
            scope: ContextScope::Pipeline("group".into(), "receiver".into()),
            name: name("topic_exp"),
            definition: ContextEntryDefinition(
                fields
                    .iter()
                    .map(|(n, k, _)| ContextEntryPart::Transferred {
                        name: name(n),
                        kind: *k,
                        store_as: name(n),
                        conditions: Default::default(),
                    })
                    .collect(),
            ),
        };
        let plan = ContextMaterializationPlan::compile(&[declaration]).unwrap();
        let binding = ContextImportBinding::Flat {
            schema: schema.clone(),
            destinations: fields
                .iter()
                .enumerate()
                .map(|(i, (n, k, _))| (i, *k, name(n)))
                .collect(),
            composite: Some(plan.clone()),
            conditions: Arc::new(
                otel_arrow_dfe_engine::context_declaration::CompiledTransferConditions::compile(
                    &schema,
                    &BTreeMap::new(),
                )
                .unwrap(),
            ),
        };
        for absent in [false, true] {
            let mut slots: Vec<_> = fields
                .iter()
                .map(|(_, _, v)| Some(TransferValue::Values(v.clone())))
                .collect();
            if absent {
                slots[4] = None;
            }
            let transferred = TransferredContext::new(schema.as_ref().clone(), slots).unwrap();
            let decoded = Context::decode_transfer(&transferred.encode().unwrap()).unwrap();
            let mut imported = decoded.import_transfer(&binding).unwrap();
            assert!(imported.transport_headers().is_none());
            assert!(imported.authorized_identity_entries().is_none());
            assert!(!imported.has_ack_or_nack_subscribers());
            assert!(imported.peer_addr().is_none());
            imported.materialize_context(&plan);
            if absent {
                let materialized = imported.materialized_context().unwrap();
                assert_eq!(
                    materialized
                        .entry(&name("topic_exp"))
                        .unwrap()
                        .members()
                        .len(),
                    4
                );
                let whole = plan.layout().resolve_composite(&name("topic_exp")).unwrap();
                assert!(materialized.resolve(&plan, &whole).is_some());
                let missing = plan
                    .layout()
                    .resolve_member(&name("topic_exp"), &name("workspace"))
                    .unwrap();
                assert!(materialized.resolve(&plan, &missing).is_none());
            } else {
                let entry = imported
                    .materialized_context()
                    .unwrap()
                    .entry(&name("topic_exp"))
                    .unwrap();
                for (n, _, expected) in &fields {
                    assert_eq!(entry.member(&name(n)).unwrap().values(), expected);
                }
                let export_schema = Arc::new(ContextTransferSchema {
                    transfer: BTreeMap::from([(
                        name("id"),
                        TransferField {
                            kind: TransferKind::Randomness,
                        },
                    )]),
                });
                let reexport = ContextExportBinding::Flat {
                    schema: export_schema,
                    required: vec![true],
                    sources: vec![vec![TransferSource::Member {
                        projection: plan
                            .layout()
                            .resolve_member(&name("topic_exp"), &name("randomness"))
                            .unwrap(),
                        plan: plan.clone(),
                    }]],
                };
                let encoded = imported
                    .export_transfer(&reexport)
                    .unwrap()
                    .encode_transfer()
                    .unwrap();
                assert_eq!(
                    TransferredContext::decode(&encoded)
                        .unwrap()
                        .value(TransferKind::Randomness, &name("id"))
                        .unwrap(),
                    &fields[2].2
                );
            }
        }
    }

    /// Scenario: default projection sees both shared and private verified claims.
    /// Guarantees: only declared identities survive; the all-visible case shares packed storage.
    #[test]
    fn context_transfer_default_identity_projection_is_selective() {
        let source = Context {
            authorized_identity: AuthorizedIdentityEntries::from_transferred(&[
                (
                    name("shared"),
                    values(
                        ContextValueKind::Text,
                        ContextValueCardinality::One,
                        &[b"visible"],
                    ),
                ),
                (
                    name("private"),
                    values(
                        ContextValueKind::Text,
                        ContextValueCardinality::Many,
                        &[b"secret"],
                    ),
                ),
            ])
            .unwrap(),
            ..Context::default()
        };
        let all = Arc::new(ContextVisibility {
            identities: [name("shared"), name("private")].into_iter().collect(),
            ..Default::default()
        });
        let exported = source
            .export_transfer(&ContextExportBinding::Visible(all))
            .unwrap();
        assert!(Arc::ptr_eq(
            source.authorized_identity.packed.as_ref().unwrap(),
            exported.authorized_identity.packed.as_ref().unwrap()
        ));
        let only_shared = Arc::new(ContextVisibility {
            identities: [name("shared")].into_iter().collect(),
            ..Default::default()
        });
        let imported = exported
            .import_transfer(&ContextImportBinding::Visible(only_shared))
            .unwrap();
        assert_eq!(imported.authorized_identity.len(), 1);
        assert!(imported.authorized_identity.get("shared").is_some());
        assert!(imported.authorized_identity.get("private").is_none());
        assert_eq!(source.authorized_identity.len(), 2);
    }
}
