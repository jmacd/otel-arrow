// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::context_declaration::{
    CompiledContextBindings, ContextConsumerSelector, ContextDeclaration, ContextEntrySelector,
    ContextEntrySelectorForm, ContextRuntimeRequirements, MaterializedContextValue,
    PreparedNodeContextDeclarations,
};
use otel_arrow_dfe_config::ContextEntryName;
use otel_arrow_dfe_config::context_policy::{
    ContextEntryDeclaration, ContextEntryDefinition, ContextEntryPart, ContextRandomnessKind,
    ContextScope,
};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

fn typed_selection(json: serde_json::Value) -> ContextConsumerSelector {
    let selectors: Vec<otel_arrow_dfe_config::ContextSelector> =
        serde_json::from_value(json).expect("typed selectors");
    ContextConsumerSelector::values(&selectors).expect("finite value selection")
}

/// Scenario: a finite grouping projection receives a legacy wildcard, none, or invalid name list.
/// Guarantees: unsupported and malformed selectors return errors without omitting dimensions.
#[test]
fn finite_projection_rejects_non_finite_or_invalid_selectors() {
    for json in [
        serde_json::json!([{"type": "none"}]),
        serde_json::json!([{"type": "all_captured"}]),
        serde_json::json!([{"type": "named"}]),
        serde_json::json!([{"type": "composite", "named": []}]),
        serde_json::json!([{"type": "composite", "named": ["tenant:customer"]}]),
        serde_json::json!([
            {"type": "named", "named": ["id"]}, {"type": "all_captured"}
        ]),
    ] {
        let selectors = serde_json::from_value::<Vec<otel_arrow_dfe_config::ContextSelector>>(json)
            .expect("shared configuration");
        assert!(ContextConsumerSelector::values(&selectors).is_err());
    }
}

fn header_context(headers: &[(&str, &str)]) -> MaterializedContext {
    use otel_arrow_dfe_config::transport_headers::{TransportHeader, TransportHeaders, ValueKind};
    let mut transport = TransportHeaders::new();
    for (key, value) in headers {
        transport.push(TransportHeader::new(
            name(key),
            ValueKind::Text,
            value.as_bytes(),
        ));
    }
    let mut context = MaterializedContext::default();
    context.set_transport_headers(transport);
    context
}

/// Scenario: typed selectors mix same-named header/identity primitives with qualified members.
/// Guarantees: tags determine primitive domains, references retain order, and composites are explicit.
#[test]
fn typed_selection_preserves_domains_and_reference_order() {
    let selector = typed_selection(serde_json::json!([
        {"type": "named", "named": ["tenant", "tenant:customer"]},
        {"type": "authorized_identity", "named": ["tenant"]},
        {"type": "composite", "named": ["tenant"]}
    ]));
    let ContextConsumerSelector::Entries { entries } = selector else {
        unreachable!()
    };
    assert_eq!(
        entries
            .iter()
            .map(|entry| &entry.target)
            .collect::<Vec<_>>(),
        [
            &ContextEntryTarget::Primitive {
                domain: ContextDomain::TransportHeader,
                name: name("tenant")
            },
            &ContextEntryTarget::CompositeMember {
                composite: name("tenant"),
                member: name("customer")
            },
            &ContextEntryTarget::Primitive {
                domain: ContextDomain::AuthorizedIdentity,
                name: name("tenant")
            },
            &ContextEntryTarget::Composite {
                name: name("tenant")
            },
        ]
    );
}

/// Scenario: some tuple dimensions are absent while same-named source domains carry different values.
/// Guarantees: missing dimensions do not collapse other values, and output keys retain field boundaries.
#[test]
fn partition_tuple_preserves_partial_absence_and_domain_values() {
    let selector = typed_selection(serde_json::json!([
        {"type": "named", "named": ["id", "optional"]},
        {"type": "authorized_identity", "named": ["id"]}
    ]));
    let binding = ContextPartitionBinding::compile(&selector, None).expect("binding");
    let empty_source = Source::default();
    let a = header_context(&[("id", "a")]);
    let b = header_context(&[("id", "b")]);
    assert_ne!(
        binding.key(&a, &empty_source),
        binding.key(&b, &empty_source)
    );
    assert_ne!(
        binding.key(&a, &empty_source),
        binding.key(
            &header_context(&[("id", "a"), ("optional", "")]),
            &empty_source
        ),
    );
    assert_ne!(
        binding.key(&a, &empty_source),
        binding.key(&header_context(&[("optional", "a")]), &empty_source),
    );
    let source_a = Source(HashMap::from([(
        name("id"),
        values(ContextValueKind::Text, false, &["a"]),
    )]));
    let source_b = Source(HashMap::from([(
        name("id"),
        values(ContextValueKind::Text, false, &["b"]),
    )]));
    assert_ne!(binding.key(&a, &source_b), binding.key(&b, &source_a));
    assert_ne!(binding.key(&a, &source_a), binding.key(&a, &empty_source));
}

/// Scenario: a consumer groups by one aliased member of a conditional composite.
/// Guarantees: its full gate still applies, but projection retains only selected snapshots and provenance.
#[test]
fn partition_member_projection_keeps_gate_without_leaking_members() {
    let plan = plan(vec![
        ContextEntryPart::AuthorizedIdentity {
            name: name("id"),
            store_as: Some(name("customer")),
        },
        ContextEntryPart::TransportHeader {
            name: name("workspace"),
            store_as: None,
        },
        ContextEntryPart::Randomness {
            name: name("correlation"),
            value: ContextRandomnessKind::Uuid7,
        },
        ContextEntryPart::TransportHeaderMatch {
            name: name("environment"),
            value: "prod".into(),
        },
    ]);
    let selected = typed_selection(serde_json::json!([
        {"type": "named", "named": ["tenant:customer"]}
    ]));
    let binding = ContextPartitionBinding::compile(&selected, Some(&plan)).expect("binding");
    let mut source = Source(HashMap::from([
        (name("id"), values(ContextValueKind::Text, false, &["a"])),
        (
            name("workspace"),
            values(ContextValueKind::Text, false, &["w"]),
        ),
        (
            name("environment"),
            values(ContextValueKind::Text, false, &["prod"]),
        ),
    ]));
    let mut context = MaterializedContext::default();
    context.materialize(&plan, &source);
    let key = binding.key(&context, &source);
    let mut projected = binding.project_materialized(&context);
    assert_eq!(
        projected
            .entry(&name("tenant"))
            .expect("entry")
            .members()
            .len(),
        1
    );
    assert_eq!(binding.key(&projected, &source), key);
    assert!(
        projected
            .resolve(
                &plan,
                &plan
                    .layout()
                    .resolve_composite(&name("tenant"))
                    .expect("whole")
            )
            .is_none()
    );
    assert!(
        projected
            .entry(&name("tenant"))
            .expect("entry")
            .member(&name("correlation"))
            .is_none()
    );
    _ = source.0.insert(
        name("id"),
        values(ContextValueKind::Text, false, &["changed"]),
    );
    projected.materialize(&plan, &source);
    assert_eq!(
        binding.key(&projected, &source),
        key,
        "do not reread primitives or regenerate omitted members"
    );
    assert_eq!(
        projected
            .entry(&name("tenant"))
            .expect("entry")
            .members()
            .len(),
        1
    );

    _ = source.0.remove(&name("workspace"));
    let mut incomplete = MaterializedContext::default();
    incomplete.materialize(&plan, &source);
    assert_eq!(
        binding.key(&incomplete, &source),
        binding.key(&MaterializedContext::default(), &source),
    );
    assert!(binding.project_materialized(&incomplete).is_empty());

    let overlap = typed_selection(serde_json::json!([
        {"type": "named", "named": ["tenant:customer", "tenant:workspace", "tenant:customer"]},
        {"type": "composite", "named": ["tenant"]},
    ]));
    let overlap =
        ContextPartitionBinding::compile(&overlap, Some(&plan)).expect("overlapping binding");
    let combined = overlap.project_materialized(&context);
    assert_eq!(combined.entries().len(), 1);
    assert_eq!(combined.entries()[0].members().len(), 3);
    assert_eq!(
        overlap.key(&combined, &source),
        overlap.key(&context, &source)
    );
}

fn name(value: &str) -> ContextEntryName {
    value.try_into().expect("valid entry name")
}

fn selection(target: ContextEntryTarget) -> ContextConsumerSelector {
    ContextConsumerSelector::Entries {
        entries: Box::new([ContextEntrySelector {
            target,
            form: ContextEntrySelectorForm::Value,
        }]),
    }
}

#[derive(Default)]
struct Source(HashMap<ContextEntryName, MaterializedContextValues>);

impl ContextValueSource for Source {
    fn values(&self, field: &ContextFieldLayout) -> Option<MaterializedContextValues> {
        self.0.get(&field.name).cloned()
    }
}

fn values(kind: ContextValueKind, many: bool, values: &[&str]) -> MaterializedContextValues {
    MaterializedContextValues::new(
        if many {
            ContextValueCardinality::Many
        } else {
            ContextValueCardinality::One
        },
        values
            .iter()
            .map(|value| MaterializedContextValue::new(None, kind, value.as_bytes()))
            .collect::<Vec<_>>(),
    )
    .expect("valid cardinality")
}

fn plan(parts: Vec<ContextEntryPart>) -> ContextMaterializationPlan {
    ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
        name: name("tenant"),
        scope: ContextScope::Engine,
        definition: ContextEntryDefinition(parts),
    }])
    .expect("valid plan")
}

/// Scenario: primitive keys differ in kind, cardinality, value boundaries, order, or presence.
/// Guarantees: full equality distinguishes every tuple even when every hash collides.
#[test]
fn partition_keys_preserve_value_shape_under_collisions() {
    #[derive(Default)]
    struct CollisionHasher;
    impl Hasher for CollisionHasher {
        fn write(&mut self, _: &[u8]) {}
        fn finish(&self) -> u64 {
            0
        }
    }
    let binding = ContextPartitionBinding::compile(
        &selection(ContextEntryTarget::Primitive {
            domain: ContextDomain::AuthorizedIdentity,
            name: name("id"),
        }),
        None,
    )
    .expect("primitive binding");
    let mut keys = HashMap::<_, _, BuildHasherDefault<CollisionHasher>>::default();
    let context = MaterializedContext::default();
    _ = keys.insert(binding.key(&context, &Source::default()), 0);
    for (index, value) in [
        values(ContextValueKind::Text, false, &[""]),
        values(ContextValueKind::Text, true, &[]),
        values(ContextValueKind::Text, false, &["secret"]),
        values(ContextValueKind::Binary, false, &["secret"]),
        values(ContextValueKind::Text, true, &["secret"]),
        values(ContextValueKind::Text, true, &["a", "bc"]),
        values(ContextValueKind::Text, true, &["ab", "c"]),
        values(ContextValueKind::Text, true, &["bc", "a"]),
    ]
    .into_iter()
    .enumerate()
    {
        let source = Source(HashMap::from([(name("id"), value)]));
        let key = binding.key(&context, &source);
        assert!(!format!("{key:?}").contains("secret"));
        assert_eq!(keys.insert(key.clone(), index + 1), None);
        assert_eq!(
            keys.get(&binding.key(&context, &source)),
            Some(&(index + 1))
        );
    }
    assert_eq!(keys.len(), 9);
}

/// Scenario: a conditional two-member composite has missing members or a failed condition.
/// Guarantees: every incomplete context uses the same missing key; complete tuples stay distinct.
#[test]
fn partition_composite_presence_is_atomic() {
    let plan = plan(vec![
        ContextEntryPart::AuthorizedIdentity {
            name: name("id"),
            store_as: None,
        },
        ContextEntryPart::TransportHeader {
            name: name("workspace"),
            store_as: None,
        },
        ContextEntryPart::TransportHeaderMatch {
            name: name("environment"),
            value: "prod".into(),
        },
    ]);
    let binding = ContextPartitionBinding::compile(
        &selection(ContextEntryTarget::Composite {
            name: name("tenant"),
        }),
        Some(&plan),
    )
    .expect("composite binding");
    let missing = binding.key(&MaterializedContext::default(), &Source::default());
    let mut source = Source::default();
    let mut complete_keys = Vec::new();
    for (id, workspace, environment) in [
        (Some("a"), None, "prod"),
        (None, Some("b"), "prod"),
        (Some("a"), Some("b"), "dev"),
        (Some("a"), Some("b"), "prod"),
        (Some("ab"), Some(""), "prod"),
    ] {
        source.0.clear();
        for (key, value) in [
            ("id", id),
            ("workspace", workspace),
            ("environment", Some(environment)),
        ] {
            if let Some(value) = value {
                _ = source
                    .0
                    .insert(name(key), values(ContextValueKind::Text, false, &[value]));
            }
        }
        let mut context = MaterializedContext::default();
        context.materialize(&plan, &source);
        let key = binding.key(&context, &source);
        if id.is_none() || workspace.is_none() || environment != "prod" {
            assert_eq!(key, missing);
            assert!(binding.project_materialized(&context).is_empty());
        } else {
            assert_ne!(key, missing);
            complete_keys.push(key);
        }
    }
    assert_ne!(complete_keys[0], complete_keys[1]);
}

/// Scenario: a random/constant composite is projected and subsequently read under changed policy.
/// Guarantees: projection preserves the snapshot and declaration; different provenance is absent.
#[test]
fn partition_projection_preserves_synthetic_snapshot_and_provenance() {
    let plan = plan(vec![
        ContextEntryPart::Randomness {
            name: name("id"),
            value: ContextRandomnessKind::Uuid7,
        },
        ContextEntryPart::Constant {
            name: name("route"),
            value: "primary".into(),
        },
    ]);
    let target = selection(ContextEntryTarget::Composite {
        name: name("tenant"),
    });
    let binding = ContextPartitionBinding::compile(&target, Some(&plan)).expect("binding");
    let source = Source::default();
    let mut context = MaterializedContext::default();
    context.materialize(&plan, &source);
    let key = binding.key(&context, &source);
    let mut projected = binding.project_materialized(&context);
    projected.materialize(&plan, &source);
    assert_eq!(binding.key(&projected, &source), key);
    assert_eq!(projected.entries().len(), 1);
    let changed_plan = ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
        name: name("tenant"),
        scope: ContextScope::Engine,
        definition: ContextEntryDefinition(vec![ContextEntryPart::Constant {
            name: name("route"),
            value: "secondary".into(),
        }]),
    }])
    .expect("changed plan");
    let changed = ContextPartitionBinding::compile(&target, Some(&changed_plan)).expect("binding");
    assert_eq!(
        changed.key(&projected, &source),
        changed.key(&MaterializedContext::default(), &source),
    );
}

/// Scenario: a node tries to bind a target it did not declare or a different authority domain.
/// Guarantees: only the exact declared value selection can be bound.
#[test]
fn partition_binding_requires_declared_selection() {
    let target = typed_selection(serde_json::json!([
        {"type": "named", "named": ["id"]},
        {"type": "authorized_identity", "named": ["id"]},
    ]));
    let declarations = [ContextDeclaration::Consumes {
        selector: target.clone(),
    }]
    .into_iter()
    .collect();
    let prepared = PreparedNodeContextDeclarations::new(declarations, &[]).expect("declarations");
    let pipeline = otel_arrow_dfe_config::PipelineKey::new("group".into(), "pipeline".into());
    let node = otel_arrow_dfe_config::NodeId::from("batch");
    let declarations =
        HashMap::from([(pipeline.clone(), HashMap::from([(node.clone(), prepared)]))]);
    let requirements = ContextRuntimeRequirements::compile(&declarations);
    let compiled = CompiledContextBindings::compile(declarations, &requirements).expect("bindings");
    assert!(compiled.bind_partition(&pipeline, &node, &target).is_ok());
    let reversed = typed_selection(serde_json::json!([
        {"type": "authorized_identity", "named": ["id"]},
        {"type": "named", "named": ["id"]},
    ]));
    assert!(
        compiled
            .bind_partition(&pipeline, &node, &reversed)
            .is_err()
    );
    assert!(
        compiled
            .bind_partition(
                &pipeline,
                &node,
                &selection(ContextEntryTarget::Primitive {
                    domain: ContextDomain::AuthorizedIdentity,
                    name: name("id"),
                })
            )
            .is_err()
    );
    assert!(
        compiled
            .bind_partition(&pipeline, &"other".into(), &target)
            .is_err()
    );
}
