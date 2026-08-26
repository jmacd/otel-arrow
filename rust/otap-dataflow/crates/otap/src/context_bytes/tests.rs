use super::super::bindings::{
    ContextEntrySetBinding, ContextValueBinding, PartitionProjectionBinding, SCHEMA_CACHE_CAPACITY,
};
use super::*;
use otel_arrow_dfe_config::context::{ContextCompiler, ContextRegisterField};
use otel_arrow_dfe_config::transport_headers_policy::NameStrategy;
use otel_arrow_dfe_config::transport_headers_policy::{
    CaptureDefaults, CaptureRule, HeaderCapturePolicy, HeaderPropagationPolicy, PropagationDefault,
    PropagationMatch, PropagationOverride, PropagationSelector, PropagationSelectorType,
};

/// Scenario: register presence spans both sides of a bitmap word boundary.
/// Guarantees: sizing, mutation, and encoded lookup share the same bit arithmetic.
#[test]
fn presence_bitmap_operations_agree_across_words() {
    let mut words = vec![0; PresenceBitmap::word_count(65)];
    for register in [0, 63, 64] {
        PresenceBitmap::set_words(&mut words, register).expect("valid register");
    }
    let mut encoded = vec![0; words.len() * size_of::<u64>()];
    for (index, word) in words.into_iter().enumerate() {
        write_u64(&mut encoded, index * size_of::<u64>(), word).expect("encode word");
    }

    assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 0), Some(true));
    assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 62), Some(false));
    assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 63), Some(true));
    assert_eq!(PresenceBitmap::is_set_encoded(&encoded, 64), Some(true));
}

/// Scenario: an envelope builder attempts to write sections out of format order.
/// Guarantees: the shared writer rejects construction before producing an envelope.
#[test]
fn envelope_writer_enforces_section_order() {
    let layout = Layout::new(1, 0, 0, 0).expect("layout");
    let mut writer = EnvelopeWriter::new(layout).expect("writer");

    assert!(writer.entries(|_| Ok(())).is_err());
    assert!(writer.finish().is_err());
}

/// Scenario: a record producer supplies compiled fields out of schema order.
/// Guarantees: the envelope contains numeric fields in canonical order, preserves repeated
/// values, and stores neither field symbols nor transport provenance.
#[test]
fn record_register_encodes_compiled_field_ordinals() {
    let mut compiler = ContextCompiler::new(3);
    let record = compiler
        .declare_record([
            (
                "tenant_id",
                ContextRegisterField::scalar(ContextScalarType::Text),
            ),
            (
                "roles",
                ContextRegisterField::repeated(ContextScalarType::Text),
            ),
            (
                "digest",
                ContextRegisterField::scalar(ContextScalarType::Bytes),
            ),
        ])
        .expect("record shape");
    let register = compiler
        .declare("routing", ContextRegisterShape::Record(record))
        .expect("record register");
    let compiled = compiler.finish();
    let tenant = compiled
        .linker()
        .resolve_field(record, "tenant_id")
        .expect("tenant field");
    let roles = compiled
        .linker()
        .resolve_field(record, "roles")
        .expect("roles field");
    let digest = compiled
        .linker()
        .resolve_field(record, "digest")
        .expect("digest field");

    let context = PdataContextBytes::from_record(
        compiled,
        register,
        [
            ContextRecordValue::new(roles, ContextValueKind::Text, b"reader"),
            ContextRecordValue::new(digest, ContextValueKind::Binary, &[1, 2, 3]),
            ContextRecordValue::new(tenant, ContextValueKind::Text, b"acme"),
            ContextRecordValue::new(roles, ContextValueKind::Text, b"writer"),
        ],
    )
    .expect("record context");

    let fields: Vec<_> = context
        .register(register)
        .expect("present record")
        .record_fields()
        .map(|value| (value.field(), value.kind(), value.value()))
        .collect();
    assert_eq!(
        fields,
        vec![
            (tenant, ContextValueKind::Text, b"acme".as_slice()),
            (roles, ContextValueKind::Text, b"reader".as_slice()),
            (roles, ContextValueKind::Text, b"writer".as_slice()),
            (digest, ContextValueKind::Binary, &[1, 2, 3]),
        ]
    );
    assert!(context.items().all(|item| item.schema_index().is_none()
        && item.wire_name().is_none()
        && item.stored_name().is_none()));
    assert!(
        !context
            .bytes
            .windows(b"tenant_id".len())
            .any(|bytes| bytes == b"tenant_id")
    );
    assert!(
        !context
            .bytes
            .windows(b"roles".len())
            .any(|bytes| bytes == b"roles")
    );
    validate(&context.bytes, context.schema()).expect("valid record envelope");
}

/// Scenario: a record producer repeats a scalar field and supplies invalid text bytes.
/// Guarantees: shape and scalar-type violations are rejected before bytes are emitted.
#[test]
fn record_register_rejects_invalid_field_values() {
    let mut compiler = ContextCompiler::new(4);
    let record = compiler
        .declare_record([(
            "tenant",
            ContextRegisterField::scalar(ContextScalarType::Text),
        )])
        .expect("record shape");
    let register = compiler
        .declare("routing", ContextRegisterShape::Record(record))
        .expect("record register");
    let compiled = compiler.finish();
    let tenant = compiled
        .linker()
        .resolve_field(record, "tenant")
        .expect("tenant field");

    assert!(matches!(
        PdataContextBytes::from_record(
            compiled.clone(),
            register,
            [
                ContextRecordValue::new(tenant, ContextValueKind::Text, b"one"),
                ContextRecordValue::new(tenant, ContextValueKind::Text, b"two"),
            ],
        ),
        Err(ContextBytesError::DuplicateRecordField { .. })
    ));
    assert!(matches!(
        PdataContextBytes::from_record(
            compiled,
            register,
            [ContextRecordValue::new(
                tenant,
                ContextValueKind::Text,
                &[0xff],
            )],
        ),
        Err(ContextBytesError::RecordFieldTypeMismatch { .. })
    ));
}

/// Scenario: an entry has duplicate typed values interleaved with a bag-only header.
/// Guarantees: bag order is preserved and the entry resolves only its ordered members.
#[test]
fn packed_context_indexes_entries_and_bag() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-tenant-bin".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: Some(ValueKindConfig::Binary),
            },
            CaptureRule {
                match_names: vec!["x-request-id".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");

    let context = PdataContextBytes::capture(
        &policy,
        [
            ("X-Tenant", b"acme".as_slice()),
            ("X-Request-Id", b"request-1".as_slice()),
            ("X-Tenant-Bin", &[0x01, 0x02]),
        ],
    )
    .expect("capture")
    .0
    .expect("context");

    let items: Vec<_> = context.items().collect();
    assert_eq!(items.len(), 3);
    // Wire name preserves transport spelling (occurrence data)
    assert_eq!(items[0].wire_name(), Some("X-Tenant"));
    assert_eq!(items[0].stored_name(), Some("tenant"));
    assert_eq!(items[1].wire_name(), Some("X-Request-Id"));
    assert_eq!(
        items[2].value(),
        Some((HeaderValueKind::Binary, &[0x01u8, 0x02][..]))
    );

    let entry = context.entry(0).expect("entry is present");
    assert_ne!(entry.hash(), 0);
    assert_eq!(
        entry.values().collect::<Vec<_>>(),
        vec![
            (HeaderValueKind::Text, b"acme".as_slice()),
            (HeaderValueKind::Binary, &[0x01u8, 0x02][..]),
        ]
    );
    validate(&context.bytes, context.schema()).expect("validates");
}

/// Scenario: one captured item occupies the first of several compiled entry slots.
/// Guarantees: trailing absent entry slots are correctly empty and the envelope validates.
#[test]
fn single_item_context_validates_with_trailing_entry_slots() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-region".to_string()],
                store_as: Some("region".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-extra".to_string()],
                store_as: Some("extra".to_string()),
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");

    let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");

    validate(&context.bytes, context.schema()).expect("validates");
    assert!(context.entry(0).is_some());
    assert!(context.entry(1).is_none());
    assert!(context.entry(2).is_none());
}

/// Scenario: a captured header uses a schema-backed stored name.
/// Guarantees: only the value is inline; wire and stored names resolve from schema.
#[test]
fn captured_stored_names_resolve_from_schema() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    validate(&context.bytes, context.schema()).expect("schema-backed context validates");
    let layout = context.layout().expect("layout");
    let item = layout
        .item_descriptor(&context.bytes, 0)
        .expect("item descriptor");

    assert_eq!(item.wire_name, BlobRange { offset: 0, len: 0 });
    assert_eq!(
        context.items().next().and_then(|item| item.wire_name()),
        Some("x-tenant")
    );
    assert_eq!(
        context.items().next().and_then(|item| item.stored_name()),
        Some("x-tenant")
    );
    // Only value bytes are in the blob. The fixed-size register tables are
    // outside the blob.
    assert_eq!(
        context.bytes.len(),
        HeaderFields::LEN
            + size_of::<u64>()
            + EntryFields::LEN
            + ItemFields::LEN
            + MemberFields::LEN
            + b"acme".len()
    );
}

/// Scenario: gRPC metadata contains unmatched ASCII plus matched ASCII and binary values.
/// Guarantees: capture screens by name and preserves decoded binary bytes and entry indexing.
#[test]
fn grpc_capture_screens_names_before_preserving_values() {
    use tonic::metadata::{Ascii, Binary, MetadataKey, MetadataMap, MetadataValue};

    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["trace-bin".to_string()],
                store_as: Some("trace".to_string()),
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");
    let mut metadata = MetadataMap::new();
    let _ = metadata.append(
        "ignored"
            .parse::<MetadataKey<Ascii>>()
            .expect("metadata key"),
        MetadataValue::try_from("skip").expect("metadata value"),
    );
    let _ = metadata.append(
        "x-tenant"
            .parse::<MetadataKey<Ascii>>()
            .expect("metadata key"),
        MetadataValue::try_from("acme").expect("metadata value"),
    );
    let _ = metadata.append_bin(
        "trace-bin"
            .parse::<MetadataKey<Binary>>()
            .expect("metadata key"),
        MetadataValue::from_bytes(&[0x01, 0x02]),
    );

    let context = PdataContextBytes::capture_grpc_metadata(&policy, &metadata)
        .expect("capture")
        .0
        .expect("context");
    let items: Vec<_> = context.items().collect();

    assert_eq!(items.len(), 2);
    assert_eq!(items[0].stored_name(), Some("tenant"));
    assert_eq!(
        items[1].value(),
        Some((HeaderValueKind::Binary, &[0x01, 0x02][..]))
    );
    assert_eq!(context.entry(0).expect("tenant entry").values().count(), 1);
    assert_eq!(context.entry(1).expect("trace entry").values().count(), 1);
}

/// Scenario: capture rules match duplicate text and binary headers while limits drop excess.
/// Guarantees: capture preserves kinds and reports every limit violation category.
#[test]
fn capture_applies_policy_and_reports_limits() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults {
            max_entries: 2,
            max_name_bytes: 12,
            max_value_bytes: 4,
            ..CaptureDefaults::default()
        },
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["trace-bin".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-long-name-value".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");

    let (context, stats) = PdataContextBytes::capture(
        &policy,
        [
            ("x-long-name-value", b"ok".as_slice()),
            ("x-tenant", b"extra".as_slice()),
            ("X-Tenant", b"acme".as_slice()),
            ("trace-bin", &[0x01, 0x02]),
            ("x-tenant", b"more".as_slice()),
        ],
    )
    .expect("capture");

    let context = context.expect("captured context");
    assert_eq!(context.items().count(), 2);
    assert_eq!(
        context.items().next().and_then(|item| item.wire_name()),
        Some("X-Tenant")
    );
    assert_eq!(
        context.items().nth(1).and_then(|item| item.value()),
        Some((HeaderValueKind::Binary, &[0x01, 0x02][..]))
    );
    assert_eq!(
        stats,
        Some(CaptureStats {
            skipped_max_entries: 1,
            skipped_name_too_long: 1,
            skipped_value_too_long: 1,
            skipped_context_too_large: 0,
        })
    );
}

/// Scenario: individually valid captured headers would exceed the 64 KiB envelope.
/// Guarantees: capture drops only the overflowing header and reports the aggregate limit.
#[test]
fn capture_drops_header_that_exceeds_context_size() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let value = vec![0; CaptureDefaults::default().max_value_bytes];
    let pairs = (0..16).map(|_| ("x", value.as_slice()));

    let (context, stats) = PdataContextBytes::capture(&policy, pairs).expect("capture");

    assert_eq!(context.expect("context").items().count(), 15);
    assert_eq!(
        stats,
        Some(CaptureStats {
            skipped_max_entries: 0,
            skipped_name_too_long: 0,
            skipped_value_too_long: 0,
            skipped_context_too_large: 1,
        })
    );
}

/// Scenario: schema-backed capture produces a context from a compiled policy.
/// Guarantees: the item carries a schema_index and the schema Arc is retained.
#[test]
fn schema_backed_capture_retains_schema() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: Some("tenant".to_string()),
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");

    assert!(Arc::ptr_eq(context.schema(), policy.schema()));
    let item = context.items().next().expect("one item");
    assert_eq!(item.schema_index(), Some(0));
    assert_eq!(item.stored_name(), Some("tenant"));
}

/// Scenario: a register is captured for an explicitly named egress mapping.
/// Guarantees: observed transport spelling is omitted from context bytes.
#[test]
fn capture_omits_unrequested_transport_name_provenance() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: Some("tenant".to_string()),
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile_for_generation_with_provenance(4, false)
    .expect("capture policy");
    let context = PdataContextBytes::capture(&policy, [("X-Tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let item = context.items().next().expect("captured register value");

    assert!(item.uses_schema_wire_name());
    assert_eq!(item.wire_name(), Some("x-tenant"));
    assert_eq!(
        context
            .schema()
            .compiled_context()
            .register_file()
            .version()
            .deployment_generation(),
        4
    );
}

/// Scenario: propagation maps a logical register to an explicit output header.
/// Guarantees: egress uses its compiled constant without reading a transport name from context.
#[test]
fn propagation_uses_explicit_compiled_output_name() {
    let capture = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-input-tenant".to_string()],
            store_as: Some("tenant".to_string()),
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile_for_generation_with_provenance(8, false)
    .expect("capture policy");
    let context = PdataContextBytes::capture(&capture, [("X-Input-Tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let propagation = HeaderPropagationPolicy::new(
        PropagationDefault {
            selector: PropagationSelector {
                selector_type: PropagationSelectorType::Named,
                named: Some(vec!["tenant".to_string()]),
            },
            output_name: Some("x-output-tenant".to_string()),
            ..PropagationDefault::default()
        },
        vec![],
    )
    .compile()
    .expect("propagation policy")
    .compile_schema(context.schema());

    let propagated = context.propagate(&propagation).collect::<Vec<_>>();

    assert_eq!(propagated.len(), 1);
    assert_eq!(propagated[0].header_name, "x-output-tenant");
    assert_eq!(propagated[0].value, b"acme");
}

/// Scenario: a value binding evaluates contexts sharing and changing schemas.
/// Guarantees: lookup preserves first-value order without runtime name matching.
#[test]
fn context_value_binding_resolves_schema_indices() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-tenant-bin".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: Some(ValueKindConfig::Binary),
            },
            CaptureRule {
                match_names: vec!["x-region".to_string()],
                store_as: Some("region".to_string()),
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(
        &policy,
        [
            ("X-Tenant", b"first".as_slice()),
            ("x-tenant-bin", b"second".as_slice()),
        ],
    )
    .expect("capture")
    .0
    .expect("context");
    let other = PdataContextBytes::capture(&policy, [("x-region", b"west".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let mut binding = ContextValueBinding::new("TENANT");

    assert_eq!(
        binding.value(&context),
        Some((HeaderValueKind::Text, b"first".as_slice()))
    );
    assert_eq!(binding.value(&other), None);
    assert_eq!(
        binding.value(&context),
        Some((HeaderValueKind::Text, b"first".as_slice()))
    );
}

/// Scenario: an entry-set binding evaluates equivalent entries across different schemas.
/// Guarantees: present entries are visited in configuration order, independent of schema order.
#[test]
fn context_entry_set_binding_uses_configuration_order_across_schemas() {
    let compile = |rules| {
        HeaderCapturePolicy::new(CaptureDefaults::default(), rules)
            .compile()
            .expect("capture policy")
    };
    let tenant = || CaptureRule {
        match_names: vec!["x-tenant".to_string()],
        store_as: Some("tenant".to_string()),
        sensitive: false,
        value_kind: None,
    };
    let region = || CaptureRule {
        match_names: vec!["x-region".to_string()],
        store_as: Some("region".to_string()),
        sensitive: false,
        value_kind: None,
    };
    let first_policy = compile(vec![tenant(), region()]);
    let second_policy = compile(vec![region(), tenant()]);
    let first = PdataContextBytes::capture(
        &first_policy,
        [
            ("x-tenant", b"acme".as_slice()),
            ("x-region", b"west".as_slice()),
        ],
    )
    .expect("capture")
    .0
    .expect("context");
    let second = PdataContextBytes::capture(
        &second_policy,
        [
            ("x-region", b"west".as_slice()),
            ("x-tenant", b"acme".as_slice()),
        ],
    )
    .expect("capture")
    .0
    .expect("context");
    let mut binding = ContextEntrySetBinding::new(["REGION", "TENANT"]);
    let collect = |binding: &mut ContextEntrySetBinding, context: &PdataContextBytes| {
        let mut visited = Vec::new();
        let count = binding.visit_present(context, |ordinal, entry| {
            visited.push((
                ordinal,
                entry
                    .values()
                    .map(|(_, value)| value.to_vec())
                    .collect::<Vec<_>>(),
            ));
        });
        (count, visited)
    };

    let expected = (
        2,
        vec![(0, vec![b"west".to_vec()]), (1, vec![b"acme".to_vec()])],
    );
    assert_eq!(collect(&mut binding, &first), expected);
    assert_eq!(collect(&mut binding, &second), expected);
    assert_eq!(collect(&mut binding, &first), expected);
}

/// Scenario: configured entries are absent from either the schema or the captured message.
/// Guarantees: missing and absent entries are skipped without hiding a present entry.
#[test]
fn context_entry_set_binding_skips_missing_and_absent_entries() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-region".to_string()],
                store_as: Some("region".to_string()),
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let mut binding = ContextEntrySetBinding::new(["unknown", "region", "tenant"]);
    let mut visited = Vec::new();

    let count = binding.visit_present(&context, |ordinal, _| visited.push(ordinal));

    assert_eq!(count, 1);
    assert_eq!(visited, [2]);
}

/// Scenario: a captured exact-name header has no explicit logical alias.
/// Guarantees: the compiler still assigns a register and eliminates its source name.
#[test]
fn context_entry_set_binding_selects_unaliased_exact_header() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(&policy, [("x-tenant", b"acme".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let mut binding = ContextEntrySetBinding::new(["x-tenant"]);
    let mut values = Vec::new();
    let register = context
        .schema()
        .compiled_context()
        .linker()
        .resolve("x-tenant")
        .expect("compiled register");

    let count = binding.visit_present(&context, |ordinal, entry| {
        values.push((
            ordinal,
            entry
                .values()
                .map(|(_, value)| value.to_vec())
                .collect::<Vec<_>>(),
        ));
    });

    assert_eq!(count, 1);
    assert_eq!(values, [(0, vec![b"acme".to_vec()])]);
    assert_eq!(
        context
            .schema()
            .compiled_context()
            .register_file()
            .shape(register),
        Some(&ContextRegisterShape::KeyValueList(
            ContextScalarType::AnyValue
        ))
    );
    assert_eq!(
        context
            .register(register)
            .expect("present register")
            .key_values()
            .collect::<Vec<_>>(),
        vec![("x-tenant", HeaderValueKind::Text, b"acme".as_slice())]
    );
}

/// Scenario: projection preserves input entries and appends a new singleton entry.
/// Guarantees: old entries/hashes/items are unchanged; the new entry slot is present,
///   contains the projected value, and uses the derived schema index.
#[test]
fn projection_preserves_input_and_appends_entry() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-request-id".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");
    let input = PdataContextBytes::capture(
        &policy,
        [
            ("x-tenant", b"acme".as_slice()),
            ("x-request-id", b"req-1".as_slice()),
        ],
    )
    .expect("capture")
    .0
    .expect("context");
    let old_hash = input.entry(0).expect("tenant entry").hash();

    let mut binding = PartitionProjectionBinding::new("partition");
    let output = binding
        .project(Some(&input), b"west", HeaderValueKind::Text)
        .expect("projection");

    // Old entry hash preserved
    assert_eq!(output.entry(0).expect("tenant entry").hash(), old_hash);
    // Original items preserved with original schema indices
    assert_eq!(
        output.items().nth(0).and_then(|i| i.schema_index()),
        Some(0)
    );
    assert_eq!(
        output.items().nth(1).and_then(|i| i.schema_index()),
        Some(1)
    );
    // Appended item has derived schema index and entry slot
    let appended = output.items().nth(2).expect("appended item");
    assert_eq!(appended.schema_index(), Some(2));
    assert_eq!(appended.entry_slot(), Some(2));
    assert_eq!(appended.wire_name(), Some("partition"));
    assert_eq!(
        appended.value(),
        Some((HeaderValueKind::Text, b"west".as_slice()))
    );
    // New entry is present and contains the projected value
    let new_entry = output.entry(2).expect("partition entry must be present");
    assert_eq!(
        new_entry.values().collect::<Vec<_>>(),
        vec![(HeaderValueKind::Text, b"west".as_slice())]
    );
    // Output schema is different from input schema
    assert!(!Arc::ptr_eq(output.schema(), input.schema()));
    assert_eq!(output.schema().len(), 3);
}

/// Scenario: projection with no input creates a singleton entry context.
/// Guarantees: the context has one item in one present entry, using the
///   standalone schema (not schema-less).
#[test]
fn projection_without_input_creates_singleton_entry() {
    let mut binding = PartitionProjectionBinding::new("partition");
    let output = binding
        .project(None, b"east", HeaderValueKind::Text)
        .expect("projection");

    assert_eq!(output.items().count(), 1);
    let item = output.items().next().expect("one item");
    assert_eq!(item.schema_index(), Some(0));
    assert_eq!(item.entry_slot(), Some(0));
    assert_eq!(item.wire_name(), Some("partition"));
    assert_eq!(
        item.value(),
        Some((HeaderValueKind::Text, b"east".as_slice()))
    );
    // The singleton entry is present
    let entry = output.entry(0).expect("singleton entry must be present");
    assert_eq!(
        entry.values().collect::<Vec<_>>(),
        vec![(HeaderValueKind::Text, b"east".as_slice())]
    );
    assert_eq!(output.schema().len(), 1);
}

/// Scenario: a mixed-case register symbol is projected with and without input context.
/// Guarantees: both projection paths expose the canonical compile-time symbol.
#[test]
fn projection_canonicalizes_configured_register_symbol() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-input".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let input = PdataContextBytes::capture(&policy, [("x-input", b"value".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let mut binding = PartitionProjectionBinding::new("X-Partition");

    let standalone = binding
        .project(None, b"east", HeaderValueKind::Text)
        .expect("standalone projection");
    let appended = binding
        .project(Some(&input), b"west", HeaderValueKind::Text)
        .expect("appended projection");

    assert_eq!(
        standalone.items().next().and_then(|item| item.wire_name()),
        Some("x-partition")
    );
    assert_eq!(
        appended.items().nth(1).and_then(|item| item.wire_name()),
        Some("x-partition")
    );
}

/// Scenario: distinct input schemas produce isolated derived schemas in the binding cache.
/// Guarantees: two different input schemas produce different derived schemas with
///   independent schema_index and entry_slot assignments.
#[test]
fn distinct_input_schemas_produce_isolated_derived_schemas() {
    let policy_a = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-a".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("policy a");
    let policy_b = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-b1".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["x-b2".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("policy b");

    let ctx_a = PdataContextBytes::capture(&policy_a, [("x-a", b"val-a".as_slice())])
        .expect("capture a")
        .0
        .expect("context a");
    let ctx_b = PdataContextBytes::capture(
        &policy_b,
        [
            ("x-b1", b"val-b1".as_slice()),
            ("x-b2", b"val-b2".as_slice()),
        ],
    )
    .expect("capture b")
    .0
    .expect("context b");

    let mut binding = PartitionProjectionBinding::new("part");
    let out_a = binding
        .project(Some(&ctx_a), b"1", HeaderValueKind::Text)
        .expect("project a");
    let out_b = binding
        .project(Some(&ctx_b), b"2", HeaderValueKind::Text)
        .expect("project b");

    // Derived schemas are distinct
    assert!(!Arc::ptr_eq(out_a.schema(), out_b.schema()));
    // Schema A has 1 original + 1 appended = 2
    assert_eq!(out_a.schema().len(), 2);
    // Schema B has 2 original + 1 appended = 3
    assert_eq!(out_b.schema().len(), 3);
    // Appended item index differs
    assert_eq!(out_a.items().nth(1).and_then(|i| i.schema_index()), Some(1));
    assert_eq!(out_b.items().nth(2).and_then(|i| i.schema_index()), Some(2));
    // The projected register follows each input register file.
    let entry_a = out_a.entry(1).expect("partition entry in a");
    assert_eq!(
        entry_a.values().collect::<Vec<_>>(),
        vec![(HeaderValueKind::Text, b"1".as_slice())]
    );
    let entry_b = out_b.entry(2).expect("partition entry in b");
    assert_eq!(
        entry_b.values().collect::<Vec<_>>(),
        vec![(HeaderValueKind::Text, b"2".as_slice())]
    );
}

/// Scenario: a near-full context cannot fit the projected partition header.
/// Guarantees: projection returns TooLarge error without corrupting the input.
#[test]
fn projection_overflow_remains_error() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults {
            max_value_bytes: 65_470,
            ..CaptureDefaults::default()
        },
        vec![CaptureRule {
            match_names: vec!["x".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    // Create a context near the 64 KiB limit
    let big_value = vec![0u8; 65_470];
    let input = PdataContextBytes::capture(&policy, [("x", big_value.as_slice())])
        .expect("capture")
        .0
        .expect("context");

    let mut binding = PartitionProjectionBinding::new("partition");
    let result = binding.project(Some(&input), b"overflow-value", HeaderValueKind::Text);
    assert!(
        result.is_err(),
        "projection should fail on near-limit context"
    );
}

/// Scenario: named propagation selects one entry and drops another item by override.
/// Guarantees: propagation applies selector, override, and stored-name semantics in place.
#[test]
fn packed_propagation_applies_named_selector_and_override() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![
            CaptureRule {
                match_names: vec!["x-tenant".to_string()],
                store_as: Some("tenant".to_string()),
                sensitive: false,
                value_kind: None,
            },
            CaptureRule {
                match_names: vec!["authorization".to_string()],
                store_as: None,
                sensitive: false,
                value_kind: None,
            },
        ],
    )
    .compile()
    .expect("capture policy");
    let context = PdataContextBytes::capture(
        &policy,
        [
            ("X-Tenant", b"acme".as_slice()),
            ("Authorization", b"secret".as_slice()),
        ],
    )
    .expect("capture")
    .0
    .expect("context");
    let propagation = HeaderPropagationPolicy::new(
        PropagationDefault {
            selector: PropagationSelector {
                selector_type: PropagationSelectorType::Named,
                named: Some(vec!["tenant".to_string(), "authorization".to_string()]),
            },
            name: NameStrategy::StoredName,
            ..PropagationDefault::default()
        },
        vec![PropagationOverride {
            match_rule: PropagationMatch {
                stored_names: vec!["authorization".to_string()],
            },
            action: PropagationAction::Drop,
            name: None,
            output_name: None,
            on_error: None,
        }],
    )
    .compile()
    .expect("propagation policy");
    let propagation = propagation.compile_schema(context.schema());

    let propagated: Vec<_> = context.propagate(&propagation).collect();
    assert_eq!(propagated.len(), 1);
    assert_eq!(propagated[0].header_name, "tenant");
    assert_eq!(propagated[0].value, b"acme");
    assert_eq!(propagated[0].value_kind, HeaderValueKind::Text);
}

/// Scenario: the binding cache is hit on repeated projections with the same input schema.
/// Guarantees: the same derived schema Arc is reused (pointer equality).
#[test]
fn binding_cache_reuses_derived_schema() {
    let policy = HeaderCapturePolicy::new(
        CaptureDefaults::default(),
        vec![CaptureRule {
            match_names: vec!["x-tenant".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        }],
    )
    .compile()
    .expect("capture policy");
    let ctx1 = PdataContextBytes::capture(&policy, [("x-tenant", b"a".as_slice())])
        .expect("capture")
        .0
        .expect("context");
    let ctx2 = PdataContextBytes::capture(&policy, [("x-tenant", b"b".as_slice())])
        .expect("capture")
        .0
        .expect("context");

    let mut binding = PartitionProjectionBinding::new("part");
    let out1 = binding
        .project(Some(&ctx1), b"1", HeaderValueKind::Text)
        .expect("project 1");
    let out2 = binding
        .project(Some(&ctx2), b"2", HeaderValueKind::Text)
        .expect("project 2");

    // Same input schema Arc -> same derived schema Arc
    assert!(Arc::ptr_eq(out1.schema(), out2.schema()));
}

/// Scenario: the binding cache exceeds SCHEMA_CACHE_CAPACITY distinct schemas.
/// Guarantees: the oldest entry is evicted (FIFO) and the binding still produces
///   correct projections for both cached and evicted schemas.
#[test]
fn binding_cache_evicts_oldest_when_full() {
    // Create SCHEMA_CACHE_CAPACITY + 1 distinct policies/schemas
    let policies: Vec<_> = (0..SCHEMA_CACHE_CAPACITY + 1)
        .map(|i| {
            HeaderCapturePolicy::new(
                CaptureDefaults::default(),
                vec![CaptureRule {
                    match_names: vec![format!("x-h{i}")],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                }],
            )
            .compile()
            .expect("capture policy")
        })
        .collect();
    let contexts: Vec<_> = policies
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let name = format!("x-h{i}");
            PdataContextBytes::capture(p, [(name.as_str(), b"v".as_slice())])
                .expect("capture")
                .0
                .expect("context")
        })
        .collect();

    let mut binding = PartitionProjectionBinding::new("part");

    // Fill the cache to capacity
    let mut schemas: Vec<Arc<CompiledHeaderSchema>> = Vec::with_capacity(SCHEMA_CACHE_CAPACITY + 1);
    for ctx in &contexts[..SCHEMA_CACHE_CAPACITY] {
        let out = binding
            .project(Some(ctx), b"x", HeaderValueKind::Text)
            .expect("project");
        schemas.push(out.schema().clone());
    }

    // The first schema is still cached
    let out_first = binding
        .project(Some(&contexts[0]), b"y", HeaderValueKind::Text)
        .expect("project first again");
    assert!(Arc::ptr_eq(out_first.schema(), &schemas[0]));

    // Add one more distinct schema -- should evict the first
    let out_overflow = binding
        .project(
            Some(&contexts[SCHEMA_CACHE_CAPACITY]),
            b"z",
            HeaderValueKind::Text,
        )
        .expect("project overflow");
    assert_eq!(out_overflow.schema().len(), 2);

    // The first schema is now evicted -- re-projection produces a new Arc
    let out_first_again = binding
        .project(Some(&contexts[0]), b"w", HeaderValueKind::Text)
        .expect("project first after eviction");
    assert!(
        !Arc::ptr_eq(out_first_again.schema(), &schemas[0]),
        "evicted schema should produce a fresh derived Arc"
    );
    // But the result is still correct
    let entry = out_first_again
        .entry(1)
        .expect("partition entry after eviction");
    assert_eq!(
        entry.values().collect::<Vec<_>>(),
        vec![(HeaderValueKind::Text, b"w".as_slice())]
    );
}
