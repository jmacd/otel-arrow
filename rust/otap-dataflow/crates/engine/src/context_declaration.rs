// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Collects factory declarations before runtime construction.
//!
//! Configuration-only declarations, layouts, requirements, and binding compilation
//! live in `otel_arrow_dfe_config::context_layout` and are re-exported here.

use crate::PipelineFactory;
use crate::error::Error as EngineError;
use otel_arrow_dfe_config::PipelineKey;
use otel_arrow_dfe_config::authorized_identity_policy::AuthorizedIdentityPolicy;
pub use otel_arrow_dfe_config::context_layout::{
    CompiledContextBindings, ContextConsumerSelector, ContextDeclaration,
    ContextDeclarationsByPipeline, ContextEntrySelector, ContextEntrySelectorForm,
    ContextRuntimeRequirements, NodeContextDeclarations, PreparedContext,
};
use otel_arrow_dfe_config::engine::ResolvedOtelDataflowSpec;
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::transport_headers_policy::TransportHeadersPolicy;
use std::collections::HashMap;

/// Derives context declarations from component configuration.
#[derive(Clone, Copy)]
pub struct ContextDeclarationProvider {
    /// Declaration callback.
    pub declarations: ContextDeclarationFn,
}

/// Derives deterministic context declarations from node configuration.
pub type ContextDeclarationFn = fn(&serde_json::Value) -> Result<NodeContextDeclarations, Error>;

/// Context declarations derived from typed node configuration.
pub trait ConfigNodeContextDeclaration: serde::de::DeserializeOwned {
    /// Declares the context reads and writes for this configuration.
    fn context_declarations(&self) -> NodeContextDeclarations;

    /// Checks these declarations against the compiled bindings.
    fn validate_context_declarations(
        &self,
        pipeline_ctx: &crate::context::PipelineContext,
    ) -> Result<(), Error> {
        pipeline_ctx
            .compiled_context_bindings()
            .validate_node_declarations(
                &pipeline_ctx.pipeline_key(),
                &pipeline_ctx.node_id(),
                &self.context_declarations(),
            )
    }
}

impl ContextDeclarationProvider {
    /// Creates a declaration provider for a configuration type.
    #[must_use]
    pub const fn from_typed_config<T>() -> Self
    where
        T: ConfigNodeContextDeclaration,
    {
        Self {
            declarations: typed_context_declarations::<T>,
        }
    }
}

fn typed_context_declarations<T>(
    config: &serde_json::Value,
) -> Result<NodeContextDeclarations, Error>
where
    T: ConfigNodeContextDeclaration,
{
    Ok(
        otel_arrow_dfe_config::validation::deserialize_typed_config::<T>(config)?
            .context_declarations(),
    )
}

impl<PData: 'static + Clone + std::fmt::Debug> PipelineFactory<PData> {
    /// Compiles startup requirements and node bindings from the same declarations.
    pub fn compile_initial_context(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
    ) -> Result<PreparedContext, EngineError> {
        let declarations = self.context_declarations(resolved)?;
        PreparedContext::compile(declarations, resolved, None)
            .map_err(|error| EngineError::ConfigError(Box::new(error)))
    }

    /// Compiles candidate bindings using the immutable installed requirements.
    pub fn compile_candidate_context(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
        installed_requirements: &ContextRuntimeRequirements,
    ) -> Result<PreparedContext, EngineError> {
        let declarations = self.context_declarations(resolved)?;
        PreparedContext::compile(declarations, resolved, Some(installed_requirements))
            .map_err(|error| EngineError::ConfigError(Box::new(error)))
    }

    fn context_declarations(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
    ) -> Result<ContextDeclarationsByPipeline, EngineError> {
        let mut declarations = ContextDeclarationsByPipeline::new();

        for pipeline in &resolved.pipelines {
            let pipeline_key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let mut declarations_by_node = HashMap::new();
            for (node_id, node_config) in pipeline.pipeline.node_iter() {
                let component_declarations = self.node_context_declarations(
                    node_config.kind(),
                    node_config.r#type.as_ref(),
                    &node_config.config,
                )?;
                let wrapper_declarations = Self::wrapper_context_declarations(
                    node_config,
                    &pipeline.policies.transport_headers,
                    &pipeline.policies.authorized_identity,
                );
                let declarations = component_declarations
                    .into_iter()
                    .chain(wrapper_declarations)
                    .collect();
                let _ = declarations_by_node.insert(node_id.clone(), declarations);
            }
            let _ = declarations.insert(pipeline_key, declarations_by_node);
        }

        Ok(declarations)
    }

    fn wrapper_context_declarations(
        node: &NodeUserConfig,
        pipeline_policy: &Option<TransportHeadersPolicy>,
        authorized_identity: &Option<AuthorizedIdentityPolicy>,
    ) -> NodeContextDeclarations {
        match node.kind() {
            NodeKind::Receiver => node
                .header_capture
                .as_ref()
                .or_else(|| {
                    pipeline_policy
                        .as_ref()
                        .map(|policy| &policy.header_capture)
                })
                .cloned()
                .map(|policy| ContextDeclaration::HeaderCapture { policy })
                .into_iter()
                .chain(
                    authorized_identity
                        .as_ref()
                        .filter(|policy| !policy.is_empty())
                        .cloned()
                        .map(|policy| ContextDeclaration::AuthorizedIdentityCapture { policy }),
                )
                .collect(),
            NodeKind::Exporter => {
                let policy = node.header_propagation.as_ref().or_else(|| {
                    pipeline_policy
                        .as_ref()
                        .map(|policy| &policy.header_propagation)
                });
                policy
                    .cloned()
                    .map(|policy| ContextDeclaration::HeaderPropagation { policy })
                    .into_iter()
                    .collect()
            }
            NodeKind::Processor => NodeContextDeclarations::default(),
        }
    }

    fn node_context_declarations(
        &self,
        kind: NodeKind,
        urn: &str,
        config: &serde_json::Value,
    ) -> Result<NodeContextDeclarations, EngineError> {
        let missing_factory = || {
            EngineError::ConfigError(Box::new(Error::InvalidUserConfig {
                error: format!("node factory `{urn}` is not registered"),
            }))
        };
        let (validate_config, context_declarations) = match kind {
            NodeKind::Receiver => {
                let factory = self
                    .get_receiver_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?;
                (factory.validate_config, factory.context_declarations)
            }
            NodeKind::Processor => {
                let factory = self
                    .get_processor_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?;
                (factory.validate_config, factory.context_declarations)
            }
            NodeKind::Exporter => {
                let factory = self
                    .get_exporter_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?;
                (factory.validate_config, factory.context_declarations)
            }
        };
        // Validate before collecting declarations. Nodes are not constructed yet.
        validate_config(config).map_err(|error| EngineError::ConfigError(Box::new(error)))?;

        let declarations = context_declarations
            .map(|provider| {
                (provider.declarations)(config)
                    .map_err(|error| EngineError::ConfigError(Box::new(error)))
            })
            .transpose()?
            .unwrap_or_default();
        // Capture and propagation declarations belong to the engine.
        if let Some(declaration) = declarations
            .iter()
            .find(|declaration| !declaration.is_component_declaration())
        {
            return Err(EngineError::ConfigError(Box::new(
                Error::InvalidUserConfig {
                    error: format!(
                        "node factory `{urn}` returned engine-owned context declaration \
                         `{declaration:?}`"
                    ),
                },
            )));
        }
        Ok(declarations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::context_policy::ContextEntryDeclaration as ConfigContextEntryDeclaration;
    use otel_arrow_dfe_config::transport_headers::{TransportHeader, TransportHeaders};
    use otel_arrow_dfe_config::transport_headers_policy::{
        CaptureDefaults, CaptureRule, HeaderCapturePolicy, HeaderPropagationPolicy,
    };
    use otel_arrow_dfe_config::{ContextEntryName, NodeId as ConfigNodeId};
    use std::sync::Arc;

    /// Typed component configuration used to exercise declaration validation.
    #[derive(serde::Deserialize)]
    struct TestDeclarationConfig {
        entry: ContextEntryName,
    }

    impl ConfigNodeContextDeclaration for TestDeclarationConfig {
        fn context_declarations(&self) -> NodeContextDeclarations {
            [ContextDeclaration::Consumes {
                selector: ContextConsumerSelector::Entries {
                    entries: vec![ContextEntrySelector {
                        name: self.entry.clone(),
                        form: ContextEntrySelectorForm::Value,
                    }]
                    .into_boxed_slice(),
                },
            }]
            .into_iter()
            .collect()
        }
    }

    fn pipeline(group: &str, name: &str) -> PipelineKey {
        PipelineKey::new(group.to_owned().into(), name.to_owned().into())
    }

    fn context_name(name: &str) -> ContextEntryName {
        name.try_into().expect("valid test context entry name")
    }

    fn unused_test_receiver(
        _: crate::context::PipelineContext,
        _: crate::node::NodeId,
        _: Arc<NodeUserConfig>,
        _: &crate::config::ReceiverConfig,
        _: &crate::capability::registry::Capabilities,
    ) -> Result<crate::receiver::ReceiverWrapper<()>, Error> {
        unreachable!("context compilation does not construct test nodes")
    }

    fn unused_test_exporter(
        _: crate::context::PipelineContext,
        _: crate::node::NodeId,
        _: Arc<NodeUserConfig>,
        _: &crate::config::ExporterConfig,
        _: &crate::capability::registry::Capabilities,
    ) -> Result<crate::exporter::ExporterWrapper<()>, Error> {
        unreachable!("context compilation does not construct test nodes")
    }

    fn unused_test_processor(
        _: crate::context::PipelineContext,
        _: crate::node::NodeId,
        _: Arc<NodeUserConfig>,
        _: &crate::config::ProcessorConfig,
        _: &crate::capability::registry::Capabilities,
    ) -> Result<crate::processor::ProcessorWrapper<()>, Error> {
        unreachable!("context compilation does not construct test nodes")
    }

    fn accept_test_config(_: &serde_json::Value) -> Result<(), Error> {
        Ok(())
    }

    static TEST_RECEIVERS: [crate::ReceiverFactory<()>; 2] = [
        crate::ReceiverFactory {
            name: "urn:test:receiver:example",
            create: unused_test_receiver,
            context_declarations: None,
            wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        },
        crate::ReceiverFactory {
            name: "urn:otel:receiver:internal_telemetry",
            create: unused_test_receiver,
            context_declarations: None,
            wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
            validate_config: accept_test_config,
        },
    ];

    static TEST_EXPORTERS: [crate::ExporterFactory<()>; 3] = [
        crate::ExporterFactory {
            name: "urn:test:exporter:example",
            create: unused_test_exporter,
            context_declarations: None,
            wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        },
        crate::ExporterFactory {
            name: "urn:otel:exporter:noop",
            create: unused_test_exporter,
            context_declarations: None,
            wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        },
        crate::ExporterFactory {
            name: "urn:otel:exporter:console",
            create: unused_test_exporter,
            context_declarations: None,
            wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        },
    ];

    static TEST_PROCESSORS: [crate::ProcessorFactory<()>; 1] = [crate::ProcessorFactory {
        name: "urn:otel:processor:type_router",
        create: unused_test_processor,
        context_declarations: None,
        wiring_contract: crate::wiring_contract::WiringContract::UNRESTRICTED,
        validate_config: accept_test_config,
    }];

    fn test_pipeline_factory() -> PipelineFactory<()> {
        PipelineFactory::new(&TEST_RECEIVERS, &TEST_PROCESSORS, &TEST_EXPORTERS, &[])
    }

    fn conditional_pipeline_yaml(composite: &str, selector: &str) -> String {
        format!(
            r#"
version: otel_dataflow/v1
policies:
  context:
    entries:
      tenant: {composite}
engine: {{}}
groups:
  default:
    pipelines:
      main:
        nodes:
          receiver:
            type: "urn:test:receiver:example"
            config: {{}}
          exporter:
            type: "urn:test:exporter:example"
            header_propagation:
              default:
                selector:
                  type: named
                  named: [{selector}]
                name: stored_name
            config: {{}}
        connections:
          - from: receiver
            to: exporter
"#
        )
    }

    fn resolve_conditional_pipeline(composite: &str, selector: &str) -> ResolvedOtelDataflowSpec {
        otel_arrow_dfe_config::engine::OtelDataflowSpec::from_yaml(&conditional_pipeline_yaml(
            composite, selector,
        ))
        .expect("conditional pipeline YAML is valid")
        .resolve()
    }

    fn declarations_by_pipeline(
        effective: NodeContextDeclarations,
    ) -> ContextDeclarationsByPipeline {
        HashMap::from([(
            pipeline("group", "pipeline"),
            HashMap::from([(ConfigNodeId::from("node"), effective)]),
        )])
    }

    fn compiled_bindings(effective: NodeContextDeclarations) -> CompiledContextBindings {
        let declarations = declarations_by_pipeline(effective);
        let requirements = ContextRuntimeRequirements::compile(&declarations);
        CompiledContextBindings::compile(declarations, &requirements)
    }

    fn context_runtime_requirements(
        effective: NodeContextDeclarations,
    ) -> ContextRuntimeRequirements {
        ContextRuntimeRequirements::compile(&declarations_by_pipeline(effective))
    }

    /// Scenario: capture aliases share a stored name.
    /// Guarantees: each stored name determines original-name retention.
    #[test]
    fn compiled_capture_policy_tracks_each_match_name() {
        let capture = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec![context_name("x-first"), context_name("x-second")],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec![context_name("x-alias-a"), context_name("x-alias-b")],
                    store_as: Some(context_name("canonical")),
                    sensitive: false,
                    value_kind: None,
                },
            ],
        );
        let bindings = compiled_bindings(
            [
                ContextDeclaration::Consumes {
                    selector: ContextConsumerSelector::Entries {
                        entries: ["x-first", "canonical"]
                            .map(|name| ContextEntrySelector {
                                name: context_name(name),
                                form: ContextEntrySelectorForm::OriginalKeyValue,
                            })
                            .into(),
                    },
                },
                ContextDeclaration::HeaderCapture { policy: capture },
            ]
            .into_iter()
            .collect(),
        );
        let capture = bindings
            .header_capture_policy(&pipeline("group", "pipeline"), &ConfigNodeId::from("node"))
            .expect("compiled capture policy");
        let mut headers = TransportHeaders::new();

        let _ = capture.capture_from_pairs(
            [
                ("X-First", b"first".as_slice()),
                ("X-Second", b"second".as_slice()),
                ("X-Alias-A", b"alias-a".as_slice()),
                ("X-Alias-B", b"alias-b".as_slice()),
            ]
            .into_iter(),
            &mut headers,
        );

        assert_eq!(headers.get(0).expect("first header").wire_name(), "X-First");
        assert_eq!(
            headers.get(1).expect("second header").wire_name(),
            "x-second"
        );
        assert_eq!(
            headers.get(2).expect("first alias").wire_name(),
            "X-Alias-A"
        );
        assert_eq!(
            headers.get(3).expect("second alias").wire_name(),
            "X-Alias-B"
        );
    }

    /// Scenario: consumers request different name representations.
    /// Guarantees: only `OriginalKeyValue` requires original names.
    #[test]
    fn declarations_require_only_the_requested_name_form() {
        let declarations: NodeContextDeclarations = [
            ContextDeclaration::Consumes {
                selector: ContextConsumerSelector::Entries {
                    entries: vec![ContextEntrySelector {
                        name: context_name("original"),
                        form: ContextEntrySelectorForm::OriginalKeyValue,
                    }]
                    .into_boxed_slice(),
                },
            },
            ContextDeclaration::Consumes {
                selector: ContextConsumerSelector::Entries {
                    entries: vec![ContextEntrySelector {
                        name: context_name("value"),
                        form: ContextEntrySelectorForm::Value,
                    }]
                    .into_boxed_slice(),
                },
            },
        ]
        .into_iter()
        .collect();
        let requirements = context_runtime_requirements(declarations);
        assert!(requirements.preserves_original_name(&context_name("original")));
        assert!(!requirements.preserves_original_name(&context_name("value")));
    }

    /// Scenario: propagation preserves arbitrary names but overrides one stored name.
    /// Guarantees: the profile uses a true default with one case-insensitive exception.
    #[test]
    fn requirements_canonicalize_default_and_overrides() {
        let propagation: HeaderPropagationPolicy = serde_json::from_value(serde_json::json!({
            "default": {
                "selector": {"type": "all_captured"},
                "name": "preserve"
            },
            "overrides": [{
                "match": {"stored_names": ["Authorization"]},
                "name": "stored_name"
            }]
        }))
        .expect("valid propagation policy");
        let requirements = context_runtime_requirements(
            [ContextDeclaration::HeaderPropagation {
                policy: propagation,
            }]
            .into_iter()
            .collect(),
        );

        assert!(!requirements.preserves_original_name(&context_name("AUTHORIZATION")));
        assert!(requirements.preserves_original_name(&context_name("X-Tenant")));
        assert!(requirements.preserves_original_name(&context_name("arbitrary-new-name")));
    }

    /// Scenario: live declarations add and remove original-name consumers.
    /// Guarantees: installed requirements allow subsets but reject unsupported names and defaults.
    #[test]
    fn installed_requirements_support_only_available_original_names() {
        let installed = context_runtime_requirements(
            [
                ContextDeclaration::Consumes {
                    selector: ContextConsumerSelector::Entries {
                        entries: vec![ContextEntrySelector {
                            name: context_name("x-tenant"),
                            form: ContextEntrySelectorForm::OriginalKeyValue,
                        }]
                        .into_boxed_slice(),
                    },
                },
                ContextDeclaration::Consumes {
                    selector: ContextConsumerSelector::Entries {
                        entries: vec![ContextEntrySelector {
                            name: context_name("authorization"),
                            form: ContextEntrySelectorForm::Value,
                        }]
                        .into_boxed_slice(),
                    },
                },
            ]
            .into_iter()
            .collect(),
        );
        let removed = context_runtime_requirements(NodeContextDeclarations::default());
        let supported = context_runtime_requirements(
            [ContextDeclaration::Consumes {
                selector: ContextConsumerSelector::Entries {
                    entries: vec![ContextEntrySelector {
                        name: context_name("X-Tenant"),
                        form: ContextEntrySelectorForm::OriginalKeyValue,
                    }]
                    .into_boxed_slice(),
                },
            }]
            .into_iter()
            .collect(),
        );
        let unsupported_name = context_runtime_requirements(
            [ContextDeclaration::Consumes {
                selector: ContextConsumerSelector::Entries {
                    entries: vec![ContextEntrySelector {
                        name: context_name("x-request-id"),
                        form: ContextEntrySelectorForm::OriginalKeyValue,
                    }]
                    .into_boxed_slice(),
                },
            }]
            .into_iter()
            .collect(),
        );
        let unsupported_default = context_runtime_requirements(
            [ContextDeclaration::HeaderPropagation {
                policy: serde_json::from_value(serde_json::json!({
                    "default": {
                        "selector": {"type": "all_captured"},
                        "name": "preserve"
                    }
                }))
                .expect("valid propagation policy"),
            }]
            .into_iter()
            .collect(),
        );

        assert!(installed.can_satisfy(&removed));
        assert!(installed.can_satisfy(&supported));
        assert!(!installed.can_satisfy(&unsupported_name));
        assert!(!installed.can_satisfy(&unsupported_default));
    }

    /// Scenario: a propagation declaration selects one original header name.
    /// Guarantees: prepared bindings keep the policy and require only that original name.
    #[test]
    fn header_propagation_policy_is_a_context_declaration() {
        let policy: HeaderPropagationPolicy = serde_json::from_value(serde_json::json!({
            "default": {
                "selector": {
                    "type": "named",
                    "named": ["preserved"]
                },
                "name": "preserve"
            }
        }))
        .expect("valid propagation policy");
        let declarations: NodeContextDeclarations = [ContextDeclaration::HeaderPropagation {
            policy: policy.clone(),
        }]
        .into_iter()
        .collect();
        let compiled = compiled_bindings(declarations.clone());

        let requirements = context_runtime_requirements(declarations.clone());
        assert!(requirements.preserves_original_name(&context_name("preserved")));
        assert!(!requirements.preserves_original_name(&context_name("other")));
        assert_eq!(
            compiled.header_propagation_policy(
                &pipeline("group", "pipeline"),
                &ConfigNodeId::from("node")
            ),
            Some(&policy),
        );
    }

    /// Scenario: node and pipeline header policies and an identity policy are configured.
    /// Guarantees: node header policies take precedence, pipeline headers provide the fallback,
    /// and authorized identity capture is declared only for receivers.
    #[test]
    fn wrapper_declarations_resolve_policy_precedence() {
        let identity_policy: AuthorizedIdentityPolicy =
            serde_json::from_value(serde_json::json!([{"claim": "sub", "store_as": "tenant"}]))
                .expect("valid authorized identity policy");
        let node_capture = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec![context_name("node")],
                store_as: None,
                sensitive: false,
                value_kind: None,
            }],
        );
        let pipeline_policy = TransportHeadersPolicy {
            header_capture: HeaderCapturePolicy::new(
                CaptureDefaults::default(),
                vec![CaptureRule {
                    match_names: vec![context_name("pipeline")],
                    store_as: None,
                    sensitive: false,
                    value_kind: None,
                }],
            ),
            ..Default::default()
        };
        let mut receiver = NodeUserConfig::new_receiver_config("urn:test:receiver:example");
        receiver.header_capture = Some(node_capture.clone());

        assert_eq!(
            PipelineFactory::<()>::wrapper_context_declarations(
                &receiver,
                &Some(pipeline_policy.clone()),
                &Some(identity_policy.clone()),
            ),
            [
                ContextDeclaration::HeaderCapture {
                    policy: node_capture,
                },
                ContextDeclaration::AuthorizedIdentityCapture {
                    policy: identity_policy.clone(),
                },
            ]
            .into_iter()
            .collect(),
        );

        let receiver = NodeUserConfig::new_receiver_config("urn:test:receiver:example");
        assert_eq!(
            PipelineFactory::<()>::wrapper_context_declarations(
                &receiver,
                &Some(pipeline_policy.clone()),
                &Some(identity_policy.clone()),
            ),
            [
                ContextDeclaration::HeaderCapture {
                    policy: pipeline_policy.header_capture.clone(),
                },
                ContextDeclaration::AuthorizedIdentityCapture {
                    policy: identity_policy.clone(),
                },
            ]
            .into_iter()
            .collect(),
        );

        let mut exporter = NodeUserConfig::new_exporter_config("urn:test:exporter:example");
        let node_propagation = HeaderPropagationPolicy::default();
        exporter.header_propagation = Some(node_propagation.clone());
        assert_eq!(
            PipelineFactory::<()>::wrapper_context_declarations(
                &exporter,
                &Some(pipeline_policy.clone()),
                &Some(identity_policy.clone()),
            ),
            [ContextDeclaration::HeaderPropagation {
                policy: node_propagation,
            }]
            .into_iter()
            .collect(),
        );

        let exporter = NodeUserConfig::new_exporter_config("urn:test:exporter:example");
        assert_eq!(
            PipelineFactory::<()>::wrapper_context_declarations(
                &exporter,
                &Some(pipeline_policy.clone()),
                &Some(identity_policy),
            ),
            [ContextDeclaration::HeaderPropagation {
                policy: pipeline_policy.header_propagation,
            }]
            .into_iter()
            .collect(),
        );
    }

    /// Scenario: an exporter selects a conditional composite transport-header member.
    /// Guarantees: collected wrapper declarations use the shared compiler before installing policy.
    #[test]
    fn wrapper_compiles_conditional_composite_header_propagation() {
        let context: otel_arrow_dfe_config::context_policy::ContextPolicy = serde_yaml::from_str(
            r#"
entries:
  tenant:
    - type: transport_header
      name: workspace
      store_as: workspace_id
    - type: transport_header_match
      name: environment
      value: production
"#,
        )
        .expect("valid context policy");
        let (name, definition) = context.entries.into_iter().next().expect("declaration");
        let declaration = ConfigContextEntryDeclaration {
            scope: otel_arrow_dfe_config::context_policy::ContextScope::Engine,
            name,
            definition,
        };
        let mut exporter = NodeUserConfig::new_exporter_config("urn:test:exporter:example");
        exporter.header_propagation = Some(
            serde_yaml::from_str(
                r#"
default:
  selector:
    type: named
    named: [tenant:workspace_id]
  name: stored_name
"#,
            )
            .expect("valid propagation policy"),
        );

        let declarations =
            PipelineFactory::<()>::wrapper_context_declarations(&exporter, &None, &None);
        let ContextDeclaration::HeaderPropagation { policy } =
            declarations.iter().next().expect("propagation declaration")
        else {
            panic!("expected header propagation declaration");
        };
        let policy = policy
            .clone()
            .compile_context(&[declaration])
            .expect("compiled policy");
        let mut headers = TransportHeaders::new();
        headers.push(TransportHeader::text(context_name("workspace"), b"acme"));
        assert_eq!(policy.propagate(&headers).count(), 0);
        headers.push(TransportHeader::text(
            context_name("environment"),
            b"production",
        ));

        let propagated = policy.propagate(&headers).collect::<Vec<_>>();
        assert_eq!(propagated.len(), 1);
        assert_eq!(propagated[0].header_name, "workspace_id");
        assert_eq!(propagated[0].value, b"acme");
    }

    /// Scenario: complete YAML changes a composite condition or selected member during a live update.
    /// Guarantees: resolution compiles an effective exporter binding and reconciliation detects both changes.
    #[test]
    fn full_yaml_compilation_tracks_conditional_composite_changes() {
        let current_composite = "[{type: transport_header, name: workspace, store_as: workspace_id}, \
            {type: transport_header, name: account, store_as: account_id}, \
            {type: transport_header_match, name: environment, value: production}]";
        let factory = test_pipeline_factory();
        let current = resolve_conditional_pipeline(current_composite, "tenant:workspace_id");
        let installed = factory
            .compile_initial_context(&current)
            .expect("initial context compiles");
        let pipeline = pipeline("default", "main");
        let exporter = ConfigNodeId::from("exporter");
        let policy = installed
            .bindings
            .header_propagation_policy(&pipeline, &exporter)
            .expect("compiled exporter propagation policy");
        let mut headers = TransportHeaders::new();
        headers.push(TransportHeader::text(context_name("workspace"), b"acme"));
        headers.push(TransportHeader::text(
            context_name("environment"),
            b"production",
        ));
        assert_eq!(policy.propagate(&headers).count(), 0);
        headers.push(TransportHeader::text(context_name("account"), b"customer"));
        let propagated = policy.propagate(&headers).collect::<Vec<_>>();
        assert_eq!(propagated.len(), 1);
        assert_eq!(propagated[0].header_name, "workspace_id");

        let changed_condition = resolve_conditional_pipeline(
            "[{type: transport_header, name: workspace, store_as: workspace_id}, \
             {type: transport_header, name: account, store_as: account_id}, \
             {type: transport_header_match, name: environment, value: staging}]",
            "tenant:workspace_id",
        );
        let condition_candidate = factory
            .compile_candidate_context(&changed_condition, &installed.runtime_requirements)
            .expect("condition candidate compiles");
        assert!(
            !installed
                .bindings
                .pipeline_bindings_match(&condition_candidate.bindings, &pipeline)
        );

        let changed_member = resolve_conditional_pipeline(current_composite, "tenant:account_id");
        let member_candidate = factory
            .compile_candidate_context(&changed_member, &installed.runtime_requirements)
            .expect("member candidate compiles");
        assert!(
            !installed
                .bindings
                .pipeline_bindings_match(&member_candidate.bindings, &pipeline)
        );

        let changed_unselected = resolve_conditional_pipeline(
            &current_composite.replace("name: account,", "name: other_account,"),
            "tenant:workspace_id",
        );
        let unselected_candidate = factory
            .compile_candidate_context(&changed_unselected, &installed.runtime_requirements)
            .expect("changed presence gate compiles");
        assert!(
            !installed
                .bindings
                .pipeline_bindings_match(&unselected_candidate.bindings, &pipeline)
        );
    }

    /// Scenario: a live update reorders the conditions of a composite context entry.
    /// Guarantees: compilation canonicalizes condition order and preserves the installed binding.
    #[test]
    fn full_yaml_compilation_ignores_composite_condition_order() {
        let current = resolve_conditional_pipeline(
            "[{type: transport_header, name: workspace, store_as: workspace_id}, \
             {type: transport_header_match, name: environment, value: production}, \
             {type: transport_header_match, name: region, value: us-east}]",
            "tenant:workspace_id",
        );
        let reordered = resolve_conditional_pipeline(
            "[{type: transport_header, name: workspace, store_as: workspace_id}, \
             {type: transport_header_match, name: region, value: us-east}, \
             {type: transport_header_match, name: environment, value: production}]",
            "tenant:workspace_id",
        );
        let factory = test_pipeline_factory();
        let installed = factory
            .compile_initial_context(&current)
            .expect("initial context compiles");
        let candidate = factory
            .compile_candidate_context(&reordered, &installed.runtime_requirements)
            .expect("reordered context compiles");

        assert!(
            installed
                .bindings
                .pipeline_bindings_match(&candidate.bindings, &pipeline("default", "main"))
        );
    }

    /// Scenario: unused composite definitions change while an exporter keeps the same binding.
    /// Guarantees: startup leaves unused definitions inert and candidate bindings remain compatible.
    #[test]
    fn full_yaml_compilation_ignores_unused_composites() {
        let composite = "[{type: transport_header, name: workspace}]";
        let original = conditional_pipeline_yaml(composite, "tenant:workspace");
        let changed = original.replace(
            "tenant: ",
            "unused: [{type: transport_header, name: unsupported:nested}]\n      tenant: ",
        );
        let resolve = |yaml: &str| {
            otel_arrow_dfe_config::engine::OtelDataflowSpec::from_yaml(yaml)
                .expect("valid config")
                .resolve()
        };
        let factory = test_pipeline_factory();
        let installed = factory
            .compile_initial_context(&resolve(&original))
            .expect("initial");
        let candidate = factory
            .compile_candidate_context(&resolve(&changed), &installed.runtime_requirements)
            .expect("unused nested definition stays inert");
        assert!(
            installed
                .bindings
                .pipeline_bindings_match(&candidate.bindings, &pipeline("default", "main"))
        );
    }

    /// Scenario: node capture overrides mask a conflicting pipeline capture alias.
    /// Guarantees: binding compilation preserves capture precedence and original wire names.
    #[test]
    fn full_yaml_compilation_uses_effective_capture_and_retention() {
        let yaml = conditional_pipeline_yaml(
            "[{type: transport_header, name: WORKSPACE, store_as: workspace_id}]",
            "tenant:workspace_id",
        )
        .replace("name: stored_name", "name: preserve")
        .replace(
            "          receiver:\n",
            "          receiver:\n            header_capture:\n              headers:\n                - match_names: [X-Workspace]\n                  store_as: Workspace\n",
        )
        .replace(
            "policies:\n",
            "policies:\n  transport_headers:\n    header_capture:\n      headers:\n        - match_names: [X-Workspace]\n          store_as: customer\n  authorized_identity:\n    - claim: sub\n      store_as: customer\n",
        );
        let resolved = otel_arrow_dfe_config::engine::OtelDataflowSpec::from_yaml(&yaml)
            .expect("valid config")
            .resolve();
        let installed = test_pipeline_factory()
            .compile_initial_context(&resolved)
            .expect("effective aliases do not collide");
        let key = pipeline("default", "main");
        let capture = installed
            .bindings
            .header_capture_policy(&key, &"receiver".into())
            .expect("capture");
        let propagation = installed
            .bindings
            .header_propagation_policy(&key, &"exporter".into())
            .expect("propagation");
        let mut headers = TransportHeaders::new();
        assert!(
            capture
                .capture_from_pairs(
                    [("X-Workspace", b"acme".as_slice())].into_iter(),
                    &mut headers
                )
                .is_none()
        );
        let output = propagation.propagate(&headers).collect::<Vec<_>>();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].header_name, "X-Workspace");
        assert_eq!(headers.get(0).expect("captured").name.as_str(), "Workspace");
    }

    /// Scenario: capture aliases differ only by case and an unselected identity uses the same name.
    /// Guarantees: composite compilation does not impose a new namespace on unrelated source policies.
    #[test]
    fn full_yaml_compilation_preserves_independent_source_names() {
        let yaml = conditional_pipeline_yaml(
            "[{type: transport_header, name: workspace, store_as: workspace_id}]",
            "tenant:workspace_id",
        ).replace(
            "policies:\n",
            "policies:\n  transport_headers:\n    header_capture:\n      headers:\n        - {match_names: [x-first], store_as: Workspace}\n        - {match_names: [x-second], store_as: workspace}\n  authorized_identity:\n    - {claim: sub, store_as: workspace}\n",
        );
        let resolved = otel_arrow_dfe_config::engine::OtelDataflowSpec::from_yaml(&yaml)
            .expect("valid config")
            .resolve();
        let installed = test_pipeline_factory()
            .compile_initial_context(&resolved)
            .expect("independent source names remain valid");
        let key = pipeline("default", "main");
        let capture = installed
            .bindings
            .header_capture_policy(&key, &"receiver".into())
            .expect("capture");
        let policy = installed
            .bindings
            .header_propagation_policy(&key, &"exporter".into())
            .expect("propagation");
        let mut headers = TransportHeaders::new();
        assert!(
            capture
                .capture_from_pairs(
                    [
                        ("x-first", b"first".as_slice()),
                        ("x-second", b"second".as_slice())
                    ]
                    .into_iter(),
                    &mut headers,
                )
                .is_none()
        );
        let output = policy.propagate(&headers).collect::<Vec<_>>();
        assert_eq!(output.len(), 2);
        assert!(
            output
                .iter()
                .all(|header| header.header_name == "workspace_id")
        );
        assert_eq!(output[0].value, b"first");
        assert_eq!(output[1].value, b"second");
    }

    /// Scenario: complete YAML contains an invalid qualified propagation selector.
    /// Guarantees: startup reports the unknown composite, unknown member, or unsupported type.
    #[test]
    fn full_yaml_compilation_reports_actionable_composite_selector_errors() {
        let cases = [
            (
                "[{type: transport_header, name: workspace, store_as: workspace_id}]",
                "missing:workspace_id",
                "unknown composite context entry `missing`",
            ),
            (
                "[{type: transport_header, name: workspace, store_as: workspace_id}]",
                "tenant:missing",
                "unknown context member `tenant:missing`",
            ),
            (
                "[{type: authorized_identity, name: customer_id}]",
                "tenant:customer_id",
                "context entry reference `tenant:customer_id` selects authorized-identity member `customer_id`, which cannot be propagated as a transport header",
            ),
        ];
        let factory = test_pipeline_factory();

        for (composite, selector, expected) in cases {
            let resolved = resolve_conditional_pipeline(composite, selector);
            let error = factory
                .compile_initial_context(&resolved)
                .expect_err("invalid selector must fail startup");
            let message = error.to_string();
            assert!(message.contains(expected), "{message}");
        }
    }

    /// Scenario: a receiver has an absent or explicitly empty authorized identity policy.
    /// Guarantees: neither form creates an authorized identity declaration or non-empty binding.
    #[test]
    fn empty_authorized_identity_policy_produces_no_binding() {
        let receiver = NodeUserConfig::new_receiver_config("urn:test:receiver:example");

        for policy in [None, Some(AuthorizedIdentityPolicy::default())] {
            let declarations =
                PipelineFactory::<()>::wrapper_context_declarations(&receiver, &None, &policy);
            assert!(declarations.is_empty());

            let compiled = compiled_bindings(declarations);
            assert!(compiled.pipeline_bindings_match(
                &compiled_bindings(NodeContextDeclarations::default()),
                &pipeline("group", "pipeline"),
            ));
            assert!(
                compiled
                    .authorized_identity_policy(
                        &pipeline("group", "pipeline"),
                        &ConfigNodeId::from("node"),
                    )
                    .is_none()
            );
        }
    }

    /// Scenario: a receiver declares an authorized identity claim projection.
    /// Guarantees: compiled node bindings retain the exact policy and
    /// live-update compatibility rejects changed projections in either
    /// comparison direction.
    #[test]
    fn authorized_identity_policy_is_a_compiled_receiver_binding() {
        let policy: AuthorizedIdentityPolicy =
            serde_json::from_value(serde_json::json!([{"claim": "sub", "store_as": "tenant"}]))
                .expect("valid authorized identity policy");
        let declarations: NodeContextDeclarations =
            [ContextDeclaration::AuthorizedIdentityCapture {
                policy: policy.clone(),
            }]
            .into_iter()
            .collect();
        let compiled = compiled_bindings(declarations);
        let changed_policy: AuthorizedIdentityPolicy = serde_json::from_value(
            serde_json::json!([{"claim": "groups", "store_as": "access_groups"}]),
        )
        .expect("valid changed authorized identity policy");
        let changed = compiled_bindings(
            [ContextDeclaration::AuthorizedIdentityCapture {
                policy: changed_policy,
            }]
            .into_iter()
            .collect(),
        );
        let pipeline = pipeline("group", "pipeline");

        assert_eq!(
            compiled.authorized_identity_policy(&pipeline, &ConfigNodeId::from("node")),
            Some(&policy),
        );
        assert!(!compiled.pipeline_bindings_match(&changed, &pipeline));
        assert!(!changed.pipeline_bindings_match(&compiled, &pipeline));
    }

    /// Scenario: a node declares a context read and a propagation policy.
    /// Guarantees: undeclared reads and nodes fail. The propagation declaration is retained.
    #[test]
    fn parsed_config_declarations_are_validated_against_compiled_policy() {
        let pipeline = pipeline("group", "pipeline");
        let node: ConfigNodeId = "node".into();
        let matching: TestDeclarationConfig =
            serde_json::from_value(serde_json::json!({"entry": "expected"}))
                .expect("valid matching config");
        let changed: TestDeclarationConfig =
            serde_json::from_value(serde_json::json!({"entry": "changed"}))
                .expect("valid changed config");
        let propagation_declaration = ContextDeclaration::HeaderPropagation {
            policy: HeaderPropagationPolicy::default(),
        };
        let declarations = matching
            .context_declarations()
            .into_iter()
            .chain(std::iter::once(propagation_declaration.clone()))
            .collect();
        let declarations = HashMap::from([(
            pipeline.clone(),
            HashMap::from([(node.clone(), declarations)]),
        )]);
        let requirements = ContextRuntimeRequirements::compile(&declarations);
        let bindings = CompiledContextBindings::compile(declarations, &requirements);

        assert!(
            bindings
                .validate_node_declarations(&pipeline, &node, &matching.context_declarations(),)
                .is_ok()
        );
        assert!(
            bindings
                .validate_node_declarations(&pipeline, &node, &changed.context_declarations())
                .is_err()
        );
        assert!(
            bindings
                .validate_node_declarations(
                    &pipeline,
                    &ConfigNodeId::from("other"),
                    &matching.context_declarations(),
                )
                .is_err()
        );
        assert!(
            CompiledContextBindings::empty()
                .validate_node_declarations(&pipeline, &node, &matching.context_declarations())
                .is_err()
        );
        let ContextDeclaration::HeaderPropagation {
            policy: propagation_policy,
        } = propagation_declaration
        else {
            unreachable!("test declaration is header propagation");
        };
        assert_eq!(
            bindings.header_propagation_policy(&pipeline, &node),
            Some(&propagation_policy)
        );
    }

    /// Scenario: a node with no declarations is missing from the compiled bindings.
    /// Guarantees: empty declarations still require a registered node in the correct pipeline.
    #[test]
    fn empty_declarations_require_a_compiled_node() {
        let key = pipeline("group", "pipeline");
        let node = ConfigNodeId::from("node");
        let declarations = NodeContextDeclarations::default();
        let bindings = compiled_bindings(declarations.clone());

        assert!(
            bindings
                .validate_node_declarations(&key, &node, &declarations)
                .is_ok()
        );
        assert!(
            bindings
                .validate_node_declarations(&pipeline("group", "other"), &node, &declarations)
                .is_err()
        );
        assert!(
            CompiledContextBindings::empty()
                .validate_node_declarations(&key, &node, &declarations)
                .is_err()
        );
    }
}
