// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Component context declarations collected before runtime construction.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use otel_arrow_dfe_config::context::{CompiledContext, ContextCompiler};
use otel_arrow_dfe_config::engine::{ResolvedOtelDataflowSpec, ResolvedPipelineConfig};
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::transport_headers_policy::{
    CompiledHeaderCapturePolicy, HeaderCapturePolicy,
};
use otel_arrow_dfe_config::{NodeId as ConfigNodeId, PipelineKey};

use crate::PipelineFactory;
use crate::error::Error as EngineError;

/// Context access identifier scoped to one node factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextAccessId(usize);

impl ContextAccessId {
    /// Creates a provider-local access identifier.
    #[must_use]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Returns the provider-local identifier.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// Generic context registers selected by one consumer binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContextReadSelector {
    /// Selects one named register.
    Register(String),
    /// Selects named registers in order; providers canonicalize unordered inputs.
    Registers {
        /// Logical context register names.
        names: Box<[String]>,
    },
    /// Selects every context register reachable at this node.
    All,
}

/// One component context declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ContextDeclaration {
    /// Adds one named value to outgoing context.
    Produces {
        /// Provider-local identity used to retrieve the compiled access.
        access: ContextAccessId,
        /// The logical header name that will be produced.
        name: String,
    },
    /// Reads context through one compiled access.
    Consumes {
        /// Provider-local identity used to retrieve the compiled access.
        access: ContextAccessId,
        /// Generic register selection.
        selector: ContextReadSelector,
    },
}

/// Deterministically describes a node factory's context access.
pub type ContextDeclarationFn = fn(&serde_json::Value) -> Result<Vec<ContextDeclaration>, Error>;

/// Opaque context policy compiled from the resolved configuration.
#[derive(Debug)]
pub struct CompiledContextPolicy {
    #[allow(dead_code)]
    compiled_context: Arc<CompiledContext>,
    #[allow(dead_code)]
    receiver_capture: HashMap<PipelineKey, HashMap<ConfigNodeId, CompiledHeaderCapturePolicy>>,
    // Retained until component bindings are compiled.
    #[allow(dead_code)]
    declarations: HashMap<PipelineKey, HashMap<ConfigNodeId, Box<[ContextDeclaration]>>>,
}

fn receiver_capture_policy<'a>(
    pipeline: &'a ResolvedPipelineConfig,
    node: &'a NodeUserConfig,
) -> Option<&'a HeaderCapturePolicy> {
    if node.kind() != NodeKind::Receiver {
        return None;
    }
    node.header_capture.as_ref().or_else(|| {
        pipeline
            .policies
            .transport_headers
            .as_ref()
            .map(|policy| &policy.header_capture)
    })
}

impl<PData: 'static + Clone + std::fmt::Debug> PipelineFactory<PData> {
    /// Compiles context policy from the complete resolved configuration.
    pub fn compile_context_policy(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
    ) -> Result<Arc<CompiledContextPolicy>, EngineError> {
        let mut declarations = HashMap::new();
        let mut register_names = BTreeSet::new();

        for pipeline in &resolved.pipelines {
            let pipeline_key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let mut declarations_by_node = HashMap::new();

            for (node_id, node_config) in pipeline.pipeline.node_iter() {
                let node_declarations = self.node_context_declarations(
                    node_config.kind(),
                    node_config.r#type.as_ref(),
                    &node_config.config,
                )?;
                for declaration in &node_declarations {
                    if let ContextDeclaration::Produces { name, .. } = declaration {
                        let _ = register_names.insert(name.trim().to_ascii_lowercase());
                    }
                }
                if let Some(capture) = receiver_capture_policy(pipeline, node_config) {
                    register_names.extend(
                        capture
                            .register_names()
                            .map(|name| name.trim().to_ascii_lowercase()),
                    );
                }
                let _ = declarations_by_node
                    .insert(node_id.clone(), node_declarations.into_boxed_slice());
            }

            let _ = declarations.insert(pipeline_key, declarations_by_node);
        }

        let mut compiler = ContextCompiler::new();
        for name in register_names {
            let _ = compiler.declare(&name).map_err(|error| {
                EngineError::ConfigError(Box::new(Error::InvalidUserConfig {
                    error: error.to_string(),
                }))
            })?;
        }

        let compiled_context = compiler.finish();
        let mut receiver_capture = HashMap::new();
        for pipeline in &resolved.pipelines {
            let pipeline_key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let mut captures_by_node = HashMap::new();
            for (node_id, node_config) in pipeline.pipeline.node_iter() {
                if let Some(capture) = receiver_capture_policy(pipeline, node_config) {
                    let compiled =
                        capture
                            .compile(compiled_context.clone(), true)
                            .map_err(|error| {
                                EngineError::ConfigError(Box::new(Error::InvalidUserConfig {
                                    error,
                                }))
                            })?;
                    let _ = captures_by_node.insert(node_id.clone(), compiled);
                }
            }
            let _ = receiver_capture.insert(pipeline_key, captures_by_node);
        }

        Ok(Arc::new(CompiledContextPolicy {
            compiled_context,
            receiver_capture,
            declarations,
        }))
    }

    fn node_context_declarations(
        &self,
        kind: NodeKind,
        urn: &str,
        config: &serde_json::Value,
    ) -> Result<Vec<ContextDeclaration>, EngineError> {
        let missing_factory = || {
            EngineError::ConfigError(Box::new(Error::InvalidUserConfig {
                error: format!("node factory `{urn}` is not registered"),
            }))
        };
        let declarations = match kind {
            NodeKind::Receiver => {
                self.get_receiver_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .context_declarations
            }
            NodeKind::Processor => {
                self.get_processor_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .context_declarations
            }
            NodeKind::Exporter => {
                self.get_exporter_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .context_declarations
            }
        };
        declarations.map_or_else(
            || Ok(Vec::new()),
            |declare| declare(config).map_err(|error| EngineError::ConfigError(Box::new(error))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::engine::OtelDataflowSpec;

    use crate::config::{ExporterConfig, ReceiverConfig};
    use crate::context::PipelineContext;
    use crate::exporter::ExporterWrapper;
    use crate::node::NodeId;
    use crate::receiver::ReceiverWrapper;
    use crate::wiring_contract::WiringContract;
    use crate::{ExporterFactory, ReceiverFactory};

    fn test_receiver_create(
        _pipeline: PipelineContext,
        _node: NodeId,
        _node_config: Arc<NodeUserConfig>,
        _config: &ReceiverConfig,
        _capabilities: &crate::capability::registry::Capabilities,
    ) -> Result<ReceiverWrapper<()>, Error> {
        panic!("test receiver must not be constructed")
    }

    fn test_exporter_create(
        _pipeline: PipelineContext,
        _node: NodeId,
        _node_config: Arc<NodeUserConfig>,
        _config: &ExporterConfig,
        _capabilities: &crate::capability::registry::Capabilities,
    ) -> Result<ExporterWrapper<()>, Error> {
        panic!("test exporter must not be constructed")
    }

    fn test_receiver_declarations(
        _config: &serde_json::Value,
    ) -> Result<Vec<ContextDeclaration>, Error> {
        Ok(vec![
            ContextDeclaration::Produces {
                access: ContextAccessId::new(0),
                name: "zeta".into(),
            },
            ContextDeclaration::Produces {
                access: ContextAccessId::new(1),
                name: "beta".into(),
            },
        ])
    }

    fn test_factory() -> PipelineFactory<()> {
        let receivers = Box::leak(Box::new([ReceiverFactory {
            name: "urn:test:receiver:context",
            create: test_receiver_create,
            wiring_contract: WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
            context_declarations: Some(test_receiver_declarations),
        }]));
        let exporters = Box::leak(Box::new([ExporterFactory {
            name: "urn:test:exporter:context",
            create: test_exporter_create,
            wiring_contract: WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
            context_declarations: None,
        }]));
        PipelineFactory::new(receivers, &[], exporters, &[])
    }

    /// Scenario: A factory callback receives node config.
    /// Guarantees: it returns the configured declaration.
    #[test]
    fn provider_callback_returns_declarations() {
        fn test_provider(config: &serde_json::Value) -> Result<Vec<ContextDeclaration>, Error> {
            let name = config
                .get("header_name")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            Ok(vec![ContextDeclaration::Produces {
                access: ContextAccessId::new(0),
                name: name.to_string(),
            }])
        }

        let config = serde_json::json!({"header_name": "x-test"});
        let decls = test_provider(&config).unwrap();
        assert_eq!(decls.len(), 1);
        match &decls[0] {
            ContextDeclaration::Produces { name, .. } => {
                assert_eq!(name, "x-test");
            }
            other => panic!("unexpected declaration: {other:?}"),
        }
    }

    /// Scenario: capture and component producers span one resolved configuration.
    /// Guarantees: one sorted register file backs every compiled receiver schema.
    #[test]
    fn compiles_one_global_register_file() {
        let mut spec = OtelDataflowSpec::from_yaml(
            r#"
version: otel_dataflow/v1
groups:
  group:
    pipelines:
      pipeline:
        policies:
          transport_headers:
            header_capture:
              headers:
                - match_names: [x-alpha]
                  store_as: alpha
        nodes:
          source:
            type: urn:test:receiver:context
            config: {}
          sink:
            type: urn:test:exporter:context
            config: {}
        connections:
          - from: source
            to: sink
"#,
        )
        .expect("valid config");
        spec.engine.observability.pipeline.nodes = Default::default();
        spec.engine.observability.pipeline.connections.clear();
        let policy = test_factory()
            .compile_context_policy(&spec.resolve())
            .expect("compiled policy");

        assert_eq!(
            policy
                .compiled_context
                .resolve("alpha")
                .expect("alpha")
                .index(),
            0
        );
        assert_eq!(
            policy
                .compiled_context
                .resolve("beta")
                .expect("beta")
                .index(),
            1
        );
        assert_eq!(
            policy
                .compiled_context
                .resolve("zeta")
                .expect("zeta")
                .index(),
            2
        );
        let pipeline = PipelineKey::new("group".into(), "pipeline".into());
        let capture = &policy.receiver_capture[&pipeline]["source"];
        assert!(Arc::ptr_eq(
            capture.schema().register_file(),
            policy.compiled_context.register_file()
        ));
    }

    /// Scenario: policy compilation indexes declarations by pipeline and node.
    /// Guarantees: each node retains only its declarations.
    #[test]
    fn compiled_policy_indexes_existing_configuration_ids() {
        let pipeline = PipelineKey::new("group".into(), "pipeline".into());
        let node: ConfigNodeId = "source".into();
        let declaration = ContextDeclaration::Produces {
            access: ContextAccessId::new(0),
            name: "tenant".into(),
        };
        let mut compiler = ContextCompiler::new();
        let _ = compiler.declare("tenant").expect("tenant");
        let policy = CompiledContextPolicy {
            compiled_context: compiler.finish(),
            receiver_capture: HashMap::new(),
            declarations: HashMap::from([(
                pipeline.clone(),
                HashMap::from([(node.clone(), vec![declaration.clone()].into_boxed_slice())]),
            )]),
        };

        assert_eq!(
            policy.declarations[&pipeline][&node].as_ref(),
            [declaration].as_slice()
        );
        assert!(!policy.declarations[&pipeline].contains_key("other"));
    }

    /// Scenario: Two access IDs select the same register.
    /// Guarantees: the declarations remain distinct.
    #[test]
    fn access_id_distinguishes_consumers() {
        let selector = ContextReadSelector::Register("x-topic".into());
        let first = ContextDeclaration::Consumes {
            access: ContextAccessId::new(0),
            selector: selector.clone(),
        };
        let second = ContextDeclaration::Consumes {
            access: ContextAccessId::new(1),
            selector,
        };
        assert_ne!(first, second);
    }

    /// Scenario: Generic selectors represent one, ordered, and all-register reads.
    /// Guarantees: selector equality preserves register selection and order.
    #[test]
    fn selectors_preserve_generic_read_contracts() {
        assert_ne!(
            ContextReadSelector::Register("tenant".into()),
            ContextReadSelector::Register("region".into())
        );
        assert_ne!(
            ContextReadSelector::Registers {
                names: vec!["tenant".into(), "region".into()].into_boxed_slice(),
            },
            ContextReadSelector::Registers {
                names: vec!["region".into(), "tenant".into()].into_boxed_slice(),
            },
        );
    }
}
