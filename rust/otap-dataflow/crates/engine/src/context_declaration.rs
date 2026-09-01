// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Component context declarations collected before runtime construction.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, OnceLock};

use linkme::distributed_slice;
use otel_arrow_dfe_config::context::{
    CompiledContext, ContextCompiler, ContextNameRetention, ContextRegisterRequirement,
};
use otel_arrow_dfe_config::engine::{ResolvedOtelDataflowSpec, ResolvedPipelineConfig};
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::node::{NodeKind, NodeUserConfig};
use otel_arrow_dfe_config::transport_headers_policy::{
    CompiledHeaderCapturePolicy, HeaderCapturePolicy, HeaderPropagationPolicy,
};
use otel_arrow_dfe_config::{ContextEntryRef, NodeId as ConfigNodeId, PipelineKey};

use crate::PipelineFactory;
use crate::error::Error as EngineError;

/// A configuration-dependent declaration provider registered by a component.
#[derive(Clone, Copy)]
pub struct ContextDeclarationProvider {
    /// The registered component's URN.
    pub urn: &'static str,
    /// Produces declarations using a component configuration.
    pub declarations: ContextDeclarationFn,
}

impl ContextDeclarationProvider {
    /// Creates a provider that derives declarations from component configuration.
    #[must_use]
    pub const fn from_config(urn: &'static str, declarations: ContextDeclarationFn) -> Self {
        Self { urn, declarations }
    }

    /// Creates a provider that derives declarations from a typed component configuration.
    #[must_use]
    pub const fn from_typed_config<T>(urn: &'static str) -> Self
    where
        T: ContextDeclarationConfig,
    {
        Self {
            urn,
            declarations: typed_context_declarations::<T>,
        }
    }
}

// `#[allow(unsafe_code)]` is required because `linkme::distributed_slice`
// emits a static with `#[link_section = "..."]`, which the engine crate's
// `-D unsafe-code` lint would otherwise reject.
/// Context declaration providers registered by components.
#[allow(unsafe_code)]
#[distributed_slice]
pub static CONTEXT_DECLARATION_PROVIDERS: [ContextDeclarationProvider];

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
    /// Selects named context entries in order.
    Entries {
        /// Logical context entry references.
        entries: Box<[ContextEntryRef]>,
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
        /// The logical context entry that will be produced.
        entry: ContextEntryRef,
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

/// Extracts context declarations from a component's typed configuration.
pub trait ContextDeclarationConfig: serde::de::DeserializeOwned {
    /// Returns the context accesses required by this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the context declaration is invalid.
    fn context_declarations(&self) -> Result<Vec<ContextDeclaration>, Error>;
}

fn typed_context_declarations<T>(
    config: &serde_json::Value,
) -> Result<Vec<ContextDeclaration>, Error>
where
    T: ContextDeclarationConfig,
{
    let config = otel_arrow_dfe_config::validation::deserialize_typed_config::<T>(config)?;
    config.context_declarations()
}

/// Generation assigned to a compiled context policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextPolicyGeneration(u64);

impl ContextPolicyGeneration {
    /// Creates a context policy generation.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Builds deterministic producer declarations from configuration-sized inputs.
#[derive(Default)]
pub struct ContextDeclarationsBuilder {
    produced_entries: BTreeSet<ContextEntryRef>,
}

impl ContextDeclarationsBuilder {
    /// Creates an empty declaration builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a produced register, normalizing its logical name.
    ///
    /// # Errors
    ///
    /// Returns an error when another configured name has the same normalized
    /// logical name.
    pub fn produce(&mut self, entry: ContextEntryRef) -> Result<(), Error> {
        if !self.produced_entries.insert(entry.clone()) {
            return Err(Error::InvalidUserConfig {
                error: format!("duplicate context entry reference: `{entry}`"),
            });
        }
        Ok(())
    }

    /// Returns produced declarations in normalized-name order with assigned IDs.
    #[must_use]
    pub fn finish(self) -> Vec<ContextDeclaration> {
        self.produced_entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| ContextDeclaration::Produces {
                access: ContextAccessId::new(index),
                entry,
            })
            .collect()
    }
}

/// Opaque context policy compiled from the resolved configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledContextPolicy {
    generation: ContextPolicyGeneration,
    compiled_context: Arc<CompiledContext>,
    receiver_capture: HashMap<PipelineKey, HashMap<ConfigNodeId, CompiledHeaderCapturePolicy>>,
    // Retained until component bindings are compiled.
    declarations: HashMap<PipelineKey, HashMap<ConfigNodeId, Box<[ContextDeclaration]>>>,
}

impl CompiledContextPolicy {
    /// Returns the generation associated with this compiled policy.
    #[must_use]
    pub const fn generation(&self) -> ContextPolicyGeneration {
        self.generation
    }

    /// Returns a copy carrying the supplied generation.
    #[must_use]
    pub fn with_generation(&self, generation: ContextPolicyGeneration) -> Arc<Self> {
        Arc::new(Self {
            generation,
            compiled_context: self.compiled_context.clone(),
            receiver_capture: self.receiver_capture.clone(),
            declarations: self.declarations.clone(),
        })
    }

    /// Returns whether two policies compile the same declarations.
    #[must_use]
    pub fn equivalent_declarations(&self, other: &Self) -> bool {
        self.declarations == other.declarations && self.compiled_context == other.compiled_context
    }
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

fn exporter_propagation_policy<'a>(
    pipeline: &'a ResolvedPipelineConfig,
    node: &'a NodeUserConfig,
) -> Option<&'a HeaderPropagationPolicy> {
    if node.kind() != NodeKind::Exporter {
        return None;
    }
    node.header_propagation.as_ref().or_else(|| {
        pipeline
            .policies
            .transport_headers
            .as_ref()
            .map(|policy| &policy.header_propagation)
    })
}

fn pipeline_capture_retention(
    pipeline: &ResolvedPipelineConfig,
    stored_name: &str,
) -> ContextNameRetention {
    pipeline
        .pipeline
        .node_iter()
        .filter_map(|(_, node_config)| exporter_propagation_policy(pipeline, node_config))
        .map(|policy| policy.capture_retention(stored_name))
        .max()
        .unwrap_or(ContextNameRetention::None)
}

fn require_register_name(
    requirements: &mut BTreeMap<String, ContextNameRetention>,
    name: &str,
    retention: ContextNameRetention,
) {
    let canonical = name.trim().to_ascii_lowercase();
    let _ = requirements
        .entry(canonical)
        .and_modify(|existing| *existing = (*existing).max(retention))
        .or_insert(retention);
}

impl<PData: 'static + Clone + std::fmt::Debug> PipelineFactory<PData> {
    /// Compiles context policy from the complete resolved configuration.
    pub fn compile_context_policy(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
    ) -> Result<Arc<CompiledContextPolicy>, EngineError> {
        let mut declarations = HashMap::new();
        let mut register_requirements = BTreeMap::new();

        for pipeline in &resolved.pipelines {
            let pipeline_key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let mut declarations_by_node = HashMap::new();
            let mut pipeline_capture_names = BTreeSet::new();

            for (node_id, node_config) in pipeline.pipeline.node_iter() {
                let node_declarations = self.node_context_declarations(
                    node_config.kind(),
                    node_config.r#type.as_ref(),
                    &node_config.config,
                )?;
                for declaration in &node_declarations {
                    if let ContextDeclaration::Produces { entry, .. } = declaration {
                        require_register_name(
                            &mut register_requirements,
                            entry.as_str(),
                            ContextNameRetention::None,
                        );
                    }
                }
                if let Some(capture) = receiver_capture_policy(pipeline, node_config) {
                    for requirement in capture.register_requirements() {
                        require_register_name(
                            &mut register_requirements,
                            requirement.name,
                            requirement.retention,
                        );
                        let _ = pipeline_capture_names
                            .insert(requirement.name.trim().to_ascii_lowercase());
                    }
                }
                let _ = declarations_by_node
                    .insert(node_id.clone(), node_declarations.into_boxed_slice());
            }

            for name in pipeline_capture_names {
                let retention = pipeline_capture_retention(pipeline, &name);
                require_register_name(&mut register_requirements, &name, retention);
            }
            let _ = declarations.insert(pipeline_key, declarations_by_node);
        }

        let mut compiler = ContextCompiler::new();
        for (name, retention) in register_requirements {
            let _ = compiler
                .declare(ContextRegisterRequirement::with_retention(&name, retention))
                .map_err(|error| {
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
                    let compiled = capture.compile(compiled_context.clone()).map_err(|error| {
                        EngineError::ConfigError(Box::new(Error::InvalidUserConfig { error }))
                    })?;
                    let _ = captures_by_node.insert(node_id.clone(), compiled);
                }
            }
            let _ = receiver_capture.insert(pipeline_key, captures_by_node);
        }

        Ok(Arc::new(CompiledContextPolicy {
            generation: ContextPolicyGeneration::default(),
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
        let _ = match kind {
            NodeKind::Receiver => {
                self.get_receiver_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .name
            }
            NodeKind::Processor => {
                self.get_processor_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .name
            }
            NodeKind::Exporter => {
                self.get_exporter_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .name
            }
        };
        context_declaration_provider(urn).map_or_else(
            || Ok(Vec::new()),
            |provider| {
                (provider.declarations)(config)
                    .map_err(|error| EngineError::ConfigError(Box::new(error)))
            },
        )
    }
}

fn context_declaration_provider(urn: &str) -> Option<ContextDeclarationProvider> {
    static PROVIDERS: OnceLock<HashMap<&'static str, ContextDeclarationProvider>> = OnceLock::new();
    PROVIDERS
        .get_or_init(|| {
            CONTEXT_DECLARATION_PROVIDERS
                .iter()
                .map(|provider| (provider.urn, *provider))
                .collect()
        })
        .get(urn)
        .copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_config::engine::OtelDataflowSpec;
    use serde::Deserialize;

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

    fn test_factory() -> PipelineFactory<()> {
        let receivers = Box::leak(Box::new([ReceiverFactory {
            name: "urn:test:receiver:context",
            create: test_receiver_create,
            wiring_contract: WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        }]));
        let exporters = Box::leak(Box::new([ExporterFactory {
            name: "urn:test:exporter:context",
            create: test_exporter_create,
            wiring_contract: WiringContract::UNRESTRICTED,
            validate_config: otel_arrow_dfe_config::validation::no_config,
        }]));
        PipelineFactory::new(receivers, &[], exporters, &[])
    }

    #[derive(Deserialize)]
    struct TestDeclarationConfig {
        #[serde(default = "default_test_entry")]
        entry: ContextEntryRef,
    }

    fn default_test_entry() -> ContextEntryRef {
        "default".into()
    }

    impl ContextDeclarationConfig for TestDeclarationConfig {
        fn context_declarations(&self) -> Result<Vec<ContextDeclaration>, Error> {
            Ok(vec![ContextDeclaration::Produces {
                access: ContextAccessId::new(0),
                entry: self.entry.clone(),
            }])
        }
    }

    /// Scenario: A typed provider receives valid component configuration.
    /// Guarantees: parsing and declaration extraction use the registered config type.
    #[test]
    fn typed_provider_parses_registered_config() {
        let provider =
            ContextDeclarationProvider::from_typed_config::<TestDeclarationConfig>("urn:test");
        let config = serde_json::json!({"entry": "X-Test"});
        let decls = (provider.declarations)(&config).unwrap();
        assert_eq!(decls.len(), 1);
        match &decls[0] {
            ContextDeclaration::Produces { entry, .. } => {
                assert_eq!(entry.as_str(), "x-test");
            }
            other => panic!("unexpected declaration: {other:?}"),
        }
    }

    /// Scenario: capture and component producers span one resolved configuration.
    /// Guarantees: one sorted register layout backs every compiled receiver schema.
    #[test]
    fn compiles_one_global_register_layout() {
        #[allow(unsafe_code)]
        #[distributed_slice(CONTEXT_DECLARATION_PROVIDERS)]
        static TEST_RECEIVER_CONTEXT_DECLARATIONS: ContextDeclarationProvider =
            ContextDeclarationProvider::from_config(
                "urn:test:receiver:context",
                test_receiver_declarations,
            );

        fn test_receiver_declarations(
            _config: &serde_json::Value,
        ) -> Result<Vec<ContextDeclaration>, Error> {
            Ok(vec![
                ContextDeclaration::Produces {
                    access: ContextAccessId::new(0),
                    entry: "zeta".into(),
                },
                ContextDeclaration::Produces {
                    access: ContextAccessId::new(1),
                    entry: "beta".into(),
                },
            ])
        }

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
            header_propagation:
              default:
                selector:
                  type: all_captured
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
            capture.schema().register_layout(),
            policy.compiled_context.register_layout()
        ));
        assert_eq!(
            capture
                .match_header("x-alpha")
                .expect("x-alpha capture")
                .schema_item
                .retention,
            ContextNameRetention::Observed
        );
    }

    /// Scenario: A typed provider receives configuration missing an optional field.
    /// Guarantees: the registered config type's Serde defaults are applied.
    #[test]
    fn typed_provider_applies_config_defaults() {
        let provider =
            ContextDeclarationProvider::from_typed_config::<TestDeclarationConfig>("urn:test");
        let decls = (provider.declarations)(&serde_json::json!({})).unwrap();

        assert_eq!(
            decls,
            vec![ContextDeclaration::Produces {
                access: ContextAccessId::new(0),
                entry: "default".into(),
            }]
        );
    }

    /// Scenario: policy compilation indexes declarations by pipeline and node.
    /// Guarantees: each node retains only its declarations.
    #[test]
    fn compiled_policy_indexes_existing_configuration_ids() {
        let pipeline = PipelineKey::new("group".into(), "pipeline".into());
        let node: ConfigNodeId = "source".into();
        let declaration = ContextDeclaration::Produces {
            access: ContextAccessId::new(0),
            entry: "tenant".into(),
        };
        let mut compiler = ContextCompiler::new();
        let _ = compiler
            .declare(ContextRegisterRequirement::new("tenant"))
            .expect("tenant");
        let policy = CompiledContextPolicy {
            generation: ContextPolicyGeneration::default(),
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
        let selector = ContextReadSelector::Entries {
            entries: vec!["x-topic".into()].into_boxed_slice(),
        };
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

    /// Scenario: Generic selectors represent ordered and all-register reads.
    /// Guarantees: selector equality preserves register selection and order.
    #[test]
    fn selectors_preserve_generic_read_contracts() {
        assert_ne!(
            ContextReadSelector::Entries {
                entries: vec!["tenant".into()].into_boxed_slice(),
            },
            ContextReadSelector::Entries {
                entries: vec!["region".into()].into_boxed_slice(),
            }
        );
        assert_ne!(
            ContextReadSelector::Entries {
                entries: vec!["tenant".into(), "region".into()].into_boxed_slice(),
            },
            ContextReadSelector::Entries {
                entries: vec!["region".into(), "tenant".into()].into_boxed_slice(),
            },
        );
    }

    /// Scenario: Producer inputs are unordered and use mixed-case names.
    /// Guarantees: finished declarations normalize names and assign sorted IDs.
    #[test]
    fn builder_normalizes_and_orders_producers() {
        let mut builder = ContextDeclarationsBuilder::new();
        builder
            .produce(ContextEntryRef::parse("X-Tenant-Id").unwrap())
            .unwrap();
        builder.produce("a-first".into()).unwrap();

        assert_eq!(
            builder.finish(),
            vec![
                ContextDeclaration::Produces {
                    access: ContextAccessId::new(0),
                    entry: "a-first".into(),
                },
                ContextDeclaration::Produces {
                    access: ContextAccessId::new(1),
                    entry: "x-tenant-id".into(),
                },
            ]
        );
    }

    /// Scenario: Producer inputs differ only by logical-name casing.
    /// Guarantees: configuration compilation rejects ambiguous register names.
    #[test]
    fn builder_rejects_duplicate_normalized_producers() {
        let mut builder = ContextDeclarationsBuilder::new();
        builder
            .produce(ContextEntryRef::parse("X-Tenant-Id").unwrap())
            .unwrap();

        assert!(matches!(
            builder.produce("x-tenant-id".into()),
            Err(Error::InvalidUserConfig { .. })
        ));
    }
}
