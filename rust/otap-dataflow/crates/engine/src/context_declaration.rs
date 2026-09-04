// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Component context declarations collected before runtime construction.

use crate::PipelineFactory;
use crate::error::Error as EngineError;
use linkme::distributed_slice;
use otel_arrow_dfe_config::engine::ResolvedOtelDataflowSpec;
use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::node::NodeKind;
use otel_arrow_dfe_config::{ContextEntryName, NodeId as ConfigNodeId, PipelineKey};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// The entry selector
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextEntrySelector {
    /// The name
    pub name: ContextEntryName,
    /// The form
    pub read: ContextEntrySelectorForm,
}

/// When an association list is captured, do we store the associated
/// field names (e.g., header names)?
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextEntrySelectorForm {
    /// Consumers use only the normalized field name.
    #[default]
    Value,
    /// Associated field names are stored in the original form.
    NormalizedKeyValue,
    /// Associated field names are stored in the original form.
    OriginalKeyValue,
}

/// Generic context registers selected by one consumer binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextConsumerSelector {
    /// Selects named context entries in order.
    Entries {
        /// Logical context entry references.
        entries: Box<[ContextEntrySelector]>,
    },
    /// Selects every context register reachable at this node.
    All,
}

/// Context access identifier scoped to one node factory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContextAccessId(usize);

/// One component context declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextDeclaration {
    /// Adds one named value to outgoing context.
    Produces {
        /// The logical context entry that will be produced.
        entry: ContextEntryName,
    },
    /// Reads context through one compiled access.
    Consumes {
        /// Generic register selection.
        selector: ContextConsumerSelector,
    },
}

/// A configuration-dependent declaration provider registered by a component.
#[derive(Clone, Copy)]
pub struct ContextDeclarationProvider {
    /// The registered component's URN.
    pub urn: &'static str,
    /// Produces declarations using a component configuration.
    pub declarations: ContextDeclarationFn,
}

/// Deterministically describes a node factory's context access.
pub type ContextDeclarationFn = fn(&serde_json::Value) -> Result<NodeContextDeclarations, Error>;

/// Implemented by node Config structs.
pub trait NodeContextDeclarator: serde::de::DeserializeOwned {
    /// Returns the context accesses required by this configuration.
    fn context_declarations(&self) -> NodeContextDeclarations;
}

// `#[allow(unsafe_code)]` is required because `linkme::distributed_slice`
// emits a static with `#[link_section = "..."]`, which the engine crate's
// `-D unsafe-code` lint would otherwise reject.
/// Context declaration providers registered by components.
#[allow(unsafe_code)]
#[distributed_slice]
pub static CONTEXT_DECLARATION_PROVIDERS: [ContextDeclarationProvider];

/// A fixed set of context bindings, equals a bi-directional mapping
/// from ContextAccessId to/from ContextDeclaration. Created by
/// collecting FromIterator<ContextDeclaration>.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeContextDeclarations {
    /// Indexed by ContextAccessId; sorted in the builder
    byid: Vec<ContextDeclaration>,
}

impl FromIterator<ContextDeclaration> for NodeContextDeclarations {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = ContextDeclaration>,
    {
        let mut uniq: Vec<_> = iter.into_iter().collect();
        uniq.sort();
        Self { byid: uniq }
    }
}

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
        T: NodeContextDeclarator,
    {
        Self {
            urn,
            declarations: typed_context_declarations::<T>,
        }
    }
}

/// Generic function used in from_typed_config.
fn typed_context_declarations<T>(
    config: &serde_json::Value,
) -> Result<NodeContextDeclarations, Error>
where
    T: NodeContextDeclarator,
{
    Ok(
        otel_arrow_dfe_config::validation::deserialize_typed_config::<T>(config)?
            .context_declarations(),
    )
}

/// Context policy compiled from resolved configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledContextPolicy {
    declarations: HashMap<PipelineKey, HashMap<ConfigNodeId, NodeContextDeclarations>>,
}

impl CompiledContextPolicy {
    /// The empty state has no declarations, bind always fails.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            declarations: HashMap::new(),
        }
    }

    /// Binds the declarations to a node.
    pub fn node_bindings(
        &self,
        pipeline: PipelineKey,
        node: ConfigNodeId,
        decls: NodeContextDeclarations,
    ) -> Result<(), Error> {
        // Note: This is where the current compiler ends. It checks that
        // that the context declarations the node wants to configure equal
        // what is present in the compiled policy.
        self.declarations
            .get(&pipeline)
            .ok_or(Error::UnrecognizedContextDeclaration {})?
            .get(&node)
            .ok_or(Error::UnrecognizedContextDeclaration {})?
            .eq(&decls)
            .then_some(())
            .ok_or(Error::UnrecognizedContextDeclaration {})
    }
}

impl<PData: 'static + Clone + std::fmt::Debug> PipelineFactory<PData> {
    /// Compiles context policy from the complete resolved configuration.
    pub fn compile_context_policy(
        &self,
        resolved: &ResolvedOtelDataflowSpec,
    ) -> Result<Arc<CompiledContextPolicy>, EngineError> {
        // @@@
        let mut declarations = HashMap::new();

        for pipeline in &resolved.pipelines {
            let pipeline_key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let mut declarations_by_node = HashMap::new();

            for (node_id, node_config) in pipeline.pipeline.node_iter() {
                let declarations = self.node_context_declarations(
                    node_config.kind(),
                    node_config.r#type.as_ref(),
                    &node_config.config,
                )?;
                let _ = declarations_by_node.insert(node_id.clone(), declarations);
            }
            let _ = declarations.insert(pipeline_key, declarations_by_node);
        }

        Ok(Arc::new(CompiledContextPolicy { declarations }))
    }

    /// Returns context declarations for a single node.
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
        let validate_config = match kind {
            NodeKind::Receiver => {
                self.get_receiver_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .validate_config
            }
            NodeKind::Processor => {
                self.get_processor_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .validate_config
            }
            NodeKind::Exporter => {
                self.get_exporter_factory_map()
                    .get(urn)
                    .ok_or_else(&missing_factory)?
                    .validate_config
            }
        };
        validate_config(config).map_err(|error| EngineError::ConfigError(Box::new(error)))?;

        Ok(context_declaration_provider(urn)
            .map(|provider| {
                (provider.declarations)(config)
                    .map_err(|error| EngineError::ConfigError(Box::new(error)))
            })
            .transpose()?
            .unwrap_or_default())
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
        entry: ContextEntryName,
    }

    fn default_test_entry() -> ContextEntryName {
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
        let selector = ContextConsumerSelector::Entries {
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
            .produce(ContextEntryName::parse("X-Tenant-Id").unwrap())
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
            .produce(ContextEntryName::parse("X-Tenant-Id").unwrap())
            .unwrap();

        assert!(matches!(
            builder.produce("x-tenant-id".into()),
            Err(Error::InvalidUserConfig { .. })
        ));
    }
}
