// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration-only node declarations and compiled context bindings.

use super::ContextLayout;
use crate::authorized_identity_policy::AuthorizedIdentityPolicy;
use crate::context_policy::ContextEntryDeclaration;
use crate::engine::ResolvedOtelDataflowSpec;
use crate::error::Error;
use crate::transport_headers_policy::{
    CompiledHeaderCapturePolicy, HeaderCapturePolicy, HeaderPropagationPolicy,
};
use crate::{ContextEntryName, NodeId as ConfigNodeId, PipelineKey};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A context entry and its requested representation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ContextEntrySelector {
    /// Configured entry name.
    pub name: ContextEntryName,
    /// Requested representation.
    pub form: ContextEntrySelectorForm,
}

/// Context entry representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextEntrySelectorForm {
    /// Value only.
    Value,
    /// Stored name and value, preserving configured spelling.
    StoredKeyValue,
    /// Original name and value.
    OriginalKeyValue,
}

/// Context entries read by a consumer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextConsumerSelector {
    /// Selects named context entries in order.
    Entries {
        /// Entries to read.
        entries: Box<[ContextEntrySelector]>,
    },
    /// Selects every context entry using its stored name.
    AllStored,
}

/// A node's declared context behavior.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ContextDeclaration {
    /// Declares a context entry produced by the node.
    Produces {
        /// Produced entry name.
        entry: ContextEntryName,
    },
    /// Declares context reads.
    Consumes {
        /// Entries to read.
        selector: ContextConsumerSelector,
    },
    /// Declares the receiver's header capture policy.
    HeaderCapture {
        /// Resolved capture policy.
        policy: HeaderCapturePolicy,
    },
    /// Declares the exporter's header propagation policy.
    HeaderPropagation {
        /// Resolved propagation policy.
        policy: HeaderPropagationPolicy,
    },
    /// Declares the receiver's authorized identity claim projection policy.
    AuthorizedIdentityCapture {
        /// Resolved authorized identity policy.
        policy: AuthorizedIdentityPolicy,
    },
}

impl ContextDeclaration {
    /// Whether this declaration may be supplied by a component factory.
    #[must_use]
    pub fn is_component_declaration(&self) -> bool {
        matches!(self, Self::Produces { .. } | Self::Consumes { .. })
    }

    fn context_runtime_requirements(&self) -> ContextRuntimeRequirements {
        let mut requirements = ContextRuntimeRequirements::none();
        match self {
            Self::Consumes {
                selector: ContextConsumerSelector::Entries { entries },
            } => {
                for entry in entries {
                    if entry.form == ContextEntrySelectorForm::OriginalKeyValue {
                        _ = requirements
                            .original_name_retention
                            .overrides
                            .insert(original_name_key(&entry.name), true);
                    }
                }
            }
            Self::Consumes {
                selector: ContextConsumerSelector::AllStored,
            }
            | Self::Produces { .. }
            | Self::HeaderCapture { .. }
            | Self::AuthorizedIdentityCapture { .. } => {}
            Self::HeaderPropagation { policy } => {
                requirements
                    .original_name_retention
                    .default_preserve_original = policy.propagates_original_name_by_default();
                policy.visit_original_name_requirement_names(|name| {
                    let preserve_original = policy.propagates_original_name(name);
                    if preserve_original
                        != requirements
                            .original_name_retention
                            .default_preserve_original
                    {
                        _ = requirements
                            .original_name_retention
                            .overrides
                            .insert(original_name_key(name), preserve_original);
                    }
                });
            }
        }
        requirements
    }
}

/// Sorted, unique context declarations.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeContextDeclarations {
    /// Sorted and deduplicated declarations.
    declarations: Box<[ContextDeclaration]>,
}

impl FromIterator<ContextDeclaration> for NodeContextDeclarations {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = ContextDeclaration>,
    {
        let mut uniq: Vec<_> = iter.into_iter().collect();
        uniq.sort();
        uniq.dedup();
        Self {
            declarations: uniq.into_boxed_slice(),
        }
    }
}

impl IntoIterator for NodeContextDeclarations {
    type Item = ContextDeclaration;
    type IntoIter = std::vec::IntoIter<ContextDeclaration>;

    fn into_iter(self) -> Self::IntoIter {
        self.declarations.into_vec().into_iter()
    }
}

impl NodeContextDeclarations {
    /// Iterates over declarations in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &ContextDeclaration> {
        self.declarations.iter()
    }

    /// Returns whether the declaration set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty()
    }

    /// Returns the declaration count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.declarations.len()
    }
}

/// Context bindings compiled for all pipelines in one resolved configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledContextBindings {
    /// Compiled node bindings indexed first by pipeline, then by node.
    by_pipeline: HashMap<PipelineKey, HashMap<ConfigNodeId, CompiledNodeBindings>>,
}

/// Declarations and transport-header policies compiled for one node.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompiledNodeBindings {
    /// Declarations supplied by the component factory.
    component_declarations: NodeContextDeclarations,
    /// Receiver header capture policy compiled for the engine requirements.
    header_capture: Option<CompiledHeaderCapturePolicy>,
    /// Exporter header propagation policy resolved from node or pipeline config.
    header_propagation: Option<HeaderPropagationPolicy>,
    /// Receiver authorized identity claim projection policy.
    authorized_identity_capture: Option<AuthorizedIdentityPolicy>,
}

/// Declarations indexed by pipeline and node identifiers.
pub type ContextDeclarationsByPipeline =
    HashMap<PipelineKey, HashMap<ConfigNodeId, NodeContextDeclarations>>;

/// Immutable engine-wide requirements used to compile context bindings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRuntimeRequirements {
    /// Requirements for retaining original transport-header names.
    original_name_retention: OriginalNameRetention,
}

/// Original-name retention as a default disposition plus name-specific overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OriginalNameRetention {
    /// Disposition for names without an explicit override.
    default_preserve_original: bool,
    /// Lowercase name-specific dispositions that differ from the default.
    overrides: BTreeMap<Box<str>, bool>,
}

/// Requirements and node bindings prepared from one resolved configuration.
#[derive(Debug, Clone)]
pub struct PreparedContext {
    /// Runtime requirements derived from these declarations.
    pub runtime_requirements: ContextRuntimeRequirements,
    /// Node bindings compiled using the selected engine requirements.
    pub bindings: Arc<CompiledContextBindings>,
}

impl PreparedContext {
    /// Resolves shared layouts before deriving retention requirements and node bindings.
    ///
    /// Candidate bindings use the installed retention profile so an update cannot
    /// silently depend on original names that running receivers have discarded.
    pub fn compile(
        mut declarations: ContextDeclarationsByPipeline,
        resolved: &ResolvedOtelDataflowSpec,
        installed: Option<&ContextRuntimeRequirements>,
    ) -> Result<Self, Error> {
        for pipeline in &resolved.pipelines {
            let key = PipelineKey::new(
                pipeline.pipeline_group_id.clone(),
                pipeline.pipeline_id.clone(),
            );
            let nodes = declarations
                .get_mut(&key)
                .ok_or_else(|| Error::InvalidUserConfig {
                    error: format!("missing context declarations for pipeline {key:?}"),
                })?;
            compile_pipeline(nodes, &pipeline.policies.context)?;
        }
        let runtime_requirements = ContextRuntimeRequirements::compile(&declarations);
        let bindings = CompiledContextBindings::compile(
            declarations,
            installed.unwrap_or(&runtime_requirements),
        );
        Ok(Self {
            runtime_requirements,
            // Immutable startup/live-update snapshots are shared with running pipelines.
            bindings: Arc::new(bindings),
        })
    }
}

fn compile_pipeline(
    nodes: &mut HashMap<ConfigNodeId, NodeContextDeclarations>,
    entries: &[ContextEntryDeclaration],
) -> Result<(), Error> {
    let references = nodes
        .values()
        .flat_map(NodeContextDeclarations::iter)
        .filter_map(|declaration| match declaration {
            ContextDeclaration::HeaderPropagation { policy } => Some(policy),
            _ => None,
        })
        .flat_map(HeaderPropagationPolicy::context_references);
    // Bindings share an immutable layout, never shared mutable request state.
    let layout = Arc::new(ContextLayout::for_references(entries, references)?);
    for declarations in nodes.values_mut() {
        *declarations = std::mem::take(declarations)
            .into_iter()
            .map(|declaration| match declaration {
                ContextDeclaration::HeaderPropagation { policy } => policy
                    .compile_layout(layout.clone())
                    .map(|policy| ContextDeclaration::HeaderPropagation { policy })
                    .map_err(|error| Error::InvalidUserConfig { error }),
                declaration => Ok(declaration),
            })
            .collect::<Result<NodeContextDeclarations, Error>>()?;
    }
    Ok(())
}

impl ContextRuntimeRequirements {
    /// Derives engine-lifetime retention requirements from resolved declarations.
    #[must_use]
    pub fn compile(declarations: &ContextDeclarationsByPipeline) -> Self {
        declarations
            .values()
            .flat_map(HashMap::values)
            .flat_map(NodeContextDeclarations::iter)
            .fold(Self::none(), |requirements, declaration| {
                requirements.union(declaration.context_runtime_requirements())
            })
    }

    fn none() -> Self {
        Self {
            original_name_retention: OriginalNameRetention {
                default_preserve_original: false,
                overrides: BTreeMap::new(),
            },
        }
    }

    fn union(self, other: Self) -> Self {
        Self {
            original_name_retention: self
                .original_name_retention
                .union(other.original_name_retention),
        }
    }

    /// Returns whether the installed requirements satisfy every candidate requirement.
    #[must_use]
    pub fn can_satisfy(&self, candidate: &Self) -> bool {
        self.original_name_retention
            .can_satisfy(&candidate.original_name_retention)
    }

    /// Returns whether captured entries with this stored name retain the original wire name.
    #[must_use]
    pub fn preserves_original_name(&self, name: &ContextEntryName) -> bool {
        self.original_name_retention.preserves_original_name(name)
    }
}

impl OriginalNameRetention {
    fn union(self, other: Self) -> Self {
        let default_preserve_original =
            self.default_preserve_original || other.default_preserve_original;
        let mut names = self
            .overrides
            .keys()
            .chain(other.overrides.keys())
            .cloned()
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        let overrides = names
            .into_iter()
            .filter_map(|name| {
                let preserve_original =
                    self.preserves_original_key(&name) || other.preserves_original_key(&name);
                (preserve_original != default_preserve_original)
                    .then_some((name, preserve_original))
            })
            .collect();
        Self {
            default_preserve_original,
            overrides,
        }
    }

    fn can_satisfy(&self, candidate: &Self) -> bool {
        if candidate.default_preserve_original && !self.default_preserve_original {
            return false;
        }
        self.overrides
            .keys()
            .chain(candidate.overrides.keys())
            .all(|name| {
                !candidate.preserves_original_key(name) || self.preserves_original_key(name)
            })
    }

    fn preserves_original_name(&self, name: &ContextEntryName) -> bool {
        self.preserves_original_key(&original_name_key(name))
    }

    fn preserves_original_key(&self, name: &str) -> bool {
        self.overrides
            .get(name)
            .copied()
            .unwrap_or(self.default_preserve_original)
    }
}

fn original_name_key(name: &ContextEntryName) -> Box<str> {
    name.as_str().to_ascii_lowercase().into()
}

impl CompiledNodeBindings {
    fn compile(
        declarations: NodeContextDeclarations,
        requirements: &ContextRuntimeRequirements,
    ) -> Self {
        let mut component_declarations = Vec::new();
        let mut header_capture = None;
        let mut header_propagation = None;
        let mut authorized_identity_capture = None;
        for declaration in declarations {
            match declaration {
                declaration @ (ContextDeclaration::Produces { .. }
                | ContextDeclaration::Consumes { .. }) => {
                    component_declarations.push(declaration);
                }
                ContextDeclaration::HeaderCapture { policy } => {
                    header_capture =
                        Some(policy.compile(|name| requirements.preserves_original_name(name)));
                }
                ContextDeclaration::HeaderPropagation { policy } => {
                    header_propagation = Some(policy);
                }
                ContextDeclaration::AuthorizedIdentityCapture { policy } => {
                    authorized_identity_capture = Some(policy);
                }
            }
        }

        Self {
            component_declarations: component_declarations.into_iter().collect(),
            header_capture,
            header_propagation,
            authorized_identity_capture,
        }
    }

    fn is_empty(&self) -> bool {
        self.component_declarations.is_empty()
            && self.header_capture.is_none()
            && self.header_propagation.is_none()
            && self.authorized_identity_capture.is_none()
    }
}

impl CompiledContextBindings {
    /// Creates an empty binding set. Node validation always fails.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            by_pipeline: HashMap::new(),
        }
    }

    /// Compiles already-resolved node declarations with a selected retention profile.
    #[must_use]
    pub fn compile(
        declarations: ContextDeclarationsByPipeline,
        requirements: &ContextRuntimeRequirements,
    ) -> Self {
        let by_pipeline = declarations
            .into_iter()
            .map(|(pipeline, nodes)| {
                let nodes = nodes
                    .into_iter()
                    .map(|(node, declarations)| {
                        (
                            node,
                            CompiledNodeBindings::compile(declarations, requirements),
                        )
                    })
                    .collect();
                (pipeline, nodes)
            })
            .collect();

        Self { by_pipeline }
    }

    /// Returns the node's compiled header capture policy.
    #[must_use]
    pub fn header_capture_policy(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
    ) -> Option<&CompiledHeaderCapturePolicy> {
        self.by_pipeline
            .get(pipeline)?
            .get(node)?
            .header_capture
            .as_ref()
    }

    /// Returns the node's header propagation policy.
    #[must_use]
    pub fn header_propagation_policy(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
    ) -> Option<&HeaderPropagationPolicy> {
        self.by_pipeline
            .get(pipeline)?
            .get(node)?
            .header_propagation
            .as_ref()
    }

    /// Returns the node's authorized identity claim projection policy.
    #[must_use]
    pub fn authorized_identity_policy(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
    ) -> Option<&AuthorizedIdentityPolicy> {
        self.by_pipeline
            .get(pipeline)?
            .get(node)?
            .authorized_identity_capture
            .as_ref()
    }

    /// Returns whether two binding sets contain identical non-empty bindings for one pipeline.
    ///
    /// Nodes without context declarations do not affect compiled bindings and
    /// may be added, removed, or renamed during an otherwise safe live update.
    #[must_use]
    pub fn pipeline_bindings_match(&self, other: &Self, pipeline: &PipelineKey) -> bool {
        let current = self.by_pipeline.get(pipeline);
        let candidate = other.by_pipeline.get(pipeline);
        let current_binding_count = current
            .into_iter()
            .flat_map(|nodes| nodes.values())
            .filter(|node| !node.is_empty())
            .count();
        let candidate_binding_count = candidate
            .into_iter()
            .flat_map(|nodes| nodes.values())
            .filter(|node| !node.is_empty())
            .count();

        current_binding_count == candidate_binding_count
            && current
                .into_iter()
                .flat_map(|nodes| nodes.iter())
                .all(|(node_id, node)| {
                    node.is_empty()
                        || candidate
                            .and_then(|nodes| nodes.get(node_id))
                            .is_some_and(|candidate_node| candidate_node == node)
                })
    }

    /// Checks component declarations against this node's compiled bindings.
    /// Call after parsing the node configuration.
    pub fn validate_node_declarations(
        &self,
        pipeline: &PipelineKey,
        node: &ConfigNodeId,
        declarations: &NodeContextDeclarations,
    ) -> Result<(), Error> {
        match self
            .by_pipeline
            .get(pipeline)
            .and_then(|nodes| nodes.get(node))
        {
            Some(expected) if expected.component_declarations == *declarations => Ok(()),
            _ => Err(Error::UnrecognizedContextDeclaration {}),
        }
    }
}
