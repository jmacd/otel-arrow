// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Ordered partition bindings over the existing context representation.

use super::{
    ContextConsumerSelector, ContextDomain, ContextEntrySelectorForm, ContextEntryTarget,
    ContextFieldLayout, ContextMaterializationPlan, ContextProjection, ContextValueCardinality,
    ContextValueKind, ContextValueSource, MaterializedContext, MaterializedContextValues,
    invalid_context,
};
use otel_arrow_dfe_config::error::Error;
use std::fmt;

/// An ordered grouping projection resolved against its pipeline's context declarations.
#[derive(Clone, Debug)]
pub struct ContextPartitionBinding {
    selector: ContextConsumerSelector,
    sources: Box<[PartitionSource]>,
}

#[derive(Clone, Debug)]
enum PartitionSource {
    Primitive(ContextFieldLayout),
    Composite {
        plan: ContextMaterializationPlan,
        projection: ContextProjection,
    },
}

/// An owned, collision-safe value tuple. Missing differs from a present empty value.
///
/// Original wire names are deliberately not part of partition identity. Values
/// retain their kind, cardinality, member boundaries, and source order.
/// Each selected reference has its own presence marker; absence does not erase
/// other dimensions. A whole composite occupies one atomically present dimension.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ContextPartitionKey(Vec<Option<Vec<PartitionMember>>>);

#[derive(Clone, PartialEq, Eq, Hash)]
struct PartitionMember {
    cardinality: ContextValueCardinality,
    values: Vec<(ContextValueKind, Box<[u8]>)>,
}

impl From<&MaterializedContextValues> for PartitionMember {
    fn from(values: &MaterializedContextValues) -> Self {
        Self {
            cardinality: values.cardinality(),
            values: values
                .values()
                .iter()
                .map(|value| (value.kind(), value.bytes().into()))
                .collect(),
        }
    }
}

impl fmt::Debug for ContextPartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContextPartitionKey")
            .field("dimensions", &self.0.len())
            .field(
                "present",
                &self.0.iter().filter(|entry| entry.is_some()).count(),
            )
            .finish()
    }
}

impl ContextPartitionBinding {
    pub(super) fn compile(
        selector: &ContextConsumerSelector,
        plan: Option<&ContextMaterializationPlan>,
    ) -> Result<Self, Error> {
        let ContextConsumerSelector::Entries { entries } = selector else {
            return Err(invalid_context(
                "partitioning requires explicit context selections",
            ));
        };
        if entries.is_empty() {
            return Err(invalid_context(
                "partitioning requires at least one context reference",
            ));
        }
        let sources = entries
            .iter()
            .map(|entry| {
                if entry.form != ContextEntrySelectorForm::Value {
                    return Err(invalid_context("partitioning requires value selections"));
                }
                Ok(match &entry.target {
                    ContextEntryTarget::Primitive { domain, name } => {
                        PartitionSource::Primitive(ContextFieldLayout {
                            domain: *domain,
                            name: name.clone(),
                        })
                    }
                    ContextEntryTarget::Composite { name }
                    | ContextEntryTarget::CompositeMember {
                        composite: name, ..
                    } => {
                        let plan = plan.ok_or_else(|| {
                            invalid_context(format!(
                                "no materialization plan for context entry `{name}`"
                            ))
                        })?;
                        let projection = match &entry.target {
                            ContextEntryTarget::CompositeMember { member, .. } => {
                                plan.layout().resolve_member(name, member)?
                            }
                            _ => plan.layout().resolve_composite(name)?,
                        };
                        PartitionSource::Composite {
                            projection,
                            plan: plan.clone(),
                        }
                    }
                })
            })
            .collect::<Result<Box<[_]>, Error>>()?;
        Ok(Self {
            selector: selector.clone(),
            sources,
        })
    }

    /// Returns the declared selection, including reference order and source domains.
    #[must_use]
    pub fn selector(&self) -> &ContextConsumerSelector {
        &self.selector
    }

    /// Visits the independently selected primitive fields.
    pub fn primitive_fields(&self) -> impl Iterator<Item = &ContextFieldLayout> {
        self.sources.iter().filter_map(|source| match source {
            PartitionSource::Primitive(field) => Some(field),
            PartitionSource::Composite { .. } => None,
        })
    }

    /// Computes a key using current primitives or an existing composite snapshot.
    #[must_use]
    pub fn key(
        &self,
        context: &MaterializedContext,
        source: &impl ContextValueSource,
    ) -> ContextPartitionKey {
        let dimensions = self
            .sources
            .iter()
            .map(|selection| match selection {
                PartitionSource::Primitive(field) => {
                    let values = match field.domain {
                        ContextDomain::TransportHeader => context.transport_values(field),
                        ContextDomain::AuthorizedIdentity => source.values(field),
                    };
                    values.map(|values| vec![PartitionMember::from(&values)])
                }
                PartitionSource::Composite { plan, projection } => {
                    context.resolve(plan, projection).map(|selected| {
                        let mut members = Vec::new();
                        selected.visit_members(|member| {
                            members.push(PartitionMember::from(member.values()));
                        });
                        members
                    })
                }
            })
            .collect();
        ContextPartitionKey(dimensions)
    }

    /// Copies only selected composite members, without re-materializing their snapshots.
    ///
    /// Primitive bindings are projected by the owning pdata context. Wire names
    /// are omitted because they are not part of partition identity.
    #[must_use]
    pub fn project_materialized(&self, context: &MaterializedContext) -> MaterializedContext {
        MaterializedContext::project_partitions(self.sources.iter().filter_map(
            |source| match source {
                PartitionSource::Primitive(_) => None,
                PartitionSource::Composite { plan, projection } => {
                    context.resolve(plan, projection)
                }
            },
        ))
    }
}

#[cfg(test)]
#[path = "partition_tests.rs"]
mod tests;
