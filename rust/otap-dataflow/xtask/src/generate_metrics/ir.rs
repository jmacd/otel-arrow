// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Intermediate representation for the metrics codegen pipeline.
//!
//! The schema parser produces these IR types from YAML; the code
//! generator consumes them to render MiniJinja templates.

use serde::{Deserialize, Serialize};

/// Top-level schema: shared attributes + metric set definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSchema {
    /// Shared attribute (dimension) definitions.
    #[serde(default)]
    pub attributes: Vec<AttributeDef>,
    /// Metric set definitions.
    pub metric_sets: Vec<MetricSetDef>,
}

/// A shared attribute definition that can be referenced as a dimension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDef {
    /// Attribute identifier (e.g., "outcome").
    pub id: String,
    /// Value type (currently only "string" supported).
    #[serde(rename = "type")]
    pub attr_type: String,
    /// Short description.
    #[serde(default)]
    pub brief: String,
    /// Enumerated values (for bounded dimensions).
    pub values: Vec<String>,
}

/// A metric set groups related metrics that share entity attributes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSetDef {
    /// Identifier used for the generated Rust type (e.g., "node_consumer").
    pub id: String,
    /// Short description.
    #[serde(default)]
    pub brief: String,
    /// Individual metrics in this set.
    pub metrics: Vec<MetricDef>,
}

/// A single metric within a metric set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDef {
    /// Canonical metric name (e.g., "node.consumer.items").
    pub name: String,
    /// Instrument archetype.
    pub instrument: Archetype,
    /// Unit string.
    #[serde(default)]
    pub unit: String,
    /// Short description.
    #[serde(default)]
    pub brief: String,
    /// Numeric value type.
    #[serde(default)]
    pub value_type: ValueType,
    /// OTAP-specific extensions.
    #[serde(rename = "x-otap")]
    pub otap: OtapConfig,
}

/// Instrument archetype — the semantic kind of the metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Archetype {
    Counter,
    Gauge,
}

/// Numeric value type for the metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    U64,
    F64,
}

impl Default for ValueType {
    fn default() -> Self {
        Self::U64
    }
}

/// Recording mode — how measurements are aggregated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingMode {
    /// Temporal sum (default for Counter).
    Counting,
    /// Value distribution (MMSC at Basic, ExpoHisto at Normal/Detailed).
    Histogram,
    /// Keep latest value (default for Gauge).
    LastValue,
}

/// Interface style — how callers report values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceStyle {
    /// Report deltas: `add(5)`.
    Delta,
    /// Report cumulative observed values: `observe(total)`.
    Cumulative,
}

impl Default for InterfaceStyle {
    fn default() -> Self {
        Self::Delta
    }
}

/// OTAP-specific metric configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtapConfig {
    /// Interface style (delta or cumulative).
    #[serde(default)]
    pub interface: InterfaceStyle,
    /// Recording mode.
    pub recording_mode: RecordingMode,
    /// Per-level configuration.
    pub levels: LevelConfigs,
    /// Optional view overrides.
    #[serde(default)]
    pub view: Option<ViewOverride>,
}

/// Per-level configuration for all three metric levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelConfigs {
    pub basic: LevelConfig,
    pub normal: LevelConfig,
    pub detailed: LevelConfig,
}

/// Configuration for a single metric level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelConfig {
    /// Active dimension attribute IDs at this level.
    #[serde(default)]
    pub dimensions: Vec<String>,
    /// ExpoHisto word count (0 = MMSC, 8/16 = ExpoHisto<8>/<16>).
    /// Only meaningful when recording_mode = histogram.
    #[serde(default)]
    pub histogram_size: usize,
}

/// Optional view overrides (name, description).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ViewOverride {
    /// Override metric name in the output.
    #[serde(default)]
    pub name: Option<String>,
    /// Override metric description in the output.
    #[serde(default)]
    pub description: Option<String>,
}

// ── Computed fields for codegen ──────────────────────────────────────

/// Resolved metric with computed layout information for code generation.
#[derive(Debug, Clone)]
pub struct ResolvedMetric {
    /// The original metric definition.
    pub def: MetricDef,
    /// Resolved dimensions with their cardinalities.
    pub dimensions: Vec<ResolvedDimension>,
    /// Per-level computed layouts.
    pub levels: ResolvedLevels,
}

/// A dimension resolved against the attribute definitions.
#[derive(Debug, Clone)]
pub struct ResolvedDimension {
    /// Attribute ID.
    pub id: String,
    /// Attribute key for OTAP encoding.
    pub key: String,
    /// Enumerated values.
    pub values: Vec<String>,
    /// Number of distinct values.
    pub cardinality: usize,
}

/// Computed layouts for all three levels.
#[derive(Debug, Clone)]
pub struct ResolvedLevels {
    pub basic: ResolvedLevelLayout,
    pub normal: ResolvedLevelLayout,
    pub detailed: ResolvedLevelLayout,
}

/// Computed layout for one level.
#[derive(Debug, Clone)]
pub struct ResolvedLevelLayout {
    /// Which dimensions are active at this level (indices into
    /// the metric's dimensions vec).
    pub active_dimensions: Vec<usize>,
    /// Total number of data points = product of active dimension cardinalities.
    pub total_points: usize,
    /// Histogram word count (0 = MMSC, >0 = ExpoHisto<N>).
    pub histogram_size: usize,
}
