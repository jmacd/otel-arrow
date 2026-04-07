// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! YAML schema parser for the metrics codegen pipeline.
//!
//! Reads a `metrics_schema.yaml` file, validates it, and resolves
//! attribute references into computed layouts suitable for code generation.

use super::ir::*;
use std::collections::HashMap;
use std::path::Path;

/// Parse a metrics schema YAML file and resolve all attribute references.
pub fn parse_and_resolve(path: &Path) -> anyhow::Result<(MetricSchema, Vec<ResolvedMetricSet>)> {
    let content = std::fs::read_to_string(path)?;
    let schema: MetricSchema = serde_yaml::from_str(&content)?;

    // Build attribute lookup.
    let attr_map: HashMap<&str, &AttributeDef> =
        schema.attributes.iter().map(|a| (a.id.as_str(), a)).collect();

    let mut resolved_sets = Vec::new();
    for set_def in &schema.metric_sets {
        let mut resolved_metrics = Vec::new();
        for metric in &set_def.metrics {
            let resolved = resolve_metric(metric, &attr_map)?;
            resolved_metrics.push(resolved);
        }
        resolved_sets.push(ResolvedMetricSet {
            id: set_def.id.clone(),
            brief: set_def.brief.clone(),
            metrics: resolved_metrics,
        });
    }

    Ok((schema, resolved_sets))
}

/// A metric set with resolved dimension information.
#[derive(Debug, Clone)]
pub struct ResolvedMetricSet {
    pub id: String,
    pub brief: String,
    pub metrics: Vec<ResolvedMetric>,
}

fn resolve_metric(
    metric: &MetricDef,
    attr_map: &HashMap<&str, &AttributeDef>,
) -> anyhow::Result<ResolvedMetric> {
    // Collect all dimension IDs referenced across all levels.
    let all_dim_ids = collect_all_dimensions(&metric.otap.levels);

    // Resolve each dimension against the attribute map.
    let mut dimensions = Vec::new();
    for dim_id in &all_dim_ids {
        let attr = attr_map.get(dim_id.as_str()).ok_or_else(|| {
            anyhow::anyhow!(
                "Metric '{}': unknown attribute '{}' referenced in dimensions",
                metric.name,
                dim_id
            )
        })?;
        dimensions.push(ResolvedDimension {
            id: attr.id.clone(),
            key: attr.id.clone(),
            values: attr.values.clone(),
            cardinality: attr.values.len(),
        });
    }

    // Validate archetype × recording mode compatibility.
    validate_archetype_mode(metric.instrument, metric.otap.recording_mode, &metric.name)?;

    // Compute per-level layouts.
    let levels = ResolvedLevels {
        basic: resolve_level(&metric.otap.levels.basic, &dimensions, &all_dim_ids),
        normal: resolve_level(&metric.otap.levels.normal, &dimensions, &all_dim_ids),
        detailed: resolve_level(&metric.otap.levels.detailed, &dimensions, &all_dim_ids),
    };

    Ok(ResolvedMetric {
        def: metric.clone(),
        dimensions,
        levels,
    })
}

/// Collect all unique dimension IDs across all levels (preserving first-seen order).
fn collect_all_dimensions(levels: &LevelConfigs) -> Vec<String> {
    let mut seen = Vec::new();
    for level in [&levels.basic, &levels.normal, &levels.detailed] {
        for dim in &level.dimensions {
            if !seen.contains(dim) {
                seen.push(dim.clone());
            }
        }
    }
    seen
}

/// Resolve a single level's layout.
fn resolve_level(
    config: &LevelConfig,
    all_dimensions: &[ResolvedDimension],
    all_dim_ids: &[String],
) -> ResolvedLevelLayout {
    let active_dimensions: Vec<usize> = config
        .dimensions
        .iter()
        .filter_map(|d| all_dim_ids.iter().position(|id| id == d))
        .collect();

    let total_points = if active_dimensions.is_empty() {
        1
    } else {
        active_dimensions
            .iter()
            .map(|&i| all_dimensions[i].cardinality)
            .product()
    };

    ResolvedLevelLayout {
        active_dimensions,
        total_points,
        histogram_size: config.histogram_size,
    }
}

/// Validate that the archetype × recording mode combination is legal.
fn validate_archetype_mode(
    archetype: Archetype,
    mode: RecordingMode,
    metric_name: &str,
) -> anyhow::Result<()> {
    match (archetype, mode) {
        // Counter: counting ✓, histogram ✓, lastvalue ✗
        (Archetype::Counter, RecordingMode::Counting) => Ok(()),
        (Archetype::Counter, RecordingMode::Histogram) => Ok(()),
        (Archetype::Counter, RecordingMode::LastValue) => Err(anyhow::anyhow!(
            "Metric '{}': Counter does not support LastValue recording mode",
            metric_name
        )),
        // Gauge: lastvalue ✓, histogram ✓, counting ✗
        (Archetype::Gauge, RecordingMode::LastValue) => Ok(()),
        (Archetype::Gauge, RecordingMode::Histogram) => Ok(()),
        (Archetype::Gauge, RecordingMode::Counting) => Err(anyhow::anyhow!(
            "Metric '{}': Gauge does not support Counting recording mode (you cannot count a gauge)",
            metric_name
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_yaml() -> &'static str {
        r#"
attributes:
  - id: outcome
    type: string
    brief: "Outcome"
    values: [success, failure, refused]
  - id: signal_type
    type: string
    brief: "Signal"
    values: [logs, metrics, traces]

metric_sets:
  - id: test_set
    brief: "Test"
    metrics:
      - name: test.counter
        instrument: counter
        unit: "{item}"
        brief: "A counter."
        value_type: u64
        x-otap:
          interface: delta
          recording_mode: counting
          levels:
            basic:
              dimensions: [outcome]
            normal:
              dimensions: [outcome, signal_type]
            detailed:
              dimensions: [outcome, signal_type]
"#
    }

    #[test]
    fn parse_yaml() {
        let schema: MetricSchema = serde_yaml::from_str(sample_yaml()).unwrap();
        assert_eq!(schema.attributes.len(), 2);
        assert_eq!(schema.metric_sets.len(), 1);
        assert_eq!(schema.metric_sets[0].metrics.len(), 1);
        assert_eq!(
            schema.metric_sets[0].metrics[0].otap.recording_mode,
            RecordingMode::Counting
        );
    }

    #[test]
    fn resolve_dimensions() {
        let schema: MetricSchema = serde_yaml::from_str(sample_yaml()).unwrap();
        let attr_map: HashMap<&str, &AttributeDef> =
            schema.attributes.iter().map(|a| (a.id.as_str(), a)).collect();

        let metric = &schema.metric_sets[0].metrics[0];
        let resolved = resolve_metric(metric, &attr_map).unwrap();

        assert_eq!(resolved.dimensions.len(), 2);
        assert_eq!(resolved.dimensions[0].id, "outcome");
        assert_eq!(resolved.dimensions[0].cardinality, 3);
        assert_eq!(resolved.dimensions[1].id, "signal_type");
        assert_eq!(resolved.dimensions[1].cardinality, 3);

        // Basic: outcome only → 3 points
        assert_eq!(resolved.levels.basic.total_points, 3);
        assert_eq!(resolved.levels.basic.active_dimensions, vec![0]);

        // Normal: outcome × signal_type → 9 points
        assert_eq!(resolved.levels.normal.total_points, 9);
        assert_eq!(resolved.levels.normal.active_dimensions, vec![0, 1]);
    }

    #[test]
    fn reject_gauge_counting() {
        let result = validate_archetype_mode(
            Archetype::Gauge,
            RecordingMode::Counting,
            "bad.gauge",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot count a gauge"));
    }

    #[test]
    fn reject_counter_lastvalue() {
        let result = validate_archetype_mode(
            Archetype::Counter,
            RecordingMode::LastValue,
            "bad.counter",
        );
        assert!(result.is_err());
    }

    #[test]
    fn allow_counter_histogram() {
        assert!(validate_archetype_mode(
            Archetype::Counter,
            RecordingMode::Histogram,
            "ok",
        )
        .is_ok());
    }

    #[test]
    fn allow_gauge_histogram() {
        assert!(validate_archetype_mode(
            Archetype::Gauge,
            RecordingMode::Histogram,
            "ok",
        )
        .is_ok());
    }
}
