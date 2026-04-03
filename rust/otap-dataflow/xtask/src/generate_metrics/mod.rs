//! Metrics code generation from SemConv YAML schemas.
//!
//! Parses schema YAML with x-otap-levels extensions and renders
//! MiniJinja templates to produce Rust source files.

pub mod schema;

use anyhow::{Context, Result};
use minijinja::Environment;
use schema::{DimensionRef, ResolvedSchema, SchemaFile};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Template context for a single metric.
#[derive(Debug, Serialize)]
struct MetricCtx {
    metric_name: String,
    rust_name: String,
    brief: String,
    unit: String,
    instrument: String,
    /// Deduplicated enum variants.
    levels: Vec<LevelCtx>,
    /// Maps each MetricLevel name to its enum variant name.
    /// E.g., basic->Basic, normal->Normal, detailed->Normal.
    level_mapping: Vec<LevelMappingEntry>,
    all_dimensions: Vec<DimCtx>,
}

/// Maps a MetricLevel to its enum variant.
#[derive(Debug, Serialize)]
struct LevelMappingEntry {
    metric_level: String,
    variant: String,
}

/// Template context for a level variant.
#[derive(Debug, Serialize, Clone)]
struct LevelCtx {
    name: String,
    total_points: usize,
    dimension_names: String,
    index_expr: String,
    attrs_per_point: usize,
}

/// Flattened attribute key/value for templates.
#[derive(Debug, Serialize, Clone)]
struct AttrKV {
    key: String,
    value: String,
}

/// A metric def for a specific level (used in precomputed_schema generation).
#[derive(Debug, Serialize, Clone)]
struct LevelMetricDef {
    name: String,
    unit: String,
    description: String,
    num_points: usize,
    flat_attributes: Vec<AttrKV>,
    attrs_per_point: usize,
}

/// Per-level collection of metric defs for the precomputed_schema function.
#[derive(Debug, Serialize)]
struct LevelDefCtx {
    level_name: String,
    metric_defs: Vec<LevelMetricDef>,
}

/// Template context for a dimension parameter.
#[derive(Debug, Serialize)]
struct DimCtx {
    attr_name: String,
    rust_type: String,
}

/// Top-level template context.
#[derive(Debug, Serialize)]
struct TemplateCtx {
    metrics: Vec<MetricCtx>,
    level_defs: Vec<LevelDefCtx>,
}

/// Map an attribute name to its Rust dimension type.
fn dimension_rust_type(attr_name: &str) -> &'static str {
    match attr_name {
        "outcome" => "Outcome",
        "signal_type" => "SignalType",
        _ => "usize",
    }
}

/// Capitalize first letter.
fn capitalize(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_uppercase().to_string() + chars.as_str(),
    }
}

/// Map a dimension name to the cardinality (number of values).
fn dimension_cardinality(dim: &DimensionRef) -> usize {
    dim.values.len()
}

/// Build the index expression for a level's dimensions.
///
/// For a single dimension `outcome` with 3 values, the expr is
/// `outcome.index()`.
///
/// For two dimensions `outcome(3) × signal_type(3)`, the expr is
/// `outcome.index() * 3 + signal_type.index()`.
fn build_index_expr(dims: &[DimensionRef]) -> String {
    if dims.is_empty() {
        return "0".to_string();
    }
    if dims.len() == 1 {
        return format!("{}.index()", dims[0].attr_name);
    }

    let mut parts = Vec::new();
    for (i, dim) in dims.iter().enumerate() {
        let suffix_product: usize = dims[i + 1..].iter().map(dimension_cardinality).product();
        if suffix_product > 1 {
            parts.push(format!("{}.index() * {}", dim.attr_name, suffix_product));
        } else {
            parts.push(format!("{}.index()", dim.attr_name));
        }
    }
    parts.join(" + ")
}

/// Build the cartesian product of attribute tuples for all data points.
fn build_point_attributes(dims: &[DimensionRef]) -> Vec<Vec<(String, String)>> {
    if dims.is_empty() {
        return vec![vec![]];
    }

    let mut result: Vec<Vec<(String, String)>> = vec![vec![]];
    for dim in dims {
        let mut new_result = Vec::new();
        for existing in &result {
            for val in &dim.values {
                let mut row = existing.clone();
                row.push((dim.attr_name.clone(), val.clone()));
                new_result.push(row);
            }
        }
        result = new_result;
    }
    result
}

/// Build template context from a resolved schema.
fn build_context(schema: &ResolvedSchema) -> TemplateCtx {
    let mut metrics = Vec::new();
    let level_order = ["basic", "normal", "detailed"];

    // Collect level data per metric for the precomputed_schema section
    let mut level_defs_map: BTreeMap<String, Vec<LevelMetricDef>> = BTreeMap::new();

    for metric_def in &schema.metrics {
        let mut levels = Vec::new();

        for level_name in &level_order {
            if let Some(dims) = metric_def.levels.get(*level_name) {
                let total_points: usize = if dims.is_empty() {
                    1
                } else {
                    dims.iter().map(dimension_cardinality).product()
                };
                let dimension_names = dims
                    .iter()
                    .map(|d| d.attr_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let index_expr = build_index_expr(dims);
                let attrs_per_point = dims.len();

                let level_ctx = LevelCtx {
                    name: level_name.to_string(),
                    total_points,
                    dimension_names,
                    index_expr,
                    attrs_per_point,
                };

                // Deduplicate: if this level has the same shape as the
                // previous, skip adding a new enum variant.
                let dominated = levels.last().is_some_and(|prev: &LevelCtx| {
                    prev.total_points == level_ctx.total_points
                        && prev.dimension_names == level_ctx.dimension_names
                });

                if !dominated {
                    levels.push(level_ctx);
                }

                // Build flat attributes for precomputed schema
                let point_attrs = build_point_attributes(dims);
                let flat_attributes: Vec<AttrKV> = point_attrs
                    .iter()
                    .flatten()
                    .map(|(k, v)| AttrKV {
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .collect();

                level_defs_map
                    .entry(level_name.to_string())
                    .or_default()
                    .push(LevelMetricDef {
                        name: metric_def.metric_name.clone(),
                        unit: metric_def.unit.clone(),
                        description: metric_def.brief.clone(),
                        num_points: total_points,
                        flat_attributes,
                        attrs_per_point,
                    });
            }
        }

        // Collect the superset of all dimensions (from the highest level)
        let all_dims: Vec<DimCtx> = metric_def
            .levels
            .values()
            .max_by_key(|dims| dims.len())
            .unwrap_or(&Vec::new())
            .iter()
            .map(|d| DimCtx {
                attr_name: d.attr_name.clone(),
                rust_type: dimension_rust_type(&d.attr_name).to_string(),
            })
            .collect();

        // Build level_mapping: each MetricLevel maps to the highest-matching
        // enum variant. If Detailed has the same shape as Normal, Detailed
        // maps to Normal.
        let mut level_mapping = Vec::new();
        let mut current_variant = "None".to_string();
        for level_name in &level_order {
            if metric_def.levels.contains_key(*level_name) {
                // Check if this level introduced a new variant
                if let Some(variant) = levels.iter().find(|l| l.name == *level_name) {
                    current_variant = capitalize(&variant.name);
                }
                // Otherwise, reuse the previous variant (dedup case)
                level_mapping.push(LevelMappingEntry {
                    metric_level: capitalize(level_name),
                    variant: current_variant.clone(),
                });
            } else {
                level_mapping.push(LevelMappingEntry {
                    metric_level: capitalize(level_name),
                    variant: "None".to_string(),
                });
            }
        }

        // Rust type name: node.consumer.items -> NodeConsumerItems
        let rust_name = metric_def
            .metric_name
            .split('.')
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    None => String::new(),
                    Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
                }
            })
            .collect::<String>();

        metrics.push(MetricCtx {
            metric_name: metric_def.metric_name.clone(),
            rust_name,
            brief: metric_def.brief.clone(),
            unit: metric_def.unit.clone(),
            instrument: metric_def.instrument.clone(),
            levels,
            level_mapping,
            all_dimensions: all_dims,
        });
    }

    // Build level_defs in order, handling fallback for levels that
    // aren't explicitly defined (they use the previous level's defs)
    let mut level_defs = Vec::new();
    let mut last_defs: Option<Vec<LevelMetricDef>> = None;
    for level_name in &level_order {
        if let Some(defs) = level_defs_map.remove(*level_name) {
            last_defs = Some(defs.clone());
            level_defs.push(LevelDefCtx {
                level_name: level_name.to_string(),
                metric_defs: defs,
            });
        } else if let Some(ref defs) = last_defs {
            // Fallback: use previous level's defs
            level_defs.push(LevelDefCtx {
                level_name: level_name.to_string(),
                metric_defs: defs.clone(),
            });
        }
    }

    TemplateCtx {
        metrics,
        level_defs,
    }
}

/// Generate Rust code for a metrics schema file.
pub fn generate(
    schema_path: &Path,
    template_dir: &Path,
    output_path: &Path,
) -> Result<()> {
    // Parse schema
    let schema_file = SchemaFile::from_file(schema_path).context("failed to parse schema YAML")?;
    let resolved = schema_file.resolve().context("failed to resolve schema")?;

    // Build template context
    let ctx = build_context(&resolved);

    // Load and render template
    let template_path = template_dir.join("counter_set.rs.j2");
    let template_source = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read template: {}", template_path.display()))?;

    let mut env = Environment::new();
    env.add_template("counter_set", &template_source)
        .context("failed to compile template")?;

    let tmpl = env
        .get_template("counter_set")
        .context("template not found")?;
    let rendered = tmpl.render(&ctx).context("failed to render template")?;

    // Write output
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(output_path, rendered)
        .with_context(|| format!("failed to write output: {}", output_path.display()))?;

    println!("Generated: {}", output_path.display());
    Ok(())
}

/// Run generate-metrics for all schema files in the workspace.
pub fn run() -> Result<()> {
    let workspace_root = Path::new(".");
    let template_dir = workspace_root.join("templates/metrics");

    // Pilot: crates/telemetry/self_metrics.yaml
    let schema_path = workspace_root.join("crates/telemetry/self_metrics.yaml");
    if schema_path.exists() {
        let output_path = workspace_root.join("crates/telemetry/src/self_metrics/generated.rs");
        generate(
            &schema_path,
            &template_dir,
            &output_path,
        )?;
    } else {
        println!("No schema found at: {}", schema_path.display());
    }

    Ok(())
}
