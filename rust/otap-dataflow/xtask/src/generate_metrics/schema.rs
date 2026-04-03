//! Schema parser for metrics YAML with x-otap-levels extensions.
//!
//! Parses OTel SemConv-compatible YAML and extracts the custom
//! `x-otap-levels` extension for level-aware dimensioning.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Top-level schema file.
#[derive(Debug, Deserialize)]
pub struct SchemaFile {
    pub groups: Vec<Group>,
}

/// A group in the schema (metric, attribute_group, etc.).
#[derive(Debug, Deserialize)]
pub struct Group {
    pub id: String,
    #[serde(rename = "type")]
    pub group_type: String,
    #[serde(default)]
    pub brief: String,
    pub metric_name: Option<String>,
    pub instrument: Option<String>,
    pub unit: Option<String>,
    #[allow(dead_code)]
    pub stability: Option<String>,
    #[serde(default)]
    pub attributes: Vec<AttributeRef>,
    #[serde(rename = "x-otap-levels")]
    pub otap_levels: Option<BTreeMap<String, LevelConfig>>,
}

/// An attribute reference in a metric group.
#[derive(Debug, Deserialize)]
pub struct AttributeRef {
    /// Inline attribute definition.
    pub id: Option<String>,
    /// Reference to a shared attribute.
    #[allow(dead_code)]
    #[serde(rename = "ref")]
    pub attr_ref: Option<String>,
    #[allow(dead_code)]
    #[serde(rename = "type")]
    pub attr_type: Option<serde_yaml::Value>,
    #[allow(dead_code)]
    pub brief: Option<String>,
    #[allow(dead_code)]
    pub examples: Option<Vec<String>>,
    #[serde(rename = "enum")]
    pub enum_def: Option<EnumDef>,
    #[allow(dead_code)]
    pub requirement_level: Option<serde_yaml::Value>,
}

/// Enum definition for an attribute.
#[derive(Debug, Deserialize)]
pub struct EnumDef {
    pub members: Vec<EnumMember>,
}

/// A single enum member.
#[derive(Debug, Deserialize)]
pub struct EnumMember {
    #[allow(dead_code)]
    pub id: String,
    pub value: String,
}

/// Level-specific configuration from x-otap-levels.
#[derive(Debug, Deserialize)]
pub struct LevelConfig {
    pub dimensions: Vec<String>,
}

/// A parsed metric definition ready for code generation.
#[derive(Debug)]
pub struct MetricDef {
    #[allow(dead_code)]
    pub id: String,
    pub metric_name: String,
    pub instrument: String,
    pub unit: String,
    pub brief: String,
    pub levels: BTreeMap<String, Vec<DimensionRef>>,
}

/// A dimension reference with its attribute metadata.
#[derive(Debug, Clone)]
pub struct DimensionRef {
    pub attr_name: String,
    pub values: Vec<String>,
}

/// A fully resolved schema ready for code generation.
#[derive(Debug)]
pub struct ResolvedSchema {
    pub metrics: Vec<MetricDef>,
    #[allow(dead_code)]
    pub attributes: BTreeMap<String, AttributeDef>,
}

/// A resolved attribute definition.
#[derive(Debug, Clone)]
pub struct AttributeDef {
    pub name: String,
    pub values: Vec<String>,
}

impl SchemaFile {
    /// Parse a schema from a YAML file.
    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let schema: SchemaFile = serde_yaml::from_str(&content)?;
        Ok(schema)
    }

    /// Resolve the schema into code-generation-ready structures.
    pub fn resolve(&self) -> anyhow::Result<ResolvedSchema> {
        // First pass: collect attribute definitions from attribute_group entries
        let mut attributes: BTreeMap<String, AttributeDef> = BTreeMap::new();
        for group in &self.groups {
            if group.group_type == "attribute_group" {
                for attr in &group.attributes {
                    if let Some(id) = &attr.id {
                        let values = attr
                            .enum_def
                            .as_ref()
                            .map(|e| e.members.iter().map(|m| m.value.clone()).collect())
                            .unwrap_or_default();
                        attributes.insert(
                            id.clone(),
                            AttributeDef {
                                name: id.clone(),
                                values,
                            },
                        );
                    }
                }
            }
        }

        // Second pass: resolve metric definitions
        let mut metrics = Vec::new();
        for group in &self.groups {
            if group.group_type != "metric" {
                continue;
            }
            let Some(ref otap_levels) = group.otap_levels else {
                continue;
            };

            let metric_name = group
                .metric_name
                .as_deref()
                .unwrap_or(&group.id)
                .to_string();
            let instrument = group.instrument.as_deref().unwrap_or("counter").to_string();
            let unit = group.unit.as_deref().unwrap_or("").to_string();

            let mut levels = BTreeMap::new();
            for (level_name, level_config) in otap_levels {
                let mut dims = Vec::new();
                for dim_name in &level_config.dimensions {
                    let attr = attributes.get(dim_name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "dimension '{}' referenced in metric '{}' level '{}' not found in attributes",
                            dim_name,
                            metric_name,
                            level_name
                        )
                    })?;
                    dims.push(DimensionRef {
                        attr_name: attr.name.clone(),
                        values: attr.values.clone(),
                    });
                }
                levels.insert(level_name.clone(), dims);
            }

            metrics.push(MetricDef {
                id: group.id.clone(),
                metric_name,
                instrument,
                unit,
                brief: group.brief.clone(),
                levels,
            });
        }

        Ok(ResolvedSchema {
            metrics,
            attributes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_pilot_schema() {
        let yaml = r#"
groups:
  - id: registry.outcome
    type: attribute_group
    brief: "Outcome attribute."
    attributes:
      - id: outcome
        type: string
        brief: "The outcome."
        enum:
          members:
            - id: success
              value: success
            - id: failure
              value: failure
            - id: refused
              value: refused

  - id: registry.signal_type
    type: attribute_group
    brief: "Signal type attribute."
    attributes:
      - id: signal_type
        type: string
        brief: "The signal type."
        enum:
          members:
            - id: traces
              value: traces
            - id: metrics
              value: metrics
            - id: logs
              value: logs

  - id: metric.node.consumer.items
    type: metric
    metric_name: node.consumer.items
    instrument: counter
    unit: "{item}"
    brief: "Items consumed by this node."
    attributes:
      - ref: outcome
      - ref: signal_type
    x-otap-levels:
      basic:
        dimensions: [outcome]
      normal:
        dimensions: [outcome, signal_type]
      detailed:
        dimensions: [outcome, signal_type]
"#;
        let schema: SchemaFile = serde_yaml::from_str(yaml).expect("should parse");
        assert_eq!(schema.groups.len(), 3);

        let resolved = schema.resolve().expect("should resolve");
        assert_eq!(resolved.metrics.len(), 1);
        assert_eq!(resolved.attributes.len(), 2);

        let metric = &resolved.metrics[0];
        assert_eq!(metric.metric_name, "node.consumer.items");
        assert_eq!(metric.instrument, "counter");

        let basic = &metric.levels["basic"];
        assert_eq!(basic.len(), 1);
        assert_eq!(basic[0].attr_name, "outcome");
        assert_eq!(basic[0].values.len(), 3);

        let normal = &metric.levels["normal"];
        assert_eq!(normal.len(), 2);
        assert_eq!(normal[0].attr_name, "outcome");
        assert_eq!(normal[1].attr_name, "signal_type");
    }
}
