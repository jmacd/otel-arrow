// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Transport header capture and propagation policy declarations.
//!
//! This policy family controls which inbound transport headers are captured
//! by receivers and which captured headers are propagated by exporters.
//!
//! Extraction and propagation are explicit and opt-in. The default behavior
//! is not to forward any inbound headers.
//!
//! TODO: Implement the sensitive capability for headers

use std::fmt;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const MAX_CAPTURE_ENTRIES: usize = 1024;

// -- Stats types --------------------------------------------------------------

/// Statistics returned when one or more matching headers cannot be captured
/// due to policy limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureStats {
    /// Matching headers skipped because `max_entries` was already reached.
    pub skipped_max_entries: usize,
    /// Matching headers skipped because the wire name exceeded `max_name_bytes`.
    pub skipped_name_too_long: usize,
    /// Matching headers skipped because the value exceeded `max_value_bytes`.
    pub skipped_value_too_long: usize,
    /// Matching headers skipped because the packed context would exceed 64 KiB.
    pub skipped_context_too_large: usize,
}

impl fmt::Display for CaptureStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "capture limits exceeded: {} skipped (max_entries), {} skipped (name too long), {} skipped (value too long), {} skipped (context too large)",
            self.skipped_max_entries,
            self.skipped_name_too_long,
            self.skipped_value_too_long,
            self.skipped_context_too_large
        )
    }
}

impl std::error::Error for CaptureStats {}

/// Transport headers policy controlling capture at receivers and
/// propagation at exporters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TransportHeadersPolicy {
    /// Header capture rules applied by receivers.
    #[serde(default)]
    pub header_capture: HeaderCapturePolicy,
    /// Header propagation rules applied by exporters.
    #[serde(default)]
    pub header_propagation: HeaderPropagationPolicy,
}

// -- Header Capture -----------------------------------------------------------

/// Policy controlling which inbound transport headers are captured by
/// receivers and stored in the pipeline context.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderCapturePolicy {
    /// Default limits applied to all captured headers.
    #[serde(default)]
    pub(crate) defaults: CaptureDefaults,
    /// Per-header capture rules. Only headers matching at least one rule
    /// are captured.
    #[serde(default)]
    pub(crate) headers: Vec<CaptureRule>,
}

impl HeaderCapturePolicy {
    /// Create a new capture policy from the given defaults and rules.
    #[must_use]
    pub fn new(defaults: CaptureDefaults, headers: Vec<CaptureRule>) -> Self {
        Self { defaults, headers }
    }

    /// Returns `true` when no capture rules are defined, meaning the policy
    /// will not capture any headers.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    /// Compiles capture rules into the bounded schema used by pdata context builders.
    ///
    /// Returns an error when the configured capture limits cannot be represented
    /// by the packed context format.
    pub fn compile(&self) -> Result<CompiledHeaderCapturePolicy, String> {
        if self.defaults.max_entries > MAX_CAPTURE_ENTRIES {
            return Err(format!(
                "header capture max_entries must not exceed {}",
                MAX_CAPTURE_ENTRIES
            ));
        }
        if self.headers.len() > usize::from(u16::MAX) + 1 {
            return Err(format!(
                "header capture supports at most {} rules",
                usize::from(u16::MAX) + 1
            ));
        }

        let mut entry_ids = AHashMap::new();
        let mut matches = AHashMap::new();
        let mut schema_matches = Vec::new();
        for (rule_id, rule) in self.headers.iter().enumerate() {
            let entry = match rule.store_as.as_ref() {
                Some(name) => {
                    let normalized = name.to_ascii_lowercase();
                    match entry_ids.get(&normalized) {
                        Some(entry) => Some(*entry),
                        None => {
                            if entry_ids.len() >= MAX_CAPTURE_ENTRIES {
                                return Err(format!(
                                    "header capture supports at most {} named entries",
                                    MAX_CAPTURE_ENTRIES
                                ));
                            }
                            let entry = u16::try_from(entry_ids.len()).map_err(|_| {
                                format!(
                                    "header capture supports at most {} named entries",
                                    MAX_CAPTURE_ENTRIES
                                )
                            })?;
                            let _ = entry_ids.insert(normalized, entry);
                            Some(entry)
                        }
                    }
                }
                None => None,
            };
            let rule_id = u16::try_from(rule_id)
                .map_err(|_| "header capture rule identifier overflow".to_string())?;
            for match_name in &rule.match_names {
                let normalized = match_name.to_ascii_lowercase();
                if let std::collections::hash_map::Entry::Vacant(slot) = matches.entry(normalized) {
                    let stored_name = rule.store_as.clone().unwrap_or_else(|| slot.key().clone());
                    let schema_item = u16::try_from(schema_matches.len()).map_err(|_| {
                        format!(
                            "header capture supports at most {} distinct matches",
                            u16::MAX
                        )
                    })?;
                    schema_matches.push(CompiledHeaderSchemaItem {
                        wire_name: slot.key().clone(),
                        stored_name: stored_name.clone(),
                        rule_id,
                        entry,
                        value_kind: rule.value_kind,
                    });
                    let _ = slot.insert(CompiledCaptureMatch {
                        schema_item,
                        rule_id,
                        entry,
                        wire_name: match_name.to_ascii_lowercase(),
                        stored_name,
                        value_kind: rule.value_kind,
                    });
                }
            }
        }
        Ok(CompiledHeaderCapturePolicy {
            defaults: self.defaults.clone(),
            header_probe: matches
                .keys()
                .map(|name| 1u64 << header_probe_bit(name.as_bytes()))
                .fold(0, |probe, bit| probe | bit),
            matches: CompiledCaptureMatches::new(matches),
            entry_count: entry_ids.len(),
            schema: Arc::new(CompiledHeaderSchema {
                items: schema_matches.into_boxed_slice(),
            }),
        })
    }

    /// Validates that this policy can be represented by the packed context schema.
    pub fn validate(&self) -> Result<(), String> {
        self.compile().map(|_| ())
    }
}

/// Immutable capture schema used by packed pdata context builders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledHeaderCapturePolicy {
    defaults: CaptureDefaults,
    header_probe: u64,
    matches: CompiledCaptureMatches,
    entry_count: usize,
    schema: Arc<CompiledHeaderSchema>,
}

impl CompiledHeaderCapturePolicy {
    /// Number of compiled `store_as` entries.
    #[must_use]
    pub const fn entry_count(&self) -> usize {
        self.entry_count
    }

    /// Finds the first capture rule matching `wire_name`.
    #[must_use]
    pub fn match_header(&self, wire_name: &str) -> Option<CompiledHeaderMatch<'_>> {
        if self.header_probe & (1u64 << header_probe_bit(wire_name.as_bytes())) == 0 {
            return None;
        }
        self.matches
            .get(wire_name)
            .map(|matched| CompiledHeaderMatch {
                schema_item: matched.schema_item,
                rule_id: matched.rule_id,
                entry: matched.entry,
                configured_name: &matched.wire_name,
                stored_name: &matched.stored_name,
                value_kind: matched.value_kind,
            })
    }

    /// Returns the configured capture limits.
    #[must_use]
    pub const fn defaults(&self) -> &CaptureDefaults {
        &self.defaults
    }

    /// Returns the immutable schema shared by contexts captured with this policy.
    #[must_use]
    pub fn schema(&self) -> &Arc<CompiledHeaderSchema> {
        &self.schema
    }
}

/// Immutable names and capture metadata referenced by packed context items.
#[derive(Debug, PartialEq, Eq)]
pub struct CompiledHeaderSchema {
    items: Box<[CompiledHeaderSchemaItem]>,
}

impl CompiledHeaderSchema {
    /// Resolves a schema-local captured item.
    #[must_use]
    pub fn item(&self, id: u16) -> Option<CompiledHeaderSchemaItemRef<'_>> {
        self.items
            .get(usize::from(id))
            .map(|item| CompiledHeaderSchemaItemRef {
                wire_name: &item.wire_name,
                stored_name: &item.stored_name,
                rule_id: item.rule_id,
                entry: item.entry,
                value_kind: item.value_kind,
            })
    }

    /// Number of distinct captured header matches in this schema.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether this schema contains no captured matches.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CompiledHeaderSchemaItem {
    wire_name: String,
    stored_name: String,
    rule_id: u16,
    entry: Option<u16>,
    value_kind: Option<ValueKindConfig>,
}

/// Borrowed compiled metadata for one captured header match.
#[derive(Debug, Clone, Copy)]
pub struct CompiledHeaderSchemaItemRef<'a> {
    /// Normalized configured wire name.
    pub wire_name: &'a str,
    /// Logical stored name.
    pub stored_name: &'a str,
    /// First-match capture rule identifier.
    pub rule_id: u16,
    /// Optional logical entry slot.
    pub entry: Option<u16>,
    /// Configured value-kind override.
    pub value_kind: Option<ValueKindConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompiledCaptureMatch {
    schema_item: u16,
    rule_id: u16,
    entry: Option<u16>,
    wire_name: String,
    stored_name: String,
    value_kind: Option<ValueKindConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CompiledCaptureMatches {
    Linear(Box<[(String, CompiledCaptureMatch)]>),
    Hashed(AHashMap<String, CompiledCaptureMatch>),
}

impl CompiledCaptureMatches {
    const LINEAR_LIMIT: usize = 8;

    fn new(matches: AHashMap<String, CompiledCaptureMatch>) -> Self {
        if matches.len() <= Self::LINEAR_LIMIT {
            let mut matches: Vec<_> = matches.into_iter().collect();
            matches.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            Self::Linear(matches.into_boxed_slice())
        } else {
            Self::Hashed(matches)
        }
    }

    fn get(&self, wire_name: &str) -> Option<&CompiledCaptureMatch> {
        match self {
            Self::Linear(matches) => matches.iter().find_map(|(name, matched)| {
                (name == wire_name || name.eq_ignore_ascii_case(wire_name)).then_some(matched)
            }),
            Self::Hashed(matches) => lookup_ascii_case_insensitive(matches, wire_name),
        }
    }
}

/// A receiver-facing result of matching one inbound header.
#[derive(Debug, Clone, Copy)]
pub struct CompiledHeaderMatch<'a> {
    /// Schema-local captured item identifier.
    pub schema_item: u16,
    /// First-match capture rule identifier.
    pub rule_id: u16,
    /// Optional `store_as` entry slot.
    pub entry: Option<u16>,
    /// Normalized configured wire name.
    pub configured_name: &'a str,
    /// Stored name: `store_as` when configured, otherwise the normalized
    /// matched wire name.
    pub stored_name: &'a str,
    /// Explicit value kind override, if configured.
    pub value_kind: Option<ValueKindConfig>,
}

/// Default limits for header capture.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureDefaults {
    /// Maximum number of headers captured per message.
    #[serde(default = "default_max_entries")]
    pub max_entries: usize,
    /// Maximum byte length of a header name.
    #[serde(default = "default_max_name_bytes")]
    pub max_name_bytes: usize,
    /// Maximum byte length of a header value.
    #[serde(default = "default_max_value_bytes")]
    pub max_value_bytes: usize,
    /// Action taken when a header violates a limit.
    #[serde(default)]
    pub on_error: ErrorAction,
}

impl Default for CaptureDefaults {
    fn default() -> Self {
        Self {
            max_entries: default_max_entries(),
            max_name_bytes: default_max_name_bytes(),
            max_value_bytes: default_max_value_bytes(),
            on_error: ErrorAction::default(),
        }
    }
}

const fn default_max_entries() -> usize {
    32
}

const fn default_max_name_bytes() -> usize {
    128
}

const fn default_max_value_bytes() -> usize {
    4096
}

/// A single header capture rule.
///
/// Headers whose wire name matches any entry in `match_names`
/// (case-insensitive) are captured.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureRule {
    /// Wire header names to match (case-insensitive).
    pub match_names: Vec<String>,
    /// Normalized logical name to store the header under. If omitted,
    /// defaults to the first matched name lowercased.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store_as: Option<String>,
    /// Whether this header contains sensitive data (e.g. auth tokens).
    /// Sensitive headers may receive special treatment in logging and
    /// debug output.
    /// TODO: Implement the sensitive capability for headers
    #[serde(default)]
    pub sensitive: bool,
    /// Override the auto-detected value kind. When omitted, binary is
    /// inferred from the gRPC `-bin` suffix convention; otherwise text
    /// is assumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_kind: Option<ValueKindConfig>,
}

/// Configured value kind for a capture rule.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValueKindConfig {
    /// UTF-8 text.
    Text,
    /// Arbitrary binary bytes.
    Binary,
}

// -- Header Propagation -------------------------------------------------------

/// Policy controlling which captured transport headers are propagated by
/// exporters onto outbound requests.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderPropagationPolicy {
    /// Default propagation behavior applied to all captured headers.
    #[serde(default)]
    pub(crate) default: PropagationDefault,
    /// Per-header overrides applied after the default.
    #[serde(default)]
    pub(crate) overrides: Vec<PropagationOverride>,
}

impl HeaderPropagationPolicy {
    /// Create a new propagation policy from the given default behavior and overrides.
    #[must_use]
    pub fn new(default: PropagationDefault, overrides: Vec<PropagationOverride>) -> Self {
        Self { default, overrides }
    }

    /// Validate the propagation policy configuration.
    ///
    /// Currently validates the default selector shape. This is the single
    /// entry-point that both pipeline-level and node-level validation use so
    /// that invalid selectors cannot be silently accepted in one path while
    /// being rejected in another.
    pub fn validate(&self) -> Result<(), String> {
        self.default.selector.validate()
    }

    /// Compiles selectors and overrides for constant-time egress lookup.
    pub fn compile(&self) -> Result<CompiledHeaderPropagationPolicy, String> {
        self.default.selector.validate()?;
        let selected_names = match self.default.selector.selector_type {
            PropagationSelectorType::Named => self
                .default
                .selector
                .named
                .as_ref()
                .into_iter()
                .flatten()
                .map(|name| name.to_ascii_lowercase())
                .collect(),
            PropagationSelectorType::AllCaptured | PropagationSelectorType::None => AHashSet::new(),
        };
        let mut overrides = AHashMap::new();
        for rule in &self.overrides {
            for stored_name in &rule.match_rule.stored_names {
                let _ = overrides.entry(stored_name.to_ascii_lowercase()).or_insert(
                    CompiledPropagationOverride {
                        action: rule.action,
                        name: rule.name.unwrap_or(self.default.name),
                    },
                );
            }
        }
        let uniform = (self.overrides.is_empty()
            && self.default.selector.selector_type != PropagationSelectorType::Named)
            .then(|| {
                let selected =
                    self.default.selector.selector_type == PropagationSelectorType::AllCaptured;
                (
                    if selected {
                        self.default.action
                    } else {
                        PropagationAction::Drop
                    },
                    self.default.name,
                )
            });
        Ok(CompiledHeaderPropagationPolicy {
            uniform,
            selector_type: self.default.selector.selector_type,
            selected_names,
            default_action: self.default.action,
            default_name: self.default.name,
            overrides,
        })
    }
}

/// Propagation policy compiled for constant-time stored-name lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledHeaderPropagationPolicy {
    uniform: Option<(PropagationAction, NameStrategy)>,
    selector_type: PropagationSelectorType,
    selected_names: AHashSet<String>,
    default_action: PropagationAction,
    default_name: NameStrategy,
    overrides: AHashMap<String, CompiledPropagationOverride>,
}

impl CompiledHeaderPropagationPolicy {
    /// Returns the precompiled decision when every stored name has identical behavior.
    #[must_use]
    pub const fn uniform_decision(&self) -> Option<(PropagationAction, NameStrategy)> {
        self.uniform
    }

    /// Compiles propagation decisions for every item in a capture schema.
    #[must_use]
    pub fn compile_schema(&self, schema: &CompiledHeaderSchema) -> CompiledSchemaPropagation {
        let decisions = (0..schema.len())
            .map(|id| {
                u16::try_from(id)
                    .ok()
                    .and_then(|id| schema.item(id))
                    .map_or(
                        CompiledPropagationDecision {
                            action: PropagationAction::Drop,
                            name: NameStrategy::Preserve,
                        },
                        |item| {
                            let (action, name) = self.resolve_stored_name(item.stored_name);
                            CompiledPropagationDecision { action, name }
                        },
                    )
            })
            .collect();
        CompiledSchemaPropagation { decisions }
    }

    /// Resolves propagation behavior from a stored context name.
    #[must_use]
    pub fn resolve_stored_name(&self, stored_name: &str) -> (PropagationAction, NameStrategy) {
        if let Some(override_rule) = lookup_ascii_case_insensitive(&self.overrides, stored_name) {
            return (override_rule.action, override_rule.name);
        }

        let selected = match self.selector_type {
            PropagationSelectorType::AllCaptured => true,
            PropagationSelectorType::None => false,
            PropagationSelectorType::Named => {
                contains_ascii_case_insensitive(&self.selected_names, stored_name)
            }
        };
        (
            if selected {
                self.default_action
            } else {
                PropagationAction::Drop
            },
            self.default_name,
        )
    }
}

/// Propagation decisions indexed by capture-schema item identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledSchemaPropagation {
    decisions: Box<[CompiledPropagationDecision]>,
}

impl CompiledSchemaPropagation {
    /// Resolves one schema-local item without a name lookup.
    #[must_use]
    pub fn decision(&self, item_id: u16) -> Option<CompiledPropagationDecision> {
        self.decisions.get(usize::from(item_id)).copied()
    }
}

/// Pre-resolved propagation behavior for one captured schema item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompiledPropagationDecision {
    /// Whether the item is propagated.
    pub action: PropagationAction,
    /// Which captured name is emitted.
    pub name: NameStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompiledPropagationOverride {
    action: PropagationAction,
    name: NameStrategy,
}

fn lookup_ascii_case_insensitive<'a, T>(
    values: &'a AHashMap<String, T>,
    name: &str,
) -> Option<&'a T> {
    values.get(name).or_else(|| {
        name.bytes()
            .any(|byte| byte.is_ascii_uppercase())
            .then(|| values.get(&name.to_ascii_lowercase()))
            .flatten()
    })
}

fn contains_ascii_case_insensitive(values: &AHashSet<String>, name: &str) -> bool {
    values.contains(name)
        || (name.bytes().any(|byte| byte.is_ascii_uppercase())
            && values.contains(&name.to_ascii_lowercase()))
}

/// Cheap discriminator compiled over configured header names.
fn header_probe_bit(name: &[u8]) -> u32 {
    let first = name.first().copied().unwrap_or(0).to_ascii_lowercase();
    let last = name.last().copied().unwrap_or(0).to_ascii_lowercase();
    ((u32::from(first).wrapping_mul(31))
        ^ (u32::from(last).wrapping_mul(17))
        ^ (name.len() as u32).wrapping_mul(7))
        & 63
}

/// Default propagation behavior.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PropagationDefault {
    /// Which captured headers to select for propagation.
    #[serde(default)]
    pub selector: PropagationSelector,
    /// Default action for selected headers.
    #[serde(default)]
    pub action: PropagationAction,
    /// How to derive the outbound header name from the stored header.
    #[serde(default)]
    pub name: NameStrategy,
    /// Action taken when a header cannot be propagated.
    #[serde(default)]
    pub on_error: ErrorAction,
}

/// Selects which captured headers are candidates for propagation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropagationSelectorType {
    /// Propagate all captured headers (subject to overrides).
    AllCaptured,
    /// Do not propagate any captured headers by default (overrides may
    /// still select specific headers).
    #[default]
    None,
    /// Propagate only headers whose stored names appear in the `named` list.
    Named,
}

/// Selects which captured headers are candidates for propagation.
///
/// The `type` field selects the strategy. When `type` is `named`,
/// the `named` field must contain the list of header names to propagate.
#[derive(Debug, Default, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PropagationSelector {
    /// The propagation selection strategy to use.
    #[serde(rename = "type", default)]
    pub selector_type: PropagationSelectorType,

    /// List of header names to propagate. Required when `type` is `named`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub named: Option<Vec<String>>,
}
impl PropagationSelector {
    /// Validate the supplied configuration.
    pub fn validate(&self) -> Result<(), String> {
        match (&self.selector_type, &self.named) {
            (PropagationSelectorType::Named, None) => {
                Err("'named' list is required when type is 'named'".into())
            }
            (PropagationSelectorType::Named, Some(names)) if names.is_empty() => {
                Err("'named' list must not be empty when type is 'named'".into())
            }
            (PropagationSelectorType::AllCaptured | PropagationSelectorType::None, Some(_)) => {
                Err("'named' must not be set when type is not 'named'".into())
            }
            _ => Ok(()),
        }
    }
}

/// Action to take for a header during propagation.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PropagationAction {
    /// Include the header on the outbound request.
    #[default]
    Propagate,
    /// Exclude the header from the outbound request.
    Drop,
}

/// Strategy for mapping the stored header name to the outbound wire name.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NameStrategy {
    /// Use the original wire name observed on ingress.
    #[default]
    Preserve,
    /// Use the normalized stored name.
    StoredName,
}

/// Action taken when a header violates a policy constraint.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorAction {
    /// Silently drop the offending header.
    #[default]
    Drop,
}

/// A per-header propagation override.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PropagationOverride {
    /// Matching criteria for this override.
    #[serde(rename = "match")]
    pub match_rule: PropagationMatch,
    /// Action to take for matched headers. Defaults to `propagate`.
    #[serde(default)]
    pub action: PropagationAction,
    /// Override the name strategy for matched headers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<NameStrategy>,
    /// Override the error action for matched headers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_error: Option<ErrorAction>,
}

/// Matching criteria for propagation overrides.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PropagationMatch {
    /// Match headers whose stored (normalized) name appears in this list.
    pub stored_names: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_capture_policy_captures_nothing() {
        let policy = HeaderCapturePolicy::default();
        assert!(policy.is_empty());
        assert_eq!(policy.defaults.max_entries, 32);
        assert_eq!(policy.defaults.max_name_bytes, 128);
        assert_eq!(policy.defaults.max_value_bytes, 4096);
        assert_eq!(policy.defaults.on_error, ErrorAction::Drop);
    }

    #[test]
    fn default_propagation_policy() {
        let policy = HeaderPropagationPolicy::default();
        assert_eq!(
            policy.default.selector.selector_type,
            PropagationSelectorType::None
        );
        assert_eq!(policy.default.action, PropagationAction::Propagate);
        assert_eq!(policy.default.name, NameStrategy::Preserve);
        assert_eq!(policy.default.on_error, ErrorAction::Drop);
        assert!(policy.overrides.is_empty());
    }

    #[test]
    fn capture_policy_serde_roundtrip() {
        let yaml = r#"
defaults:
  max_entries: 16
  max_name_bytes: 64
  max_value_bytes: 2048
  on_error: drop
headers:
  - match_names: ["x-tenant-id"]
    store_as: tenant_id
  - match_names: ["authorization"]
    sensitive: true
  - match_names: ["x-request-id"]
"#;
        let policy: HeaderCapturePolicy = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(policy.defaults.max_entries, 16);
        assert_eq!(policy.defaults.on_error, ErrorAction::Drop);
        assert_eq!(policy.headers.len(), 3);
        assert_eq!(policy.headers[0].store_as.as_deref(), Some("tenant_id"));
        assert!(policy.headers[1].sensitive);
        assert_eq!(policy.headers[2].match_names, vec!["x-request-id"]);

        // roundtrip
        let json = serde_json::to_string(&policy).expect("serialize");
        let back: HeaderCapturePolicy = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, policy);
    }

    #[test]
    fn propagation_policy_serde_roundtrip() {
        let yaml = r#"
default:
  selector: 
    type: all_captured
  action: propagate
  name: preserve
  on_error: drop
overrides:
  - match:
      stored_names: ["authorization"]
    action: drop
"#;
        let policy: HeaderPropagationPolicy = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(policy.overrides.len(), 1);
        assert_eq!(
            policy.overrides[0].match_rule.stored_names,
            vec!["authorization"]
        );
        assert_eq!(policy.overrides[0].action, PropagationAction::Drop);

        let json = serde_json::to_string(&policy).expect("serialize");
        let back: HeaderPropagationPolicy = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, policy);
    }

    #[test]
    fn full_transport_headers_policy_serde() {
        let yaml = r#"
header_capture:
  defaults:
    max_entries: 32
  headers:
    - match_names: ["x-tenant-id"]
      store_as: tenant_id
header_propagation:
  default:
    selector:
        type: all_captured
  overrides:
    - match:
        stored_names: ["authorization"]
      action: drop
"#;
        let policy: TransportHeadersPolicy = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(policy.header_capture.headers.len(), 1);
        assert_eq!(policy.header_propagation.overrides.len(), 1);
    }

    #[test]
    fn selector_named_variant() {
        let yaml = r#"!
type: named
named:
    - tenant_id
    - request_id
"#;
        let selector: PropagationSelector = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(selector.selector_type, PropagationSelectorType::Named);
        assert_eq!(
            selector.named,
            Some(vec!["tenant_id".to_string(), "request_id".to_string()])
        );
    }

    #[test]
    fn selector_validate_all_captured_valid() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::AllCaptured,
            named: None,
        };
        assert!(selector.validate().is_ok());
    }

    #[test]
    fn selector_validate_none_valid() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::None,
            named: None,
        };
        assert!(selector.validate().is_ok());
    }

    #[test]
    fn selector_validate_named_valid() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::Named,
            named: Some(vec!["tenant_id".to_string()]),
        };
        assert!(selector.validate().is_ok());
    }

    #[test]
    fn selector_validate_named_missing_list() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::Named,
            named: None,
        };
        let err = selector.validate().unwrap_err();
        assert!(err.contains("'named' list is required"));
    }

    #[test]
    fn selector_validate_named_empty_list() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::Named,
            named: Some(vec![]),
        };
        let err = selector.validate().unwrap_err();
        assert!(err.contains("must not be empty"));
    }

    #[test]
    fn selector_validate_all_captured_with_named_field() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::AllCaptured,
            named: Some(vec!["tenant_id".to_string()]),
        };
        let err = selector.validate().unwrap_err();
        assert!(err.contains("'named' must not be set"));
    }

    #[test]
    fn selector_validate_none_with_named_field() {
        let selector = PropagationSelector {
            selector_type: PropagationSelectorType::None,
            named: Some(vec!["tenant_id".to_string()]),
        };
        let err = selector.validate().unwrap_err();
        assert!(err.contains("'named' must not be set"));
    }

    #[test]
    fn propagation_policy_validate_delegates_to_selector() {
        let policy = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::Named,
                    named: None,
                },
                ..Default::default()
            },
            vec![],
        );
        let err = policy.validate().unwrap_err();
        assert!(err.contains("'named' list is required"));
    }

    #[test]
    fn propagation_policy_validate_valid() {
        let policy = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::AllCaptured,
                    named: None,
                },
                ..Default::default()
            },
            vec![],
        );
        assert!(policy.validate().is_ok());
    }

    /// Scenario: a capture policy declares more rules than a u16 identifier can represent.
    /// Guarantees: policy compilation rejects the configuration instead of aliasing rule IDs.
    #[test]
    fn capture_policy_rejects_too_many_rules() {
        let rule = CaptureRule {
            match_names: vec!["x-test".to_string()],
            store_as: None,
            sensitive: false,
            value_kind: None,
        };
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![rule; usize::from(u16::MAX) + 2],
        );

        assert!(policy.compile().is_err());
    }

    /// Scenario: a capture policy declares 1,025 named entries.
    /// Guarantees: compilation rejects schemas above the 1,024-entry packed-context policy limit.
    #[test]
    fn capture_policy_rejects_too_many_named_entries() {
        let rules = (0..=MAX_CAPTURE_ENTRIES)
            .map(|index| CaptureRule {
                match_names: vec![format!("x-test-{index}")],
                store_as: Some(format!("entry-{index}")),
                sensitive: false,
                value_kind: None,
            })
            .collect();
        let policy = HeaderCapturePolicy::new(CaptureDefaults::default(), rules);

        assert!(policy.compile().is_err());
    }

    /// Scenario: max_entries exceeds the 1,024-entry capture policy limit.
    /// Guarantees: policy compilation rejects an unreasonably large per-message capture bound.
    #[test]
    fn capture_policy_rejects_unrepresentable_max_entries() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults {
                max_entries: MAX_CAPTURE_ENTRIES + 1,
                ..CaptureDefaults::default()
            },
            vec![],
        );

        assert!(policy.compile().is_err());
    }

    /// Scenario: max_entries and named entries are exactly at the configured policy limit.
    /// Guarantees: compilation accepts the 1,024-entry boundary supported by packed contexts.
    #[test]
    fn capture_policy_accepts_maximum_entries() {
        let rules = (0..MAX_CAPTURE_ENTRIES)
            .map(|index| CaptureRule {
                match_names: vec![format!("x-test-{index}")],
                store_as: Some(format!("entry-{index}")),
                sensitive: false,
                value_kind: None,
            })
            .collect();
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults {
                max_entries: MAX_CAPTURE_ENTRIES,
                ..CaptureDefaults::default()
            },
            rules,
        );

        let compiled = policy.compile().expect("maximum capture entry count");
        assert_eq!(compiled.entry_count(), MAX_CAPTURE_ENTRIES);
    }

    /// Scenario: multiple case variants and rules declare the same wire header.
    /// Guarantees: compiled capture lookup remains case-insensitive and preserves first-rule wins.
    #[test]
    fn compiled_capture_preserves_first_match_semantics() {
        let policy = HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![
                CaptureRule {
                    match_names: vec!["x-tenant".to_string()],
                    store_as: Some("tenant".to_string()),
                    sensitive: false,
                    value_kind: None,
                },
                CaptureRule {
                    match_names: vec!["X-TENANT".to_string()],
                    store_as: Some("ignored".to_string()),
                    sensitive: false,
                    value_kind: Some(ValueKindConfig::Binary),
                },
            ],
        )
        .compile()
        .expect("capture policy");

        let matched = policy.match_header("X-Tenant").expect("matched header");
        assert_eq!(matched.rule_id, 0);
        assert_eq!(matched.stored_name, "tenant");
        assert_eq!(matched.value_kind, None);
    }

    /// Scenario: named selection and duplicate overrides use mixed-case stored names.
    /// Guarantees: compiled propagation is case-insensitive and preserves first-override wins.
    #[test]
    fn compiled_propagation_preserves_policy_precedence() {
        let policy = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::Named,
                    named: Some(vec!["Tenant".to_string()]),
                },
                ..PropagationDefault::default()
            },
            vec![
                PropagationOverride {
                    match_rule: PropagationMatch {
                        stored_names: vec!["Authorization".to_string()],
                    },
                    action: PropagationAction::Drop,
                    name: Some(NameStrategy::StoredName),
                    on_error: None,
                },
                PropagationOverride {
                    match_rule: PropagationMatch {
                        stored_names: vec!["authorization".to_string()],
                    },
                    action: PropagationAction::Propagate,
                    name: None,
                    on_error: None,
                },
            ],
        )
        .compile()
        .expect("propagation policy");

        assert_eq!(
            policy.resolve_stored_name("TENANT"),
            (PropagationAction::Propagate, NameStrategy::Preserve)
        );
        assert_eq!(
            policy.resolve_stored_name("AUTHORIZATION"),
            (PropagationAction::Drop, NameStrategy::StoredName)
        );
        assert_eq!(
            policy.resolve_stored_name("other"),
            (PropagationAction::Drop, NameStrategy::Preserve)
        );
    }

    /// Scenario: a named selector has no propagation overrides.
    /// Guarantees: compilation retains per-name selection instead of treating every name uniformly.
    #[test]
    fn compiled_named_propagation_is_not_uniform() {
        let policy = HeaderPropagationPolicy::new(
            PropagationDefault {
                selector: PropagationSelector {
                    selector_type: PropagationSelectorType::Named,
                    named: Some(vec!["tenant".to_string()]),
                },
                ..PropagationDefault::default()
            },
            vec![],
        )
        .compile()
        .expect("propagation policy");

        assert_eq!(policy.uniform_decision(), None);
        assert_eq!(
            policy.resolve_stored_name("tenant"),
            (PropagationAction::Propagate, NameStrategy::Preserve)
        );
        assert_eq!(
            policy.resolve_stored_name("other"),
            (PropagationAction::Drop, NameStrategy::Preserve)
        );
    }
}
