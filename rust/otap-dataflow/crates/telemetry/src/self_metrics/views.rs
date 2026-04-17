// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! View-based instrument renaming.
//!
//! A [`ViewResolver`] applies scoped renaming of instrument names and
//! descriptions. Views are resolved at export time: the static
//! `MetricsDescriptor` is unchanged, but the effective name/description
//! returned by the resolver may differ.

use otap_df_config::pipeline::telemetry::metrics::views::ViewConfig;

/// Resolved name and description for a single instrument field.
#[derive(Debug, Clone)]
pub struct ResolvedField {
    /// The effective instrument name (original or overridden by a view).
    pub name: String,
    /// The effective description (original or overridden by a view).
    pub description: String,
}

/// Resolves view-based renaming for instrument fields.
///
/// The resolver matches `(scope_name, instrument_name)` against the
/// configured [`ViewConfig`] list. The first matching view wins.
#[derive(Debug, Clone, Default)]
pub struct ViewResolver {
    views: Vec<ViewConfig>,
}

impl ViewResolver {
    /// Creates a new resolver from a list of view configurations.
    #[must_use]
    pub fn new(views: Vec<ViewConfig>) -> Self {
        Self { views }
    }

    /// Resolves the effective name and description for an instrument.
    ///
    /// `scope_name` is the `MetricsDescriptor::name` (the `#[metric_set]`
    /// scope). `instrument_name` and `description` are the original field
    /// values from `MetricsField`.
    #[must_use]
    pub fn resolve(
        &self,
        scope_name: &str,
        instrument_name: &str,
        description: &str,
    ) -> ResolvedField {
        for view in &self.views {
            if let Some(ref sel_name) = view.selector.instrument_name {
                if sel_name != instrument_name {
                    continue;
                }
            }
            if let Some(ref sel_scope) = view.selector.scope_name {
                if sel_scope != scope_name {
                    continue;
                }
            }
            // Match found — apply overrides.
            return ResolvedField {
                name: view
                    .stream
                    .name
                    .as_deref()
                    .unwrap_or(instrument_name)
                    .to_string(),
                description: view
                    .stream
                    .description
                    .as_deref()
                    .unwrap_or(description)
                    .to_string(),
            };
        }
        // No view matched — use originals.
        ResolvedField {
            name: instrument_name.to_string(),
            description: description.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_config::pipeline::telemetry::metrics::views::{
        MetricSelector, MetricStream, ViewConfig,
    };

    fn view(
        instrument_name: Option<&str>,
        scope_name: Option<&str>,
        name: Option<&str>,
        description: Option<&str>,
    ) -> ViewConfig {
        ViewConfig {
            selector: MetricSelector {
                instrument_name: instrument_name.map(String::from),
                scope_name: scope_name.map(String::from),
            },
            stream: MetricStream {
                name: name.map(String::from),
                description: description.map(String::from),
            },
        }
    }

    #[test]
    fn no_views_returns_original() {
        let resolver = ViewResolver::default();
        let r = resolver.resolve("scope", "counter1", "A counter");
        assert_eq!(r.name, "counter1");
        assert_eq!(r.description, "A counter");
    }

    #[test]
    fn matching_instrument_name_renames() {
        let resolver = ViewResolver::new(vec![view(
            Some("counter1"),
            None,
            Some("renamed"),
            Some("New desc"),
        )]);
        let r = resolver.resolve("scope", "counter1", "Old desc");
        assert_eq!(r.name, "renamed");
        assert_eq!(r.description, "New desc");
    }

    #[test]
    fn scoped_match() {
        let resolver = ViewResolver::new(vec![view(
            Some("counter1"),
            Some("my_scope"),
            Some("renamed"),
            None,
        )]);
        // Matching scope
        let r = resolver.resolve("my_scope", "counter1", "desc");
        assert_eq!(r.name, "renamed");
        assert_eq!(r.description, "desc");

        // Non-matching scope falls through
        let r = resolver.resolve("other_scope", "counter1", "desc");
        assert_eq!(r.name, "counter1");
    }

    #[test]
    fn first_match_wins() {
        let resolver = ViewResolver::new(vec![
            view(Some("counter1"), None, Some("first"), None),
            view(Some("counter1"), None, Some("second"), None),
        ]);
        let r = resolver.resolve("scope", "counter1", "desc");
        assert_eq!(r.name, "first");
    }
}
