// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension configuration types.
//!
//! Extensions are components that provide capabilities (e.g. authentication,
//! enrichment) to nodes.  They are declared in the pipeline config alongside
//! nodes and connections.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for a single extension instance.
///
/// Each entry in the pipeline's `extensions` map describes one extension:
///
/// ```yaml
/// extensions:
///   my_auth:
///     type: bearer-token
///     config:
///       token: "secret"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExtensionConfig {
    /// Extension type name — must match a registered extension factory.
    pub r#type: String,

    /// Extension-specific configuration blob, passed verbatim to the factory.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-preserve-unknown-fields" = true))]
    pub config: serde_json::Value,
}
