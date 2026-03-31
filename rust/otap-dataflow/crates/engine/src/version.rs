// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Centralized runtime version for the OTAP Dataflow engine.
//!
//! All components that need a version string should call [`version()`] rather
//! than using `CARGO_PKG_VERSION` directly.
//!
//! # Precedence (highest → lowest)
//!
//! 1. **Runtime env var** `OTAP_DATAFLOW_VERSION` — checked once at first
//!    call. Useful for container deployments where the image version should
//!    take precedence.
//! 2. **Compile-time env var** `OTAP_DATAFLOW_VERSION` — baked in by
//!    `option_env!()` when the variable is present during compilation (e.g.
//!    in CI or custom distro builds). Cache invalidation is handled by
//!    `cargo xtask` (see `xtask/src/build_env.rs`).
//! 3. **`CARGO_PKG_VERSION`** of the engine crate — the fallback default.
//!
//! # Examples
//!
//! Build with a stamped version (via xtask for correct cache invalidation):
//!
//! ```sh
//! OTAP_DATAFLOW_VERSION=2.1.0 cargo xtask check
//! ```
//!
//! Override at runtime (e.g. in a container entrypoint):
//!
//! ```sh
//! export OTAP_DATAFLOW_VERSION=2.1.0-alpine
//! ./df_engine --config config.yaml
//! ```

use std::sync::OnceLock;

/// Returns the authoritative runtime version string.
///
/// The result is computed once and cached for the lifetime of the process.
/// See the [module documentation](self) for the precedence rules.
pub fn version() -> &'static str {
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION.get_or_init(|| {
        // 1. Runtime env var (highest priority).
        std::env::var("OTAP_DATAFLOW_VERSION")
            .ok()
            // 2. Compile-time env var (same name, baked in by rustc).
            .or_else(|| option_env!("OTAP_DATAFLOW_VERSION").map(String::from))
            // 3. Engine crate CARGO_PKG_VERSION (fallback).
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_returns_non_empty_string() {
        let v = version();
        assert!(!v.is_empty(), "version() must not return an empty string");
    }

    #[test]
    fn version_is_stable_across_calls() {
        let v1 = version();
        let v2 = version();
        assert_eq!(
            v1, v2,
            "version() should return the same value on every call"
        );
        // Both must be the exact same pointer (OnceLock caching).
        assert!(std::ptr::eq(v1, v2));
    }
}
