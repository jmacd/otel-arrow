// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Build-environment tracking for compile-time `option_env!()` variables.
//!
//! Cargo's incremental fingerprinting does **not** track arbitrary environment
//! variables — only source-file mtimes, compiler flags, and variables
//! explicitly declared by a `build.rs` via `cargo::rerun-if-env-changed`.
//!
//! Rather than scattering `build.rs` files across crates, we centralise the
//! tracking here in xtask. Before any cargo invocation, [`sync`] compares
//! the current values of every tracked variable against a cache in
//! `target/.xtask-build-env`. If any value changed (including becoming set
//! or unset), we *touch* the source files that read the variable via
//! `option_env!()`, which bumps their mtime and forces cargo to recompile
//! the owning crate.
//!
//! # Adding a new build-time environment variable
//!
//! 1. Add an entry to [`TRACKED`] with the variable name and every source
//!    file that references it via `option_env!()`.
//! 2. In the source file, read the value with `option_env!("YOUR_VAR")`.
//! 3. That's it — `cargo xtask check` (and `quick-check` / `check-benches`)
//!    will automatically invalidate the right crates when the variable
//!    changes.
//!
//! # Limitations
//!
//! Direct `cargo build` (bypassing xtask) does **not** get automatic cache
//! invalidation. Users who build outside xtask with changed env vars should
//! run `cargo clean -p <crate>` first, or simply use xtask.

use std::collections::BTreeMap;
use std::fs::{self, FileTimes};
use std::path::Path;
use std::time::SystemTime;

/// Tracked build-environment variables.
///
/// Each entry maps an environment variable name to the source files that
/// reference it via `option_env!()`. When the variable's value changes
/// between xtask runs, those files are touched to invalidate cargo's cache.
const TRACKED: &[(&str, &[&str])] = &[("OTAP_DATAFLOW_VERSION", &["crates/engine/src/version.rs"])];

const CACHE_PATH: &str = "target/.xtask-build-env";

/// Compare tracked env vars against the cache and touch affected files if
/// any value changed. Must be called before the first cargo invocation.
pub fn sync() -> anyhow::Result<()> {
    let cached = read_cache();
    let mut current = BTreeMap::new();
    let mut touched = Vec::new();

    for &(var, files) in TRACKED {
        let value = std::env::var(var).ok();
        let cached_value = cached.get(var).cloned().flatten();

        if value != cached_value {
            for &file in files {
                touch_file(Path::new(file))?;
                touched.push(file);
            }
            match &value {
                Some(v) => println!("  {var}={v} (changed)"),
                None => println!("  {var} unset (changed)"),
            }
        }

        current.insert(var.to_string(), value);
    }

    if !touched.is_empty() {
        println!(
            "🔄 Build env changed — touched {} file(s) to invalidate cargo cache.",
            touched.len()
        );
    }

    write_cache(&current)
}

// -- helpers ----------------------------------------------------------------

fn read_cache() -> BTreeMap<String, Option<String>> {
    let Ok(content) = fs::read_to_string(CACHE_PATH) else {
        return BTreeMap::new();
    };
    let mut map = BTreeMap::new();
    for line in content.lines() {
        if let Some((key, value)) = line.split_once('=') {
            let value = if value == "<unset>" {
                None
            } else {
                Some(value.to_string())
            };
            map.insert(key.to_string(), value);
        }
    }
    map
}

fn write_cache(current: &BTreeMap<String, Option<String>>) -> anyhow::Result<()> {
    fs::create_dir_all("target")?;
    let content: String = current
        .iter()
        .map(|(k, v)| format!("{}={}", k, v.as_deref().unwrap_or("<unset>")))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(CACHE_PATH, content)?;
    Ok(())
}

fn touch_file(path: &Path) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.exists(),
        "tracked build-env file does not exist: {}",
        path.display()
    );
    let f = fs::File::options().write(true).open(path)?;
    f.set_times(FileTimes::new().set_modified(SystemTime::now()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracked_files_exist() {
        // Verify that every file listed in TRACKED actually exists in the
        // workspace so that `sync()` won't fail at runtime.
        for &(_var, files) in TRACKED {
            for &file in files {
                assert!(
                    Path::new(file).exists(),
                    "tracked build-env file missing: {file}"
                );
            }
        }
    }

    #[test]
    fn cache_round_trip() {
        let mut map = BTreeMap::new();
        map.insert("A".to_string(), Some("1".to_string()));
        map.insert("B".to_string(), None);

        // Use a temp path so tests don't interfere with real builds.
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("build-env-cache");

        let content: String = map
            .iter()
            .map(|(k, v)| format!("{}={}", k, v.as_deref().unwrap_or("<unset>")))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&path, &content).expect("write cache");

        let read_back = fs::read_to_string(&path).expect("read cache");
        let mut parsed = BTreeMap::new();
        for line in read_back.lines() {
            if let Some((key, value)) = line.split_once('=') {
                let value = if value == "<unset>" {
                    None
                } else {
                    Some(value.to_string())
                };
                parsed.insert(key.to_string(), value);
            }
        }
        assert_eq!(map, parsed);
    }
}
