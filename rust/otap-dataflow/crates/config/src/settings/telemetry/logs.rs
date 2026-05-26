// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Logs level configurations.

use crate::error::Error;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Internal logs configuration.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct LogsConfig {
    /// The log level for internal engine logs.
    ///
    /// Accepts either a simple level keyword (`off`, `debug`, `info`, `warn`, `error`)
    /// or a full [`RUST_LOG`-style directive string][env-filter] for fine-grained
    /// control (e.g., `"info,typespec_client_core=warn,azure_core=off"`).
    ///
    /// The value is passed directly to [`tracing_subscriber::EnvFilter`]. When not
    /// specified, the default is `"info,h2=off,hyper=off"` which silences known
    /// noisy HTTP dependencies.
    ///
    /// The `RUST_LOG` environment variable, if set, takes precedence over this field.
    ///
    /// [env-filter]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
    #[serde(default)]
    pub level: LogLevel,

    /// Logging provider configuration.
    #[serde(default = "default_providers")]
    pub providers: LoggingProviders,

    /// Internal log tap configuration.
    #[serde(default)]
    pub tap: InternalLogTapConfig,

    /// Per-thread log sampler configuration.
    ///
    /// When `enabled`, each engine pipeline thread installs a
    /// thread-local CCKR sampler that bounds internal-log output to
    /// roughly `reservoir_capacity` records per period (with up to `preserve_capacity`
    /// novelty-preserve records). When `enabled` is `false` (the default)
    /// every log event is forwarded individually, matching pre-existing
    /// behaviour.
    #[serde(default)]
    pub sampler: InternalLogSamplerConfig,
}

/// Per-thread CCKR log sampler configuration.
///
/// See `crates/telemetry/src/sampler/design.md` for the algorithm.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternalLogSamplerConfig {
    /// Enable CCKR sampling for internal logs.
    #[serde(default)]
    pub enabled: bool,

    /// Reservoir capacity `T` per period per thread.
    ///
    /// Must be at least `MIN_RESERVOIR_CAPACITY` (32) when sampling is enabled.
    /// This minimum ensures the Chao1 richness estimator has sufficient events
    /// for reliable frequency estimates, matching the internal `min_period_count`
    /// threshold used by the sampler's flush logic.
    #[serde(default = "default_reservoir_capacity")]
    pub reservoir_capacity: usize,

    /// Novelty-preserve cap `R` per period per thread.
    #[serde(default = "default_sampler_preserve_capacity")]
    pub preserve_capacity: usize,
}

/// Minimum reservoir capacity required for Chao1 estimator stability.
///
/// This matches the internal `min_period_count` threshold in the CCKR
/// sampler's flush logic. Below this threshold, frequency estimates
/// become unreliable and the adaptive weighting degrades.
pub const MIN_RESERVOIR_CAPACITY: usize = 32;

const fn default_reservoir_capacity() -> usize {
    128
}

const fn default_sampler_preserve_capacity() -> usize {
    128
}

impl Default for InternalLogSamplerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            reservoir_capacity: default_reservoir_capacity(),
            preserve_capacity: default_sampler_preserve_capacity(),
        }
    }
}

/// Configuration for the internal log tap used by admin/MCP-style consumers.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct InternalLogTapConfig {
    /// Enable the internal in-memory log tap.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of retained log entries.
    #[serde(default = "default_tap_max_entries")]
    pub max_entries: usize,

    /// Maximum retained bytes across all retained log entries.
    #[serde(default = "default_tap_max_bytes")]
    pub max_bytes: usize,
}

/// Log level for dataflow engine logs.
///
/// Accepts either a simple level keyword (`off`, `debug`, `info`, `warn`, `error`)
/// or a full [`RUST_LOG`-style directive string][env-filter] for fine-grained
/// control (e.g., `"info,typespec_client_core=warn,azure_core=off"`).
///
/// See the [`EnvFilter` directives documentation][env-filter] for the full syntax.
///
/// Defaults to `"info,h2=off,hyper=off"`.
///
/// [env-filter]: https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html#directives
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(transparent)]
pub struct LogLevel(String);

impl Default for LogLevel {
    fn default() -> Self {
        Self("info,h2=off,hyper=off".to_string())
    }
}

impl LogLevel {
    /// Returns the filter directive string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Logging providers for different execution contexts.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
pub struct LoggingProviders {
    /// Provider mode for non-engine threads. This defines the global Tokio
    /// `tracing` subscriber. Default is ConsoleAsync.
    #[serde(default = "default_global_provider")]
    pub global: ProviderMode,

    /// Provider mod for engine/pipeline threads. This defines how the
    /// engine thread / core sets the Tokio `tracing`
    /// subscriber. Default is ConsoleAsync. Internal logs will be flushed
    /// by either the Internal Telemetry Receiver or the main pipeline
    /// controller.
    #[serde(default = "default_engine_provider")]
    pub engine: ProviderMode,

    /// Provider mode for nodes downstream of Internal Telemetry receiver.
    /// This defaults to Noop to avoid internal feedback.
    #[serde(default = "default_internal_provider")]
    pub internal: ProviderMode,

    /// Provider mode for admin threads. Cannot be ConsoleAsync as
    /// that would create a feedback loop. Defaults to ConsoleDirect.
    #[serde(default = "default_admin_provider")]
    pub admin: ProviderMode,
}

impl LoggingProviders {
    /// Returns true if any provider uses ITS mode.
    #[must_use]
    pub const fn uses_its_provider(&self) -> bool {
        self.global.uses_its_provider()
            || self.engine.uses_its_provider()
            || self.admin.uses_its_provider()
    }

    /// Returns true if this uses an console_async provider.
    #[must_use]
    pub const fn uses_console_async_provider(&self) -> bool {
        self.global.uses_console_async_provider()
            || self.engine.uses_console_async_provider()
            || self.admin.uses_console_async_provider()
    }
}

/// Logs producer: how log events are captured and routed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProviderMode {
    /// Log events are silently ignored.
    Noop,

    /// Delivery using the internal telemetry system.
    ITS,

    /// Asynchronous console logging. The caller writes to a channel
    /// the same as ITS delivery, but bypasses the internal pipeline
    /// with console logging.
    #[serde(rename = "console_async")]
    ConsoleAsync,

    /// Synchronous console logging. Note! This can block the
    /// producing thread.  The caller writes directly to the console.
    #[serde(rename = "console_direct")]
    ConsoleDirect,
}

impl ProviderMode {
    /// Is this any console logging mode?
    #[must_use]
    const fn uses_any_internal_provider(&self) -> bool {
        matches!(self, Self::ITS | Self::ConsoleAsync)
    }

    /// Is this a console_async mode?
    #[must_use]
    pub const fn uses_console_async_provider(&self) -> bool {
        matches!(self, Self::ConsoleAsync)
    }

    /// Is this the ITS mode?
    #[must_use]
    pub const fn uses_its_provider(&self) -> bool {
        matches!(self, Self::ITS)
    }
}

const fn default_global_provider() -> ProviderMode {
    ProviderMode::ConsoleAsync
}

const fn default_engine_provider() -> ProviderMode {
    ProviderMode::ConsoleAsync
}

const fn default_internal_provider() -> ProviderMode {
    ProviderMode::Noop
}

const fn default_admin_provider() -> ProviderMode {
    ProviderMode::ConsoleDirect
}

const fn default_providers() -> LoggingProviders {
    LoggingProviders {
        global: default_global_provider(),
        engine: default_engine_provider(),
        internal: default_internal_provider(),
        admin: default_admin_provider(),
    }
}

const fn default_tap_max_entries() -> usize {
    2048
}

const fn default_tap_max_bytes() -> usize {
    16 * 1024 * 1024
}

impl Default for InternalLogTapConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_entries: default_tap_max_entries(),
            max_bytes: default_tap_max_bytes(),
        }
    }
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::default(),
            providers: default_providers(),
            tap: InternalLogTapConfig::default(),
            sampler: InternalLogSamplerConfig::default(),
        }
    }
}

impl LogsConfig {
    /// Validate the logs configuration.
    ///
    /// Returns an error if:
    /// - `internal` is configured to use ITS, ConsoleAsync (needs_reporter())
    /// - `admin` is configured to use ConsoleAsync (would loop to itself)
    pub fn validate(&self) -> Result<(), Error> {
        if self.providers.internal.uses_any_internal_provider() {
            return Err(Error::InvalidUserConfig {
                error: format!(
                    "internal provider is invalid: {:?}",
                    self.providers.internal
                ),
            });
        }
        // Admin provider cannot use ConsoleAsync because the observed-state-store
        // runs in an admin thread and implements console_async logging. Using
        // ConsoleAsync for admin would create a feedback loop.
        if self.providers.admin == ProviderMode::ConsoleAsync {
            return Err(Error::InvalidUserConfig {
                error: "admin provider cannot be 'console_async' (would create feedback loop); \
                        use 'console_direct' or another mode instead"
                    .into(),
            });
        }
        if self.tap.enabled && self.tap.max_entries == 0 {
            return Err(Error::InvalidUserConfig {
                error: "logs.tap.max_entries must be greater than zero".into(),
            });
        }
        if self.tap.enabled && self.tap.max_bytes == 0 {
            return Err(Error::InvalidUserConfig {
                error: "logs.tap.max_bytes must be greater than zero".into(),
            });
        }
        if self.tap.enabled
            && !self.providers.uses_console_async_provider()
            && !self.providers.uses_its_provider()
        {
            return Err(Error::InvalidUserConfig {
                error: "logs.tap.enabled requires at least one async log provider ('console_async' or 'its')".into(),
            });
        }

        if self.sampler.enabled {
            if self.sampler.reservoir_capacity < MIN_RESERVOIR_CAPACITY {
                return Err(Error::InvalidUserConfig {
                    error: format!(
                        "logs.sampler.reservoir_capacity must be at least {} when sampler is enabled (required for Chao1 estimator stability)",
                        MIN_RESERVOIR_CAPACITY
                    ),
                });
            }
            // v1 limitation: the sampler only supports the ITS engine
            // provider. ConsoleAsync's downstream consumer
            // (ObservedStateStore) does not yet handle batched log events.
            if self.providers.engine != ProviderMode::ITS {
                return Err(Error::InvalidUserConfig {
                    error: format!(
                        "logs.sampler.enabled requires logs.providers.engine = 'its' (got {:?})",
                        self.providers.engine
                    ),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to parse YAML into LogsConfig.
    fn parse(yaml: &str) -> LogsConfig {
        serde_yaml::from_str(yaml).unwrap()
    }

    /// Helper to create LoggingProviders with specified modes.
    fn providers(
        global: ProviderMode,
        engine: ProviderMode,
        internal: ProviderMode,
        admin: ProviderMode,
    ) -> LoggingProviders {
        LoggingProviders {
            global,
            engine,
            internal,
            admin,
        }
    }

    /// Helper to create a config with custom providers.
    fn config_with(
        global: ProviderMode,
        engine: ProviderMode,
        internal: ProviderMode,
        admin: ProviderMode,
    ) -> LogsConfig {
        LogsConfig {
            providers: providers(global, engine, internal, admin),
            ..Default::default()
        }
    }

    /// Asserts validation fails with expected substring in error message.
    fn assert_invalid(config: &LogsConfig, expected_msg: &str) {
        let err = config.validate().unwrap_err();
        assert!(matches!(err, Error::InvalidUserConfig { .. }));
        assert!(
            err.to_string().contains(expected_msg),
            "Expected '{}' in: {}",
            expected_msg,
            err
        );
    }

    #[test]
    fn test_defaults() {
        // Manual Default impl matches serde defaults
        let config = LogsConfig::default();
        assert_eq!(config.level.as_str(), "info,h2=off,hyper=off");
        assert_eq!(config.providers.global, ProviderMode::ConsoleAsync);
        assert_eq!(config.providers.engine, ProviderMode::ConsoleAsync);
        assert_eq!(config.providers.internal, ProviderMode::Noop);
        assert_eq!(config.providers.admin, ProviderMode::ConsoleDirect);
        assert!(!config.tap.enabled);
        assert_eq!(config.tap.max_entries, 2048);
        assert_eq!(config.tap.max_bytes, 16 * 1024 * 1024);

        // Serde defaults should match Rust Default
        let parsed = parse("{}");
        assert_eq!(parsed.level, config.level);
        assert_eq!(parsed.providers.global, config.providers.global);
        assert_eq!(parsed.providers.engine, config.providers.engine);
        assert_eq!(parsed.providers.internal, config.providers.internal);
        assert_eq!(parsed.providers.admin, config.providers.admin);
        assert_eq!(parsed.tap.enabled, config.tap.enabled);
        assert_eq!(parsed.tap.max_entries, config.tap.max_entries);
        assert_eq!(parsed.tap.max_bytes, config.tap.max_bytes);
    }

    #[test]
    fn test_log_level_parsing_simple() {
        for name in ["off", "debug", "info", "warn", "error"] {
            let config = parse(&format!("level: {name}"));
            assert_eq!(config.level.as_str(), name);
        }
    }

    #[test]
    fn test_log_level_parsing_directive_string() {
        let config = parse("level: \"info,typespec_client_core=warn\"");
        assert_eq!(config.level.as_str(), "info,typespec_client_core=warn");

        let config = parse("level: \"warn,azure_core=off,h2=off\"");
        assert_eq!(config.level.as_str(), "warn,azure_core=off,h2=off");
    }

    #[test]
    fn test_provider_mode_parsing() {
        let config = parse("providers: { global: noop, engine: its, internal: console_direct }");
        assert_eq!(config.providers.global, ProviderMode::Noop);
        assert_eq!(config.providers.engine, ProviderMode::ITS);
        assert_eq!(config.providers.internal, ProviderMode::ConsoleDirect);
    }

    #[test]
    fn test_needs_reporter() {
        use ProviderMode::*;
        let cases = [
            (Noop, false),
            (ITS, true),
            (ConsoleDirect, false),
            (ConsoleAsync, true),
        ];
        for (mode, expected) in cases {
            assert_eq!(mode.uses_any_internal_provider(), expected, "{mode:?}");
        }
    }

    #[test]
    fn test_validate_default_succeeds() {
        assert!(LogsConfig::default().validate().is_ok());
    }

    #[test]
    fn test_validate_internal_cannot_use_reporter() {
        use ProviderMode::*;
        let config = config_with(Noop, Noop, ITS, Noop);
        assert_invalid(&config, "internal provider is invalid");

        let config = config_with(Noop, Noop, ConsoleAsync, Noop);
        assert_invalid(&config, "internal provider is invalid");
    }

    #[test]
    fn test_validate_admin_cannot_use_console_async() {
        use ProviderMode::*;
        // Admin ConsoleAsync should fail validation
        let config = LogsConfig {
            providers: providers(ConsoleAsync, ConsoleAsync, Noop, ConsoleAsync),
            ..Default::default()
        };
        assert_invalid(&config, "admin provider cannot be 'console_async'");

        // Others should succeed
        for admin in [Noop, ITS, ConsoleDirect] {
            let config = LogsConfig {
                providers: providers(ConsoleAsync, ConsoleAsync, Noop, admin),
                ..Default::default()
            };
            assert!(config.validate().is_ok());
        }
    }

    #[test]
    fn test_validate_tap_requires_positive_limits() {
        let mut config = LogsConfig::default();
        config.tap.enabled = true;

        config.tap.max_entries = 0;
        assert_invalid(&config, "logs.tap.max_entries");

        config.tap.max_entries = 1;
        config.tap.max_bytes = 0;
        assert_invalid(&config, "logs.tap.max_bytes");
    }

    #[test]
    fn test_validate_tap_requires_async_provider() {
        use ProviderMode::*;

        let mut config = LogsConfig {
            providers: providers(Noop, ConsoleDirect, Noop, ConsoleDirect),
            ..Default::default()
        };
        config.tap.enabled = true;
        assert_invalid(
            &config,
            "logs.tap.enabled requires at least one async log provider",
        );

        for providers in [
            providers(ConsoleAsync, Noop, Noop, ConsoleDirect),
            providers(Noop, ITS, Noop, ConsoleDirect),
            providers(ITS, ConsoleDirect, Noop, ConsoleDirect),
        ] {
            let config = LogsConfig {
                providers,
                tap: InternalLogTapConfig {
                    enabled: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            config
                .validate()
                .expect("async provider should support tap");
        }
    }

    #[test]
    fn test_uses_its_provider() {
        use ProviderMode::*;
        assert!(!providers(Noop, Noop, Noop, Noop).uses_its_provider());
        assert!(!providers(ConsoleAsync, ConsoleAsync, Noop, ConsoleDirect).uses_its_provider());
        assert!(providers(ITS, Noop, Noop, Noop).uses_its_provider());
        assert!(providers(Noop, ITS, Noop, Noop).uses_its_provider());
        assert!(providers(Noop, Noop, Noop, ITS).uses_its_provider());
        assert!(!providers(Noop, Noop, ITS, Noop).uses_its_provider());
    }

    #[test]
    fn test_sampler_defaults() {
        let config = LogsConfig::default();
        assert!(!config.sampler.enabled);
        assert_eq!(config.sampler.reservoir_capacity, 128);
        assert_eq!(config.sampler.preserve_capacity, 128);

        let parsed = parse("{}");
        assert_eq!(parsed.sampler, config.sampler);
    }

    #[test]
    fn test_sampler_parsing() {
        let parsed =
            parse("sampler: { enabled: true, reservoir_capacity: 64, preserve_capacity: 32 }");
        assert!(parsed.sampler.enabled);
        assert_eq!(parsed.sampler.reservoir_capacity, 64);
        assert_eq!(parsed.sampler.preserve_capacity, 32);
    }

    #[test]
    fn test_validate_sampler_disabled_ok_with_any_engine() {
        use ProviderMode::*;
        // Default sampler is disabled; any valid engine provider should pass.
        for engine in [Noop, ITS, ConsoleAsync, ConsoleDirect] {
            let config = config_with(Noop, engine, Noop, ConsoleDirect);
            config
                .validate()
                .expect("sampler-disabled config should validate");
        }
    }

    #[test]
    fn test_validate_sampler_target_minimum() {
        use ProviderMode::*;
        let mut config = config_with(Noop, ITS, Noop, ConsoleDirect);
        config.sampler.enabled = true;

        // reservoir_capacity < MIN_RESERVOIR_CAPACITY should be rejected
        for reservoir_capacity in [0, 1, 16, MIN_RESERVOIR_CAPACITY - 1] {
            config.sampler.reservoir_capacity = reservoir_capacity;
            assert_invalid(&config, "logs.sampler.reservoir_capacity");
        }

        // reservoir_capacity >= MIN_RESERVOIR_CAPACITY should validate
        config.sampler.reservoir_capacity = MIN_RESERVOIR_CAPACITY;
        config
            .validate()
            .expect("sampler with reservoir_capacity=MIN_RESERVOIR_CAPACITY should validate");

        config.sampler.reservoir_capacity = 128;
        config
            .validate()
            .expect("sampler with reservoir_capacity=128 should validate");
    }

    #[test]
    fn test_validate_sampler_requires_its_engine_provider() {
        use ProviderMode::*;
        // ITS engine: should validate.
        let mut config = config_with(Noop, ITS, Noop, ConsoleDirect);
        config.sampler.enabled = true;
        config
            .validate()
            .expect("sampler with ITS engine should validate");

        // Non-ITS engine providers: rejected (v1 limitation).
        for engine in [Noop, ConsoleAsync, ConsoleDirect] {
            let mut config = config_with(Noop, engine, Noop, ConsoleDirect);
            config.sampler.enabled = true;
            assert_invalid(&config, "logs.sampler.enabled requires");
        }
    }
}
