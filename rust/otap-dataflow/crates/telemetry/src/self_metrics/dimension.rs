// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Dimension trait for bounded enum attribute types.
//!
//! Dimensions are the attribute keys used to index metric data points.
//! Each dimension is a bounded enum with compile-time cardinality, enabling
//! generated code to use fixed-size arrays for storage.
//!
//! Codegen produces `Dimension` impls for each attribute enum defined in
//! the metric schema YAML. The two pilot dimensions are defined here:
//! [`Outcome`] and [`SignalType`].

use std::fmt::Debug;

/// A bounded enumeration used as a metric attribute dimension.
///
/// Implementations provide compile-time metadata (cardinality, attribute key,
/// value strings) and a runtime index for array-based storage.
pub trait Dimension: Copy + Debug + 'static {
    /// Number of distinct values.
    const CARDINALITY: usize;
    /// The OTel attribute key for this dimension (e.g., `"outcome"`).
    const ATTRIBUTE_KEY: &'static str;
    /// String representation of each enum variant, indexed by [`Dimension::index`].
    const ATTRIBUTE_VALUES: &'static [&'static str];
    /// Returns the zero-based index of this variant.
    fn index(self) -> usize;
}

/// Request outcome dimension: success, failure, or refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The operation completed successfully.
    Success,
    /// The operation failed due to an error.
    Failure,
    /// The operation was refused (e.g., back-pressure).
    Refused,
}

impl Dimension for Outcome {
    const CARDINALITY: usize = 3;
    const ATTRIBUTE_KEY: &'static str = "outcome";
    const ATTRIBUTE_VALUES: &'static [&'static str] = &["success", "failure", "refused"];

    #[inline]
    fn index(self) -> usize {
        self as usize
    }
}

/// Signal type dimension: logs, metrics, or traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    /// Log signal.
    Logs,
    /// Metric signal.
    Metrics,
    /// Trace signal.
    Traces,
}

impl Dimension for SignalType {
    const CARDINALITY: usize = 3;
    const ATTRIBUTE_KEY: &'static str = "signal_type";
    const ATTRIBUTE_VALUES: &'static [&'static str] = &["logs", "metrics", "traces"];

    #[inline]
    fn index(self) -> usize {
        self as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_indices() {
        assert_eq!(Outcome::Success.index(), 0);
        assert_eq!(Outcome::Failure.index(), 1);
        assert_eq!(Outcome::Refused.index(), 2);
    }

    #[test]
    fn outcome_values_match_indices() {
        for (i, &val) in Outcome::ATTRIBUTE_VALUES.iter().enumerate() {
            let outcome = match i {
                0 => Outcome::Success,
                1 => Outcome::Failure,
                2 => Outcome::Refused,
                _ => unreachable!(),
            };
            assert_eq!(outcome.index(), i);
            assert!(!val.is_empty());
        }
    }

    #[test]
    fn signal_type_indices() {
        assert_eq!(SignalType::Logs.index(), 0);
        assert_eq!(SignalType::Metrics.index(), 1);
        assert_eq!(SignalType::Traces.index(), 2);
    }

    #[test]
    fn cardinality_matches_values_len() {
        assert_eq!(Outcome::CARDINALITY, Outcome::ATTRIBUTE_VALUES.len());
        assert_eq!(SignalType::CARDINALITY, SignalType::ATTRIBUTE_VALUES.len());
    }
}
