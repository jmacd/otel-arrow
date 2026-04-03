//! Dimension trait for bounded enum types used as metric attributes.
//!
//! A dimension is a fixed-cardinality enum whose variants map to OTel
//! attribute key/value pairs. Generated metric code uses dimensions to
//! index into counter arrays and to build precomputed attribute tables.

use otap_df_config::SignalType;

/// A bounded enum type that serves as a metric attribute dimension.
///
/// Each variant maps to an attribute key/value pair. The cardinality is
/// known at compile time, allowing counter arrays to be sized statically.
pub trait Dimension: Copy + 'static {
    /// Number of distinct values (enum variants).
    const CARDINALITY: usize;

    /// The OTel attribute key for this dimension (e.g., `"outcome"`).
    const ATTRIBUTE_KEY: &'static str;

    /// The OTel attribute values, indexed by `self.index()`.
    const ATTRIBUTE_VALUES: &'static [&'static str];

    /// Returns the index of this variant in `0..CARDINALITY`.
    fn index(self) -> usize;
}

/// Request outcome dimension for producer/consumer metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The request succeeded.
    Success,
    /// The request failed.
    Failure,
    /// The request was refused.
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

impl Dimension for SignalType {
    const CARDINALITY: usize = 3;
    const ATTRIBUTE_KEY: &'static str = "signal_type";
    const ATTRIBUTE_VALUES: &'static [&'static str] = &["traces", "metrics", "logs"];

    #[inline]
    fn index(self) -> usize {
        match self {
            SignalType::Traces => 0,
            SignalType::Metrics => 1,
            SignalType::Logs => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_dimension_properties() {
        assert_eq!(Outcome::CARDINALITY, 3);
        assert_eq!(Outcome::ATTRIBUTE_KEY, "outcome");
        assert_eq!(Outcome::ATTRIBUTE_VALUES.len(), Outcome::CARDINALITY);

        assert_eq!(Outcome::Success.index(), 0);
        assert_eq!(Outcome::Failure.index(), 1);
        assert_eq!(Outcome::Refused.index(), 2);
    }

    #[test]
    fn signal_type_dimension_properties() {
        assert_eq!(SignalType::CARDINALITY, 3);
        assert_eq!(SignalType::ATTRIBUTE_KEY, "signal_type");
        assert_eq!(SignalType::ATTRIBUTE_VALUES.len(), SignalType::CARDINALITY);

        assert_eq!(SignalType::Traces.index(), 0);
        assert_eq!(SignalType::Metrics.index(), 1);
        assert_eq!(SignalType::Logs.index(), 2);
    }
}
