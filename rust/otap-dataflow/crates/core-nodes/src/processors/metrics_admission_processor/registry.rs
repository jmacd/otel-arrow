// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Per-core metric type registry (the "manifest") for the admission processor.
//!
//! The registry records the canonical type descriptor of every metric name it
//! has observed (within the processor's tenant) and flags *name conflicts* --
//! two streams sharing a name but carrying different descriptors. Following the
//! OpenTelemetry metrics SDK's instrument-conflict handling, the default policy
//! is to admit both streams and warn once; an optional strict mode rejects the
//! conflicting points instead.
//!
//! This phase-0 implementation keeps the registry in memory, one per processor
//! replica. Because the engine runs a thread-per-core model and (from phase 1)
//! shuffles by metric name, each core owns the authoritative registry for its
//! names with no cross-core coordination -- the classic hard part collapses into
//! ordinary sharding.

use hashbrown::HashMap;

/// The *manifesting* (conflict-checked) properties of a metric: instrument type,
/// monotonicity, and unit.
///
/// Temporality and value type (int vs double) are deliberately excluded: users
/// are permitted to mix them within one identity, so a difference in either must
/// never be treated as a type conflict (ingest-queue design, "Identity and type
/// model").
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDescriptor {
    /// Instrument-type discriminant (the OTAP `DataType`: gauge, sum,
    /// histogram, exponential histogram, summary), stored as its `u8` value.
    pub instrument_type: u8,
    /// Whether the instrument is monotonic (meaningful for sums; conventionally
    /// `true` for histograms, `false` for gauges/summaries).
    pub is_monotonic: bool,
    /// The metric unit, as the raw UTF-8 bytes carried by the OTAP `unit`
    /// column.
    pub unit: Box<[u8]>,
}

/// One registry entry: the canonical (primary) descriptor for a name plus
/// whether a conflicting descriptor has already been observed (so the warning is
/// emitted only once).
#[derive(Debug, Clone)]
struct Entry {
    primary: TypeDescriptor,
    conflicting: bool,
}

/// What happened when an observed `(name, descriptor)` pair was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// The name was not previously known; its descriptor became the primary.
    Registered,
    /// The name was known and the descriptor matched the primary.
    Matched,
    /// The name was known and the descriptor differs from the primary -- the
    /// first time this conflict is seen (warn once).
    NewConflict,
    /// The name was known and was already flagged as conflicting (suppress the
    /// warning, but the point is still conflicting for strict mode).
    KnownConflict,
}

/// A per-core registry recording the canonical type descriptor of every metric
/// name and flagging name conflicts.
#[derive(Debug, Default)]
pub struct TypeRegistry {
    entries: HashMap<Box<[u8]>, Entry>,
}

impl TypeRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an observed `(name, descriptor)` pair, returning what happened.
    ///
    /// The first descriptor seen for a name becomes its primary (canonical for
    /// display). A later point whose descriptor differs is a name conflict; the
    /// primary is left unchanged (both streams coexist) and the entry is flagged
    /// so subsequent identical conflicts are reported as [`Observation::KnownConflict`].
    pub fn observe(&mut self, name: &[u8], descriptor: &TypeDescriptor) -> Observation {
        if let Some(entry) = self.entries.get_mut(name) {
            if &entry.primary == descriptor {
                Observation::Matched
            } else if entry.conflicting {
                Observation::KnownConflict
            } else {
                entry.conflicting = true;
                Observation::NewConflict
            }
        } else {
            let _ = self.entries.insert(
                name.to_vec().into_boxed_slice(),
                Entry {
                    primary: descriptor.clone(),
                    conflicting: false,
                },
            );
            Observation::Registered
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn desc(instrument_type: u8, is_monotonic: bool, unit: &str) -> TypeDescriptor {
        TypeDescriptor {
            instrument_type,
            is_monotonic,
            unit: unit.as_bytes().to_vec().into_boxed_slice(),
        }
    }

    #[test]
    fn register_then_match() {
        let mut reg = TypeRegistry::new();
        let d = desc(1, true, "By");
        assert_eq!(reg.observe(b"requests", &d), Observation::Registered);
        assert_eq!(reg.observe(b"requests", &d), Observation::Matched);
    }

    #[test]
    fn conflict_warned_once_then_known() {
        let mut reg = TypeRegistry::new();
        let sum = desc(1, true, "By");
        let gauge = desc(0, false, "By");
        assert_eq!(reg.observe(b"x", &sum), Observation::Registered);
        // Different instrument type for the same name -> conflict.
        assert_eq!(reg.observe(b"x", &gauge), Observation::NewConflict);
        assert_eq!(reg.observe(b"x", &gauge), Observation::KnownConflict);
        // The primary is unchanged: the original descriptor still matches.
        assert_eq!(reg.observe(b"x", &sum), Observation::Matched);
    }

    #[test]
    fn unit_difference_is_a_conflict() {
        let mut reg = TypeRegistry::new();
        assert_eq!(
            reg.observe(b"x", &desc(1, true, "By")),
            Observation::Registered
        );
        assert_eq!(
            reg.observe(b"x", &desc(1, true, "s")),
            Observation::NewConflict
        );
    }
}
