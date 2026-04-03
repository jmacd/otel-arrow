//! OTAP-native metrics SDK with codegen-driven Arrow encoding.
//!
//! This crate provides runtime types that generated metrics code targets:
//! - [`Dimension`] trait for bounded enum types used as metric attributes
//! - [`PrecomputedMetricSchema`] for init-time Arrow batch construction
//! - [`CounterDataPointsBuilder`] for runtime data point encoding

pub mod assembly;
pub mod collectable;
pub mod collector;
pub mod dimension;
pub mod precomputed;
