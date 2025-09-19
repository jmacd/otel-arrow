//! OTAP Sampling Receiver Module
//!
//! This module implements a sophisticated sampling receiver that uses DataFusion
//! to query parquet files and perform configurable sampling operations.

pub mod config;
pub mod error; 
pub mod query_engine;
pub mod otap_reconstructor;
pub mod sampling_receiver;

// TODO: Uncomment when we work on sampling algorithms in Phase 4
// pub mod sampler_udf;
// pub mod sampler_udf; // TODO: Fix and re-enable for weighted sampling

// Sample code for UDF implementation - disabled until DataFusion is properly integrated
// #[allow(dead_code)]
// mod sampler_udf;

// Re-export the main receiver and factory
pub use sampling_receiver::{SamplingReceiver, SAMPLING_RECEIVER_FACTORY, SAMPLING_RECEIVER_URN};
