//! Rate limiting utilities for the OTAP dataflow engine.
//!
//! This crate provides token bucket-based rate limiting functionality
//! for controlling the flow of data through the OTAP dataflow pipeline.

mod error;
mod processor;
mod tokenbucket;

pub use error::Error;
pub use processor::{RateLimitConfig, RateLimitProcessor};
pub use tokenbucket::{Limit, Limiter, Reservation};
