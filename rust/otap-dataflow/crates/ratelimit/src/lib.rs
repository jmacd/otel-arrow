//! Rate limiting utilities for the OTAP dataflow engine.
//!
//! This crate provides token bucket-based rate limiting functionality
//! for controlling the flow of data through the OTAP dataflow pipeline.

mod error;
mod tokenbucket;
mod processor;

pub use processor::{RateLimitProcessor, RateLimitConfig};
pub use tokenbucket::{Clock, Limit, Limiter, SystemClock, Reservation};
pub use error::Error;
