// SPDX-License-Identifier: Apache-2.0

//! Errors for the rate limit crate.

/// Errors that can occur when using the rate limiter.
#[derive(thiserror::Error, Debug)]
#[allow(dead_code)] // Error variants are used but thiserror generates code that confuses dead code analysis
pub enum Error {
    /// The rate limit is invalid (not finite, positive, or zero).
    #[error("Invalid rate limit: {rate}. Rate must be finite and positive.")]
    InvalidLimit {
        /// The invalid rate value.
        rate: f64,
    },

    /// The burst size is invalid (zero).
    #[error("Invalid burst size: {burst}. Burst must be greater than zero.")]
    InvalidBurst {
        /// The invalid burst size.
        burst: usize,
    },

    /// The token count is invalid (zero).
    #[error("Invalid token count: {count}. Token count must be greater than zero.")]
    InvalidTokenCount {
        /// The invalid token count.
        count: usize,
    },

    /// The burst size exceeded the configured limit.
    #[error("Burst size exceeded: {request} < {burst}")]
    BurstExceeded {
        /// Requested weight
        request: usize,

        /// Burst allowance
        burst: usize,
    },
}
