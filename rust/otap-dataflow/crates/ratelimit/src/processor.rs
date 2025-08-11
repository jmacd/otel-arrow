//! Simple rate limiting processor implementation.
//!
//! This processor uses a token bucket algorithm to limit the rate of
//! messages flowing through the pipeline.

use crate::Error;
use crate::tokenbucket::{Clock, Limit, Limiter, MonoClock};
use async_trait::async_trait;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error as EngineError;
use otap_df_engine::local::processor::{EffectHandler, Processor};
use otap_df_engine::message::Message;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::marker::PhantomData;

/// Configuration for the rate limit processor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Rate limit in messages per second (f64)
    pub limit: f64,

    /// Maximum burst size (usize) determines the widthof the time
    /// window used for the limit. Requests larger than burst are
    /// not allowed.
    pub burst: usize,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            limit: 1000.0,
            burst: 10000,
        }
    }
}

/// A simple rate limiting processor that drops messages when rate limit is exceeded.
///
/// Uses a token bucket algorithm with configurable rate limit and burst size.
/// When the rate limit is exceeded, messages are dropped silently.
pub struct RateLimitProcessor<PData> {
    /// The rate limiter instance
    pub limiter: Limiter<MonoClock>,

    /// Phantom data to maintain the PData type
    _phantom: PhantomData<PData>,
}

impl<PData> RateLimitProcessor<PData> {
    /// Creates a new rate limit processor with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the rate limit configuration is invalid.
    pub fn new(config: RateLimitConfig) -> Result<Self, Error> {
        let limit = Limit::new(config.limit)?;
        let limiter = Limiter::new(limit, config.burst, MonoClock)?;

        Ok(Self {
            limiter,
            _phantom: PhantomData,
        })
    }
}

#[async_trait(?Send)]
impl<PData> Processor<PData> for RateLimitProcessor<PData> {
    async fn process(
        &mut self,
        msg: Message<PData>,
        effect_handler: &mut EffectHandler<PData>,
    ) -> Result<(), EngineError<PData>> {
        match msg {
            Message::PData(data) => {
                // Try to reserve a token for this message
                match self.limiter.reserve_n(1) {
                    Ok(reservation) => {
                        // Check if we can process immediately (no delay)
                        if reservation.delay_from(MonoClock.now()).is_zero() {
                            // Rate limit allows this message, forward it
                            effect_handler.send_message(data).await?;
                        }
                        // If there's a delay, we drop the message (simple implementation)
                        // A more sophisticated implementation could delay the message
                    }
                    Err(Error::BurstExceeded { .. }) => {
                        // Message exceeds burst limit, drop it
                        // Could add metrics/logging here in the future
                    }
                    Err(e) => {
                        // Other errors (shouldn't happen with valid input)
                        return Err(EngineError::ProcessorError {
                            processor: Cow::Borrowed("ratelimit"),
                            error: format!("Rate limiter error: {}", e),
                        });
                    }
                }
            }
            Message::Control(control_msg) => {
                // Handle control messages (shutdown, config changes, etc.)
                match control_msg {
                    NodeControlMsg::Shutdown {
                        deadline: _,
                        reason: _,
                    } => {
                        // Graceful shutdown - nothing special needed for rate limiter
                    }
                    NodeControlMsg::Config { config: _ } => {
                        // Configuration updates could be handled here in the future
                    }
                    NodeControlMsg::TimerTick {} => {
                        // Timer ticks could be used for cleanup or metrics
                    }
                    NodeControlMsg::Ack { .. } => {
                        // Acknowledgment messages - no special handling needed
                    }
                    NodeControlMsg::Nack { .. } => {
                        // Negative acknowledgment messages - no special handling needed
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limit_config_default() {
        let config = RateLimitConfig::default();
        assert_eq!(config.limit, 1000.0);
        assert_eq!(config.burst, 10000);
    }

    #[test]
    fn test_rate_limit_processor_creation() {
        let config = RateLimitConfig {
            limit: 10.0,
            burst: 5,
        };

        let processor: RateLimitProcessor<String> = RateLimitProcessor::new(config).unwrap();
        // Test that the processor was created successfully
        assert_eq!(processor.limiter.tokens_at(MonoClock.now()), 5.0);
    }

    #[test]
    fn test_invalid_rate_limit() {
        let config = RateLimitConfig {
            limit: 0.0, // Invalid rate
            burst: 5,
        };

        let result: Result<RateLimitProcessor<String>, _> = RateLimitProcessor::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_burst() {
        let config = RateLimitConfig {
            limit: 10.0,
            burst: 0, // Invalid burst
        };

        let result: Result<RateLimitProcessor<String>, _> = RateLimitProcessor::new(config);
        assert!(result.is_err());
    }
}
