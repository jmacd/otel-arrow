//! This is a single-threaded, async-friendly rate limiter based on the token bucket algorithm.
//! It implements a reservation system similar to Go's x/time/rate package but without mutexes,
//! designed specifically for single-threaded async environments.
//!
//! The rate limiter is explicitly !Send and !Sync, making it suitable only for use within
//! a single thread or task.

use std::time::{Duration, Instant};

pub use super::error::Error;

/// A trait for abstracting time operations to enable testing.
pub trait Clock {
    /// Returns the current instant in time.
    fn now(&self) -> Instant;
}

/// Standard monotonic clock implementation.
#[derive(Debug, Clone, Copy, Default)]
pub struct MonoClock;

impl Clock for MonoClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

impl<T: Clock> Clock for &T {
    fn now(&self) -> Instant {
        (*self).now()
    }
}

/// Rate limit expressed as tokens per second.
/// Must be finite and positive.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Limit(f64);

impl Limit {
    /// Creates a new rate limit from tokens per second.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidLimit` if the rate is not finite, positive, or zero.
    pub fn new(tokens_per_second: f64) -> Result<Self, Error> {
        if !tokens_per_second.is_finite() || tokens_per_second <= 0.0 {
            return Err(Error::InvalidLimit {
                rate: tokens_per_second,
            });
        }
        Ok(Limit(tokens_per_second))
    }

    /// Returns the rate as tokens per second.
    #[allow(dead_code)]
    fn rate(self) -> f64 {
        self.0
    }

    /// Converts tokens to the duration required to accumulate them at this rate.
    #[allow(dead_code)]
    fn duration_from_tokens(self, tokens: f64) -> Duration {
        let seconds = tokens / self.0;
        Duration::from_secs_f64(seconds.max(0.0))
    }

    /// Converts a duration to the number of tokens that could be accumulated at this rate.
    #[allow(dead_code)]
    fn tokens_from_duration(self, duration: Duration) -> f64 {
        duration.as_secs_f64() * self.0
    }
}

/// A reservation for future token consumption.
///
/// This represents a commitment to consume tokens at a specific time in the future.
/// The reservation can be checked for validity and cancelled if needed.
#[derive(Debug)]
pub struct Reservation {
    /// Number of tokens reserved.
    /// TODO: Not used, as cancel() and cancel_at() are not implemented.
    _tokens: usize,

    /// Time when the tokens can be consumed.
    time_to_act: Instant,
}

impl Reservation {
    /// Returns the delay from a specific time until the reserved tokens can be consumed.
    ///
    /// Returns `Duration::ZERO` if the tokens can be consumed immediately.
    pub fn delay_from(&self, now: Instant) -> Duration {
        if self.time_to_act <= now {
            Duration::ZERO
        } else {
            self.time_to_act - now
        }
    }
}

/// A rate limiter implementing the token bucket algorithm.
///
/// This limiter is designed for single-threaded use and is explicitly !Send and !Sync.
/// It provides a reservation-based interface that allows callers to reserve tokens
/// for future consumption without blocking.
pub struct Limiter<C: Clock> {
    /// The rate limit (tokens per second).
    limit: Limit,
    /// Maximum burst size (maximum tokens available).
    burst: usize,
    /// Current number of available tokens.
    tokens: f64,
    /// Last time the token count was updated.
    last: Instant,
    /// Time of the latest rate-limited event (past or future).
    last_event: Instant,
    /// Clock for time operations.
    clock: C,
}

impl<C: Clock> Limiter<C> {
    /// Creates a new rate limiter with the specified rate and burst size.
    ///
    /// # Arguments
    ///
    /// * `rate` - The rate limit in tokens per second
    /// * `burst` - Maximum number of tokens that can be consumed at once
    /// * `clock` - Clock implementation for time operations
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidBurst` if burst is zero.
    pub fn new(rate: Limit, burst: usize, clock: C) -> Result<Self, Error> {
        if burst == 0 {
            return Err(Error::InvalidBurst { burst });
        }

        let now = clock.now();
        Ok(Limiter {
            limit: rate,
            burst,
            tokens: burst as f64,
            last: now,
            last_event: now,
            clock,
        })
    }

    /// Returns the number of tokens available at a specific time.
    pub fn tokens_at(&self, at: Instant) -> f64 {
        self.advance_at(at)
    }

    /// Returns the number of tokens available now.
    pub fn tokens(&mut self) -> f64 {
        self.tokens_at(self.clock.now())
    }

    /// Attempts to reserve `n` tokens for immediate or future consumption.
    ///
    /// Returns a `Reservation` that indicates when the tokens can be consumed.
    /// If the reservation's `ok()` method returns false, the request exceeds
    /// the burst limit and should be rejected.
    ///
    /// # Arguments
    ///
    /// * `n` - Number of tokens to reserve
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidTokenCount` if `n` is zero.
    pub fn reserve_n(&mut self, n: usize) -> Result<Reservation, Error> {
        if n == 0 {
            return Err(Error::InvalidTokenCount { count: n });
        }

        let now = self.clock.now();
        self.reserve_n_at(now, n)
    }

    /// Attempts to reserve `n` tokens at a specific time.
    pub fn reserve_n_at(&mut self, at: Instant, n: usize) -> Result<Reservation, Error> {
        if n == 0 {
            return Err(Error::InvalidTokenCount { count: n });
        }

        let tokens = self.advance(at);

        // Calculate remaining tokens after this request
        let remaining_tokens = tokens - n as f64;

        // Calculate wait duration if we don't have enough tokens
        let wait_duration = if remaining_tokens < 0.0 {
            self.limit.duration_from_tokens(-remaining_tokens)
        } else {
            Duration::ZERO
        };

        // Check if request is within burst limit
        let ok = n <= self.burst;

        if ok {
            let reservation = Reservation {
                _tokens: n,
                time_to_act: at + wait_duration,
            };

            // Update limiter state
            self.last = at;
            self.tokens = remaining_tokens;
            self.last_event = reservation.time_to_act;

            Ok(reservation)
        } else {
            Err(Error::BurstExceeded {
                request: n,
                burst: self.burst,
            })
        }
    }

    /// Advances the token count based on elapsed time and returns the new count.
    /// Does not modify the limiter state.
    fn advance_at(&self, at: Instant) -> f64 {
        // Handle time going backwards by using the later of the two times
        let effective_last = if at < self.last { at } else { self.last };

        let elapsed = at.saturating_duration_since(effective_last);
        let delta = self.limit.tokens_from_duration(elapsed);
        let tokens = self.tokens + delta;

        // Cap at burst size
        tokens.min(self.burst as f64)
    }

    /// Advances the token count and updates the limiter state.
    fn advance(&mut self, at: Instant) -> f64 {
        let tokens = self.advance_at(at);
        self.tokens = tokens;
        self.last = at;
        tokens
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Mock clock for testing that can be advanced manually.
    #[derive(Debug)]
    struct MockClock {
        base: Instant,
        nanos: AtomicU64,
    }

    impl MockClock {
        fn new() -> Self {
            Self {
                base: Instant::now(),
                nanos: AtomicU64::new(0),
            }
        }

        fn advance(&self, duration: Duration) {
            let _ = self
                .nanos
                .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            let nanos = self.nanos.load(Ordering::Relaxed);
            self.base + Duration::from_nanos(nanos)
        }
    }

    #[test]
    fn test_limit_creation() {
        assert!(Limit::new(1.0).is_ok());
        assert!(Limit::new(0.1).is_ok());
        assert!(Limit::new(1000.0).is_ok());

        assert!(Limit::new(0.0).is_err());
        assert!(Limit::new(-1.0).is_err());
        assert!(Limit::new(f64::INFINITY).is_err());
        assert!(Limit::new(f64::NAN).is_err());
    }

    #[test]
    fn test_limiter_creation() {
        let clock = MockClock::new();
        let rate = Limit::new(1.0).unwrap();

        assert!(Limiter::new(rate, 1, clock).is_ok());

        let clock = MockClock::new();
        assert!(Limiter::new(rate, 0, clock).is_err());
    }

    #[test]
    fn test_immediate_reservation() {
        let clock = MockClock::new();
        let rate = Limit::new(1.0).unwrap();
        let mut limiter = Limiter::new(rate, 5, clock).unwrap();

        // Should be able to reserve up to burst immediately
        for i in 1..=5 {
            let reservation = limiter.reserve_n(1).expect("allowed");
            assert_eq!(
                reservation.delay_from(reservation.time_to_act),
                Duration::ZERO
            );
            assert_eq!(limiter.tokens(), (5 - i) as f64);
        }
    }

    #[test]
    fn test_delayed_reservation() {
        let clock = MockClock::new();
        let rate = Limit::new(1.0).unwrap(); // 1 token per second
        let mut limiter = Limiter::new(rate, 1, clock).unwrap();

        // First reservation should be immediate
        let res1 = limiter.reserve_n(1).unwrap();
        assert_eq!(res1.delay_from(res1.time_to_act), Duration::ZERO);

        // Second reservation should be delayed by 1 second
        let res2 = limiter.reserve_n(1).unwrap();
        assert_eq!(res2.delay_from(res1.time_to_act), Duration::from_secs(1));
    }

    #[test]
    fn test_burst_limit_exceeded() {
        let clock = MockClock::new();
        let rate = Limit::new(1.0).unwrap();
        let mut limiter = Limiter::new(rate, 5, clock).unwrap();

        // Requesting more than burst should fail
        assert!(matches!(
            limiter.reserve_n(6),
            Err(Error::BurstExceeded { .. })
        ));
    }

    #[test]
    fn test_token_replenishment() {
        let clock = MockClock::new();
        let rate = Limit::new(2.0).unwrap(); // 2 tokens per second
        let mut limiter = Limiter::new(rate, 5, &clock).unwrap();

        assert_eq!(rate.rate(), 2.0);

        // Use all tokens
        let _res = limiter.reserve_n(5).unwrap();
        assert_eq!(limiter.tokens(), 0.0);

        // Advance time by 1 second - should add 2 tokens
        clock.advance(Duration::from_secs(1));
        assert_eq!(limiter.tokens(), 2.0);

        // Advance by another 2 seconds - should be capped at burst (5)
        clock.advance(Duration::from_secs(2));
        assert_eq!(limiter.tokens(), 5.0);
    }

    #[test]
    fn test_zero_tokens_error() {
        let clock = MockClock::new();
        let rate = Limit::new(1.0).unwrap();
        let mut limiter = Limiter::new(rate, 5, clock).unwrap();

        assert!(limiter.reserve_n(0).is_err());
    }
}
