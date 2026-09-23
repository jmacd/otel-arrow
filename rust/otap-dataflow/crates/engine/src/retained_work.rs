// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Runtime-local retained-work accounting primitives.
//!
//! This module owns accounting state only. Runtime installation, attribution,
//! telemetry export, and production charge sites are layered on separately.

use std::cell::Cell;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;

/// Identifies the counter whose checked arithmetic failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalAccountingCounter {
    /// Logical bytes retained by known-size items.
    RetainedBytes,
    /// Number of retained items whose size is unknown.
    UnknownItems,
    /// Number of tickets dropped without explicit completion.
    AbandonedItems,
    /// Known bytes dropped without explicit completion.
    AbandonedBytes,
}

/// Describes an accounting arithmetic failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalAccountingError {
    /// Adding a charge or diagnostic would exceed the counter range.
    Overflow(LocalAccountingCounter),
    /// Settling a ticket would reduce a counter below zero.
    Underflow(LocalAccountingCounter),
}

impl fmt::Display for LocalAccountingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow(counter) => write!(formatter, "{counter:?} counter overflow"),
            Self::Underflow(counter) => write!(formatter, "{counter:?} counter underflow"),
        }
    }
}

impl std::error::Error for LocalAccountingError {}

/// Point-in-time values from one runtime-local retained-work account.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalRetainedSnapshot {
    /// Logical bytes retained by known-size items.
    pub retained_bytes: u64,
    /// Number of retained items whose size is unknown.
    pub unknown_items: u64,
    /// Number of tickets dropped without explicit completion.
    pub abandoned_items: u64,
    /// Known bytes dropped without explicit completion.
    pub abandoned_bytes: u64,
    /// Number of detected accounting arithmetic failures.
    pub corruption_count: u64,
}

/// Accounting state owned by one pinned runtime.
///
/// The account is deliberately neither `Send` nor `Sync`. A later runtime
/// wiring layer is responsible for creating and exposing one on its owner
/// runtime.
///
/// ```compile_fail
/// use otel_arrow_dfe_engine::retained_work::LocalRetainedAccount;
///
/// let account = LocalRetainedAccount::new();
/// std::thread::spawn(move || drop(account));
/// ```
#[derive(Debug)]
pub struct LocalRetainedAccount {
    retained_bytes: Cell<u64>,
    unknown_items: Cell<u64>,
    abandoned_items: Cell<u64>,
    abandoned_bytes: Cell<u64>,
    corruption_count: Cell<u64>,
    _not_send: PhantomData<Rc<()>>,
}

impl LocalRetainedAccount {
    /// Creates an empty runtime-local account.
    #[must_use]
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            retained_bytes: Cell::new(0),
            unknown_items: Cell::new(0),
            abandoned_items: Cell::new(0),
            abandoned_bytes: Cell::new(0),
            corruption_count: Cell::new(0),
            _not_send: PhantomData,
        })
    }

    /// Starts one retained-work interval and returns its ownership ticket.
    ///
    /// `Some(bytes)` records a known logical size. `None` records one
    /// unknown-size item without guessing its byte size.
    #[inline]
    pub fn charge(
        self: &Rc<Self>,
        bytes: Option<u64>,
    ) -> Result<LocalRetainedTicket, LocalAccountingError> {
        let charge = match bytes {
            Some(bytes) => {
                self.checked_add(
                    &self.retained_bytes,
                    bytes,
                    LocalAccountingCounter::RetainedBytes,
                )?;
                LocalRetainedCharge::Known(bytes)
            }
            None => {
                self.checked_add(&self.unknown_items, 1, LocalAccountingCounter::UnknownItems)?;
                LocalRetainedCharge::Unknown
            }
        };

        Ok(LocalRetainedTicket {
            account: Rc::clone(self),
            charge,
            active: true,
        })
    }

    /// Returns the current local counters.
    #[must_use]
    pub fn snapshot(&self) -> LocalRetainedSnapshot {
        LocalRetainedSnapshot {
            retained_bytes: self.retained_bytes.get(),
            unknown_items: self.unknown_items.get(),
            abandoned_items: self.abandoned_items.get(),
            abandoned_bytes: self.abandoned_bytes.get(),
            corruption_count: self.corruption_count.get(),
        }
    }

    fn settle(&self, charge: LocalRetainedCharge) -> Result<(), LocalAccountingError> {
        match charge {
            LocalRetainedCharge::Known(bytes) => self.checked_sub(
                &self.retained_bytes,
                bytes,
                LocalAccountingCounter::RetainedBytes,
            ),
            LocalRetainedCharge::Unknown => {
                self.checked_sub(&self.unknown_items, 1, LocalAccountingCounter::UnknownItems)
            }
        }
    }

    fn record_abandonment(&self, charge: LocalRetainedCharge) {
        let _ = self.checked_add(
            &self.abandoned_items,
            1,
            LocalAccountingCounter::AbandonedItems,
        );
        if let LocalRetainedCharge::Known(bytes) = charge {
            let _ = self.checked_add(
                &self.abandoned_bytes,
                bytes,
                LocalAccountingCounter::AbandonedBytes,
            );
        }
    }

    fn checked_add(
        &self,
        cell: &Cell<u64>,
        value: u64,
        counter: LocalAccountingCounter,
    ) -> Result<(), LocalAccountingError> {
        let Some(next) = cell.get().checked_add(value) else {
            self.record_corruption();
            return Err(LocalAccountingError::Overflow(counter));
        };
        cell.set(next);
        Ok(())
    }

    fn checked_sub(
        &self,
        cell: &Cell<u64>,
        value: u64,
        counter: LocalAccountingCounter,
    ) -> Result<(), LocalAccountingError> {
        let Some(next) = cell.get().checked_sub(value) else {
            self.record_corruption();
            return Err(LocalAccountingError::Underflow(counter));
        };
        cell.set(next);
        Ok(())
    }

    fn record_corruption(&self) {
        if let Some(next) = self.corruption_count.get().checked_add(1) {
            self.corruption_count.set(next);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalRetainedCharge {
    Known(u64),
    Unknown,
}

/// Owns one charge in a [`LocalRetainedAccount`].
///
/// Call [`complete`](Self::complete) on every normal terminal path. Dropping
/// an active ticket still refunds its charge, but records the interval as
/// abandoned. The ticket is deliberately neither `Send` nor `Sync` because it
/// holds an `Rc` to runtime-local state.
#[derive(Debug)]
#[must_use = "the ticket must be completed normally or dropped as abandoned"]
pub struct LocalRetainedTicket {
    account: Rc<LocalRetainedAccount>,
    charge: LocalRetainedCharge,
    active: bool,
}

impl LocalRetainedTicket {
    /// Returns the known logical byte charge, or `None` for an unknown size.
    #[must_use]
    pub const fn bytes(&self) -> Option<u64> {
        match self.charge {
            LocalRetainedCharge::Known(bytes) => Some(bytes),
            LocalRetainedCharge::Unknown => None,
        }
    }

    /// Completes this retention interval normally and refunds its charge.
    #[inline]
    pub fn complete(mut self) -> Result<(), LocalAccountingError> {
        self.active = false;
        self.account.settle(self.charge)
    }
}

impl Drop for LocalRetainedTicket {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        self.active = false;
        let _ = self.account.settle(self.charge);
        self.account.record_abandonment(self.charge);
    }
}

/// Failure to charge a per-context retained-work bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalRetainedBucketError {
    /// A new, distinct context would exceed the registry's bucket limit.
    CapacityExhausted,
    /// The underlying retained-work account rejected the charge.
    Accounting(LocalAccountingError),
}

impl fmt::Display for LocalRetainedBucketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityExhausted => {
                write!(formatter, "retained-work bucket capacity exhausted")
            }
            Self::Accounting(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LocalRetainedBucketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CapacityExhausted => None,
            Self::Accounting(error) => Some(error),
        }
    }
}

impl From<LocalAccountingError> for LocalRetainedBucketError {
    fn from(error: LocalAccountingError) -> Self {
        Self::Accounting(error)
    }
}

struct LocalRetainedBucket<K> {
    key: K,
    account: Rc<LocalRetainedAccount>,
}

/// Bounded, runtime-local retained-work accounts indexed by context.
///
/// The caller supplies a precomputed projection hash and an equality closure
/// over borrowed context data. Equal contexts must have the same projection
/// hash; distinct contexts may have the same hash and are kept in separate
/// buckets. An owned key is created only when a new bucket is admitted.
///
/// Buckets stay registered until the registry is dropped, even after their
/// outstanding charges settle. Capacity therefore bounds the number of
/// distinct contexts admitted over the registry's lifetime. Tickets retain
/// their account independently and can settle after the registry is dropped.
/// The registry, like its accounts, is neither `Send` nor `Sync`.
pub struct LocalRetainedBuckets<K> {
    by_hash: HashMap<u64, Vec<LocalRetainedBucket<K>>>,
    bucket_count: usize,
    max_buckets: usize,
}

impl<K> LocalRetainedBuckets<K> {
    /// Creates an empty registry with a limit on distinct context buckets.
    #[must_use]
    pub fn new(max_buckets: usize) -> Self {
        Self {
            by_hash: HashMap::new(),
            bucket_count: 0,
            max_buckets,
        }
    }

    /// Returns the number of registered distinct contexts.
    #[must_use]
    pub const fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    /// Returns the maximum number of distinct contexts this registry admits.
    #[must_use]
    pub const fn max_buckets(&self) -> usize {
        self.max_buckets
    }

    /// Returns the current counters for a context, if it is registered.
    ///
    /// `equals` compares an owned key with borrowed context data captured by
    /// the closure. Lookup does not allocate, including when hashes collide.
    #[must_use]
    pub fn snapshot<E>(&self, projection_hash: u64, mut equals: E) -> Option<LocalRetainedSnapshot>
    where
        E: FnMut(&K) -> bool,
    {
        self.by_hash
            .get(&projection_hash)?
            .iter()
            .find(|bucket| equals(&bucket.key))
            .map(|bucket| bucket.account.snapshot())
    }

    /// Charges the existing context or registers and charges a new one.
    ///
    /// `equals` compares each candidate's owned key with borrowed context
    /// data captured by the closure. `create_key` runs only for a new context
    /// with available capacity; neither an existing lookup nor a rejected new
    /// context needs an owned key. The returned ticket must be completed or
    /// dropped, even if this registry has since been dropped.
    pub fn charge<E, C>(
        &mut self,
        projection_hash: u64,
        mut equals: E,
        create_key: C,
        bytes: Option<u64>,
    ) -> Result<LocalRetainedTicket, LocalRetainedBucketError>
    where
        E: FnMut(&K) -> bool,
        C: FnOnce() -> K,
    {
        if let Some(bucket) = self
            .by_hash
            .get(&projection_hash)
            .and_then(|candidates| candidates.iter().find(|bucket| equals(&bucket.key)))
        {
            return Ok(bucket.account.charge(bytes)?);
        }
        if self.bucket_count == self.max_buckets {
            return Err(LocalRetainedBucketError::CapacityExhausted);
        }

        let key = create_key();
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(bytes)?;
        self.by_hash
            .entry(projection_hash)
            .or_default()
            .push(LocalRetainedBucket { key, account });
        self.bucket_count += 1;
        Ok(ticket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a known-size ticket reaches its normal terminal path.
    /// Guarantees: completion refunds the bytes once and records no abandonment.
    #[test]
    fn known_charge_completes_normally() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(Some(42)).expect("charge should fit");

        assert_eq!(ticket.bytes(), Some(42));
        assert_eq!(account.snapshot().retained_bytes, 42);
        ticket.complete().expect("completion should settle");
        assert_eq!(account.snapshot(), LocalRetainedSnapshot::default());
    }

    /// Scenario: an unknown-size ticket reaches its normal terminal path.
    /// Guarantees: completion decrements the unknown count without guessing bytes.
    #[test]
    fn unknown_charge_completes_normally() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(None).expect("charge should fit");

        assert_eq!(ticket.bytes(), None);
        assert_eq!(account.snapshot().unknown_items, 1);
        ticket.complete().expect("completion should settle");
        assert_eq!(account.snapshot(), LocalRetainedSnapshot::default());
    }

    /// Scenario: a known-size ticket is dropped without explicit completion.
    /// Guarantees: drop refunds once and records both the abandoned item and bytes.
    #[test]
    fn unresolved_known_ticket_refunds_and_records_abandonment() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(Some(17)).expect("charge should fit");

        drop(ticket);

        assert_eq!(
            account.snapshot(),
            LocalRetainedSnapshot {
                abandoned_items: 1,
                abandoned_bytes: 17,
                ..LocalRetainedSnapshot::default()
            }
        );
    }

    /// Scenario: an unknown-size ticket is dropped without explicit completion.
    /// Guarantees: drop refunds the unknown item and records no invented byte count.
    #[test]
    fn unresolved_unknown_ticket_refunds_and_records_abandonment() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(None).expect("charge should fit");

        drop(ticket);

        assert_eq!(
            account.snapshot(),
            LocalRetainedSnapshot {
                abandoned_items: 1,
                ..LocalRetainedSnapshot::default()
            }
        );
    }

    /// Scenario: normal completion consumes a ticket and then runs its destructor.
    /// Guarantees: the destructor neither refunds twice nor records abandonment.
    #[test]
    fn completion_prevents_drop_from_settling_twice() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(Some(9)).expect("charge should fit");

        ticket.complete().expect("completion should settle");

        assert_eq!(account.snapshot(), LocalRetainedSnapshot::default());
    }

    /// Scenario: a known-size charge would overflow the byte counter.
    /// Guarantees: the charge is rejected without mutation and corruption is visible.
    #[test]
    fn charge_overflow_is_rejected_and_recorded() {
        let account = LocalRetainedAccount::new();
        account.retained_bytes.set(u64::MAX);

        let error = account
            .charge(Some(1))
            .expect_err("overflowing charge must fail");

        assert_eq!(
            error,
            LocalAccountingError::Overflow(LocalAccountingCounter::RetainedBytes)
        );
        assert_eq!(account.snapshot().retained_bytes, u64::MAX);
        assert_eq!(account.snapshot().corruption_count, 1);
    }

    /// Scenario: corrupted account state cannot cover a ticket's known-byte refund.
    /// Guarantees: completion reports underflow, leaves state bounded, and records corruption.
    #[test]
    fn settlement_underflow_is_reported_and_recorded() {
        let account = LocalRetainedAccount::new();
        let ticket = account.charge(Some(5)).expect("charge should fit");
        account.retained_bytes.set(0);

        let error = ticket
            .complete()
            .expect_err("corrupted settlement must fail");

        assert_eq!(
            error,
            LocalAccountingError::Underflow(LocalAccountingCounter::RetainedBytes)
        );
        assert_eq!(account.snapshot().retained_bytes, 0);
        assert_eq!(account.snapshot().corruption_count, 1);
        assert_eq!(account.snapshot().abandoned_items, 0);
    }

    /// Scenario: distinct contexts have the same projection hash.
    /// Guarantees: full equality keeps their known and unknown charges separate.
    #[test]
    fn colliding_contexts_keep_separate_accounts() {
        let mut buckets = LocalRetainedBuckets::new(2);
        let first = buckets
            .charge(
                7,
                |key: &String| key == "first",
                || "first".into(),
                Some(12),
            )
            .expect("first context should fit");
        let second = buckets
            .charge(7, |key| key == "second", || "second".into(), None)
            .expect("colliding context should fit");

        assert_eq!(buckets.bucket_count(), 2);
        assert_eq!(
            buckets.snapshot(7, |key| key == "first"),
            Some(LocalRetainedSnapshot {
                retained_bytes: 12,
                ..LocalRetainedSnapshot::default()
            })
        );
        assert_eq!(
            buckets.snapshot(7, |key| key == "second"),
            Some(LocalRetainedSnapshot {
                unknown_items: 1,
                ..LocalRetainedSnapshot::default()
            })
        );
        assert_eq!(buckets.snapshot(8, |key| key == "first"), None);

        first.complete().expect("first context should settle");
        second.complete().expect("second context should settle");
    }

    /// Scenario: an existing context is charged when its hash also has collisions.
    /// Guarantees: lookup reuses the matching account without calling create_key.
    #[test]
    fn existing_context_does_not_create_a_key() {
        let mut buckets = LocalRetainedBuckets::new(2);
        let first = buckets
            .charge(3, |key: &String| key == "first", || "first".into(), Some(4))
            .expect("first context should fit");
        let second = buckets
            .charge(3, |key| key == "second", || "second".into(), Some(6))
            .expect("second context should fit");
        let again = buckets
            .charge(
                3,
                |key| key == "first",
                || panic!("existing context must not create an owned key"),
                Some(5),
            )
            .expect("existing context must work even at capacity");

        assert_eq!(buckets.bucket_count(), 2);
        assert_eq!(
            buckets
                .snapshot(3, |key| key == "first")
                .map(|s| s.retained_bytes),
            Some(9)
        );
        assert_eq!(
            buckets
                .snapshot(3, |key| key == "second")
                .map(|s| s.retained_bytes),
            Some(6)
        );
        first.complete().expect("first charge should settle");
        second.complete().expect("second charge should settle");
        again.complete().expect("reused charge should settle");
    }

    /// Scenario: a distinct context arrives after all bucket slots are occupied.
    /// Guarantees: capacity rejects it without constructing a key or merging charges.
    #[test]
    fn bucket_capacity_rejects_distinct_contexts() {
        let mut buckets = LocalRetainedBuckets::new(1);
        let ticket = buckets
            .charge(9, |key: &String| key == "first", || "first".into(), Some(8))
            .expect("first context should fit");

        let error = buckets
            .charge(
                9,
                |key| key == "other",
                || panic!("rejected context must not create an owned key"),
                Some(5),
            )
            .expect_err("distinct context with the same hash must be rejected");

        assert_eq!(error, LocalRetainedBucketError::CapacityExhausted);
        assert_eq!(buckets.max_buckets(), 1);
        assert_eq!(buckets.bucket_count(), 1);
        assert_eq!(
            buckets
                .snapshot(9, |key| key == "first")
                .map(|s| s.retained_bytes),
            Some(8)
        );
        assert_eq!(buckets.snapshot(9, |key| key == "other"), None);
        ticket.complete().expect("original charge should settle");

        let error = buckets
            .charge(
                10,
                |key| key == "third",
                || panic!("idle buckets still count toward capacity"),
                None,
            )
            .expect_err("idle buckets stay registered");
        assert_eq!(error, LocalRetainedBucketError::CapacityExhausted);
    }

    /// Scenario: all tickets outlive the registry that created their context.
    /// Guarantees: normal completion and abandoned drop still settle their shared account.
    #[test]
    fn tickets_settle_after_registry_drop() {
        let mut buckets = LocalRetainedBuckets::new(1);
        let completed = buckets
            .charge(
                1,
                |key: &String| key == "context",
                || "context".into(),
                Some(7),
            )
            .expect("context should fit");
        let abandoned = buckets
            .charge(
                1,
                |key| key == "context",
                || panic!("existing context must not create a key"),
                None,
            )
            .expect("context should be reused");
        let account = Rc::clone(&completed.account);

        drop(buckets);
        completed.complete().expect("ticket should settle");
        assert_eq!(account.snapshot().unknown_items, 1);
        drop(abandoned);
        assert_eq!(
            account.snapshot(),
            LocalRetainedSnapshot {
                abandoned_items: 1,
                ..LocalRetainedSnapshot::default()
            }
        );
    }

    /// Scenario: the registry has no capacity for even its first context.
    /// Guarantees: zero capacity rejects without constructing a key or accounting charge.
    #[test]
    fn zero_bucket_capacity_rejects_first_context() {
        let mut buckets = LocalRetainedBuckets::<String>::new(0);
        let error = buckets
            .charge(
                1,
                |_| false,
                || panic!("no key should be created at zero capacity"),
                Some(1),
            )
            .expect_err("zero-capacity registry must reject the first context");
        assert_eq!(error, LocalRetainedBucketError::CapacityExhausted);
        assert_eq!(buckets.bucket_count(), 0);
    }

    /// Scenario: another charge to an existing context overflows its account.
    /// Guarantees: the registry reports accounting failure without adding a bucket.
    #[test]
    fn existing_bucket_propagates_accounting_failure() {
        let mut buckets = LocalRetainedBuckets::new(1);
        let ticket = buckets
            .charge(
                1,
                |key: &String| key == "context",
                || "context".into(),
                Some(u64::MAX),
            )
            .expect("initial charge should fit");
        let error = buckets
            .charge(
                1,
                |key| key == "context",
                || panic!("existing context must not create a key"),
                Some(1),
            )
            .expect_err("overflow must be reported");

        assert_eq!(
            error,
            LocalRetainedBucketError::Accounting(LocalAccountingError::Overflow(
                LocalAccountingCounter::RetainedBytes
            ))
        );
        assert_eq!(buckets.bucket_count(), 1);
        assert_eq!(
            buckets.snapshot(1, |key| key == "context"),
            Some(LocalRetainedSnapshot {
                retained_bytes: u64::MAX,
                corruption_count: 1,
                ..LocalRetainedSnapshot::default()
            })
        );
        ticket.complete().expect("initial charge should settle");
    }
}
