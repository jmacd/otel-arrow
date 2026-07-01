// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The durable (quiver-backed) partition-dispatch seam (durable-dispatch Layer B).
//!
//! Durability is an *additive backend* selected per location (durable-dispatch
//! D27): the same partition-dispatch topic runs over the in-memory backend (the
//! default) or a quiver backend (the durable option), with identical
//! dispatch/ownership semantics. The generic engine broker and the controller
//! must stay free of quiver and OTAP specifics, so the durable backend is
//! constructed through this trait, implemented by the payload-owning crate
//! (`crates/otap` implements it for `OtapPdata`).

use std::path::PathBuf;
use std::sync::Arc;

use crate::error::Error;
use crate::topic::backend::TopicState;
use crate::topic::partitioned::Partitioned;
use otap_df_config::TopicName;

/// Behavior when a durable topic's disk budget is exhausted (durable-dispatch
/// Layer B "QoS mapping"; ingest-queue D6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurableRetentionPolicy {
    /// Lossless: apply backpressure to publishers until space is reclaimed.
    Backpressure,
    /// Loss-tolerant: drop the oldest persisted data to admit new data.
    DropOldest,
}

/// Configuration for constructing a durable, quiver-backed partition-dispatch
/// topic. The controller derives this from the topic spec and passes it to
/// [`DurableDispatchPayload::create_durable_partition_dispatch_topic`].
#[derive(Clone, Debug)]
pub struct DurableDispatchConfig {
    /// Base directory under which one per-owner durable store is created
    /// (`{data_dir}/{topic}/owner_{o}`).
    pub data_dir: PathBuf,
    /// The number of partitions `N` (matches the split-by-key node's `N`).
    pub num_partitions: usize,
    /// The number of durable owners `M` the partitions are placed across
    /// (durable-dispatch D24, one quiver per owner). Must be in
    /// `1..=num_partitions`; a static `balanced(N, M)` placement maps each
    /// partition to one owner. `M == N` reproduces the per-partition layout.
    pub num_owners: usize,
    /// Per-partition in-flight delivery capacity.
    pub capacity: usize,
    /// Total disk budget in bytes, shared across the topic's owners.
    pub disk_budget_bytes: u64,
    /// Behavior when the disk budget is exhausted.
    pub retention: DurableRetentionPolicy,
}

/// A payload type that supports a durable, quiver-backed partition-dispatch
/// backend (durable-dispatch Layer B). The generic engine and controller stay
/// free of quiver and OTAP; `crates/otap` implements this for `OtapPdata`.
///
/// The trait is a constructor seam: it lets the controller build the durable
/// `TopicState` for whatever concrete payload type the engine runs, without the
/// controller depending on quiver or the OTAP data model.
pub trait DurableDispatchPayload: Partitioned + Send + Sync + 'static + Sized {
    /// Build a durable partition-dispatch topic state backed by quiver.
    ///
    /// Returns [`Error::DurableDispatchUnsupported`] for payload types that have
    /// no durable backend (for example the planning-only unit payload).
    fn create_durable_partition_dispatch_topic(
        name: TopicName,
        config: DurableDispatchConfig,
    ) -> Result<Arc<dyn TopicState<Self>>, Error>;
}

/// The unit payload has no durable backend. This impl lets the planning-only
/// `Controller::<()>` satisfy the bound; constructing such a topic is rejected.
impl DurableDispatchPayload for () {
    fn create_durable_partition_dispatch_topic(
        _name: TopicName,
        _config: DurableDispatchConfig,
    ) -> Result<Arc<dyn TopicState<Self>>, Error> {
        Err(Error::DurableDispatchUnsupported)
    }
}
