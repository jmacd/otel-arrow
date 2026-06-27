// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The [`Partitioned`] routing seam for partition-dispatch topics.
//!
//! The topic broker is generic over its payload type and must not parse
//! payload-specific keys (durable-dispatch design D25). A partition-dispatch
//! topic therefore reads only an integer partition tag through this trait, which
//! the crate that owns the payload type implements -- for OTAP, `crates/otap`
//! implements it for `OtapPdata` by reading the partition that a split-by-key
//! (Layer A) node stamped on the request context. That crate is the single place
//! where the context is "picked apart", and it is the same seam where tenant
//! descriptors will surface later.

/// A payload that can report the partition it was routed to by a split-by-key
/// node, for partition-dispatch delivery (Layer C).
pub trait Partitioned {
    /// The partition index assigned to this message, or `None` if it was not
    /// tagged (in which case a partition-dispatch topic has nowhere to route it
    /// and drops it).
    fn partition(&self) -> Option<u32>;
}

/// The unit payload carries no partition. This lets the controller's
/// planning-only `Controller::<()>` paths satisfy the `Partitioned` bound that
/// partition-dispatch topic creation requires, without any real payload.
impl Partitioned for () {
    fn partition(&self) -> Option<u32> {
        None
    }
}
