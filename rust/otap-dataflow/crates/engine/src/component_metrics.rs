// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Component-oriented metrics for the OTAP engine following the
//! [Pipeline Component Telemetry RFC](https://github.com/open-telemetry/opentelemetry-collector/blob/main/docs/rfcs/component-universal-telemetry.md).
//!
//! This module implements RFC-aligned instrumentation for pipeline components:
//! - `produced.items` - count of items produced by a component
//! - `produced.bytes` - byte size of produced items (beyond RFC scope)
//! - `consumed.items` - count of items consumed by a component, with outcome
//! - `consumed.bytes` - byte size of consumed items (beyond RFC scope)
//!
//! ## Outcomes
//!
//! Per the RFC, consumed items are attributed with an `outcome` that can be:
//! - `success` - items were processed successfully (Ack)
//! - `failure` - items could not be processed (non-permanent Nack)
//! - `refused` - items were refused, typically due to backpressure (permanent Nack)
//!
//! ## Auto-instrumentation
//!
//! The engine automatically tracks these metrics at graph edges when components
//! use the `send_message_subscribed` API. The forward path captures `produced`
//! metrics, while the return path (Ack/Nack delivery) captures `consumed` metrics
//! with the appropriate outcome.
//!
//! ## Instrumented Trait
//!
//! Pipeline data types can implement the [`Instrumented`] trait to provide the
//! item count and optional byte size for automatic metric recording. This allows
//! generic pipeline code to record metrics without knowing the specific PData type.

use otap_df_telemetry::instrument::Counter;
use otap_df_telemetry::metrics::MetricSet;
use otap_df_telemetry_macros::metric_set;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// Trait for pipeline data types that can report their telemetry characteristics.
///
/// Implement this trait for your PData type to enable automatic metric recording.
/// The engine will call these methods when recording `produced.items/bytes` and
/// `consumed.items/bytes` metrics at graph edges.
///
/// # Examples
///
/// ```ignore
/// impl Instrumented for MyPData {
///     fn num_items(&self) -> u64 {
///         self.records.len() as u64
///     }
///     
///     fn num_bytes(&self) -> Option<u64> {
///         Some(self.payload_size as u64)
///     }
/// }
/// ```
pub trait Instrumented {
    /// Returns the number of logical items/records in this data unit.
    ///
    /// For telemetry data, this is typically the number of spans, metrics, or log records.
    fn num_items(&self) -> u64;

    /// Returns the byte size of the payload, if known.
    ///
    /// This is optional and may return `None` if the size is not readily available
    /// or would be too expensive to compute.
    fn num_bytes(&self) -> Option<u64> {
        None
    }
}

/// Outcome values per RFC specification.
pub mod outcome {
    /// Items were processed successfully.
    pub const SUCCESS: &str = "success";
    /// Items could not be processed (transient failure).
    pub const FAILURE: &str = "failure";
    /// Items were refused, typically due to backpressure or permanent error.
    pub const REFUSED: &str = "refused";
}

/// Metrics for items produced by a component (forward path).
///
/// These metrics are recorded when a component successfully sends data downstream.
#[metric_set(name = "component.produced")]
#[derive(Debug, Default, Clone)]
pub struct ProducedMetrics {
    /// Count of items successfully produced (sent downstream).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items successfully produced.
    /// This goes beyond the RFC scope but is available in our codebase.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items produced by a component with success outcome.
///
/// These are recorded when an Ack is received for data the component produced,
/// indicating downstream success.
#[metric_set(name = "component.produced.success")]
#[derive(Debug, Default, Clone)]
pub struct ProducedSuccessMetrics {
    /// Count of items produced with success outcome (Ack received).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items produced with success outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items produced by a component with failure outcome.
///
/// These are recorded when a non-permanent Nack is received for data the component produced.
#[metric_set(name = "component.produced.failure")]
#[derive(Debug, Default, Clone)]
pub struct ProducedFailureMetrics {
    /// Count of items produced with failure outcome (non-permanent Nack).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items produced with failure outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items produced by a component with refused outcome.
///
/// These are recorded when a permanent Nack is received for data the component produced.
#[metric_set(name = "component.produced.refused")]
#[derive(Debug, Default, Clone)]
pub struct ProducedRefusedMetrics {
    /// Count of items produced with refused outcome (permanent Nack).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items produced with refused outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items consumed by a component with successful outcome.
///
/// These are recorded when an Ack is received, indicating downstream success.
#[metric_set(name = "component.consumed.success")]
#[derive(Debug, Default, Clone)]
pub struct ConsumedSuccessMetrics {
    /// Count of items consumed with success outcome (Ack received).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items consumed with success outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items consumed by a component with failure outcome.
///
/// These are recorded when a non-permanent Nack is received.
#[metric_set(name = "component.consumed.failure")]
#[derive(Debug, Default, Clone)]
pub struct ConsumedFailureMetrics {
    /// Count of items consumed with failure outcome (non-permanent Nack).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items consumed with failure outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Metrics for items consumed by a component with refused outcome.
///
/// These are recorded when a permanent Nack is received (backpressure, dropped).
#[metric_set(name = "component.consumed.refused")]
#[derive(Debug, Default, Clone)]
pub struct ConsumedRefusedMetrics {
    /// Count of items consumed with refused outcome (permanent Nack).
    #[metric(name = "items", unit = "{item}")]
    pub items: Counter<u64>,

    /// Byte size of items consumed with refused outcome.
    #[metric(name = "bytes", unit = "{By}")]
    pub bytes: Counter<u64>,
}

/// Aggregated component metrics state for a single node.
///
/// Tracks produced items (forward path) and consumed items by outcome (return path).
/// Both produced and consumed metrics have outcome-based variants per the RFC.
pub struct ComponentMetricsState {
    /// Metrics for successfully produced items (on send).
    produced: MetricSet<ProducedMetrics>,
    /// Metrics for produced items with success outcome (Ack received).
    produced_success: MetricSet<ProducedSuccessMetrics>,
    /// Metrics for produced items with failure outcome (non-permanent Nack).
    produced_failure: MetricSet<ProducedFailureMetrics>,
    /// Metrics for produced items with refused outcome (permanent Nack).
    produced_refused: MetricSet<ProducedRefusedMetrics>,
    /// Metrics for consumed items with success outcome.
    consumed_success: MetricSet<ConsumedSuccessMetrics>,
    /// Metrics for consumed items with failure outcome.
    consumed_failure: MetricSet<ConsumedFailureMetrics>,
    /// Metrics for consumed items with refused outcome.
    consumed_refused: MetricSet<ConsumedRefusedMetrics>,
}

impl std::fmt::Debug for ComponentMetricsState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentMetricsState").finish_non_exhaustive()
    }
}

impl ComponentMetricsState {
    /// Creates a new component metrics state with the given metric sets.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        produced: MetricSet<ProducedMetrics>,
        produced_success: MetricSet<ProducedSuccessMetrics>,
        produced_failure: MetricSet<ProducedFailureMetrics>,
        produced_refused: MetricSet<ProducedRefusedMetrics>,
        consumed_success: MetricSet<ConsumedSuccessMetrics>,
        consumed_failure: MetricSet<ConsumedFailureMetrics>,
        consumed_refused: MetricSet<ConsumedRefusedMetrics>,
    ) -> Self {
        Self {
            produced,
            produced_success,
            produced_failure,
            produced_refused,
            consumed_success,
            consumed_failure,
            consumed_refused,
        }
    }

    /// Records items and bytes produced (sent downstream).
    #[inline]
    pub fn record_produced(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.produced.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.produced.bytes.add(bytes);
        }
    }

    /// Records items and bytes produced with success outcome (Ack received).
    #[inline]
    pub fn record_produced_success(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.produced_success.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.produced_success.bytes.add(bytes);
        }
    }

    /// Records items and bytes produced with failure outcome (non-permanent Nack).
    #[inline]
    pub fn record_produced_failure(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.produced_failure.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.produced_failure.bytes.add(bytes);
        }
    }

    /// Records items and bytes produced with refused outcome (permanent Nack).
    #[inline]
    pub fn record_produced_refused(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.produced_refused.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.produced_refused.bytes.add(bytes);
        }
    }

    /// Records items and bytes consumed with success outcome (Ack).
    #[inline]
    pub fn record_consumed_success(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.consumed_success.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.consumed_success.bytes.add(bytes);
        }
    }

    /// Records items and bytes consumed with failure outcome (non-permanent Nack).
    #[inline]
    pub fn record_consumed_failure(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.consumed_failure.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.consumed_failure.bytes.add(bytes);
        }
    }

    /// Records items and bytes consumed with refused outcome (permanent Nack).
    #[inline]
    pub fn record_consumed_refused(&mut self, num_items: u64, num_bytes: Option<u64>) {
        self.consumed_refused.items.add(num_items);
        if let Some(bytes) = num_bytes {
            self.consumed_refused.bytes.add(bytes);
        }
    }

    /// Reports all component metrics to the metrics reporter.
    #[inline]
    pub fn report(
        &mut self,
        metrics_reporter: &mut otap_df_telemetry::reporter::MetricsReporter,
    ) -> Result<(), otap_df_telemetry::error::Error> {
        metrics_reporter.report(&mut self.produced)?;
        metrics_reporter.report(&mut self.produced_success)?;
        metrics_reporter.report(&mut self.produced_failure)?;
        metrics_reporter.report(&mut self.produced_refused)?;
        metrics_reporter.report(&mut self.consumed_success)?;
        metrics_reporter.report(&mut self.consumed_failure)?;
        metrics_reporter.report(&mut self.consumed_refused)?;
        Ok(())
    }
}

/// Local (non-Send) handle to component metrics for single-threaded pipelines.
pub type LocalComponentMetricsHandle = Rc<RefCell<ComponentMetricsState>>;

/// Shared (Send) handle to component metrics for multi-threaded pipelines.
pub type SharedComponentMetricsHandle = Arc<Mutex<ComponentMetricsState>>;

/// Enum wrapper for either local or shared component metrics handle.
#[derive(Clone, Debug)]
pub enum ComponentMetricsHandle {
    /// Local (non-Send) handle for single-threaded pipelines.
    Local(LocalComponentMetricsHandle),
    /// Shared (Send) handle for multi-threaded pipelines.
    Shared(SharedComponentMetricsHandle),
}

impl ComponentMetricsHandle {
    /// Records items produced (sent downstream).
    #[inline]
    pub fn record_produced(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_produced(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_produced(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items produced with success outcome (Ack received).
    #[inline]
    pub fn record_produced_success(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_produced_success(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_produced_success(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items produced with failure outcome (non-permanent Nack).
    #[inline]
    pub fn record_produced_failure(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_produced_failure(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_produced_failure(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items produced with refused outcome (permanent Nack).
    #[inline]
    pub fn record_produced_refused(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_produced_refused(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_produced_refused(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items consumed with success outcome.
    #[inline]
    pub fn record_consumed_success(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_consumed_success(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_consumed_success(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items consumed with failure outcome.
    #[inline]
    pub fn record_consumed_failure(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_consumed_failure(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_consumed_failure(num_items, num_bytes);
                }
            }
        }
    }

    /// Records items consumed with refused outcome.
    #[inline]
    pub fn record_consumed_refused(&self, num_items: u64, num_bytes: Option<u64>) {
        match self {
            ComponentMetricsHandle::Local(handle) => {
                if let Ok(mut metrics) = handle.try_borrow_mut() {
                    metrics.record_consumed_refused(num_items, num_bytes);
                }
            }
            ComponentMetricsHandle::Shared(handle) => {
                if let Ok(mut metrics) = handle.try_lock() {
                    metrics.record_consumed_refused(num_items, num_bytes);
                }
            }
        }
    }

    /// Reports metrics to the reporter.
    #[inline]
    pub fn report(
        &self,
        metrics_reporter: &mut otap_df_telemetry::reporter::MetricsReporter,
    ) -> Result<(), otap_df_telemetry::error::Error> {
        match self {
            ComponentMetricsHandle::Local(handle) => match handle.try_borrow_mut() {
                Ok(mut metrics) => metrics.report(metrics_reporter),
                Err(_) => Ok(()),
            },
            ComponentMetricsHandle::Shared(handle) => match handle.try_lock() {
                Ok(mut metrics) => metrics.report(metrics_reporter),
                Err(_) => Ok(()),
            },
        }
    }
}

/// Registry for collecting component metrics handles across a pipeline.
#[derive(Default)]
pub struct ComponentMetricsRegistry {
    handles: Vec<ComponentMetricsHandle>,
}

impl ComponentMetricsRegistry {
    /// Registers a component metrics handle.
    pub fn register(&mut self, handle: ComponentMetricsHandle) {
        self.handles.push(handle);
    }

    /// Consumes the registry and returns the collected handles.
    pub fn into_handles(self) -> Vec<ComponentMetricsHandle> {
        self.handles
    }
}

/// Blanket implementation of Instrumented for unit type (for testing).
impl Instrumented for () {
    fn num_items(&self) -> u64 {
        0
    }

    fn num_bytes(&self) -> Option<u64> {
        None
    }
}

/// Blanket implementation of Instrumented for String (for testing).
impl Instrumented for String {
    fn num_items(&self) -> u64 {
        1 // A String represents 1 item for testing purposes
    }

    fn num_bytes(&self) -> Option<u64> {
        Some(self.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_telemetry::registry::TelemetryRegistryHandle;
    use otap_df_telemetry::testing::EmptyAttributes;

    fn create_test_state() -> ComponentMetricsState {
        // Use the registry to properly create registered metric sets
        let registry = TelemetryRegistryHandle::new();

        ComponentMetricsState::new(
            registry.register_metric_set::<ProducedMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ProducedSuccessMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ProducedFailureMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ProducedRefusedMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ConsumedSuccessMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ConsumedFailureMetrics>(EmptyAttributes()),
            registry.register_metric_set::<ConsumedRefusedMetrics>(EmptyAttributes()),
        )
    }

    #[test]
    fn test_record_produced() {
        let mut state = create_test_state();
        state.record_produced(10, Some(1024));
        assert_eq!(state.produced.items.get(), 10);
        assert_eq!(state.produced.bytes.get(), 1024);
    }

    #[test]
    fn test_record_consumed_success() {
        let mut state = create_test_state();
        state.record_consumed_success(5, Some(512));
        assert_eq!(state.consumed_success.items.get(), 5);
        assert_eq!(state.consumed_success.bytes.get(), 512);
    }

    #[test]
    fn test_record_produced_success() {
        let mut state = create_test_state();
        state.record_produced_success(7, Some(700));
        assert_eq!(state.produced_success.items.get(), 7);
        assert_eq!(state.produced_success.bytes.get(), 700);
    }

    #[test]
    fn test_record_produced_failure() {
        let mut state = create_test_state();
        state.record_produced_failure(4, Some(400));
        assert_eq!(state.produced_failure.items.get(), 4);
        assert_eq!(state.produced_failure.bytes.get(), 400);
    }

    #[test]
    fn test_record_produced_refused() {
        let mut state = create_test_state();
        state.record_produced_refused(2, Some(200));
        assert_eq!(state.produced_refused.items.get(), 2);
        assert_eq!(state.produced_refused.bytes.get(), 200);
    }

    #[test]
    fn test_record_consumed_failure() {
        let mut state = create_test_state();
        state.record_consumed_failure(3, Some(256));
        assert_eq!(state.consumed_failure.items.get(), 3);
        assert_eq!(state.consumed_failure.bytes.get(), 256);
    }

    #[test]
    fn test_record_consumed_refused() {
        let mut state = create_test_state();
        state.record_consumed_refused(2, Some(128));
        assert_eq!(state.consumed_refused.items.get(), 2);
        assert_eq!(state.consumed_refused.bytes.get(), 128);
    }

    #[test]
    fn test_local_handle() {
        let state = create_test_state();
        let handle = ComponentMetricsHandle::Local(Rc::new(RefCell::new(state)));

        handle.record_produced(10, Some(1000));
        handle.record_consumed_success(8, Some(800));
        handle.record_consumed_failure(1, Some(100));
        handle.record_consumed_refused(1, Some(100));

        if let ComponentMetricsHandle::Local(rc) = handle {
            let state = rc.borrow();
            assert_eq!(state.produced.items.get(), 10);
            assert_eq!(state.consumed_success.items.get(), 8);
            assert_eq!(state.consumed_failure.items.get(), 1);
            assert_eq!(state.consumed_refused.items.get(), 1);
        }
    }

    #[test]
    fn test_shared_handle() {
        let state = create_test_state();
        let handle = ComponentMetricsHandle::Shared(Arc::new(Mutex::new(state)));

        handle.record_produced(20, Some(2000));
        handle.record_consumed_success(15, Some(1500));

        if let ComponentMetricsHandle::Shared(arc) = handle {
            let state = arc.lock().unwrap();
            assert_eq!(state.produced.items.get(), 20);
            assert_eq!(state.consumed_success.items.get(), 15);
        }
    }

    #[test]
    fn test_registry() {
        let mut registry = ComponentMetricsRegistry::default();
        let state1 = create_test_state();
        let state2 = create_test_state();

        registry.register(ComponentMetricsHandle::Local(Rc::new(RefCell::new(state1))));
        registry.register(ComponentMetricsHandle::Shared(Arc::new(Mutex::new(state2))));

        let handles = registry.into_handles();
        assert_eq!(handles.len(), 2);
    }
}
