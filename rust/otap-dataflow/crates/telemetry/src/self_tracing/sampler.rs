// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pluggable sampler interface for self-tracing spans.
//!
//! This is a dependency-free port of the composable sampler design prototyped
//! in [jmacd/rust-sampler](https://github.com/jmacd/rust-sampler) for OTEP 235
//! and OTEP 4321. It does not depend on the OpenTelemetry SDK, because the
//! internal telemetry path has its own manual OTLP encoder and its own
//! no-allocation [`SpanContext`].
//!
//! A [`Sampler`] returns a [`SamplingIntent`] given [`SamplingParameters`]. The
//! fixed [`evaluate`] driver performs the parts that are not pluggable: it
//! derives randomness, reconciles the parent threshold against the parent
//! sampled flag, invokes the sampler, and builds the child [`SpanContext`].

use super::span::{
    OtelTraceState, Randomness, SpanContext, SpanId, Threshold, TraceFlags, TraceId,
};

/// The OpenTelemetry span kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum SpanKind {
    /// An internal operation, the default for engine spans.
    #[default]
    Internal,
    /// A server-side request handler.
    Server,
    /// A client-side request.
    Client,
    /// A producer of asynchronous work.
    Producer,
    /// A consumer of asynchronous work.
    Consumer,
}

/// The result of a sampler decision for one span.
#[derive(Clone, Copy, Debug)]
pub struct SamplingIntent {
    /// The rejection threshold to apply, or `None` to drop the span.
    pub threshold: Option<Threshold>,
    /// Whether the threshold may be propagated to descendants as a consistent
    /// probability.
    pub threshold_reliable: bool,
}

/// Inputs to a sampler decision.
#[derive(Clone, Copy, Debug)]
pub struct SamplingParameters<'a> {
    /// The parent span context, or `None` for a root span.
    pub parent: Option<SpanContext>,
    /// The parent threshold after reconciliation, or `None` when it does not
    /// propagate.
    pub parent_threshold: Option<Threshold>,
    /// Whether the parent threshold was found reliable.
    pub parent_threshold_reliable: bool,
    /// The trace identifier of the span being sampled.
    pub trace_id: TraceId,
    /// The randomness used for the decision.
    pub randomness: Randomness,
    /// The span name.
    pub name: &'a str,
    /// The span-start callsite identity, the hash of the span name. A
    /// span-start sampler keys its threshold table on this value.
    pub start_callsite: u64,
    /// The span kind.
    pub kind: SpanKind,
}

/// A pluggable sampler.
pub trait Sampler: Send + Sync + 'static {
    /// Return the sampling intent for the given parameters.
    fn sampling_intent(&self, params: &SamplingParameters<'_>) -> SamplingIntent;
}

impl Sampler for Box<dyn Sampler> {
    fn sampling_intent(&self, params: &SamplingParameters<'_>) -> SamplingIntent {
        (**self).sampling_intent(params)
    }
}

/// A predicate used by [`ComposableSampler::RuleBased`] to select a delegate.
pub trait Predicate: Send + Sync + 'static {
    /// Return true when this predicate matches the given parameters.
    fn matches(&self, params: &SamplingParameters<'_>) -> bool;
}

/// A predicate that matches every span.
#[derive(Clone, Copy, Debug)]
pub struct Always;

impl Predicate for Always {
    fn matches(&self, _params: &SamplingParameters<'_>) -> bool {
        true
    }
}

/// A predicate that matches only root spans, those with no parent.
#[derive(Clone, Copy, Debug)]
pub struct IsRoot;

impl Predicate for IsRoot {
    fn matches(&self, params: &SamplingParameters<'_>) -> bool {
        params.parent.is_none()
    }
}

/// The built-in composable samplers.
///
/// These mirror the OTEP 4321 composable samplers, restricted to the variants
/// that do not require sampler-supplied attributes.
pub enum ComposableSampler {
    /// Always record, with a reliable 100% threshold.
    AlwaysOn,
    /// Never record.
    AlwaysOff,
    /// Record with a fixed threshold derived from a probability.
    TraceIdRatio(Threshold),
    /// Honor the parent threshold when there is a parent, otherwise delegate to
    /// a root sampler.
    ParentThreshold(Box<dyn Sampler>),
    /// Evaluate rules in order, delegating to the first matching sampler.
    RuleBased(Vec<(Box<dyn Predicate>, Box<dyn Sampler>)>),
}

impl Sampler for ComposableSampler {
    fn sampling_intent(&self, params: &SamplingParameters<'_>) -> SamplingIntent {
        match self {
            ComposableSampler::AlwaysOn => SamplingIntent {
                threshold: Some(Threshold::ALWAYS),
                threshold_reliable: true,
            },
            ComposableSampler::AlwaysOff => SamplingIntent {
                threshold: None,
                threshold_reliable: false,
            },
            ComposableSampler::TraceIdRatio(threshold) => SamplingIntent {
                threshold: Some(*threshold),
                threshold_reliable: true,
            },
            ComposableSampler::ParentThreshold(root) => {
                if params.parent.is_none() {
                    root.sampling_intent(params)
                } else {
                    SamplingIntent {
                        threshold: params.parent_threshold,
                        threshold_reliable: params.parent_threshold_reliable,
                    }
                }
            }
            ComposableSampler::RuleBased(rules) => {
                for (predicate, sampler) in rules {
                    if predicate.matches(params) {
                        return sampler.sampling_intent(params);
                    }
                }
                SamplingIntent {
                    threshold: None,
                    threshold_reliable: false,
                }
            }
        }
    }
}

/// The outcome of [`evaluate`]: whether the span records, and the child context
/// to propagate.
#[derive(Clone, Copy, Debug)]
pub struct SamplingDecision {
    /// Whether the span should emit START and END events.
    pub sampled: bool,
    /// The child span context to store and propagate.
    pub context: SpanContext,
}

/// Run the fixed sampling driver for a new span.
///
/// The driver derives randomness, reconciles the parent threshold against the
/// parent sampled flag, invokes the pluggable `sampler`, decides sampled when
/// `threshold <= randomness`, and assembles the child [`SpanContext`].
///
/// `start_callsite` is the span-start callsite identity. It keys the span-start
/// sampler's threshold table and rides on every in-span record through the
/// child context. The caller chooses how to derive it, for example from the
/// span name with
/// [`span_start_identity`](super::sampling::callsite::span_start_identity) or
/// from the Tokio `tracing` callsite with
/// [`callsite_identity`](super::sampling::callsite::callsite_identity).
#[must_use]
pub fn evaluate(
    sampler: &dyn Sampler,
    parent: Option<SpanContext>,
    trace_id: TraceId,
    span_id: SpanId,
    name: &str,
    start_callsite: u64,
    kind: SpanKind,
) -> SamplingDecision {
    // Randomness is a trace-level property: inherit an explicit rv, otherwise
    // derive it from the trace id.
    let randomness = parent
        .and_then(|p| p.ot.rv)
        .unwrap_or_else(|| Randomness::from_trace_id(trace_id));

    let parsed_parent_threshold = parent.and_then(|p| p.ot.th);
    let parent_sampled = parent.is_some_and(SpanContext::is_sampled);

    // A parent threshold propagates only when it agrees with the parent flag.
    let mut parent_threshold = parsed_parent_threshold;
    let mut parent_threshold_reliable = false;
    if let Some(pt) = parent_threshold {
        if pt.is_sampled(randomness) && parent_sampled {
            parent_threshold_reliable = true;
        } else {
            parent_threshold = None;
        }
    }

    let params = SamplingParameters {
        parent,
        parent_threshold,
        parent_threshold_reliable,
        trace_id,
        randomness,
        name,
        start_callsite,
        kind,
    };
    let intent = sampler.sampling_intent(&params);
    let sampled = intent.threshold.is_some_and(|th| th.is_sampled(randomness));

    // Propagate the threshold only when the sampler marked it reliable.
    let child_th = if intent.threshold_reliable {
        intent.threshold
    } else {
        None
    };

    // The random flag is a trace-level property: inherit it, and set it for a
    // root span because generated trace ids have uniform low bits.
    let random = parent.is_none_or(|p| p.flags.is_random());
    let flags = TraceFlags::new(sampled, random);

    // Preserve an explicit rv when the trace carried one, otherwise leave it
    // implicit via the random flag.
    let rv = parent.and_then(|p| p.ot.rv);

    let context = SpanContext {
        trace_id,
        span_id,
        flags,
        ot: OtelTraceState { rv, th: child_th },
        // The local sampling decision routes in-span logs onto the span-log
        // path. A child of a locally-sampled parent is itself in-span, so it
        // inherits the bit when the sampler keeps it by inheritance.
        locally_sampled: sampled,
        start_callsite,
    };
    SamplingDecision { sampled, context }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(sampler: &dyn Sampler) -> SamplingDecision {
        evaluate(
            sampler,
            None,
            TraceId(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            SpanId(0x1),
            "root",
            super::super::sampling::callsite::span_start_identity("root"),
            SpanKind::Internal,
        )
    }

    #[test]
    fn always_on_samples_and_propagates_threshold() {
        let d = root(&ComposableSampler::AlwaysOn);
        assert!(d.sampled);
        assert!(d.context.flags.is_sampled());
        assert!(d.context.flags.is_random());
        assert_eq!(d.context.ot.th, Some(Threshold::ALWAYS));
    }

    #[test]
    fn always_off_drops() {
        let d = root(&ComposableSampler::AlwaysOff);
        assert!(!d.sampled);
        assert!(!d.context.flags.is_sampled());
        assert_eq!(d.context.ot.th, None);
    }

    #[test]
    fn trace_id_ratio_decides_by_randomness() {
        // A trace id whose low 56 bits are all ones has maximum randomness, so
        // any threshold below NEVER samples it.
        let d = evaluate(
            &ComposableSampler::TraceIdRatio(Threshold::from_probability(0.5)),
            None,
            TraceId(u128::MAX),
            SpanId(1),
            "s",
            0,
            SpanKind::Internal,
        );
        assert!(d.sampled);

        // A trace id whose low 56 bits are zero has minimum randomness, so a 50%
        // threshold does not sample it.
        let d = evaluate(
            &ComposableSampler::TraceIdRatio(Threshold::from_probability(0.5)),
            None,
            TraceId(0),
            SpanId(1),
            "s",
            0,
            SpanKind::Internal,
        );
        assert!(!d.sampled);
    }

    #[test]
    fn parent_threshold_honored_for_child() {
        // A sampled parent carrying a reliable 100% threshold yields a sampled
        // child under ParentThreshold.
        let parent = root(&ComposableSampler::AlwaysOn).context;
        let sampler = ComposableSampler::ParentThreshold(Box::new(ComposableSampler::AlwaysOff));
        let d = evaluate(
            &sampler,
            Some(parent),
            parent.trace_id,
            SpanId(2),
            "child",
            0,
            SpanKind::Internal,
        );
        assert!(d.sampled);
    }

    #[test]
    fn parent_threshold_root_uses_delegate() {
        // With no parent, ParentThreshold delegates to its root sampler.
        let sampler = ComposableSampler::ParentThreshold(Box::new(ComposableSampler::AlwaysOff));
        let d = root(&sampler);
        assert!(!d.sampled);
    }

    #[test]
    fn rule_based_selects_first_match() {
        let rules: Vec<(Box<dyn Predicate>, Box<dyn Sampler>)> = vec![
            (Box::new(IsRoot), Box::new(ComposableSampler::AlwaysOn)),
            (Box::new(Always), Box::new(ComposableSampler::AlwaysOff)),
        ];
        let d = root(&ComposableSampler::RuleBased(rules));
        assert!(d.sampled);
    }
}
