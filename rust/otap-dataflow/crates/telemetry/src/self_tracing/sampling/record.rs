// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The retained-record machinery shared by the single-stream processor and the
//! all-CPU aggregator.
//!
//! Both stages retain a record by reference count so the criteria can share
//! ownership, merge the per-criterion annotations by allocation identity, and
//! flush each record once carrying the three adjusted counts. This module owns
//! that common shape so the two stages produce byte-identical output.

use std::collections::HashMap;
use std::rc::Rc;

use super::super::span::{SpanContext, Threshold};

/// One record leaving a window flush, annotated with the three adjusted counts.
/// A value of exactly zero means the record is present but not a member of that
/// population.
pub struct FlushedRecord<R> {
    /// The emitted payload.
    pub payload: R,
    /// Criterion one: the global-independent log adjusted count.
    pub logs_adjusted_count: f64,
    /// The span adjusted count for a record in a locally-sampled span.
    pub traces_adjusted_count: f64,
    /// Criterion two: the in-span log adjusted count.
    pub span_logs_adjusted_count: f64,
}

/// A retained record held behind a reference count so the two criteria can
/// share ownership. The payload is cloned out once at flush.
pub(crate) struct Retained<R> {
    pub(crate) payload: R,
    pub(crate) trace: Option<SpanContext>,
}

/// The adjusted span count carried by a span context, derived from its
/// consistent-probability threshold. A context without a threshold counts as a
/// single span.
#[must_use]
pub(crate) fn trace_adjusted_count(ctx: &SpanContext) -> f64 {
    ctx.ot.th.map_or(1.0, Threshold::adjusted_count)
}

/// The accumulating annotation for one retained record during a flush.
pub(crate) struct Annotated<R> {
    record: Rc<Retained<R>>,
    logs: f64,
    span_logs: f64,
}

impl<R: Clone> Annotated<R> {
    /// Get or create the annotation slot for a record, keyed by allocation
    /// identity so a record kept by both criteria is annotated once.
    pub(crate) fn slot<'a>(
        map: &'a mut HashMap<usize, Annotated<R>>,
        record: &Rc<Retained<R>>,
    ) -> &'a mut Annotated<R> {
        let key = Rc::as_ptr(record) as usize;
        map.entry(key).or_insert_with(|| Annotated {
            record: Rc::clone(record),
            logs: 0.0,
            span_logs: 0.0,
        })
    }

    /// Set the criterion-one global-independent log adjusted count.
    pub(crate) fn set_logs(&mut self, logs: f64) {
        self.logs = logs;
    }

    /// Set the criterion-two in-span log adjusted count.
    pub(crate) fn set_span_logs(&mut self, span_logs: f64) {
        self.span_logs = span_logs;
    }

    /// Flush the annotation into a [`FlushedRecord`] with all three counts. The
    /// span count rides on every record in a locally-sampled span; a record
    /// outside any sampled span carries an explicit zero.
    pub(crate) fn into_flushed(self) -> FlushedRecord<R> {
        let traces = match self.record.trace {
            Some(ctx) if ctx.locally_sampled => trace_adjusted_count(&ctx),
            _ => 0.0,
        };
        FlushedRecord {
            payload: self.record.payload.clone(),
            logs_adjusted_count: self.logs,
            traces_adjusted_count: traces,
            span_logs_adjusted_count: self.span_logs,
        }
    }
}
