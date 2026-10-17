// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Structured AnyValue event fields using a downcast to `&(dyn
//! std::error::Error + 'static)`.
//!
//! A [`StructBox`] owns a [`StructuredValue`] and exposes it through
//! [`StructBox::as_struct`]; the direct field encoder
//! recognizes it and writes an AnyValue map/array instead of text.
//!
//! Callers, to avoid allocations near logging callsites, store a
//! `StructBox<T>` instead of a `Box<T>` and use `as_struct()` inside
//! logging statements. Use [`StructArc`] instead when shared ownership
//! is needed; its logging interface is the same.
//!
//! ```
//! use otel_arrow_dfe_telemetry::otel_info;
//! use otel_arrow_dfe_telemetry::self_tracing::{StructBox, StructuredValue, ValueWriter};
//!
//! struct State { x: i64, y: i64 }
//!
//! impl StructuredValue for State {
//!     fn write(&self, w: &mut dyn ValueWriter) {
//!         w.map(|m| {
//!             m.entry("x", |v| v.write_i64(self.x));
//!             m.entry("y", |v| v.write_i64(self.y));
//!         });
//!     }
//! }
//! struct Component {
//!     state: StructBox<State>,
//! }
//!
//! impl Component {
//!     fn report(&self) {
//!         otel_info!("complex.state", state = self.state.as_struct());
//!     }
//! }
//! # let component = Component { state: StructBox::new(State { x: 1, y: 2 }) };
//! # component.report();
//! ```
//!
//! Encoded values are limited to [`MAX_DEPTH`] levels of nesting.
//! When depth or size limits cause data loss this is counted once in
//! the record's `dropped_attributes_count`.

use std::any::Any;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

/// Maximum nesting of maps and arrays; deeper values are dropped.
pub const MAX_DEPTH: usize = 16;

/// A value that can describe itself as an OTLP AnyValue.
pub trait StructuredValue: Any + Send + Sync {
    /// Writes exactly one value to `w`.
    fn write(&self, w: &mut dyn ValueWriter);
}

/// Receives one AnyValue. Writing nothing is an empty value.
pub trait ValueWriter {
    /// Writes a string, maybe truncated.
    fn write_str(&mut self, value: &str);
    /// Writes the `Debug` rendering of `value` as a string, formatted in
    /// place and maybe truncated.
    fn write_debug(&mut self, value: &dyn fmt::Debug);
    /// Writes a signed integer.
    fn write_i64(&mut self, value: i64);
    /// Writes a floating point number.
    fn write_f64(&mut self, value: f64);
    /// Writes a boolean.
    fn write_bool(&mut self, value: bool);
    /// Writes a map; `f` adds the entries.
    fn write_map(&mut self, f: &mut dyn FnMut(&mut dyn MapWriter));
    /// Writes an array; `f` adds the elements.
    fn write_array(&mut self, f: &mut dyn FnMut(&mut dyn ArrayWriter));
}

/// Receives the entries of a map.
pub trait MapWriter {
    /// Adds one entry; `f` writes its value. An entry that does not fit
    /// is dropped whole.
    fn write_entry(&mut self, key: &str, f: &mut dyn FnMut(&mut dyn ValueWriter));
}

/// Receives the elements of an array.
pub trait ArrayWriter {
    /// Adds one element; `f` writes its value. Once an element does not
    /// fit, it and all later elements are dropped.
    fn write_element(&mut self, f: &mut dyn FnMut(&mut dyn ValueWriter));
}

impl dyn ValueWriter + '_ {
    /// Writes a map, with `f` adding the entries.
    pub fn map(&mut self, mut f: impl FnMut(&mut dyn MapWriter)) {
        self.write_map(&mut f);
    }

    /// Writes an array, with `f` adding the elements.
    pub fn array(&mut self, mut f: impl FnMut(&mut dyn ArrayWriter)) {
        self.write_array(&mut f);
    }
}

impl dyn MapWriter + '_ {
    /// Adds an entry, with `f` writing its value.
    pub fn entry(&mut self, key: &str, mut f: impl FnMut(&mut dyn ValueWriter)) {
        self.write_entry(key, &mut f);
    }
}

impl dyn ArrayWriter + '_ {
    /// Adds an element, with `f` writing its value.
    pub fn element(&mut self, mut f: impl FnMut(&mut dyn ValueWriter)) {
        self.write_element(&mut f);
    }
}

/// A typed, shared value that can be borrowed as a structured tracing field.
///
/// Like [`Arc`], cloning shares the allocation, dereferencing borrows `T`, and
/// [`get_mut`](Self::get_mut) requires unique ownership, including no weak
/// references. The private carrier erases `T` for the encoder; constructors
/// preserve its identity for checked downcasts during typed access.
pub struct StructArc<T: StructuredValue> {
    carrier: StructCarrier,
    marker: PhantomData<T>,
}

impl<T: StructuredValue> StructArc<T> {
    /// Allocates shared storage for a value that will be logged repeatedly.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self::from(Arc::new(value))
    }

    /// Borrows the stored carrier for a logging field.
    ///
    /// Pass directly as a field value, without `?` or `%`.
    #[must_use]
    pub fn as_struct(&self) -> &(dyn Error + 'static) {
        &self.carrier
    }

    /// Returns mutable access only when there are no other strong or weak
    /// references, matching [`Arc::get_mut`].
    pub fn get_mut(this: &mut Self) -> Option<&mut T> {
        let value: &mut dyn Any = this.carrier.value_mut()?;
        Some(
            value
                .downcast_mut::<T>()
                .expect("StructArc carrier must contain T"),
        )
    }
}

impl<T: StructuredValue> From<Arc<T>> for StructArc<T> {
    fn from(value: Arc<T>) -> Self {
        Self {
            carrier: StructCarrier::Shared(value),
            marker: PhantomData,
        }
    }
}

impl<T: StructuredValue> Clone for StructArc<T> {
    fn clone(&self) -> Self {
        let StructCarrier::Shared(value) = &self.carrier else {
            unreachable!("StructArc must contain shared storage");
        };
        Self {
            carrier: StructCarrier::Shared(Arc::clone(value)),
            marker: PhantomData,
        }
    }
}

impl<T: StructuredValue> Deref for StructArc<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let value: &dyn Any = self.carrier.value();
        value
            .downcast_ref::<T>()
            .expect("StructArc carrier must contain T")
    }
}

impl<T: StructuredValue + fmt::Debug> fmt::Debug for StructArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// A typed, exclusively owned value that can be borrowed as a structured field.
///
/// Like [`Box`], dereferencing borrows `T` and mutable dereferencing provides
/// exclusive access. Logging borrows the stored carrier without allocating.
pub struct StructBox<T: StructuredValue> {
    carrier: StructCarrier,
    marker: PhantomData<T>,
}

impl<T: StructuredValue> StructBox<T> {
    /// Allocates exclusive storage for a value that will be logged repeatedly.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self::from(Box::new(value))
    }

    /// Borrows the stored carrier for a logging field.
    ///
    /// Pass directly as a field value, without `?` or `%`.
    #[must_use]
    pub fn as_struct(&self) -> &(dyn Error + 'static) {
        &self.carrier
    }
}

impl<T: StructuredValue> From<Box<T>> for StructBox<T> {
    fn from(value: Box<T>) -> Self {
        Self {
            carrier: StructCarrier::Owned(value),
            marker: PhantomData,
        }
    }
}

impl<T: StructuredValue> Deref for StructBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        let value: &dyn Any = self.carrier.value();
        value
            .downcast_ref::<T>()
            .expect("StructBox carrier must contain T")
    }
}

impl<T: StructuredValue> DerefMut for StructBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let value: &mut dyn Any = self
            .carrier
            .value_mut()
            .expect("StructBox must exclusively own its value");
        value
            .downcast_mut::<T>()
            .expect("StructBox carrier must contain T")
    }
}

impl<T: StructuredValue + fmt::Debug> fmt::Debug for StructBox<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// One downcast target for structured fields, regardless of ownership kind.
pub(super) enum StructCarrier {
    Shared(Arc<dyn StructuredValue>),
    Owned(Box<dyn StructuredValue>),
}

impl StructCarrier {
    pub(super) fn value(&self) -> &dyn StructuredValue {
        match self {
            Self::Shared(value) => value.as_ref(),
            Self::Owned(value) => value.as_ref(),
        }
    }

    fn value_mut(&mut self) -> Option<&mut dyn StructuredValue> {
        match self {
            Self::Shared(value) => Arc::get_mut(value),
            Self::Owned(value) => Some(value.as_mut()),
        }
    }
}

// Subscribers that do not understand structured fields see a placeholder.
impl fmt::Debug for StructCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<structured>")
    }
}

impl fmt::Display for StructCarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl Error for StructCarrier {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct State {
        count: i64,
    }

    impl StructuredValue for State {
        fn write(&self, w: &mut dyn ValueWriter) {
            w.write_i64(self.count);
        }
    }

    /// Scenario: a non-Clone value is accessed and shared through typed handles.
    /// Guarantees: dereferencing retains its type, cloning shares storage, and mutation requires a sole owner.
    #[test]
    fn typed_access_and_unique_mutation() {
        let mut state = StructArc::new(State { count: 1 });
        assert_eq!(state.count, 1);
        StructArc::get_mut(&mut state).expect("sole owner").count = 2;
        let other = state.clone();
        assert!(std::ptr::eq(&*state, &*other));
        assert_eq!(other.count, 2);
        assert!(StructArc::get_mut(&mut state).is_none());
        drop(other);
        StructArc::get_mut(&mut state)
            .expect("sole owner again")
            .count = 3;
        assert_eq!(state.count, 3);
    }

    /// Scenario: an existing Arc with other strong and weak references becomes a structured handle.
    /// Guarantees: conversion reuses storage and get_mut preserves Arc's strong and weak ownership rules.
    #[test]
    fn existing_arc_ownership_is_preserved() {
        let original = Arc::new(State { count: 7 });
        let weak = Arc::downgrade(&original);
        let mut state = StructArc::from(Arc::clone(&original));
        assert!(std::ptr::eq(&*state, &*original));
        assert!(StructArc::get_mut(&mut state).is_none());
        drop(original);
        assert!(StructArc::get_mut(&mut state).is_none());
        drop(weak);
        assert_eq!(StructArc::get_mut(&mut state).expect("sole owner").count, 7);
    }

    /// Scenario: a structured field borrows the stored erased carrier.
    /// Guarantees: field creation neither clones the Arc nor prevents unique mutation after the borrow ends.
    #[test]
    fn field_borrows_without_cloning() {
        let original = Arc::new(State { count: 11 });
        let weak = Arc::downgrade(&original);
        let mut state = StructArc::from(original);
        let field = state.as_struct();
        assert_eq!(weak.strong_count(), 1);
        assert!(std::ptr::eq(
            field
                .downcast_ref::<StructCarrier>()
                .expect("known carrier"),
            &state.carrier,
        ));
        drop(weak);
        assert!(StructArc::get_mut(&mut state).is_some());
    }

    /// Scenario: a non-Clone value is exclusively owned by a structured box.
    /// Guarantees: typed access and mutation work, and logging borrows the existing carrier.
    #[test]
    fn boxed_value_supports_typed_mutation_and_borrowing() {
        let mut state = StructBox::new(State { count: 1 });
        state.count = 2;
        assert_eq!(state.count, 2);
        let field = state.as_struct();
        assert!(std::ptr::eq(
            field
                .downcast_ref::<StructCarrier>()
                .expect("known carrier"),
            &state.carrier,
        ));
        state.count = 3;
        assert_eq!(state.count, 3);
    }

    /// Scenario: an existing Box is converted to a structured handle.
    /// Guarantees: conversion reuses the allocation and preserves mutable typed access.
    #[test]
    fn existing_box_storage_is_preserved() {
        let original = Box::new(State { count: 7 });
        let address = std::ptr::from_ref(&*original);
        let mut state = StructBox::from(original);
        assert_eq!(std::ptr::from_ref(&*state), address);
        state.count = 8;
        assert_eq!(state.count, 8);
    }
}
