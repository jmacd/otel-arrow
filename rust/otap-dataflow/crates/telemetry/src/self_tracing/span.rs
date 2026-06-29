// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! No-allocation trace context for self-tracing spans.
//!
//! A [`SpanContext`] is a fixed-size `Copy` value that carries the W3C trace
//! identifiers, the trace flags, and the OpenTelemetry probability sampling
//! fields `rv` and `th` described in the
//! [tracestate probability sampling][spec] specification. It never allocates,
//! and arbitrary vendor `tracestate` members are intentionally not retained.
//!
//! [spec]: https://opentelemetry.io/docs/specs/otel/trace/tracestate-probability-sampling/

use std::cell::Cell;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Number of bits of randomness and threshold precision per OTEP 235.
const SAMPLING_BITS: u32 = 56;

/// The number of distinct 56-bit values, which is `2^56`. A threshold equal to
/// this value rejects every span, and the randomness range is `[0, this)`.
const MAX_ADJUSTED_COUNT: u64 = 1 << SAMPLING_BITS;

/// Mask selecting the low 56 bits used for randomness.
const RANDOM_MASK: u64 = MAX_ADJUSTED_COUNT - 1;

/// Maximum number of hexadecimal digits for an `rv` or `th` value, which is
/// `56 / 4`.
const MAX_SAMPLING_HEX: usize = 14;

/// Default number of significant hexadecimal digits when encoding a threshold
/// from a probability.
const DEFAULT_SAMPLING_PRECISION: u32 = 4;

/// Largest probability that is not treated as "always sample". Values above
/// this round to [`Threshold::ALWAYS`].
const MAX_SUPPORTED_PROBABILITY: f64 = 1.0 - (1.0 / ((1u64 << 52) as f64));

/// Smallest probability that is not treated as "never sample". Values below
/// this round to [`Threshold::NEVER`].
const MIN_SUPPORTED_PROBABILITY: f64 = 1.0 / (MAX_ADJUSTED_COUNT as f64);

const HEX: &[u8; 16] = b"0123456789abcdef";

#[inline]
fn hex_digit(nibble: u8) -> u8 {
    HEX[(nibble & 0x0f) as usize]
}

#[inline]
fn push_str<const N: usize>(target: &mut StackStr<N>, value: &str) {
    for byte in value.bytes() {
        target.push(byte);
    }
}

/// A small stack buffer holding an ASCII string.
///
/// Used to format identifiers and sampling values for injection without heap
/// allocation. The capacity `N` is chosen by the caller to fit the longest
/// possible value.
#[derive(Clone, Copy)]
pub struct StackStr<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> StackStr<N> {
    #[inline]
    fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
        }
    }

    #[inline]
    fn push(&mut self, byte: u8) {
        if self.len < N {
            self.buf[self.len] = byte;
            self.len += 1;
        }
    }

    /// Returns the formatted string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.buf[..self.len]).unwrap_or("")
    }
}

impl<const N: usize> std::fmt::Display for StackStr<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A 16-byte W3C trace identifier, stored big-endian as a `u128`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TraceId(pub u128);

impl TraceId {
    /// The invalid all-zero trace identifier.
    pub const INVALID: TraceId = TraceId(0);

    /// Construct from 16 big-endian bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }

    /// Return the 16 big-endian bytes used by the OTLP wire format.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    /// Returns true when the identifier is not the all-zero value.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }

    /// Generate a random trace identifier whose low 56 bits are uniform, as
    /// required for implicit randomness under OTEP 235.
    #[must_use]
    pub fn generate() -> Self {
        let hi = next_random_u64();
        let lo = next_random_u64();
        let value = (u128::from(hi) << 64) | u128::from(lo);
        if value == 0 {
            TraceId(1)
        } else {
            TraceId(value)
        }
    }

    /// Format as 32 lowercase hexadecimal digits.
    #[must_use]
    pub fn to_hex(self) -> StackStr<32> {
        let mut s = StackStr::new();
        let mut i = 0;
        while i < 32 {
            let shift = (31 - i) * 4;
            s.push(hex_digit(((self.0 >> shift) & 0x0f) as u8));
            i += 1;
        }
        s
    }
}

impl std::fmt::Debug for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// An 8-byte W3C span identifier, stored big-endian as a `u64`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SpanId(pub u64);

impl SpanId {
    /// The invalid all-zero span identifier.
    pub const INVALID: SpanId = SpanId(0);

    /// Construct from 8 big-endian bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }

    /// Return the 8 big-endian bytes used by the OTLP wire format.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Returns true when the identifier is not the all-zero value.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }

    /// Generate a random, non-zero span identifier.
    #[must_use]
    pub fn generate() -> Self {
        let value = next_random_u64();
        if value == 0 { SpanId(1) } else { SpanId(value) }
    }

    /// Format as 16 lowercase hexadecimal digits.
    #[must_use]
    pub fn to_hex(self) -> StackStr<16> {
        let mut s = StackStr::new();
        let mut i = 0;
        while i < 16 {
            let shift = (15 - i) * 4;
            s.push(hex_digit(((self.0 >> shift) & 0x0f) as u8));
            i += 1;
        }
        s
    }
}

impl std::fmt::Debug for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// W3C trace flags. Bit 0 is the "sampled" flag, bit 1 is the "random" flag.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TraceFlags(u8);

impl TraceFlags {
    /// The "sampled" flag, bit 0.
    pub const SAMPLED: u8 = 0x01;

    /// The "random" flag, bit 1, indicating the low 56 trace-id bits are
    /// uniformly random.
    pub const RANDOM: u8 = 0x02;

    /// Construct from the raw flag byte.
    #[must_use]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    /// Construct from the two recognized flags.
    #[must_use]
    pub const fn new(sampled: bool, random: bool) -> Self {
        let mut bits = 0;
        if sampled {
            bits |= Self::SAMPLED;
        }
        if random {
            bits |= Self::RANDOM;
        }
        Self(bits)
    }

    /// Returns the raw flag byte.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Returns true when the "sampled" flag is set.
    #[must_use]
    pub const fn is_sampled(self) -> bool {
        self.0 & Self::SAMPLED != 0
    }

    /// Returns true when the "random" flag is set.
    #[must_use]
    pub const fn is_random(self) -> bool {
        self.0 & Self::RANDOM != 0
    }

    /// Format as 2 lowercase hexadecimal digits.
    #[must_use]
    pub fn to_hex(self) -> StackStr<2> {
        let mut s = StackStr::new();
        s.push(hex_digit(self.0 >> 4));
        s.push(hex_digit(self.0));
        s
    }
}

impl std::fmt::Debug for TraceFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// OTEP 235 randomness, the low 56 bits used for the sampling decision.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Randomness(u64);

impl Randomness {
    /// Derive randomness from the low 56 bits of a trace identifier.
    #[must_use]
    pub const fn from_trace_id(trace_id: TraceId) -> Self {
        Self((trace_id.0 as u64) & RANDOM_MASK)
    }

    /// Parse an explicit `rv` value, which must be exactly 14 hexadecimal
    /// digits.
    #[must_use]
    pub fn from_rv_value(rv: &str) -> Option<Self> {
        if rv.len() != MAX_SAMPLING_HEX || !rv.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        u64::from_str_radix(rv, 16).ok().map(Self)
    }

    /// Returns the 56-bit randomness value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Format as exactly 14 lowercase hexadecimal digits, the `rv` encoding.
    #[must_use]
    pub fn to_hex(self) -> StackStr<14> {
        let mut s = StackStr::new();
        let mut i = 0;
        while i < MAX_SAMPLING_HEX {
            let shift = (MAX_SAMPLING_HEX - 1 - i) * 4;
            s.push(hex_digit(((self.0 >> shift) & 0x0f) as u8));
            i += 1;
        }
        s
    }
}

impl std::fmt::Debug for Randomness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// OTEP 235 rejection threshold in the 56-bit space.
///
/// A span is sampled when `threshold <= randomness`. The value `0` samples
/// everything and the value `2^56` rejects everything.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Threshold(u64);

impl Threshold {
    /// Sample every span, equivalent to 100% probability.
    pub const ALWAYS: Threshold = Threshold(0);

    /// Reject every span, equivalent to 0% probability.
    pub const NEVER: Threshold = Threshold(MAX_ADJUSTED_COUNT);

    /// Construct a threshold from a sampling probability in `[0.0, 1.0]` using
    /// the default precision.
    #[must_use]
    pub fn from_probability(fraction: f64) -> Self {
        Self::from_probability_with_precision(fraction, DEFAULT_SAMPLING_PRECISION)
    }

    /// Construct a threshold from a sampling probability with an explicit number
    /// of significant hexadecimal digits.
    #[must_use]
    pub fn from_probability_with_precision(fraction: f64, precision: u32) -> Self {
        if fraction > MAX_SUPPORTED_PROBABILITY {
            return Self::ALWAYS;
        }
        if fraction < MIN_SUPPORTED_PROBABILITY {
            return Self::NEVER;
        }

        // `leading_fs` counts leading hexadecimal `f` digits, raising precision
        // so small probabilities keep significant digits.
        let leading_fs = (fraction.log2() / -4.0).floor() as u32;
        let final_precision = (precision + leading_fs).clamp(1, MAX_SAMPLING_HEX as u32);

        let scaled = (fraction * MAX_ADJUSTED_COUNT as f64).round() as u64;
        let mut threshold = MAX_ADJUSTED_COUNT - scaled;

        let shift = 4 * (MAX_SAMPLING_HEX as u32 - final_precision);
        if shift != 0 {
            let half = 1u64 << (shift - 1);
            threshold += half;
            threshold >>= shift;
            threshold <<= shift;
        }
        Threshold(threshold)
    }

    /// Parse a `th` value of 1 to 14 hexadecimal digits, left-aligned in the
    /// 56-bit space with implicit trailing zeros.
    #[must_use]
    pub fn from_th_value(th: &str) -> Option<Self> {
        if th.is_empty()
            || th.len() > MAX_SAMPLING_HEX
            || !th.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return None;
        }
        let value = u64::from_str_radix(th, 16).ok()?;
        let shift = (MAX_SAMPLING_HEX - th.len()) * 4;
        Some(Threshold(value << shift))
    }

    /// Returns the raw 56-bit threshold value.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// Returns true when a span with the given randomness is sampled.
    #[must_use]
    pub const fn is_sampled(self, randomness: Randomness) -> bool {
        self.0 <= randomness.0
    }

    /// Format as the `th` encoding: up to 14 hexadecimal digits with trailing
    /// zeros trimmed, and a single `0` for [`Threshold::ALWAYS`].
    ///
    /// This is meaningful for thresholds in the range `[ALWAYS, NEVER)`.
    #[must_use]
    pub fn to_hex(self) -> StackStr<14> {
        let mut s = StackStr::new();
        let mut i = 0;
        while i < MAX_SAMPLING_HEX {
            let shift = (MAX_SAMPLING_HEX - 1 - i) * 4;
            s.push(hex_digit(((self.0 >> shift) & 0x0f) as u8));
            i += 1;
        }
        while s.len > 1 && s.buf[s.len - 1] == b'0' {
            s.len -= 1;
        }
        s
    }
}

impl std::fmt::Debug for Threshold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// The OpenTelemetry `ot` sub-state, restricted to the probability sampling
/// fields. All other `tracestate` members are intentionally dropped to keep the
/// context allocation-free.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct OtelTraceState {
    /// Explicit randomness, the `rv` field, when present.
    pub rv: Option<Randomness>,
    /// Rejection threshold, the `th` field, when present.
    pub th: Option<Threshold>,
}

impl std::fmt::Debug for OtelTraceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelTraceState")
            .field("rv", &self.rv)
            .field("th", &self.th)
            .finish()
    }
}

/// Immutable per-span identity and sampling context.
///
/// This is the unit of propagation. It is `Copy`, fixed-size, and never boxed on
/// the hot path.
#[derive(Clone, Copy, Default)]
pub struct SpanContext {
    /// The trace identifier, shared by all spans in a trace.
    pub trace_id: TraceId,
    /// The span identifier, unique within the trace.
    pub span_id: SpanId,
    /// The W3C trace flags.
    pub flags: TraceFlags,
    /// The OpenTelemetry probability sampling sub-state.
    pub ot: OtelTraceState,
}

impl SpanContext {
    /// Returns true when both identifiers are valid.
    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.trace_id.is_valid() && self.span_id.is_valid()
    }

    /// Returns true when the "sampled" flag is set.
    #[must_use]
    pub const fn is_sampled(self) -> bool {
        self.flags.is_sampled()
    }

    /// Format the W3C `traceparent` header value, always 55 ASCII characters.
    #[must_use]
    pub fn traceparent(self) -> StackStr<55> {
        let mut s = StackStr::new();
        s.push(b'0');
        s.push(b'0');
        s.push(b'-');
        push_str(&mut s, self.trace_id.to_hex().as_str());
        s.push(b'-');
        push_str(&mut s, self.span_id.to_hex().as_str());
        s.push(b'-');
        push_str(&mut s, self.flags.to_hex().as_str());
        s
    }

    /// Format the OpenTelemetry `tracestate` member carrying the probability
    /// sampling fields, or `None` when neither `th` nor `rv` is present.
    ///
    /// Only the OpenTelemetry `ot` member is produced. Other vendor members are
    /// not retained by this context.
    #[must_use]
    pub fn tracestate(self) -> Option<StackStr<40>> {
        if self.ot.th.is_none() && self.ot.rv.is_none() {
            return None;
        }
        let mut s = StackStr::new();
        push_str(&mut s, "ot=");
        let mut wrote_field = false;
        if let Some(th) = self.ot.th {
            push_str(&mut s, "th:");
            push_str(&mut s, th.to_hex().as_str());
            wrote_field = true;
        }
        if let Some(rv) = self.ot.rv {
            if wrote_field {
                s.push(b';');
            }
            push_str(&mut s, "rv:");
            push_str(&mut s, rv.to_hex().as_str());
        }
        Some(s)
    }
}

impl std::fmt::Debug for SpanContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpanContext")
            .field("trace_id", &self.trace_id)
            .field("span_id", &self.span_id)
            .field("flags", &self.flags)
            .field("ot", &self.ot)
            .finish()
    }
}

thread_local! {
    static RNG_STATE: Cell<u64> = Cell::new(seed_rng());
}

/// Seed a per-thread generator from a process-global counter and the wall
/// clock, hashed through a randomly keyed hasher so threads do not share a
/// sequence.
fn seed_rng() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(counter);
    hasher.write_u64(nanos);
    let seed = hasher.finish();
    if seed == 0 {
        0x9E37_79B9_7F4A_7C15
    } else {
        seed
    }
}

/// Draw the next pseudo-random `u64` from the per-thread generator using
/// xorshift64*.
fn next_random_u64() -> u64 {
    RNG_STATE.with(|cell| {
        let mut x = cell.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        cell.set(x);
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_hex_roundtrip() {
        let id = TraceId::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        assert_eq!(id.to_hex().as_str(), "00112233445566778899aabbccddeeff");
        assert_eq!(TraceId::from_bytes(id.to_bytes()), id);
    }

    #[test]
    fn span_id_hex() {
        let id = SpanId(0x0102_0304_0506_0708);
        assert_eq!(id.to_hex().as_str(), "0102030405060708");
    }

    #[test]
    fn trace_flags() {
        let f = TraceFlags::new(true, true);
        assert!(f.is_sampled() && f.is_random());
        assert_eq!(f.to_hex().as_str(), "03");
        assert_eq!(TraceFlags::new(true, false).to_hex().as_str(), "01");
        assert_eq!(TraceFlags::default().to_hex().as_str(), "00");
    }

    #[test]
    fn randomness_from_trace_id_low_56_bits() {
        let id = TraceId(0xffff_ffff_ffff_ffff_00ab_cdef_0123_4567);
        let r = Randomness::from_trace_id(id);
        assert_eq!(r.value(), 0x00ab_cdef_0123_4567 & RANDOM_MASK);
        assert_eq!(r.to_hex().as_str().len(), 14);
    }

    #[test]
    fn randomness_rv_roundtrip() {
        let r = Randomness::from_rv_value("0123456789abcd").expect("valid rv");
        assert_eq!(r.to_hex().as_str(), "0123456789abcd");
        assert!(Randomness::from_rv_value("123").is_none());
        assert!(Randomness::from_rv_value("0123456789abcg").is_none());
    }

    #[test]
    fn threshold_always_never() {
        assert_eq!(Threshold::ALWAYS.to_hex().as_str(), "0");
        assert!(Threshold::ALWAYS.is_sampled(Randomness(0)));
        assert!(!Threshold::NEVER.is_sampled(Randomness(RANDOM_MASK)));
    }

    #[test]
    fn threshold_th_roundtrip() {
        // "c" left-aligned means 0xc followed by 13 zero nibbles.
        let t = Threshold::from_th_value("c").expect("valid th");
        assert_eq!(t.to_hex().as_str(), "c");
        let t2 = Threshold::from_th_value("08").expect("valid th");
        assert_eq!(t2.to_hex().as_str(), "08");
        assert!(Threshold::from_th_value("").is_none());
        assert!(Threshold::from_th_value("123456789abcdef").is_none());
    }

    #[test]
    fn threshold_from_probability_half() {
        // 50% sampling: threshold is the midpoint of the 56-bit space.
        let t = Threshold::from_probability(0.5);
        assert_eq!(t.to_hex().as_str(), "8");
        // A randomness at or above the threshold samples; below does not.
        assert!(t.is_sampled(Randomness(MAX_ADJUSTED_COUNT / 2)));
        assert!(!t.is_sampled(Randomness(MAX_ADJUSTED_COUNT / 2 - 1)));
    }

    #[test]
    fn threshold_probability_extremes() {
        assert_eq!(Threshold::from_probability(1.0), Threshold::ALWAYS);
        assert_eq!(Threshold::from_probability(0.0), Threshold::NEVER);
    }

    #[test]
    fn generated_ids_are_valid_and_distinct() {
        let a = TraceId::generate();
        let b = TraceId::generate();
        assert!(a.is_valid() && b.is_valid());
        assert_ne!(a, b);
        let s = SpanId::generate();
        assert!(s.is_valid());
    }

    #[test]
    fn span_context_default_is_invalid() {
        assert!(!SpanContext::default().is_valid());
    }
}
