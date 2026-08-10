// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Where an extractor reads a value from.
//!
//! An extractor is Envoy's rate-limit action: given a request it produces one
//! value for one key, or it fails and its whole token fails with it.
//!
//! The source kinds are separate types rather than one string-keyed lookup
//! because the trust rules differ. A transport header is whatever the sender
//! typed. An authorized claim can only reach the compiler through
//! [`ContextEncoder::offer_authorized_claim`](crate::ContextEncoder::offer_authorized_claim),
//! which an authorization extension owns, so a claim can never be forged by
//! naming a header. Keeping the sources apart in the type system is what makes
//! that boundary checkable rather than conventional.

/// Whether a value is human-readable text or opaque bytes.
///
/// This is a property of the key, not of the request that filled it, so a key is
/// declared with its kind once and every extractor that feeds it must agree.
/// The engine's existing transport-header apparatus infers this per captured
/// header from the gRPC `-bin` suffix; here the inference happens at compile
/// time, where a disagreement is an error rather than a surprise, and the packed
/// context spends nothing on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueKind {
    /// Printable text.
    Text,
    /// Opaque bytes, carried base64-encoded on protocols that require it.
    Binary,
}

impl ValueKind {
    /// Returns the configuration-facing name of this kind.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Binary => "binary",
        }
    }

    /// Returns the kind a gRPC wire name implies.
    #[must_use]
    pub fn implied_by_name(name: &str) -> Self {
        if name
            .as_bytes()
            .get(name.len().saturating_sub(BINARY_NAME_SUFFIX.len())..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(BINARY_NAME_SUFFIX.as_bytes()))
        {
            Self::Binary
        } else {
            Self::Text
        }
    }
}

/// The gRPC convention marking a header as carrying binary.
const BINARY_NAME_SUFFIX: &str = "-bin";

/// What to do when a request offers a key more than once.
///
/// Repetition is real: a request may carry a header several times, and an
/// identity may carry a multi-valued claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repetition {
    /// Keep the first value and ignore the rest.
    First,
    /// Keep the last value offered.
    Last,
    /// Fail the request if more than one value is offered.
    Reject,
    /// Keep every value, in the order offered.
    ///
    /// A key that keeps every value cannot be tested by value, because equality
    /// against a repeated value is not defined and an Envoy descriptor entry
    /// holds exactly one value. Such a key may still be read and bagged.
    All,
}

impl Repetition {
    /// Returns whether this repetition can yield more than one value.
    #[must_use]
    pub const fn is_repeated(self) -> bool {
        matches!(self, Self::All)
    }
}

/// Which part of the peer's network address to take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerAddressPart {
    /// The address alone, without the ephemeral port.
    Address,
    /// The address and port, formatted as the transport presents it.
    AddressAndPort,
}

/// Reads a value from a transport header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportHeaderSource {
    /// The wire names to match, compared without regard to case.
    ///
    /// Several names may feed one key, which is how a deployment accepts both
    /// its own header and a legacy one for the same field.
    pub names: Vec<String>,
    /// What to do when the header appears more than once.
    pub repetition: Repetition,
    /// Reject a value longer than this, in bytes.
    pub max_value_bytes: Option<usize>,
    /// Remember which of [`names`](Self::names) matched.
    ///
    /// An exporter that propagates a header under its original name needs this.
    /// Because the candidate names are compile-time constants, the context
    /// stores a one-byte ordinal rather than the name itself.
    pub preserve_matched_name: bool,
}

/// Reads a value from the authenticated principal.
///
/// Only an authorization extension can offer these, which is what keeps a claim
/// from being spoofed by a header of the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizedClaimSource {
    /// The claim name, compared exactly.
    pub claim: String,
    /// What to do when the claim carries more than one value.
    pub repetition: Repetition,
}

/// Reads a value the producing node computed rather than one the request
/// carried, such as a partition value or an idempotency key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedValueSource {
    /// The name the producing node offers the value under.
    pub name: String,
    /// What to do when the node offers the value more than once.
    pub repetition: Repetition,
}

/// Where one extractor reads its value from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractorSource {
    /// A header the sender supplied. Untrusted on its own.
    TransportHeader(TransportHeaderSource),
    /// The peer's network address, as the transport observed it.
    PeerAddress(PeerAddressPart),
    /// A claim an authorization extension proved.
    AuthorizedClaim(AuthorizedClaimSource),
    /// A value the producing node computed.
    Derived(DerivedValueSource),
}

impl ExtractorSource {
    /// Returns how this source handles repetition.
    #[must_use]
    pub fn repetition(&self) -> Repetition {
        match self {
            Self::TransportHeader(source) => source.repetition,
            Self::PeerAddress(_) => Repetition::Last,
            Self::AuthorizedClaim(source) => source.repetition,
            Self::Derived(source) => source.repetition,
        }
    }

    /// Returns whether this source preserves which of its names matched.
    #[must_use]
    pub fn preserves_matched_name(&self) -> bool {
        match self {
            Self::TransportHeader(source) => source.preserve_matched_name,
            _ => false,
        }
    }

    /// Returns a short description for startup diagnostics.
    #[must_use]
    pub fn describe(&self) -> String {
        match self {
            Self::TransportHeader(source) => {
                format!("transport header {}", source.names.join(" or "))
            }
            Self::PeerAddress(PeerAddressPart::Address) => "peer address".to_owned(),
            Self::PeerAddress(PeerAddressPart::AddressAndPort) => {
                "peer address and port".to_owned()
            }
            Self::AuthorizedClaim(source) => format!("authorized claim {}", source.claim),
            Self::Derived(source) => format!("derived value {}", source.name),
        }
    }
}
