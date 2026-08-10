// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The metadata compiler: the engine's request-metadata configuration, compiled
//! once into an immutable epoch, and each request's metadata packed into one
//! reference-counted allocation.
//!
//! This crate implements the compiler of RFC 0003. It depends on no other engine
//! crate, holds no configuration syntax, and knows nothing about pipelines. A
//! caller translates its own configuration into the declarations here, and the
//! pipeline graph arrives only as an opaque producer-to-consumer relation.
//!
//! # The model
//!
//! The model is a translation of Envoy's rate-limit descriptor machinery, so an
//! operator who knows Envoy already knows this.
//!
//! | Here | Envoy | Rule |
//! |---|---|---|
//! | extractor | rate-limit *action* | produces one `key=value`, or fails |
//! | token | *descriptor* | an ordered list of extractors; **all** must succeed |
//! | condition | a configured descriptor | names its token's full key sequence; a key with no value is a wildcard |
//! | condition set | one consumer's descriptor map | the ordered conditions one consumer declares |
//!
//! Two consequences follow from Envoy and shape everything else. A token is
//! **all-or-nothing**, so every key of a resolved token is present and "is this
//! key set" is answered by the resolved-token bitmap rather than by a per-key
//! bit. And a consumer's answer is over **all** the tokens that matched, not the
//! first, because Envoy denies a request if any matched descriptor denies. A
//! router reads the first matching branch; a limiter reads all matching branches.
//!
//! Whether a node *requires* a token is a separate question. It is an admission
//! contract belonging to the node, expressed with [`Requirement`], and it is
//! declared here only so reachability knows the token is observed.
//!
//! # Using it
//!
//! Declare, compile, then hold the identifiers:
//!
//! ```text
//! let mut compiler = MetadataCompiler::new(Epoch::new(1), Limits::default());
//! let tenant_id = compiler.declare_key("tenant_id", ValueKind::Text);
//! let from_header = compiler.declare_extractor(tenant_id, header("x-tenant-id"));
//! let edge = compiler.declare_token("edge", &[from_header]);
//! let (compiled, report) = compiler.compile()?;
//! ```
//!
//! `report` lists what reachability removed, for the caller to log at startup.
//!
//! Build a context per request, offering only what the request carried:
//!
//! ```text
//! let mut encoder = compiled.encoder(receiver, &mut scratch);
//! encoder.offer_transport_header("x-tenant-id", b"acme");
//! let context = encoder.finish()?;
//! ```
//!
//! Read it through a view, which checks the epoch once:
//!
//! ```text
//! let view = compiled.view(&context)?;
//! view.slot_value(slot);
//! let matches = view.consumer(router)?.matches(route_set);
//! ```
//!
//! # Guarantees
//!
//! - A context that carries nothing allocates nothing; a context that carries
//!   anything is exactly one allocation, and cloning it is a refcount bump.
//! - Construction scratch is reused and does not allocate at steady state.
//! - Compiled state is immutable within an epoch, so construction and reads take
//!   no locks and every per-core pipeline shares one copy.
//! - Matching is exact. Every literal is interned, so a value no condition
//!   declared encodes to a reserved unknown symbol and matches nothing. Hashing
//!   only ever finds a candidate, which is then compared byte for byte.
//! - Every dimension is bounded by [`Limits`]; exceeding one is an error rather
//!   than an allocation.

mod branch_table;
mod compiled;
mod compiler;
mod condition;
mod context;
mod declaration;
mod dictionary;
mod encoder;
mod error;
mod hashing;
mod ids;
mod layout;
mod limits;
mod name_index;
mod pair_slot;
mod plan;
mod reachability;
mod scratch;
mod signature;
mod source;

pub use compiled::{CompileReport, CompiledMetadata};
pub use compiler::MetadataCompiler;
pub use condition::{Condition, ConditionEntry, ConditionMatch, KeyPredicate};
pub use context::{
    ConditionMatches, ConsumerMetadataView, ContextViewError, EpochMismatch, MetadataContext,
    MetadataView, MissingRequiredTokens, SlotValues,
};
pub use declaration::{MetadataField, Requirement};
pub use dictionary::Symbol;
pub use encoder::ContextEncoder;
pub use error::{CompileError, CompileProblem, CompileWarning, EncodeError};
pub use ids::{
    BranchIndex, ConditionSetId, ConsumerId, Epoch, ExtractorId, KeyId, MetadataFieldId,
    PairSlotId, ProducerId, SignatureId, SymbolSlotId, TokenId, ValueSlotId,
};
pub use limits::Limits;
pub use scratch::MetadataScratch;
pub use source::{
    AuthorizedClaimSource, DerivedValueSource, ExtractorSource, PeerAddressPart, Repetition,
    TransportHeaderSource, ValueKind,
};
