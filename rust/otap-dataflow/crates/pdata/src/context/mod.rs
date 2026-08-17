// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Pdata context entries: a compiler and two lookup tables.
//!
//! **Status: prototype.** This module is not wired into the pipeline
//! yet. It exists to demonstrate the mechanism proposed in RFC 0004,
//! "Pdata context entries for multitenancy support". See
//! `examples/pdata_context.rs` for a runnable end-to-end walkthrough.
//!
//! # The problem
//!
//! A message arriving at a receiver carries raw material: transport
//! headers, authorization claims, a peer address. Downstream nodes want
//! to route, batch, rate-limit, meter and store by *tenant*, where a
//! tenant may be one claim, one header, or several of them combined
//! under a condition. Doing that naively means every node re-parses
//! headers and re-derives keys on every message.
//!
//! # The mechanism
//!
//! Split the work into a configuration-time phase and a message-time
//! phase, and make the configuration-time phase produce tables that the
//! message-time phase can only *index*.
//!
//! ```text
//!  configuration time                       message time
//!  ------------------------------------     ----------------------------------
//!  policies.context.entries (YAML)          headers, claims, peer address
//!        |                                        |
//!        v                                        v
//!  ContextCompiler                          ContextBuilder
//!    - intern each referenced source          - set_header / set_claim / ...
//!      into a dense SourceSlot                  one hash probe per ARRIVING
//!    - compile each entry into                  value, then an array store
//!      conditions + dimensions                  |
//!    - fix the record layout                    v
//!        |                                  build(): one walk of the entry
//!        v                                  table, appending key bytes and
//!  Arc<ContextSchema>  ------------------>  precomputing one hash per entry
//!        |                                        |
//!        | schema.entry("product_user")           v
//!        | -> EntryHandle  (once, at setup)  ContextRecord
//!        |                                        |
//!        +--------- node holds handle ----------> record.key(handle)
//!                                                 record.hash(handle)
//!                                                 -- bit test + constant-offset
//!                                                    load, no hashing, no search
//! ```
//!
//! This is Envoy's custom inline header design applied to pdata
//! context. In Envoy, filters register the headers they care about, the
//! registry is finalized before any traffic flows, each registered
//! header gets a fixed slot in every header map, and lookup by handle is
//! O(1) while unregistered headers fall back to the general map. Here
//! the registry is driven by configuration instead of by compilation
//! units, and there is no fallback path at all: a value no entry
//! mentions is dropped at the door.
//!
//! # The two tables
//!
//! **The source table** answers "does anything read this?". It is keyed
//! by (kind, name) and is consulted once per arriving header or claim.
//! Because the configuration is closed, the answer is usually no, and
//! the cost of no is a single failed hash probe with no allocation.
//!
//! **The entry table** answers "what is this message's tenant?". Each
//! row is a tiny program: check these conditions, then concatenate these
//! dimensions. Rows are evaluated once per message, in one pass.
//!
//! Everything a node does afterwards is an array index, because the
//! record layout was fixed when the tables were.
//!
//! # The record
//!
//! One contiguous allocation per message:
//!
//! ```text
//! +--------------------+ 0
//! | presence bitmap    |  which entries resolved
//! +--------------------+ hash_off
//! | entry hashes       |  ready-made partition keys
//! +--------------------+ dim_off
//! | dimension index    |  (offset, length) per dimension
//! +--------------------+ data_off
//! | key data           |  dimensions of one entry are contiguous, so
//! +--------------------+  an entry key is a single slice
//! ```
//!
//! A record is cloned by cloning two `Arc`s, so fan-out and splitting
//! carry context without copying it.
//!
//! # Vocabulary
//!
//! | term | meaning |
//! |---|---|
//! | source | a raw value: a header, a claim, a network attribute, a constant, a generator |
//! | entry | a user-named context value, declared under `policies.context.entries` |
//! | dimension | one value component of an entry; an entry with N components is N-dimensional |
//! | condition | a component that gates an entry's presence without contributing a dimension |
//! | handle | a dense index into the entry table, resolved once and held by a node |
//! | key | an entry's dimensions concatenated, the thing routing and batching compare |

mod compiler;
mod config;
mod record;
mod schema;

pub use compiler::{CompileError, ContextCompiler};
pub use config::{Component, ContextPolicy, EntryDecl, EntryRequirement, SourceKind, ValueKind};
pub use record::{ContextBuilder, ContextRecord};
pub use schema::{
    BindError, ContextRef, ContextSchema, DimHandle, EntryHandle, RecordLayout, SourceDesc,
};
