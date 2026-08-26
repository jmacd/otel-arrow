// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Packed pdata context -- schema-backed only.
//!
//! Configuration symbols are compiled into dense register identifiers. The
//! executable register file contains value shapes and numeric slots, but no
//! logical names or transport names. Each context retains its immutable
//! compiler version so several generations can remain in flight concurrently.
//!
//! - A register is a scalar, scalar list, key/value, key/value list, or record.
//! - A value item carries a schema instruction index, typed bytes, and optional
//!   source-name provenance.
//! - Presence is one bit per register.
//! - A member is a value-item index belonging to a register.
//! - Counts determine the length of each fixed-size table.
//! - Transport names remain in ingress/egress instructions. Observed-name bytes
//!   enter the context only when the compiler marks provenance as live.
//!
//! The envelope keeps fixed-size indexes before one variable-size blob:
//!
//! ```text
//! +--------------------------------------------------------------------------+
//! | envelope header (8 bytes)                                                |
//! +--------------------------------------------------------------------------+
//! | version u16 | registers u16 | items u16 | member count u16               |
//! +--------------------------------------------------------------------------+
//! | register presence bitmap (presence words * 8 bytes)                      |
//! +--------------------------------------------------------------------------+
//! | register descriptors (register count * 12 bytes)                         |
//! +--------------------------------------------------------------------------+
//! | item descriptors (item count * 12 bytes, in arrival order)               |
//! +--------------------------------------------------------------------------+
//! | register members (member count * 4-byte member descriptors)              |
//! +--------------------------------------------------------------------------+
//! | blob: observed wire-name occurrences and values                          |
//! +--------------------------------------------------------------------------+
//! ```
//!
//! Each present register selects an ordered range from the member table. Each
//! member identifies an item and, for records, its compiled field position:
//!
//! ```text
//! register descriptor (12 bytes)
//! +------------------+------------------+------------------------------------+
//! | first member u16 | member count u16 | typed value hash u64               |
//! +------------------+------------------+------------------------------------+
//!
//! item descriptor (12 bytes)
//! fixed fields (4 bytes)
//! +--------------------+--------+------+
//! | schema_index u16   | kind u8| _pad |
//! +--------------------+--------+------+
//! blob ranges (2 * 4 bytes)
//! +----------------+---------------------------+-----------------------------+
//! | wire name occ  | blob offset u16           | byte length u16             |
//! | value          | blob offset u16           | byte length u16             |
//! +----------------+---------------------------+-----------------------------+
//! ```
//!
//! ```text
//! member descriptor (4 bytes)
//! +----------------+------------------+
//! | item index u16 | field ordinal u16|
//! +----------------+------------------+
//! ```
//!
//! Non-record registers use `u16::MAX` for the field ordinal. Record registers
//! use only numeric field positions; field names never enter the envelope.
//!
//! A zero-length source-name range means the ingress instruction supplies any
//! statically known key. A non-empty range is explicit runtime provenance.

mod bindings;
mod packed;

pub use bindings::{
    ContextEntrySetBinding, ContextRegisterSetBinding, ContextRegisterValueBinding,
    ContextScalarProjectionBinding, ContextValueBinding, PartitionProjectionBinding,
};
pub use packed::{
    ContextBytesError, ContextEntry, ContextItem, ContextItems, ContextPropagation,
    ContextRecordValue, ContextRegister, ContextValueKind, HeaderValueKind, PdataContextBytes,
    PropagatedContextItem,
};
