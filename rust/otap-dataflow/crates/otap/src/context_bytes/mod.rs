// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Schema-backed pdata context.

mod packed;

pub use packed::{
    ContextBytesError, ContextItem, ContextItems, ContextRegister, ContextValueKind,
    PdataContextBytes,
};
