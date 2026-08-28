// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use prost::Message;

use crate::pb;

/// Errors returned when decoding a Sparkplug payload.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// The payload bytes are not valid Sparkplug protobuf.
    #[error("failed to decode Sparkplug payload: {0}")]
    Prost(#[from] prost::DecodeError),
}

/// A decoded Sparkplug payload wrapper.
#[derive(Debug, Clone, PartialEq)]
pub struct SparkplugPayload {
    inner: pb::Payload,
}

impl SparkplugPayload {
    /// Decodes a Sparkplug payload from protobuf bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self {
            inner: pb::Payload::decode(bytes)?,
        })
    }

    /// Encodes the payload back into protobuf bytes.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        self.inner.encode_to_vec()
    }

    /// Returns the top-level Sparkplug payload timestamp.
    #[must_use]
    pub fn timestamp(&self) -> u64 {
        self.inner.timestamp.unwrap_or_default()
    }

    /// Returns the top-level Sparkplug sequence number.
    #[must_use]
    pub fn seq(&self) -> u64 {
        self.inner.seq.unwrap_or_default()
    }

    /// Returns the payload metrics.
    #[must_use]
    pub fn metrics(&self) -> &[pb::payload::Metric] {
        &self.inner.metrics
    }

    /// Returns the underlying generated protobuf payload.
    #[must_use]
    pub fn as_inner(&self) -> &pb::Payload {
        &self.inner
    }

    /// Consumes the wrapper and returns the underlying protobuf payload.
    #[must_use]
    pub fn into_inner(self) -> pb::Payload {
        self.inner
    }
}

impl From<pb::Payload> for SparkplugPayload {
    fn from(inner: pb::Payload) -> Self {
        Self { inner }
    }
}
