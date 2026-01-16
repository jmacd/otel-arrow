// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Connector exporter that bridges an OTAP dataflow pipeline to an external consumer
//! via shared memory channels.
//!
//! This exporter sends OTLP bytes (converting from OTAP Arrow format if necessary)
//! through a flume bounded channel to a consumer running in a separate thread.
//! The consumer can process the data (potentially via FFI) and return success/failure
//! status using opaque request identifiers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    OTAP Dataflow Pipeline (thread-per-core)             │
//! │                                                                         │
//! │  ┌─────────────┐     ┌───────────────────┐     ┌──────────────────┐    │
//! │  │  Receiver   │────▶│    Processors     │────▶│ConnectorExporter │    │
//! │  └─────────────┘     └───────────────────┘     └────────┬─────────┘    │
//! │                                                         │              │
//! └─────────────────────────────────────────────────────────┼──────────────┘
//!                                                           │
//!                                              flume channel│(bounded)
//!                                                           │
//!                                                           ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                         Consumer Thread                                 │
//! │                                                                         │
//! │  ┌────────────────────┐                    ┌─────────────────────┐     │
//! │  │ ConnectorConsumer  │───────────────────▶│   FFI / Processing  │     │
//! │  │   (recv requests)  │◀───────────────────│   (external code)   │     │
//! │  └────────────────────┘   response channel └─────────────────────┘     │
//! │                                                                         │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

pub mod channel;
pub mod config;
pub mod exporter;

pub use channel::{
    ConnectorConsumer, ConnectorRequest, ConnectorResponse, RequestId, ResponseStatus,
    create_connector_channel,
};
pub use config::Config;
pub use exporter::{CONNECTOR_EXPORTER_URN, ConnectorExporter};
