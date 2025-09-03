// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the pipeline data that is passed between pipeline components.
//!
//! Internally, the data can be represented in the following formats:
//! - OTLP Bytes - contain the OTLP service request messages serialized as protobuf
//! - OTAP Arrow Bytes - the data is contained in `BatchArrowRecords` type which
//!   contains the Arrow batches for each payload, serialized as Arrow IPC. This type is
//!   what we'd receive from the OTAP GRPC service.
//! - OTAP Arrow Records - the data is contained in Arrow `[RecordBatch]`s organized by
//!   for efficient access by the arrow payload type.
//!
//! This module also contains conversions between the various types using the `From`
//! and `TryFrom` traits. For example:
//! ```
//! # use std::sync::Arc;
//! # use arrow::array::{RecordBatch, UInt16Array};
//! # use arrow::datatypes::{DataType, Field, Schema};
//! # use otel_arrow_rust::otap::{OtapArrowRecords, Logs};
//! # use otel_arrow_rust::proto::opentelemetry::{
//!     arrow::v1::ArrowPayloadType,
//!     collector::logs::v1::ExportLogsServiceRequest,
//!     common::v1::{AnyValue, InstrumentationScope, KeyValue},
//!     logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
//!     resource::v1::Resource    
//! };
//! # use otap_df_otap::{pdata::{OtapPdata, OtlpProtoBytes}, grpc::OtapArrowBytes};
//! # use prost::Message;
//! let otlp_service_req = ExportLogsServiceRequest::new(vec![
//!    ResourceLogs::build(Resource::default())
//!       .scope_logs(vec![
//!            ScopeLogs::build(InstrumentationScope::default())
//!                .log_records(vec![
//!                    LogRecord::build(2u64, SeverityNumber::Info, "event")
//!                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
//!                        .finish(),
//!                ])
//!                .finish(),
//!        ])
//!        .finish(),
//!  ]);
//! let mut buf = Vec::new();
//! otlp_service_req.encode(&mut buf).unwrap();
//!
//! let otap_pdata = OtapPdata::from(OtlpProtoBytes::ExportLogsRequest(buf));
//!
//! // convert to Otap Arrow Records
//! let otap_arrow_records: OtapArrowRecords = otap_pdata.try_into().unwrap();
//!
//! // convert to OTAP Arrow Bytes
//! let otap_pdata: OtapPdata = otap_arrow_records.into();
//! let otap_arrow_bytes: OtapArrowBytes = otap_pdata.try_into().unwrap();
//! ```
//!
//! Internally, conversions are happening using various utility functions:
//! ```text
//!                                      ┌───────────────────────┐                                            
//!                                      │                       │                                            
//!                                      │      OTLP Bytes       │                                            
//!                                      │                       │                                            
//!                                      └───┬───────────────────┘                                            
//!                                          │                 ▲                                              
//!                                          │                 │                                              
//!                                          │                 │                                              
//!                                          ▼                 │                                              
//!    otap_df_otap::encoder::encode_<signal>_otap_batch    otel_arrow_rust::otlp::<signal>::<signal_>_from()
//!                                          │                 ▲                                              
//!                                          │                 │                                              
//!                                          │                 │                                              
//!                                          ▼                 │                                              
//!                                      ┌─────────────────────┴───┐                                          
//!                                      │                         │                                          
//!                                      │    OTAP Arrow Records   │                                          
//!                                      │                         │                                          
//!                                      └───┬─  ──────────────────┘                                          
//!                                          │                 ▲                                              
//!                                          │                 │                                              
//!                                          ▼                 │                                              
//!       otel_arrow_rust::Producer::produce_bar()    otel_arrow_rust::Consumer::consume_bar()                
//!                                          │                 ▲                                              
//!                                          │                 │                                              
//!                                          ▼                 │                                              
//!                                       ┌────────────────────┴───┐                                         
//!                                       │                        │                                         
//!                                       │    OTAP Arrow Bytes    │                                         
//!                                       │                        │                                         
//!                                       └────────────────────────┘                                         
//!                                                                           
//! ```

use crate::encoder::encode_spans_otap_batch;
use crate::{encoder::encode_logs_otap_batch, grpc::OtapArrowBytes};
use otap_df_config::experimental::SignalType;
use otap_df_pdata_views::otlp::bytes::logs::RawLogsData;
use otap_df_pdata_views::otlp::bytes::traces::RawTraceData;
use otel_arrow_rust::otap::{OtapArrowRecords, from_record_messages};
use otel_arrow_rust::otlp::{logs::logs_from, metrics::metrics_from, traces::traces_from};
use otel_arrow_rust::{Consumer, Producer};
use prost::{EncodeError, Message};

pub use super::context::{Context, Register, ReplyState, ReplyTo};

/// module contains related to pdata
pub mod error {
    /// Errors related to pdata
    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        /// Wrapper for error that occurred converting pdata
        #[error("An error occurred converting pdata: {error}")]
        ConversionError {
            /// The error that occurred
            error: String,
        },

        /// An error involving the assembly of an OtapPdata context
        /// and payload.
        #[error("A request state error")]
        RequestStateError,

        /// An invalid data conversion attempted on a register.
        #[error("A register error")]
        RegisterError,
    }

    impl From<Error> for otap_df_engine::error::Error {
        fn from(e: Error) -> Self {
            otap_df_engine::error::Error::PdataConversionError {
                error: format!("{e}"),
            }
        }
    }
}

/// Pipeline data represented as protobuf serialized OTLP request messages
#[derive(Clone, Debug)]
pub enum OtlpProtoBytes {
    /// protobuf serialized ExportLogsServiceRequest
    ExportLogsRequest(Vec<u8>),
    /// protobuf serialized ExportMetricsServiceRequest
    ExportMetricsRequest(Vec<u8>),
    /// protobuf serialized ExportTracesServiceRequest
    ExportTracesRequest(Vec<u8>),
}

impl OtlpProtoBytes {
    /// Get a borrowed reference to the serialized proto bytes slice
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            OtlpProtoBytes::ExportLogsRequest(bytes)
            | OtlpProtoBytes::ExportMetricsRequest(bytes)
            | OtlpProtoBytes::ExportTracesRequest(bytes) => bytes.as_slice(),
        }
    }
}

/// Container for the various representations of the telemetry data
#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum OtapPayload {
    /// data is serialized as a protobuf service message for one of the OTLP GRPC services
    OtlpBytes(OtlpProtoBytes),

    /// data is contained in `BatchArrowRecords`, which contain ArrowIPC serialized
    OtapArrowBytes(OtapArrowBytes),

    /// data is contained in `OtapBatch` which contains Arrow `RecordBatches` for OTAP payload type
    OtapArrowRecords(OtapArrowRecords),
}

/// Context + container for telemetry data
#[derive(Clone, Debug)]
pub struct OtapPdata {
    pub(crate) context: Context,
    pub(crate) payload: OtapPayload,
}

/* -------- Signal type -------- */

impl OtapPdata {
    /// Returns the type of signal represented by this `OtapPdata` instance.
    #[must_use]
    pub fn signal_type(&self) -> SignalType {
        self.payload.signal_type()
    }

    /// True if the payload is empty. By definition, we can skip sending an
    /// empty request.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
}

impl OtapPayload {
    /// Returns the type of signal represented by this `OtapPdata` instance.
    #[must_use]
    pub fn signal_type(&self) -> SignalType {
        match self {
            Self::OtlpBytes(value) => value.signal_type(),
            Self::OtapArrowBytes(value) => value.signal_type(),
            Self::OtapArrowRecords(value) => value.signal_type(),
        }
    }

    /// True if the payload is empty. By definition, we can skip sending an
    /// empty request.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::OtlpBytes(value) => value.is_empty(),
            Self::OtapArrowBytes(value) => value.is_empty(),
            Self::OtapArrowRecords(value) => value.is_empty(),
        }
    }

    /// Returns the number of items of the primary signal (spans, data
    /// points, log records).
    #[cfg(test)]
    #[must_use]
    pub fn num_items(self) -> usize {
        match self {
            Self::OtlpBytes(value) => value.num_items(),
            Self::OtapArrowBytes(value) => value.num_items(),
            Self::OtapArrowRecords(value) => value.num_items(),
        }
    }
}

/* -------- Trait implementations -------- */

/// Helper methods that internal representations of OTAP PData should implement
trait OtapPdataHelpers: Clone + Into<OtapPayload> {
    /// Returns the type of signal represented by this `OtapPdata` instance.
    fn signal_type(&self) -> SignalType;

    /// Number of items
    #[cfg(test)]
    fn num_items(self) -> usize;

    /// Return true if there is no data.
    fn is_empty(&self) -> bool;

    /// Make an empty clone (preserve signal type).
    fn clone_empty(&self) -> Self;
}

impl OtapPdataHelpers for OtapArrowRecords {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::Logs(_) => SignalType::Logs,
            Self::Metrics(_) => SignalType::Metrics,
            Self::Traces(_) => SignalType::Traces,
        }
    }

    fn clone_empty(&self) -> Self {
        match self {
            Self::Logs(_) => Self::Logs(Default::default()),
            Self::Metrics(_) => Self::Metrics(Default::default()),
            Self::Traces(_) => Self::Traces(Default::default()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Logs(_) => {
                self.get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::Logs)
                    .map_or(true, |batch| batch.num_rows() == 0)
            }
            Self::Traces(_) => {
                self.get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::Spans)
                    .map_or(true, |batch| batch.num_rows() == 0)
            }
            Self::Metrics(_) => {
                self.get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::UnivariateMetrics)
                    .map_or(true, |batch| batch.num_rows() == 0)
            }
        }
    }

    #[cfg(test)]
    fn num_items(self) -> usize {
        match self {
            Self::Logs(_) => {
                self
                    .get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::Logs)
                    .map_or(0, |batch| batch.num_rows())
            }
            Self::Traces(_) => {
		self
                    .get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::Spans)
                    .map_or(0, |batch| batch.num_rows())
            }
            Self::Metrics(_) => {
                self.get(otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType::UnivariateMetrics)
                    .map_or(0, |batch| batch.num_rows())
            }
        }
    }
}

impl OtapPdataHelpers for OtlpProtoBytes {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::ExportLogsRequest(_) => SignalType::Logs,
            Self::ExportMetricsRequest(_) => SignalType::Metrics,
            Self::ExportTracesRequest(_) => SignalType::Traces,
        }
    }

    fn clone_empty(&self) -> Self {
        match self {
            Self::ExportLogsRequest(_) => Self::ExportLogsRequest(Vec::new()),
            Self::ExportMetricsRequest(_) => Self::ExportMetricsRequest(Vec::new()),
            Self::ExportTracesRequest(_) => Self::ExportTracesRequest(Vec::new()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::ExportLogsRequest(bytes) => bytes.is_empty(),
            Self::ExportMetricsRequest(bytes) => bytes.is_empty(),
            Self::ExportTracesRequest(bytes) => bytes.is_empty(),
        }
    }

    /// Number of items
    #[cfg(test)]
    fn num_items(self) -> usize {
        match self {
            Self::ExportLogsRequest(bytes) => {
                let logs_data_view = RawLogsData::new(bytes);
                use otap_df_pdata_views::views::logs::{
                    LogsDataView, ResourceLogsView, ScopeLogsView,
                };
                logs_data_view
                    .resources()
                    .flat_map(|rl| rl.scopes())
                    .flat_map(|sl| sl.log_records())
                    .count()
            }
            Self::ExportTracesRequest(bytes) => {
                let traces_data_view = RawTraceData::new(bytes);
                use otap_df_pdata_views::views::trace::{
                    ResourceSpansView, ScopeSpansView, TracesView,
                };
                traces_data_view
                    .resources()
                    .flat_map(|rs| rs.scopes())
                    .flat_map(|ss| ss.spans())
                    .count()
            }
            Self::ExportMetricsRequest(bytes) => {
                // Metrics view is not implemented yet
                panic!("ToDo")
            }
        }
    }
}

impl OtapPdataHelpers for OtapArrowBytes {
    fn signal_type(&self) -> SignalType {
        match self {
            Self::ArrowLogs(_) => SignalType::Logs,
            Self::ArrowMetrics(_) => SignalType::Metrics,
            Self::ArrowTraces(_) => SignalType::Traces,
        }
    }

    fn clone_empty(&self) -> Self {
        use otel_arrow_rust::proto::opentelemetry::arrow::v1::BatchArrowRecords;
        match self {
            Self::ArrowLogs(_) => Self::ArrowLogs(BatchArrowRecords::default()),
            Self::ArrowMetrics(_) => Self::ArrowMetrics(BatchArrowRecords::default()),
            Self::ArrowTraces(_) => Self::ArrowTraces(BatchArrowRecords::default()),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::ArrowLogs(data) => data.arrow_payloads.is_empty(),
            Self::ArrowMetrics(data) => data.arrow_payloads.is_empty(),
            Self::ArrowTraces(data) => data.arrow_payloads.is_empty(),
        }
    }

    #[cfg(test)]
    fn num_items(self) -> usize {
        match self.try_into() {
            Ok(records) => {
                let records: OtapArrowRecords = records;
                records.num_items()
            }
            Err(_) => 0,
        }
    }
}

/* -------- Conversion implementations -------- */

impl OtapPdata {
    /// Construct new OtapData from context and payload.
    pub fn new(context: Context, payload: OtapPayload) -> Self {
        Self { context, payload }
    }

    /// Construct new OtapData with payload using default context.
    pub fn new_default(payload: OtapPayload) -> Self {
        Self {
            context: Context::default(),
            payload,
        }
    }

    /// Constructs a request holder for returning a retryable request by
    /// cloning the request payload in its (potentially modified) state.
    pub fn new_reply<T: OtapPdataHelpers>(context: Context, payload: &T) -> Self {
        let has_reply = context.has_reply_state();
        Self {
            context: context,
            payload: if !has_reply {
                payload.clone_empty().into()
            } else {
                payload.clone().into()
            },
        }
    }

    /// The return destination of the reply.
    pub fn return_node_id(&self) -> usize {
        self.context.as_ref().expect("has context").reply_node_id()
    }

    /// To form a Nack message, this is the potentially-cloned payload.
    pub fn take_reply_payload(&mut self) -> Option<OtapPayload> {
        self.payload.take()
    }

    /// Removes the current element on stack, drops the node_id
    /// (because it has arrived) and returns the state.
    pub fn pop_stack(&self) -> ReplyState {
        self.context.stack.pop().expect("has_reply").state
    }

    /// To send the request, take it.
    pub fn take_request_payload(&mut self) -> OtapPayload {
        self.payload.take().expect("called once")
    }

    /// Take the context.
    pub fn take_context(&mut self) -> Context {
        self.context.take().expect("called once")
    }

    /// Mutate the context.
    pub fn mut_context(&mut self) -> &mut Context {
        &mut self.context
    }
}

impl From<OtapArrowRecords> for OtapPayload {
    fn from(value: OtapArrowRecords) -> Self {
        Self::OtapArrowRecords(value)
    }
}

impl From<OtapArrowBytes> for OtapPayload {
    fn from(value: OtapArrowBytes) -> Self {
        Self::OtapArrowBytes(value)
    }
}

impl From<OtlpProtoBytes> for OtapPayload {
    fn from(value: OtlpProtoBytes) -> Self {
        Self::OtlpBytes(value)
    }
}

impl TryFrom<OtapPayload> for OtapArrowRecords {
    type Error = error::Error;

    fn try_from(value: OtapPayload) -> Result<Self, Self::Error> {
        match value {
            OtapPayload::OtapArrowBytes(value) => value.try_into(),
            OtapPayload::OtapArrowRecords(value) => Ok(value),
            OtapPayload::OtlpBytes(value) => value.try_into(),
        }
    }
}

impl TryFrom<OtapPayload> for OtlpProtoBytes {
    type Error = error::Error;

    fn try_from(value: OtapPayload) -> Result<Self, Self::Error> {
        match value {
            OtapPayload::OtapArrowBytes(value) => value.try_into(),
            OtapPayload::OtapArrowRecords(value) => value.try_into(),
            OtapPayload::OtlpBytes(value) => Ok(value),
        }
    }
}

impl TryFrom<OtapPayload> for OtapArrowBytes {
    type Error = error::Error;

    fn try_from(value: OtapPayload) -> Result<Self, Self::Error> {
        match value {
            OtapPayload::OtapArrowBytes(value) => Ok(value),
            OtapPayload::OtapArrowRecords(value) => value.try_into(),
            OtapPayload::OtlpBytes(value) => value.try_into(),
        }
    }
}

impl TryFrom<OtapArrowRecords> for OtlpProtoBytes {
    type Error = error::Error;

    fn try_from(value: OtapArrowRecords) -> Result<Self, Self::Error> {
        let map_otlp_conversion_error =
            |error: otel_arrow_rust::error::Error| error::Error::ConversionError {
                error: format!("error generating OTLP request: {error}"),
            };

        let map_prost_encode_error = |error: EncodeError| error::Error::ConversionError {
            error: format!("error encoding protobuf: {error}"),
        };

        match value {
            OtapArrowRecords::Logs(_) => {
                let export_logs_svc_req = logs_from(value).map_err(map_otlp_conversion_error)?;
                let mut bytes = vec![];
                export_logs_svc_req
                    .encode(&mut bytes)
                    .map_err(map_prost_encode_error)?;
                Ok(Self::ExportLogsRequest(bytes))
            }
            OtapArrowRecords::Metrics(_) => {
                let export_metrics_svc_req =
                    metrics_from(value).map_err(map_otlp_conversion_error)?;
                let mut bytes = vec![];
                export_metrics_svc_req
                    .encode(&mut bytes)
                    .map_err(map_prost_encode_error)?;
                Ok(Self::ExportMetricsRequest(bytes))
            }
            OtapArrowRecords::Traces(_) => {
                let export_traces_svc_req =
                    traces_from(value).map_err(map_otlp_conversion_error)?;
                let mut bytes = vec![];
                export_traces_svc_req
                    .encode(&mut bytes)
                    .map_err(map_prost_encode_error)?;
                Ok(Self::ExportTracesRequest(bytes))
            }
        }
    }
}

impl TryFrom<OtlpProtoBytes> for OtapArrowRecords {
    type Error = error::Error;

    fn try_from(value: OtlpProtoBytes) -> Result<Self, Self::Error> {
        let map_error = |error: crate::encoder::error::Error| error::Error::ConversionError {
            error: format!("error encoding OTAP Batch: {error}"),
        };
        match value {
            OtlpProtoBytes::ExportLogsRequest(bytes) => {
                let logs_data_view = RawLogsData::new(&bytes);
                let otap_batch = encode_logs_otap_batch(&logs_data_view).map_err(map_error)?;

                Ok(otap_batch)
            }
            OtlpProtoBytes::ExportTracesRequest(bytes) => {
                let trace_data_view = RawTraceData::new(&bytes);
                let otap_batch = encode_spans_otap_batch(&trace_data_view).map_err(map_error)?;

                Ok(otap_batch)
            }
            _ => {
                // TODO add conversions when we support
                // https://github.com/open-telemetry/otel-arrow/issues/768
                Err(error::Error::ConversionError {
                    error: "converting from OTLP Bytes for this signal type not yet supported"
                        .to_string(),
                })
            }
        }
    }
}

impl TryFrom<OtlpProtoBytes> for OtapArrowBytes {
    type Error = error::Error;

    fn try_from(value: OtlpProtoBytes) -> Result<Self, Self::Error> {
        let otap_batch: OtapArrowRecords = value.try_into()?;
        otap_batch.try_into()
    }
}

impl TryFrom<OtapArrowBytes> for OtlpProtoBytes {
    type Error = error::Error;

    fn try_from(value: OtapArrowBytes) -> Result<Self, Self::Error> {
        let otap_batch: OtapArrowRecords = value.try_into()?;
        otap_batch.try_into()
    }
}

impl TryFrom<OtapArrowBytes> for OtapArrowRecords {
    type Error = error::Error;

    fn try_from(value: OtapArrowBytes) -> Result<Self, Self::Error> {
        let map_error = |error: otel_arrow_rust::error::Error| error::Error::ConversionError {
            error: format!("error decoding BatchArrowRecords: {error}"),
        };
        let mut consumer = Consumer::default();
        match value {
            OtapArrowBytes::ArrowLogs(mut bar) => {
                let record_messages = consumer.consume_bar(&mut bar).map_err(map_error)?;
                Ok(Self::Logs(from_record_messages(record_messages)))
            }
            OtapArrowBytes::ArrowMetrics(mut bar) => {
                let record_messages = consumer.consume_bar(&mut bar).map_err(map_error)?;
                Ok(Self::Metrics(from_record_messages(record_messages)))
            }
            OtapArrowBytes::ArrowTraces(mut bar) => {
                let record_messages = consumer.consume_bar(&mut bar).map_err(map_error)?;
                Ok(Self::Traces(from_record_messages(record_messages)))
            }
        }
    }
}

impl TryFrom<OtapArrowRecords> for OtapArrowBytes {
    type Error = error::Error;

    fn try_from(mut otap_batch: OtapArrowRecords) -> Result<Self, Self::Error> {
        let mut producer = Producer::new();
        let bar =
            producer
                .produce_bar(&mut otap_batch)
                .map_err(|e| error::Error::ConversionError {
                    error: format!("error encoding BatchArrowRecords: {e}"),
                })?;
        Ok(match otap_batch {
            OtapArrowRecords::Logs(_) => Self::ArrowLogs(bar),
            OtapArrowRecords::Metrics(_) => Self::ArrowMetrics(bar),
            OtapArrowRecords::Traces(_) => Self::ArrowTraces(bar),
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use otel_arrow_rust::{
        otap::OtapArrowRecords,
        proto::opentelemetry::{
            collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
            common::v1::{AnyValue, InstrumentationScope, KeyValue},
            logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
            resource::v1::Resource,
            trace::v1::{
                ResourceSpans, ScopeSpans, Span, Status,
                span::{Event, Link},
                status::StatusCode,
            },
        },
    };

    #[test]
    fn test_conversion_logs() {
        let otlp_service_req = ExportLogsServiceRequest::new(vec![
            ResourceLogs::build(Resource::default())
                .scope_logs(vec![
                    ScopeLogs::build(InstrumentationScope::default())
                        .log_records(vec![
                            LogRecord::build(2u64, SeverityNumber::Info, "event")
                                .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                                .finish(),
                        ])
                        .finish(),
                ])
                .finish(),
        ]);
        let mut otlp_bytes = vec![];
        otlp_service_req.encode(&mut otlp_bytes).unwrap();

        let pdata: OtapPdata = OtlpProtoBytes::ExportLogsRequest(otlp_bytes).into();

        // test can go OtlpProtoBytes -> OtapBatch & back
        let otap_batch: OtapArrowRecords = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
        let pdata: OtapPdata = otap_batch.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otlp_bytes, OtlpProtoBytes::ExportLogsRequest(_)));
        let pdata: OtapPdata = otlp_bytes.into();

        // test can go OtlpProtoBytes -> OTAPData
        let otap_data: OtapArrowBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_data, OtapArrowBytes::ArrowLogs(_)));
        let pdata: OtapPdata = otap_data.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otlp_bytes, OtlpProtoBytes::ExportLogsRequest(_)));
        let pdata: OtapPdata = otlp_bytes.into();

        // test can go otap_batch -> OTAPData
        let otap_batch: OtapArrowRecords = pdata.try_into().map(|(_, v)| v).unwrap();
        let pdata: OtapPdata = otap_batch.into();
        let otap_data: OtapArrowBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_data, OtapArrowBytes::ArrowLogs(_)));
        let pdata: OtapPdata = otap_data.into();

        let otap_batch: OtapArrowRecords = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
    }

    // TODO add additional tests for converting between metrics once we have the ability to convert
    //  between OTLP bytes -> OTAP for this signal types
    // https://github.com/open-telemetry/otel-arrow/issues/768

    fn roundtrip_otlp_otap_logs(otlp_service_req: ExportLogsServiceRequest) {
        let mut otlp_bytes = vec![];
        otlp_service_req.encode(&mut otlp_bytes).unwrap();
        let pdata: OtapPdata = OtlpProtoBytes::ExportLogsRequest(otlp_bytes).into();

        // test can go OtlpProtoBytes -> OtapBatch & back
        let otap_batch: OtapArrowRecords = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_batch, OtapArrowRecords::Logs(_)));
        let pdata: OtapPdata = otap_batch.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        let bytes = match otlp_bytes {
            OtlpProtoBytes::ExportLogsRequest(bytes) => bytes,
            _ => panic!("unexpected otlp bytes pdata variant"),
        };

        let result = ExportLogsServiceRequest::decode(bytes.as_ref()).unwrap();
        assert_eq!(otlp_service_req, result);
    }

    fn roundtrip_otlp_otap_traces(otlp_service_req: ExportTraceServiceRequest) {
        let mut otlp_bytes = vec![];
        otlp_service_req.encode(&mut otlp_bytes).unwrap();
        let pdata: OtapPdata = OtlpProtoBytes::ExportTracesRequest(otlp_bytes).into();

        // test can go OtlpBytes -> OtapBatch & back
        let otap_batch: OtapArrowRecords = pdata.try_into().map(|(_, v)| v).unwrap();
        assert!(matches!(otap_batch, OtapArrowRecords::Traces(_)));
        let pdata: OtapPdata = otap_batch.into();

        let otlp_bytes: OtlpProtoBytes = pdata.try_into().map(|(_, v)| v).unwrap();
        let bytes = match otlp_bytes {
            OtlpProtoBytes::ExportTracesRequest(bytes) => bytes,
            _ => panic!("unexpected otlp bytes pdata variant"),
        };

        let result = ExportTraceServiceRequest::decode(bytes.as_ref()).unwrap();
        assert_eq!(otlp_service_req, result);
    }

    #[test]
    fn test_otlp_otap_logs_roundtrip() {
        // test to ensure the correct attributes are assigned to the correct log message after
        // roundtrip encoding/decoding

        let otlp_service_req = ExportLogsServiceRequest::new(vec![
            ResourceLogs::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val1"))],
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope {
                    attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val1"))],
                    ..Default::default()
                })
                .log_records(vec![
                    LogRecord::build(1u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                    LogRecord::build(2u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val2"))])
                        .finish(),
                    LogRecord::build(3u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val3"))])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
            ResourceLogs::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val2"))],
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope {
                    name: "Scope2".into(),
                    attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val2"))],
                    ..Default::default()
                })
                .log_records(vec![
                    LogRecord::build(4u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val4"))])
                        .finish(),
                    LogRecord::build(5u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val5"))])
                        .finish(),
                ])
                .finish(),
                ScopeLogs::build(InstrumentationScope {
                    attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val3"))],
                    ..Default::default()
                })
                .log_records(vec![
                    LogRecord::build(6u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val6"))])
                        .finish(),
                    LogRecord::build(7u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val7"))])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
        ]);

        roundtrip_otlp_otap_logs(otlp_service_req);
    }

    #[test]
    fn test_otlp_otap_logs_repeated_attributes() {
        // check to ensure attributes that are repeated are correctly encoded and decoding when
        // doing round-trip between OTLP and OTAP. This test is needed because OTAP attributes'
        // parent IDs can be in multiple formats: plain encoded, and quasi-delta encoded (where
        // delta encoding is used for sequential runs of some key-value pairs).

        let otlp_service_req = ExportLogsServiceRequest::new(vec![
            ResourceLogs::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope::build("scope1").attributes(vec![
                    KeyValue::new("scope_key", AnyValue::new_string("val")),
                ]))
                .log_records(vec![
                    // Add some logs with repeated attributes
                    LogRecord::build(1u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                    LogRecord::build(2u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                    LogRecord::build(3u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
            // also add some scopes and resources where the attributes repeat ...
            ResourceLogs::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope::build("scope2").attributes(vec![
                    KeyValue::new("scope_key", AnyValue::new_string("val")),
                ]))
                .log_records(vec![
                    LogRecord::build(4u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
            ResourceLogs::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val"))],
                ..Default::default()
            })
            .scope_logs(vec![
                ScopeLogs::build(InstrumentationScope::build("scope2").attributes(vec![
                    KeyValue::new("scope_key", AnyValue::new_string("val")),
                ]))
                .log_records(vec![
                    LogRecord::build(7u64, SeverityNumber::Info, "")
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val"))])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
        ]);

        roundtrip_otlp_otap_logs(otlp_service_req);
    }

    #[test]
    fn test_otlp_otap_traces_roundtrip() {
        let otlp_service_req = ExportTraceServiceRequest::new(vec![
            ResourceSpans::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val1"))],
                ..Default::default()
            })
            .scope_spans(vec![
                ScopeSpans::build(InstrumentationScope {
                    attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val1"))],
                    ..Default::default()
                })
                .spans(vec![
                    Span::build(u128::to_be_bytes(1), u64::to_be_bytes(1), "albert", 1u64)
                        .end_time_unix_nano(4u64)
                        .status(Status::new("status1", StatusCode::Ok))
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val1"))])
                        .links(vec![
                            Link::build(u128::to_be_bytes(10), u64::to_be_bytes(10)).finish(),
                            // this Link's trace_id repeats with the next one. doing this to ensure
                            // their parent IDs don't get interpreted as delta encoded
                            Link::build(u128::to_be_bytes(11), u64::to_be_bytes(11))
                                .attributes(vec![
                                    KeyValue::new("link_key", AnyValue::new_string("val0")),
                                    // repeating the attr here with the next one for same reason
                                    // as repeating link with trace ID
                                    KeyValue::new("link_key_r", AnyValue::new_string("val1")),
                                ])
                                .finish(),
                        ])
                        .events(vec![
                            Event::build("event0", 0u64).finish(),
                            // this event has the repeating name with the next one. doing this to
                            // ensure their parent IDs don't get interpreted as delta encoded
                            Event::build("event1", 1u64)
                                .attributes(vec![
                                    KeyValue::new("evt_key", AnyValue::new_string("val0")),
                                    // repeating the attr here with the next one for same reason
                                    // as repeating link with trace ID
                                    KeyValue::new("evt_key_r", AnyValue::new_string("val1")),
                                ])
                                .finish(),
                        ])
                        .finish(),
                    Span::build(u128::to_be_bytes(2), u64::to_be_bytes(2), "terry", 2u64)
                        .end_time_unix_nano(3u64)
                        .status(Status::new("status1", StatusCode::Ok))
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val2"))])
                        .links(vec![
                            // this Link's trace_id repeats with the previous, and  next one.
                            // doing this to ensure their parent IDs don't get interpreted as
                            // delta encoded
                            Link::build(u128::to_be_bytes(11), u64::to_be_bytes(20))
                                .attributes(vec![
                                    KeyValue::new("link_key_r", AnyValue::new_string("val1")),
                                    KeyValue::new("link_key", AnyValue::new_string("val2")),
                                ])
                                .finish(),
                        ])
                        .events(vec![
                            // this event has the repeating name with the next one and previous one
                            // doing this to ensure their parent IDs don't get interpreted as
                            // delta encoded
                            Event::build("event1", 1u64)
                                .attributes(vec![
                                    KeyValue::new("evt_key_r", AnyValue::new_string("val1")),
                                    KeyValue::new("evt_key", AnyValue::new_string("val2")),
                                ])
                                .finish(),
                        ])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
            ResourceSpans::build(Resource {
                attributes: vec![KeyValue::new("res_key", AnyValue::new_string("val2"))],
                ..Default::default()
            })
            .scope_spans(vec![
                ScopeSpans::build(InstrumentationScope {
                    attributes: vec![KeyValue::new("scope_key", AnyValue::new_string("val3"))],
                    ..Default::default()
                })
                .spans(vec![
                    Span::build(u128::to_be_bytes(3), u64::to_be_bytes(3), "albert", 3u64)
                        .end_time_unix_nano(4u64)
                        .status(Status::new("status1", StatusCode::Ok))
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val1"))])
                        .links(vec![
                            // this Link's trace_id repeats with the previous one. do this to ensure they
                            // don't get interpreted as delta encoded
                            Link::build(u128::to_be_bytes(11), u64::to_be_bytes(30)).finish(),
                            Link::build(u128::to_be_bytes(31), u64::to_be_bytes(31)).finish(),
                        ])
                        .events(vec![
                            // this event has the repeating name with the previous one. doing this
                            // to ensure their parent IDs don't get interpreted as delta encoded
                            Event::build("event1", 2u64).finish(),
                        ])
                        .finish(),
                    Span::build(u128::to_be_bytes(4), u64::to_be_bytes(4), "terry", 4u64)
                        .end_time_unix_nano(5u64)
                        .status(Status::new("status1", StatusCode::Ok))
                        .attributes(vec![KeyValue::new("key", AnyValue::new_string("val4"))])
                        .links(vec![
                            Link::build(u128::to_be_bytes(40), u64::to_be_bytes(40)).finish(),
                        ])
                        .finish(),
                ])
                .finish(),
            ])
            .finish(),
        ]);

        roundtrip_otlp_otap_traces(otlp_service_req);
    }

    #[test]
    fn test_signal_type() {
        // Test signal_type for OtlpProtoBytes variants
        let logs_bytes = OtlpProtoBytes::ExportLogsRequest(vec![]);
        let metrics_bytes = OtlpProtoBytes::ExportMetricsRequest(vec![]);
        let traces_bytes = OtlpProtoBytes::ExportTracesRequest(vec![]);

        assert_eq!(logs_bytes.signal_type(), SignalType::Logs);
        assert_eq!(metrics_bytes.signal_type(), SignalType::Metrics);
        assert_eq!(traces_bytes.signal_type(), SignalType::Traces);

        // Test signal_type for OtapArrowRecords variants
        let logs_records = OtapArrowRecords::Logs(Default::default());
        let metrics_records = OtapArrowRecords::Metrics(Default::default());
        let traces_records = OtapArrowRecords::Traces(Default::default());

        assert_eq!(logs_records.signal_type(), SignalType::Logs);
        assert_eq!(metrics_records.signal_type(), SignalType::Metrics);
        assert_eq!(traces_records.signal_type(), SignalType::Traces);

        // Test signal_type for OtapArrowBytes variants
        let logs_bytes = OtapArrowBytes::ArrowLogs(Default::default());
        let metrics_bytes = OtapArrowBytes::ArrowMetrics(Default::default());
        let traces_bytes = OtapArrowBytes::ArrowTraces(Default::default());

        assert_eq!(logs_bytes.signal_type(), SignalType::Logs);
        assert_eq!(metrics_bytes.signal_type(), SignalType::Metrics);
        assert_eq!(traces_bytes.signal_type(), SignalType::Traces);

        // Test signal_type for OtapPdata variants
        let pdata_logs = OtapPdata::OtlpBytes {
            context: Default::default(),
            value: OtlpProtoBytes::ExportLogsRequest(vec![]),
        };
        let pdata_metrics = OtapPdata::OtapArrowRecords {
            context: Default::default(),
            value: OtapArrowRecords::Metrics(Default::default()),
        };
        let pdata_traces = OtapPdata::OtapArrowBytes {
            context: Default::default(),
            value: OtapArrowBytes::ArrowTraces(Default::default()),
        };

        assert_eq!(pdata_logs.signal_type(), SignalType::Logs);
        assert_eq!(pdata_metrics.signal_type(), SignalType::Metrics);
        assert_eq!(pdata_traces.signal_type(), SignalType::Traces);
    }
}
