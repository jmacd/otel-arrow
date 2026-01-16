// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Channel types for communication between the connector exporter and consumer.
//!
//! This module defines FFI-friendly request/response types and the channel
//! infrastructure for cross-thread communication.

use bytes::Bytes;
use otap_df_config::SignalType;
use std::sync::atomic::{AtomicU64, Ordering};

/// Opaque identifier for correlating requests and responses.
///
/// This is FFI-friendly (repr(C), Copy) and can be passed across thread
/// and FFI boundaries without issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct RequestId(pub u64);

impl RequestId {
    /// Returns the raw u64 value for FFI usage.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

/// Thread-safe generator for unique request IDs.
#[derive(Debug, Default)]
pub struct RequestIdGenerator {
    next_id: AtomicU64,
}

impl RequestIdGenerator {
    /// Create a new ID generator starting at 1.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    /// Generate the next unique request ID.
    pub fn next(&self) -> RequestId {
        RequestId(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

/// Status of a processed request, returned by the consumer.
#[derive(Debug, Clone)]
#[repr(C)]
pub enum ResponseStatus {
    /// The request was processed successfully.
    Success,
    /// The request failed with the given error message.
    Failure,
}

impl ResponseStatus {
    /// Returns true if the status indicates success.
    #[inline]
    pub fn is_success(&self) -> bool {
        matches!(self, ResponseStatus::Success)
    }
}

/// A request sent from the exporter to the consumer.
///
/// Contains OTLP bytes payload along with metadata needed for processing
/// and response correlation.
#[derive(Debug, Clone)]
pub struct ConnectorRequest {
    /// Unique identifier for this request, used to correlate responses.
    pub id: RequestId,
    /// The type of telemetry signal (Logs, Metrics, Traces).
    pub signal_type: SignalType,
    /// The OTLP protobuf bytes payload.
    pub payload: Bytes,
}

impl ConnectorRequest {
    /// Create a new connector request.
    pub fn new(id: RequestId, signal_type: SignalType, payload: Bytes) -> Self {
        Self {
            id,
            signal_type,
            payload,
        }
    }

    /// Returns the size of the payload in bytes.
    #[inline]
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }
}

/// A response sent from the consumer back to the exporter.
#[derive(Debug, Clone)]
pub struct ConnectorResponse {
    /// The request ID this response corresponds to.
    pub id: RequestId,
    /// Whether the request was processed successfully.
    pub status: ResponseStatus,
    /// Optional error message when status is Failure.
    pub error_message: Option<String>,
}

impl ConnectorResponse {
    /// Create a successful response.
    pub fn success(id: RequestId) -> Self {
        Self {
            id,
            status: ResponseStatus::Success,
            error_message: None,
        }
    }

    /// Create a failure response with an error message.
    pub fn failure(id: RequestId, error_message: impl Into<String>) -> Self {
        Self {
            id,
            status: ResponseStatus::Failure,
            error_message: Some(error_message.into()),
        }
    }

    /// Returns true if this response indicates success.
    #[inline]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }
}

/// Handle for the consumer side of the connector channel.
///
/// The consumer runs in its own thread and pulls requests from the channel,
/// processes them (potentially via FFI), and sends responses back.
pub struct ConnectorConsumer {
    /// Receiver for incoming requests from the exporter.
    request_rx: flume::Receiver<ConnectorRequest>,
    /// Sender for responses back to the exporter.
    response_tx: flume::Sender<ConnectorResponse>,
}

impl ConnectorConsumer {
    /// Create a new consumer handle.
    pub(crate) fn new(
        request_rx: flume::Receiver<ConnectorRequest>,
        response_tx: flume::Sender<ConnectorResponse>,
    ) -> Self {
        Self {
            request_rx,
            response_tx,
        }
    }

    /// Blocking receive of the next request.
    ///
    /// Returns `None` if the channel is disconnected.
    pub fn recv(&self) -> Option<ConnectorRequest> {
        self.request_rx.recv().ok()
    }

    /// Try to receive a request without blocking.
    ///
    /// Returns `None` if no request is available or the channel is disconnected.
    pub fn try_recv(&self) -> Option<ConnectorRequest> {
        self.request_rx.try_recv().ok()
    }

    /// Receive with a timeout.
    ///
    /// Returns `None` if the timeout expires or the channel is disconnected.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<ConnectorRequest> {
        self.request_rx.recv_timeout(timeout).ok()
    }

    /// Send a response back to the exporter.
    ///
    /// Returns `Err` if the exporter has been dropped.
    pub fn respond(&self, response: ConnectorResponse) -> Result<(), ConnectorResponse> {
        self.response_tx.send(response).map_err(|e| e.into_inner())
    }

    /// Send a success response for the given request ID.
    pub fn respond_success(&self, id: RequestId) -> Result<(), ConnectorResponse> {
        self.respond(ConnectorResponse::success(id))
    }

    /// Send a failure response for the given request ID.
    pub fn respond_failure(
        &self,
        id: RequestId,
        error: impl Into<String>,
    ) -> Result<(), ConnectorResponse> {
        self.respond(ConnectorResponse::failure(id, error))
    }

    /// Returns the number of pending requests in the channel.
    pub fn pending_requests(&self) -> usize {
        self.request_rx.len()
    }

    /// Returns true if the request channel is empty.
    pub fn is_empty(&self) -> bool {
        self.request_rx.is_empty()
    }

    /// Returns true if the exporter side has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.request_rx.is_disconnected()
    }
}

/// Sender half used by the exporter to send requests.
#[derive(Clone)]
pub struct ConnectorSender {
    /// Sender for requests to the consumer.
    request_tx: flume::Sender<ConnectorRequest>,
    /// Receiver for responses from the consumer.
    response_rx: flume::Receiver<ConnectorResponse>,
}

impl ConnectorSender {
    /// Create a new sender handle.
    pub(crate) fn new(
        request_tx: flume::Sender<ConnectorRequest>,
        response_rx: flume::Receiver<ConnectorResponse>,
    ) -> Self {
        Self {
            request_tx,
            response_rx,
        }
    }

    /// Send a request to the consumer (blocking).
    pub fn send(&self, request: ConnectorRequest) -> Result<(), ConnectorRequest> {
        self.request_tx.send(request).map_err(|e| e.into_inner())
    }

    /// Send a request to the consumer (async).
    pub async fn send_async(&self, request: ConnectorRequest) -> Result<(), ConnectorRequest> {
        self.request_tx
            .send_async(request)
            .await
            .map_err(|e| e.into_inner())
    }

    /// Try to send a request without blocking.
    pub fn try_send(&self, request: ConnectorRequest) -> Result<(), TrySendError> {
        self.request_tx.try_send(request).map_err(|e| match e {
            flume::TrySendError::Full(req) => TrySendError::Full(req),
            flume::TrySendError::Disconnected(req) => TrySendError::Disconnected(req),
        })
    }

    /// Receive a response from the consumer (blocking).
    pub fn recv_response(&self) -> Option<ConnectorResponse> {
        self.response_rx.recv().ok()
    }

    /// Receive a response from the consumer (async).
    pub async fn recv_response_async(&self) -> Option<ConnectorResponse> {
        self.response_rx.recv_async().await.ok()
    }

    /// Try to receive a response without blocking.
    pub fn try_recv_response(&self) -> Option<ConnectorResponse> {
        self.response_rx.try_recv().ok()
    }

    /// Returns the number of pending responses.
    pub fn pending_responses(&self) -> usize {
        self.response_rx.len()
    }

    /// Returns true if the consumer side has been dropped.
    pub fn is_disconnected(&self) -> bool {
        self.request_tx.is_disconnected()
    }
}

/// Error returned when try_send fails.
#[derive(Debug)]
pub enum TrySendError {
    /// The channel is full.
    Full(ConnectorRequest),
    /// The consumer has been dropped.
    Disconnected(ConnectorRequest),
}

/// Creates a new connector channel pair.
///
/// # Arguments
///
/// * `request_capacity` - The bounded capacity for the request channel.
/// * `response_capacity` - The bounded capacity for the response channel.
///
/// # Returns
///
/// A tuple of `(ConnectorSender, ConnectorConsumer)` where:
/// - `ConnectorSender` is used by the exporter to send requests and receive responses
/// - `ConnectorConsumer` is used by the consumer thread to receive requests and send responses
pub fn create_connector_channel(
    request_capacity: usize,
    response_capacity: usize,
) -> (ConnectorSender, ConnectorConsumer) {
    let (request_tx, request_rx) = flume::bounded(request_capacity);
    let (response_tx, response_rx) = flume::bounded(response_capacity);

    let sender = ConnectorSender::new(request_tx, response_rx);
    let consumer = ConnectorConsumer::new(request_rx, response_tx);

    (sender, consumer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_request_id_generator() {
        let gen = RequestIdGenerator::new();
        let id1 = gen.next();
        let id2 = gen.next();
        let id3 = gen.next();

        assert_eq!(id1.as_u64(), 1);
        assert_eq!(id2.as_u64(), 2);
        assert_eq!(id3.as_u64(), 3);
    }

    #[test]
    fn test_connector_response() {
        let success = ConnectorResponse::success(RequestId(1));
        assert!(success.is_success());
        assert!(success.error_message.is_none());

        let failure = ConnectorResponse::failure(RequestId(2), "something went wrong");
        assert!(!failure.is_success());
        assert_eq!(
            failure.error_message.as_deref(),
            Some("something went wrong")
        );
    }

    #[test]
    fn test_channel_basic_flow() {
        let (sender, consumer) = create_connector_channel(10, 10);

        // Send a request
        let request = ConnectorRequest::new(RequestId(1), SignalType::Logs, Bytes::from("test"));

        sender.send(request).expect("send should succeed");

        // Consumer receives it
        let received = consumer.recv().expect("should receive request");
        assert_eq!(received.id, RequestId(1));
        assert_eq!(received.signal_type, SignalType::Logs);

        // Consumer sends response
        consumer
            .respond_success(RequestId(1))
            .expect("respond should succeed");

        // Exporter receives response
        let response = sender.recv_response().expect("should receive response");
        assert_eq!(response.id, RequestId(1));
        assert!(response.is_success());
    }

    #[test]
    fn test_channel_async_flow() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (sender, consumer) = create_connector_channel(10, 10);

            let request =
                ConnectorRequest::new(RequestId(42), SignalType::Metrics, Bytes::from("async"));

            sender.send_async(request).await.expect("send should work");

            let received = consumer.recv().expect("should receive");
            assert_eq!(received.id, RequestId(42));

            consumer
                .respond_failure(RequestId(42), "test error")
                .expect("respond should work");

            let response = sender
                .recv_response_async()
                .await
                .expect("should receive response");
            assert_eq!(response.id, RequestId(42));
            assert!(!response.is_success());
            assert_eq!(response.error_message.as_deref(), Some("test error"));
        });
    }
}
