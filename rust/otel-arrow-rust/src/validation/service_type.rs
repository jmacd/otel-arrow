// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Service type abstractions for validation testing.  This abstraction
// enables Logs, Traces, and Metrics to look generically similar.

use snafu::ResultExt;
use std::fmt::Debug;
use tokio::sync::mpsc;

use super::error;
use crate::validation::tcp_stream;

/// A trait that abstracts over the input side of service types (client operations)
pub trait ServiceInputType: Debug + Send + Sync + 'static {
    /// The request type for this service
    type Request: Clone + PartialEq + Send + Sync + 'static;

    /// The response type for this service
    type Response: Default + Send + 'static;

    /// The client type for this service
    type Client;

    /// The name of this service type (for logging and identification)
    fn signal() -> &'static str;

    /// The protocol used by this service type (e.g., "otlp")
    fn protocol() -> &'static str;

    /// Create a new client for this service
    async fn connect_client(endpoint: String) -> error::Result<Self::Client>;

    /// Send data through the client
    async fn send_data(
        client: &mut Self::Client,
        request: Self::Request,
    ) -> error::Result<Self::Response>;
}

/// A trait that abstracts over the output side of service types (server operations)
pub trait ServiceOutputType: Debug + Send + Sync + 'static {
    /// The request type for this service
    type Request: Clone + PartialEq + Send + Sync + 'static;

    /// Server type to add to the tonic server
    type Server;

    /// The name of this service type (for logging and identification)
    fn signal() -> &'static str;

    /// The protocol used by this service type (e.g., "otlp").  This
    /// is expected to match the receiver and exporter name used in
    /// the test, hence OTAP uses "otelarrow".
    fn protocol() -> &'static str;

    /// Create a server with the given receiver and listener stream
    fn create_server(
        receiver: TestReceiver<Self::Request>,
        incoming: crate::validation::tcp_stream::ShutdownableTcpListenerStream,
    ) -> tokio::task::JoinHandle<error::Result<()>>;
}

/// Generic test receiver that can be used for any service
#[derive(Debug, Clone)]
pub struct TestReceiver<T> {
    pub request_tx: mpsc::Sender<T>,
}

impl<T: Send + 'static> TestReceiver<T> {
    /// Generic method to process export requests for any service type
    pub async fn process_export_request<R>(
        &self,
        request: tonic::Request<T>,
        service_name: &str,
    ) -> Result<tonic::Response<R>, tonic::Status>
    where
        R: Default,
    {
        let request_inner = request.into_inner();

        // Forward the received request to the test channel
        if let Err(err) = self.request_tx.send(request_inner).await {
            return Err(tonic::Status::internal(format!(
                "Failed to send {} data to test channel: {}",
                service_name, err
            )));
        }

        Ok(tonic::Response::new(R::default()))
    }
}

/// Helper function to create a TCP listener with a dynamically allocated port
async fn create_listener_with_port() -> error::Result<(tokio::net::TcpListener, u16)> {
    // Bind to a specific address with port 0 for dynamic port allocation
    let addr = "127.0.0.1:0";
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(error::InputOutputSnafu { desc: "bind" })?;

    let port = listener
        .local_addr()
        .context(error::InputOutputSnafu {
            desc: "local_address",
        })?
        .port();

    Ok((listener, port))
}

/// Helper function to start a test receiver for any service output type
pub async fn start_test_receiver<T: ServiceOutputType>() -> error::Result<(
    tokio::task::JoinHandle<error::Result<()>>,
    mpsc::Receiver<T::Request>,
    u16,                              // actual port number that was assigned
    tokio::sync::oneshot::Sender<()>, // shutdown channel
)> {
    let (listener, port) = create_listener_with_port().await?;

    let (handle, request_rx, shutdown_tx) = create_service_server::<T>(listener).await?;

    Ok((handle, request_rx, port, shutdown_tx))
}

// We've replaced this with the specialized metrics version
// /// Helper function to start a test receiver with multiple worker threads
// pub async fn start_test_receiver_with_threads<T: ServiceOutputType>(threads: usize) -> error::Result<(
//     Vec<tokio::task::JoinHandle<error::Result<()>>>,
//     mpsc::Receiver<T::Request>,
//     u16,                              // actual port number that was assigned
//     Vec<tokio::sync::oneshot::Sender<()>>, // shutdown channels
// )> {
//     let (listener, port) = create_listener_with_port().await?;
//
//     let (handles, request_rx, shutdown_txs) = create_service_server_with_threads::<T>(listener, threads).await?;
//
//     Ok((handles, request_rx, port, shutdown_txs))
// }

/// Generic helper function to create a TCP server for any service output type
async fn create_service_server<T: ServiceOutputType + ?Sized>(
    listener: tokio::net::TcpListener,
) -> error::Result<(
    tokio::task::JoinHandle<error::Result<()>>,
    mpsc::Receiver<T::Request>,
    tokio::sync::oneshot::Sender<()>,
)> {
    // Create a channel for receiving data.
    let (request_tx, request_rx) = mpsc::channel::<T::Request>(1);

    let receiver = TestReceiver { request_tx };

    // Convert the listener to a stream of connections with a shutdown channel.
    let (incoming, shutdown_tx) = tcp_stream::create_shutdownable_tcp_listener(listener);

    let handle = T::create_server(receiver, incoming);

    Ok((handle, request_rx, shutdown_tx))
}

/// Generic helper function to create a TCP server with multiple receiver threads for metrics service
/// 
/// This is a specialized version only for metrics benchmarking
pub async fn start_metrics_test_receiver_with_threads(threads: usize) -> error::Result<(
    Vec<tokio::task::JoinHandle<error::Result<()>>>,
    mpsc::Receiver<crate::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest>,
    u16,                              // actual port number that was assigned
    Vec<tokio::sync::oneshot::Sender<()>>, // shutdown channels
)> {
    use crate::proto::opentelemetry::collector::metrics::v1::{
        metrics_service_server::MetricsServiceServer,
        ExportMetricsServiceRequest
    };
    
    // Create a listener with a dynamically allocated port
    let (listener, port) = create_listener_with_port().await?;
    
    // Create a channel for receiving data with sufficient buffer
    let (request_tx, request_rx) = mpsc::channel::<ExportMetricsServiceRequest>(threads * 10);
    
    let mut handles = Vec::with_capacity(1);
    let mut shutdown_txs = Vec::with_capacity(1);

    // Create the receiver
    let receiver = TestReceiver { request_tx };

    // Create a single server with tonic's Server
    let (incoming, shutdown_tx) = tcp_stream::create_shutdownable_tcp_listener(listener);
    
    // Create a handle for our server - tonic will handle concurrency internally
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            // Set the maximum number of concurrent requests
            .concurrency_limit_per_connection(threads)
            .add_service(MetricsServiceServer::new(receiver))
            .serve_with_incoming(incoming)
            .await
            .context(error::TonicTransportSnafu)
    });
    
    handles.push(handle);
    shutdown_txs.push(shutdown_tx);

    Ok((handles, request_rx, port, shutdown_txs))
}
