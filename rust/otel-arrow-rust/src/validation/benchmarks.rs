// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This module provides benchmark implementations for measuring
// OTLP round trip cost through a collector.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Barrier;
use tokio::time;

use snafu::ResultExt;

// Import the proto definitions correctly
use crate::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, ResourceMetrics, ScopeMetrics,
    NumberDataPoint, number_data_point, metric,
};
use crate::proto::opentelemetry::common::v1::{
    AnyValue, KeyValue, InstrumentationScope, any_value,
};
use crate::proto::opentelemetry::resource::v1::Resource;
use crate::proto::opentelemetry::collector::metrics::v1::ExportMetricsServiceRequest;

use super::collector::{CollectorProcess, COLLECTOR_PATH, TEST_TIMEOUT_SECONDS};
use super::error;
use super::service_type::{self, ServiceInputType};
use super::otlp::{OTLPMetricsInputType, OTLPMetricsOutputType};

// Configuration settings
const MAX_CONCURRENCY: usize = 100;       // Maximum number of concurrent clients (threads × clients_per_thread)
const TEST_DURATION_SECONDS: u64 = 10;    // Duration of each benchmark test
const WARMUP_DURATION_SECONDS: u64 = 2;   // Warmup time before measuring throughput

/// Simple statistics collection for benchmark runs
#[derive(Clone)]
struct BenchStats {
    throughput: f64,
    total_requests: usize,
    errors: usize,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    threads: usize,      // Number of OS threads used for this run
    clients: usize,      // Number of async clients per thread
}

impl BenchStats {
    fn new(throughput: f64, total_requests: usize, errors: usize, threads: usize, clients: usize) -> Self {
        // In a real implementation, we would calculate actual percentiles
        // Here we're just providing placeholder values
        Self {
            throughput,
            total_requests,
            errors,
            latency_p50_ms: 0.0, // placeholder
            latency_p95_ms: 0.0, // placeholder
            latency_p99_ms: 0.0, // placeholder
            threads,
            clients,
        }
    }
    
    fn as_csv_row(&self) -> String {
        format!(
            "{},{},{:.2},{},{},{:.2},{:.2},{:.2}",
            self.threads,
            self.clients,
            self.throughput,
            self.total_requests,
            self.errors,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms
        )
    }
}

/// Create a simple metric request with a single gauge datapoint
fn create_metric_request() -> ExportMetricsServiceRequest {
    // Get current timestamp
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    // Create a gauge with a single data point
    let gauge = Gauge {
        data_points: vec![NumberDataPoint {
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "test-service".to_string(),
                        )),
                    }),
                },
                KeyValue {
                    key: "operation".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "benchmark".to_string(),
                        )),
                    }),
                },
            ],
            start_time_unix_nano: now,
            time_unix_nano: now,
            value: Some(number_data_point::Value::AsDouble(1.0)),
            exemplars: vec![],
            flags: 0,
        }],
    };

    // Create a metric with the gauge data
    let metric = Metric {
        name: "test_metric".to_string(),
        description: "A test metric for benchmarking".to_string(),
        unit: "1".to_string(),
        // Add empty metadata field to fix compilation error
        metadata: vec![],
        data: Some(metric::Data::Gauge(gauge)),
    };

    // Create resource metrics with the metric
    let resource_metrics = ResourceMetrics {
        resource: Some(Resource {
            attributes: vec![
                KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(
                            "benchmark-service".to_string(),
                        )),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            // Add empty entity_refs field to fix compilation error
            entity_refs: vec![],
        }),
        scope_metrics: vec![
            ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "benchmark".to_string(),
                    version: "1.0.0".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                metrics: vec![metric],
                schema_url: "".to_string(),
            },
        ],
        schema_url: "".to_string(),
    };

    // Create the final export request
    ExportMetricsServiceRequest {
        resource_metrics: vec![resource_metrics],
    }
}

/// Generate a modified collector config with GOMAXPROCS=1
fn generate_collector_config(
    receiver_port: u16,
    exporter_port: u16,
) -> String {
    format!(
        r#"
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 127.0.0.1:{receiver_port}

exporters:
  otlp:
    endpoint: 127.0.0.1:{exporter_port}
    compression: none
    tls:
      insecure: true
    wait_for_ready: true
    timeout: 2s
    sending_queue:
      enabled: false
    retry_on_failure:
      enabled: false

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [otlp]
  telemetry:
    metrics:
      level: none
    logs:
      level: info
"#
    )
}

/// A worker that continuously sends requests to the collector
async fn request_worker(
    client: Arc<tokio::sync::Mutex<<OTLPMetricsInputType as ServiceInputType>::Client>>, 
    barrier: Arc<Barrier>,
    stop_flag: Arc<AtomicBool>,
    request_count: Arc<AtomicUsize>,
    error_count: Arc<AtomicUsize>,
) -> error::Result<()> {
    // Wait for all workers to be ready before starting
    barrier.wait().await;
    
    let request = create_metric_request();
    let client_timeout = std::time::Duration::from_millis(500); // 500ms timeout for client operations

    while !stop_flag.load(Ordering::SeqCst) {
        // Clone the request for this iteration
        let req = request.clone();
        
        // Use select to either process a request or periodically check if we should stop
        let process_request = async {
            // Acquire the mutex to use the client with timeout
            let mut client_guard = match tokio::time::timeout(client_timeout, client.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    // Timeout acquiring the lock, count as an error and continue
                    error_count.fetch_add(1, Ordering::SeqCst);
                    return;
                }
            };
            
            // Send the request with timeout
            match tokio::time::timeout(
                client_timeout,
                OTLPMetricsInputType::send_data(&mut *client_guard, req)
            ).await {
                Ok(Ok(_)) => {
                    request_count.fetch_add(1, Ordering::SeqCst);
                }
                Ok(Err(_)) | Err(_) => {
                    error_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        };
        
        // This will either process a request or timeout after 100ms to check stop_flag again
        tokio::select! {
            _ = process_request => {},
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Just a periodic check of the stop flag
                if stop_flag.load(Ordering::SeqCst) {
                    break;
                }
            }
        }
        
        // The client_guard is automatically dropped at the end of the process_request block
    }

    Ok(())
}

/// Run the benchmark with specific thread and client counts
async fn run_concurrency_benchmark(
    threads: usize,
    clients_per_thread: usize,
) -> error::Result<BenchStats> {
    // Generate random ports in the high u16 range to avoid conflicts
    let random_value = rand::random::<u16>();
    let receiver_port = 40000 + (random_value % 25000);

    // Start the test receiver server 
    let (server_handle, mut request_rx, exporter_port, server_shutdown_tx) =
        service_type::start_test_receiver::<OTLPMetricsOutputType>().await?;
        
    // Spawn a task to continuously drain the request channel to avoid backpressure
    let drain_task = tokio::spawn(async move {
        while let Some(_) = request_rx.recv().await {
            // Just drain the requests
        }
    });

    // Generate and start the collector with GOMAXPROCS=1
    let collector_config = generate_collector_config(receiver_port, exporter_port);
    
    // Start the collector with GOMAXPROCS=1 environment variable
    let mut env_vars = std::collections::HashMap::new();
    env_vars.insert("GOMAXPROCS".to_string(), "1".to_string());
    
    // Create the collector process with environment variables and timeout
    let mut collector = tokio::time::timeout(
        std::time::Duration::from_secs(10), // 10 second timeout for collector startup
        CollectorProcess::start_with_env(
            COLLECTOR_PATH.clone(), 
            &collector_config, 
            Some(env_vars)
        )
    ).await
	.context(error::TestTimeoutSnafu)??;

    // Create client to send test data with timeout
    let client_endpoint = format!("http://127.0.0.1:{}", receiver_port);
    let client_result = tokio::time::timeout(
        std::time::Duration::from_secs(10), // 10 second timeout for client connection
        OTLPMetricsInputType::connect_client(client_endpoint)
    ).await;
    
    // Handle potential timeout or error
    let client = match client_result {
        Ok(Ok(client)) => client,
        Ok(Err(e)) => {
            println!("Error connecting to collector: {:?}", e);
            // Make sure to shut down the collector before returning
            let _ = collector.shutdown().await;
            return Err(e);
        },
        Err(_timeout_error) => {
            println!("Timeout connecting to collector");
            // Make sure to shut down the collector before returning
            let _ = collector.shutdown().await;
            // Use an appropriate error
            return Err(error::Error::NoResponse {});
        }
    };
    
    // Wrap the client in an Arc<Mutex> so it can be shared between tasks
    let client = Arc::new(tokio::sync::Mutex::new(client));
    
    // Create shared counters and signals
    let request_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    
    // Total number of async tasks = threads * clients_per_thread
    let total_clients = threads * clients_per_thread;
    let barrier = Arc::new(Barrier::new(total_clients + 1)); // +1 for the main thread
    
    // Spawn thread workers with multiple clients each
    let mut thread_handles = Vec::with_capacity(threads);
    
    // For each thread, spawn a Tokio runtime in a separate OS thread
    for _ in 0..threads {
        let client_clone = client.clone();
        let barrier_clone = barrier.clone();
        let stop_flag_clone = stop_flag.clone();
        let request_count_clone = request_count.clone();
        let error_count_clone = error_count.clone();
        let clients_count = clients_per_thread;
        
        // Spawn a new OS thread with its own Tokio runtime
        thread_handles.push(std::thread::spawn(move || {
            // Create a new tokio runtime for this thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
                
            rt.block_on(async {
                // Spawn client workers within this thread's runtime
                let mut task_handles = Vec::with_capacity(clients_count);
                
                for _ in 0..clients_count {
                    let client_inner = client_clone.clone();
                    let barrier_inner = barrier_clone.clone();
                    let stop_flag_inner = stop_flag_clone.clone();
                    let request_count_inner = request_count_clone.clone();
                    let error_count_inner = error_count_clone.clone();
                    
                    task_handles.push(tokio::spawn(async move {
                        request_worker(
                            client_inner,
                            barrier_inner,
                            stop_flag_inner,
                            request_count_inner,
                            error_count_inner,
                        ).await
                    }));
                }
                
                // Wait for all tasks in this thread to complete
                for handle in task_handles {
                    let _ = handle.await;
                }
            });
        }));
    }
    
    // Wait for all workers to be ready
    barrier.wait().await;
    
    // Warmup period
    time::sleep(Duration::from_secs(WARMUP_DURATION_SECONDS)).await;
    
    // Reset counters after warmup
    request_count.store(0, Ordering::SeqCst);
    error_count.store(0, Ordering::SeqCst);
    
    // Start measurement
    let start_time = Instant::now();
    
    // Run for the test duration
    time::sleep(Duration::from_secs(TEST_DURATION_SECONDS)).await;
    
    // Stop the test
    let elapsed = start_time.elapsed();
    stop_flag.store(true, Ordering::SeqCst);
    
    // Calculate throughput and collect statistics
    let total_requests = request_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    
    // Create statistics object
    let stats = BenchStats::new(throughput, total_requests, errors, threads, clients_per_thread);
    
    // Print statistics
    println!(
        "Threads: {}, Clients/Thread: {}, Total Clients: {}, Throughput: {:.2} req/s, Requests: {}, Errors: {}", 
        threads, 
        clients_per_thread,
        threads * clients_per_thread,
        stats.throughput,
        stats.total_requests,
        stats.errors
    );
    
    // Wait for all thread workers to finish with a timeout
    for handle in thread_handles {
        // We can't directly apply a timeout to thread::join, but we can handle errors gracefully
        // If this becomes an issue, we'd need to implement a more complex solution with channels
        match handle.join() {
            Ok(_) => {},
            Err(e) => {
                println!("Warning: Thread join error: {:?}", e);
                // Continue with cleanup even if thread join fails
            }
        }
    }
    
    // Clean up
    let _ = server_shutdown_tx.send(());
    
    // Shut down the collector
    collector.shutdown().await?;
    
    // Wait for the server to shut down with a timeout
    tokio::time::timeout(
        std::time::Duration::from_secs(TEST_TIMEOUT_SECONDS),
        server_handle,
    )
    .await
    .context(error::TestTimeoutSnafu)?
    .context(error::JoinSnafu)??;
    
    // Abort the drain task
    drain_task.abort();
    
    Ok(stats)
}

/// Run the full benchmark to find the optimal concurrency configuration
pub async fn run_otlp_round_trip_benchmark() -> error::Result<(usize, usize)> {
    println!("Running OTLP Round Trip Benchmark...");
    println!("Finding optimal concurrency configuration with GOMAXPROCS=1");
    println!("Testing threads=[1-8] and clients=[1-16]");
    
    let mut results = Vec::new();
    let mut max_throughput = 0.0;
    let mut optimal_threads = 1;
    let mut optimal_clients = 1;
    
    // Define the range of threads and clients to test - up to 16 clients and 8 threads
    // We'll test fewer combinations to speed up the benchmark
    let thread_counts = [1, 2, 4, 8]; // Number of OS threads
    let client_counts = [1, 2, 4, 8, 16]; // Number of async clients per thread
    
    // Run the benchmark for each combination of threads and clients
    for &threads in &thread_counts {
        for &clients in &client_counts {
            // Skip combinations that would exceed MAX_CONCURRENCY
            if threads * clients > MAX_CONCURRENCY {
                continue;
            }
            
            // Run with timeout and handle errors gracefully
            println!("Starting benchmark with {} threads and {} clients per thread...", threads, clients);
            
            match tokio::time::timeout(
                std::time::Duration::from_secs(TEST_DURATION_SECONDS * 3), // Give it 3x the test duration as max time
                run_concurrency_benchmark(threads, clients)
            ).await {
                Ok(Ok(stats)) => {
                    // Update max throughput if higher
                    if stats.throughput > max_throughput {
                        max_throughput = stats.throughput;
                        optimal_threads = threads;
                        optimal_clients = clients;
                    }
                    
                    // Store the result
                    results.push(stats.clone());
                },
                Ok(Err(e)) => {
                    println!("Error running benchmark with {} threads and {} clients: {:?}", 
                             threads, clients, e);
                    // Continue with next configuration rather than failing the whole test
                    continue;
                },
                Err(_) => {
                    println!("Timeout running benchmark with {} threads and {} clients", 
                             threads, clients);
                    // Continue with next configuration
                    continue;
                }
            }
        }
    }
    
    println!("\nResults Summary:");
    println!("Threads\tClients\tTotal\tThroughput (req/s)\tRequests\tErrors");
    println!("------------------------------------------------------------------");
    
    // Sort results by throughput for better readability
    results.sort_by(|a, b| b.throughput.partial_cmp(&a.throughput).unwrap());
    
    for stats in &results {
        println!("{}\t{}\t{}\t{:.2}\t\t{}\t\t{}", 
                stats.threads, 
                stats.clients,
                stats.threads * stats.clients,
                stats.throughput, 
                stats.total_requests, 
                stats.errors);
    }
    
    println!("\nOptimal configuration: {} threads, {} clients per thread (total: {})",
            optimal_threads, optimal_clients, optimal_threads * optimal_clients);
    println!("Maximum throughput: {:.2} req/s", max_throughput);
    
    // Create a CSV output for easier analysis
    println!("\nCSV Format:");
    println!("threads,clients,throughput,requests,errors,p50_ms,p95_ms,p99_ms");
    
    for stats in &results {
        println!("{}", stats.as_csv_row());
    }
    
    Ok((optimal_threads, optimal_clients))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    #[ignore] // This is a benchmark, not a regular test
    async fn test_otlp_round_trip_benchmark() {
        let (threads, clients) = run_otlp_round_trip_benchmark().await.unwrap();
        println!("Benchmark completed. Optimal configuration: {} threads, {} clients per thread", threads, clients);
    }
}
