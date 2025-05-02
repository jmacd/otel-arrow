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
const MAX_CONCURRENCY: usize = 100;       // Maximum number of concurrent requests to test
const TEST_DURATION_SECONDS: u64 = 10;    // Duration of each concurrency level test
const WARMUP_DURATION_SECONDS: u64 = 2;   // Warmup time before measuring throughput
const THROUGHPUT_THRESHOLD: f64 = 0.95;   // Threshold to determine saturation (as fraction of max)
const SATURATION_REPEAT: usize = 2;       // Number of consecutive runs needed to confirm saturation

/// Simple statistics collection for benchmark runs
#[derive(Clone)]
struct BenchStats {
    throughput: f64,
    total_requests: usize,
    errors: usize,
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
}

impl BenchStats {
    fn new(throughput: f64, total_requests: usize, errors: usize) -> Self {
        // In a real implementation, we would calculate actual percentiles
        // Here we're just providing placeholder values
        Self {
            throughput,
            total_requests,
            errors,
            latency_p50_ms: 0.0, // placeholder
            latency_p95_ms: 0.0, // placeholder
            latency_p99_ms: 0.0, // placeholder
        }
    }
    
    fn as_csv_row(&self) -> String {
        format!(
            "{:.2},{},{},{:.2},{:.2},{:.2}",
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

    while !stop_flag.load(Ordering::SeqCst) {
        // Clone the request for this iteration
        let req = request.clone();
        
        // Acquire the mutex to use the client
        let mut client_guard = client.lock().await;
        
        // Send the request
        match OTLPMetricsInputType::send_data(&mut *client_guard, req).await {
            Ok(_) => {
                request_count.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                error_count.fetch_add(1, Ordering::SeqCst);
            }
        }

        // Release the mutex by dropping the guard
        drop(client_guard);
    }

    Ok(())
}

/// Run the benchmark at a specific concurrency level
async fn run_concurrency_benchmark(
    concurrency: usize,
) -> error::Result<(BenchStats, usize)> {
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
    
    // Create the collector process with environment variables
    let mut collector = CollectorProcess::start_with_env(
        COLLECTOR_PATH.clone(), 
        &collector_config, 
        Some(env_vars)
    ).await?;

    // Create client to send test data
    let client_endpoint = format!("http://127.0.0.1:{}", receiver_port);
    let client = OTLPMetricsInputType::connect_client(client_endpoint).await?;
    
    // Wrap the client in an Arc<Mutex> so it can be shared between tasks
    let client = Arc::new(tokio::sync::Mutex::new(client));
    
    // Create shared counters and signals
    let request_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(concurrency + 1)); // +1 for the main thread
    
    // Spawn worker tasks
    let mut handles = Vec::with_capacity(concurrency);
    
    for _ in 0..concurrency {
        let client_clone = client.clone();
        let barrier_clone = barrier.clone();
        let stop_flag_clone = stop_flag.clone();
        let request_count_clone = request_count.clone();
        let error_count_clone = error_count.clone();
        
        handles.push(tokio::spawn(async move {
            request_worker(
                client_clone,
                barrier_clone,
                stop_flag_clone,
                request_count_clone,
                error_count_clone,
            ).await
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
    let stats = BenchStats::new(throughput, total_requests, errors);
    
    // Print statistics
    println!(
        "Concurrency: {}, Throughput: {:.2} req/s, Total Requests: {}, Errors: {}", 
        concurrency, 
        stats.throughput,
        stats.total_requests,
        stats.errors
    );
    
    // Wait for all workers to finish
    for handle in handles {
        let _ = handle.await;
    }
    
    // Clean up
    let _ = server_shutdown_tx.send(());
    
    // Shut down the collector
    collector.shutdown().await?;
    
    // Wait for the server to shut down
    tokio::time::timeout(
        std::time::Duration::from_secs(TEST_TIMEOUT_SECONDS),
        server_handle,
    )
    .await
    .context(error::TestTimeoutSnafu)?
    .context(error::JoinSnafu)??;
    
    // Abort the drain task
    drain_task.abort();
    
    Ok((stats, concurrency))
}

/// Run the full benchmark to find the optimal concurrency level
pub async fn run_otlp_round_trip_benchmark() -> error::Result<usize> {
    println!("Running OTLP Round Trip Benchmark...");
    println!("Finding optimal concurrency level with GOMAXPROCS=1");
    
    let mut results = Vec::new();
    let mut max_throughput = 0.0;
    let mut optimal_concurrency = 1;
    let mut saturation_count = 0;
    
    // Start with concurrency of 1 and increase
    for concurrency in [1, 2, 4, 8, 16, 32, 64, 128].into_iter() {
        if concurrency > MAX_CONCURRENCY {
            break;
        }
        
        let (stats, n) = run_concurrency_benchmark(concurrency).await?;
        
        // Update max throughput if higher
        if stats.throughput > max_throughput {
            max_throughput = stats.throughput;
            optimal_concurrency = concurrency;
            saturation_count = 0; // Reset saturation counter when we find a new maximum
        }
        
        // Store the result
        results.push((n, stats.clone()));
        
        // If we've seen at least 2 results and current throughput is at least 95% of max
        // and there's less than 5% improvement from previous, we may be approaching saturation
        if results.len() >= 2 {
            let prev_stats = &results[results.len() - 2].1;
            let current_stats = &results[results.len() - 1].1;
            
            let improvement = (current_stats.throughput - prev_stats.throughput) / prev_stats.throughput;
            let saturation = current_stats.throughput / max_throughput;
            
            if saturation >= THROUGHPUT_THRESHOLD && improvement < 0.05 {
                saturation_count += 1;
                
                // If we've seen saturation for multiple consecutive runs, we're done
                if saturation_count >= SATURATION_REPEAT {
                    break;
                }
            } else {
                saturation_count = 0; // Reset counter if we haven't seen saturation
            }
        }
    }
    
    println!("\nResults Summary:");
    println!("Concurrency\tThroughput (req/s)\tTotal Requests\tErrors");
    println!("-----------------------------------------------------------");
    
    for (concurrency, stats) in &results {
        println!("{}\t\t{:.2}\t\t{}\t\t{}", 
                concurrency, 
                stats.throughput, 
                stats.total_requests, 
                stats.errors);
    }
    
    println!("\nOptimal concurrency level: {}", optimal_concurrency);
    println!("Maximum throughput: {:.2} req/s", max_throughput);
    
    // Create a CSV output for easier analysis
    println!("\nCSV Format:");
    println!("concurrency,throughput,requests,errors,p50_ms,p95_ms,p99_ms");
    
    for (concurrency, stats) in &results {
        println!("{},{}", concurrency, stats.as_csv_row());
    }
    
    Ok(optimal_concurrency)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    #[ignore] // This is a benchmark, not a regular test
    async fn test_otlp_round_trip_benchmark() {
        let result = run_otlp_round_trip_benchmark().await.unwrap();
        println!("Benchmark completed. Optimal concurrency: {}", result);
    }
}
