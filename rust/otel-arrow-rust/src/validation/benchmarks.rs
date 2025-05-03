// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// This module provides benchmark implementations for measuring
// OTLP round trip cost through a collector.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
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

use super::collector::{CollectorProcess, COLLECTOR_PATH};
use super::error;
use super::service_type::{self, ServiceInputType};
use super::otlp::OTLPMetricsInputType;

// Configuration settings
const MAX_CONCURRENCY: usize = 100;       // Maximum number of concurrent clients (threads × clients_per_thread)
const TEST_DURATION_SECONDS: u64 = 10;    // Duration of each benchmark test
const WARMUP_DURATION_SECONDS: u64 = 2;   // Warmup time before measuring throughput
const REQUEST_TIMEOUT_MS: u64 = 500;      // Timeout for individual requests in milliseconds
const MEMORY_SAMPLE_INTERVAL_MS: u64 = 100; // Frequency to sample memory usage in milliseconds

/// Memory usage tracking for processes
struct MemoryUsageTracker {
    pid: u32,                           // Process ID to monitor
    samples: Vec<u64>,                  // Memory samples in KB
}

impl MemoryUsageTracker {
    fn new(pid: u32) -> Self {
        Self {
            pid,
            samples: Vec::new(),
        }
    }
    
    /// Read current memory usage (RSS) from /proc/{pid}/status
    fn sample_memory(&mut self) -> std::io::Result<()> {
        let path = format!("/proc/{}/status", self.pid);
        let content = std::fs::read_to_string(&path)?;
        
        // Parse the VmRSS line from the status file
        for line in content.lines() {
            if line.starts_with("VmRSS:") {
                // VmRSS format: "VmRSS:     12345 kB"
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(rss_kb) = parts[1].parse::<u64>() {
                        self.samples.push(rss_kb);
                        return Ok(());
                    }
                }
            }
        }
        
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Could not find VmRSS in proc status file"
        ))
    }
    
    /// Get average memory usage in KB
    fn average_memory_kb(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        
        let sum: u64 = self.samples.iter().sum();
        sum as f64 / self.samples.len() as f64
    }
    
    /// Get peak memory usage in KB
    fn peak_memory_kb(&self) -> u64 {
        self.samples.iter().max().cloned().unwrap_or(0)
    }
}

/// Latency tracking for percentile calculations
struct LatencyTracker {
    latencies: Vec<f64>, // Latencies in milliseconds
}

impl LatencyTracker {
    fn new() -> Self {
        Self {
            latencies: Vec::new(),
        }
    }
    
    fn add_latency(&mut self, latency: Duration) {
        // Convert duration to milliseconds and store
        self.latencies.push(latency.as_secs_f64() * 1000.0);
    }
    
    fn calculate_percentile(&self, percentile: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        
        // Create a sorted copy for percentile calculation
        let mut sorted = self.latencies.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let index = (percentile / 100.0 * (sorted.len() as f64 - 1.0)) as usize;
        sorted[index]
    }
    
    fn p50(&self) -> f64 {
        self.calculate_percentile(50.0)
    }
    
    fn p95(&self) -> f64 {
        self.calculate_percentile(95.0)
    }
    
    fn p99(&self) -> f64 {
        self.calculate_percentile(99.0)
    }
}

/// Simple statistics collection for benchmark runs
#[derive(Clone)]
struct BenchStats {
    throughput: f64,
    total_requests: usize,
    errors: usize,
    error_rate: f64,      // Error rate as a percentage (errors / (total_requests + errors) * 100)
    latency_p50_ms: f64,
    latency_p95_ms: f64,
    latency_p99_ms: f64,
    threads: usize,      // Number of OS threads used for this run
    clients: usize,      // Number of async clients per thread
    mem_avg_kb: f64,     // Average memory usage in KB
    mem_peak_kb: u64,    // Peak memory usage in KB
}

impl BenchStats {
    fn new(
        throughput: f64, 
        total_requests: usize, 
        errors: usize, 
        threads: usize, 
        clients: usize,
        p50: f64,
        p95: f64,
        p99: f64,
        mem_avg_kb: f64,
        mem_peak_kb: u64,
    ) -> Self {
        // Calculate error rate as a percentage
        let error_rate = if total_requests + errors > 0 {
            (errors as f64 / (total_requests + errors) as f64) * 100.0
        } else {
            0.0
        };
        
        Self {
            throughput,
            total_requests,
            errors,
            error_rate,
            latency_p50_ms: p50,
            latency_p95_ms: p95,
            latency_p99_ms: p99,
            threads,
            clients,
            mem_avg_kb,
            mem_peak_kb,
        }
    }
    
    fn as_csv_row(&self) -> String {
        format!(
            "{},{},{:.2},{},{},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
            self.threads,
            self.clients,
            self.throughput,
            self.total_requests,
            self.errors,
            self.error_rate,
            self.latency_p50_ms,
            self.latency_p95_ms,
            self.latency_p99_ms,
            self.mem_avg_kb,
            self.mem_peak_kb
        )
    }
    
    /// Returns true if the error rate is above an acceptable threshold
    fn has_high_error_rate(&self) -> bool {
        // Consider a configuration to have a "high" error rate if more than 5% of requests fail
        self.error_rate > 5.0
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
    latency_tracker: Arc<Mutex<LatencyTracker>>,
) -> error::Result<()> {
    // Wait for all workers to be ready before starting
    barrier.wait().await;
    
    let request = create_metric_request();
    let client_timeout = std::time::Duration::from_millis(REQUEST_TIMEOUT_MS); // Use configurable timeout

    while !stop_flag.load(Ordering::SeqCst) {
        // Clone the request for this iteration
        let req = request.clone();
        
        // Use select to either process a request or periodically check if we should stop
        let process_request = async {
            // Start timing for latency measurement
            let start_time = Instant::now();
            
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
                    // Record successful request
                    request_count.fetch_add(1, Ordering::SeqCst);
                    
                    // Record latency for successful requests only
                    let latency = start_time.elapsed();
                    if let Ok(mut tracker) = latency_tracker.lock() {
                        tracker.add_latency(latency);
                    }
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

    // Start the test receiver server with multiple threads (equal to the maximum client threads)
    // In our case, that's 4 threads since we're testing threads=[1-4]
    const MAX_RECEIVER_THREADS: usize = 4;
    
    let (server_handles, mut request_rx, exporter_port, server_shutdown_txs) =
        service_type::start_metrics_test_receiver_with_threads(MAX_RECEIVER_THREADS).await?;
        
    // Spawn a task to continuously drain the request channel to avoid backpressure
    let drain_task = tokio::spawn(async move {
        while let Some(_request) = request_rx.recv().await {
            // Just drain the requests, we don't need to process them for the benchmark
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
    
    // Get the collector process ID for memory tracking
    let collector_pid = collector.get_pid().expect("Failed to get collector PID");
    
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
    let latency_tracker = Arc::new(Mutex::new(LatencyTracker::new()));
    
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
        let latency_tracker_clone = latency_tracker.clone();
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
                    let latency_tracker_inner = latency_tracker_clone.clone();
                    
                    task_handles.push(tokio::spawn(async move {
                        request_worker(
                            client_inner,
                            barrier_inner,
                            stop_flag_inner,
                            request_count_inner,
                            error_count_inner,
                            latency_tracker_inner,
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
    
    // Start a memory sampling task
    let memory_sample_interval = Duration::from_millis(MEMORY_SAMPLE_INTERVAL_MS);
    let memory_stop_flag = Arc::new(AtomicBool::new(false));
    let memory_stop_flag_clone = memory_stop_flag.clone();
    let memory_handle = tokio::spawn(async move {
        let mut tracker = MemoryUsageTracker::new(collector_pid);
        while !memory_stop_flag_clone.load(Ordering::SeqCst) {
            if let Err(e) = tracker.sample_memory() {
                eprintln!("Failed to sample memory: {}", e);
            }
            time::sleep(memory_sample_interval).await;
        }
        tracker
    });
    
    // Run for the test duration
    time::sleep(Duration::from_secs(TEST_DURATION_SECONDS)).await;
    
    // Stop the test
    let elapsed = start_time.elapsed();
    stop_flag.store(true, Ordering::SeqCst);
    
    // Calculate throughput and collect statistics
    let total_requests = request_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);
    let throughput = total_requests as f64 / elapsed.as_secs_f64();
    
    // Calculate latency percentiles
    let (p50, p95, p99) = match latency_tracker.lock() {
        Ok(tracker) => {
            (tracker.p50(), tracker.p95(), tracker.p99())
        },
        Err(_) => {
            println!("Warning: Failed to obtain latency tracker lock, using default values");
            (0.0, 0.0, 0.0)
        }
    };
    
    // Retrieve memory statistics
    memory_stop_flag.store(true, Ordering::SeqCst);
    let memory_tracker = match tokio::time::timeout(Duration::from_secs(2), memory_handle).await {
        Ok(Ok(tracker)) => tracker,
        _ => {
            println!("Warning: Failed to retrieve memory tracker, using default values");
            MemoryUsageTracker::new(0) // Use a dummy tracker
        }
    };
    
    let mem_avg_kb = memory_tracker.average_memory_kb();
    let mem_peak_kb = memory_tracker.peak_memory_kb();
    
    // Create statistics object
    let stats = BenchStats::new(
        throughput, 
        total_requests, 
        errors, 
        threads, 
        clients_per_thread,
        p50,
        p95,
        p99,
        mem_avg_kb,
        mem_peak_kb
    );
    
    // Print statistics
    println!(
        "Threads: {}, Clients/Thread: {}, Total Clients: {}, Throughput: {:.2} req/s, Requests: {}, Errors: {}, Latency(ms): p50={:.2}, p95={:.2}, p99={:.2}, Memory(KB): avg={:.2}, peak={}", 
        threads, 
        clients_per_thread,
        threads * clients_per_thread,
        stats.throughput,
        stats.total_requests,
        stats.errors,
        stats.latency_p50_ms,
        stats.latency_p95_ms,
        stats.latency_p99_ms,
        stats.mem_avg_kb,
        stats.mem_peak_kb
    );
    
    // Wait for all thread workers to complete gracefully
    for handle in thread_handles {
        let _ = handle.join();
    }
    
    // Shut down the collector
    println!("Shutting down collector...");
    if let Err(e) = collector.shutdown().await {
        eprintln!("Error shutting down collector: {:?}", e);
    }
    
    // Send shutdown signal to the test receiver servers
    for server_shutdown_tx in server_shutdown_txs {
        let _ = server_shutdown_tx.send(());
    }
    
    // Wait for server tasks to terminate with timeout
    for handle in server_handles {
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle
        ).await;
    }
    
    // Abort the drain task
    drain_task.abort();
    
    Ok(stats)
}

/// Run the full benchmark to find the optimal concurrency configuration
pub async fn run_otlp_round_trip_benchmark() -> error::Result<(usize, usize)> {
    println!("Running OTLP Round Trip Benchmark...");
    println!("Finding optimal concurrency configuration with GOMAXPROCS=1");
    println!("Testing threads=[1-4] and clients=[1-4]");
    
    let mut results = Vec::new();
    let mut max_throughput = 0.0;
    let mut optimal_threads = 1;
    let mut optimal_clients = 1;
    
    let thread_counts = [1, 2, 3, 4]; // Number of OS threads
    let client_counts = [1, 2, 3, 4]; // Number of async clients per thread
    
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
                    // Update max throughput if higher and error rate is acceptable
                    if stats.throughput > max_throughput && !stats.has_high_error_rate() {
                        max_throughput = stats.throughput;
                        optimal_threads = threads;
                        optimal_clients = clients;
                    }
                    
                    // Store the result
                    results.push(stats.clone());
                    
                    // Log if error rate is high
                    if stats.has_high_error_rate() {
                        println!("Warning: Configuration with {} threads and {} clients has a high error rate: {:.2}%",
                                threads, clients, stats.error_rate);
                    }
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
    println!("Threads\tClients\tTotal\tThroughput (req/s)\tRequests\tErrors\t\tError Rate");
    println!("--------------------------------------------------------------------------------");
    
    // Sort results by throughput for better readability
    results.sort_by(|a, b| b.throughput.partial_cmp(&a.throughput).unwrap());
    
    for stats in &results {
        let error_indicator = if stats.has_high_error_rate() { " (!)" } else { "" };
        println!("{}\t{}\t{}\t{:.2}\t\t{}\t\t{}\t\t{:.2}%{}", 
                stats.threads, 
                stats.clients,
                stats.threads * stats.clients,
                stats.throughput, 
                stats.total_requests, 
                stats.errors,
                stats.error_rate,
                error_indicator);
    }
    
    // Find stats for the optimal configuration to report error rate
    let optimal_stats = results.iter()
        .find(|s| s.threads == optimal_threads && s.clients == optimal_clients);
    
    println!("\nOptimal configuration: {} threads, {} clients per thread (total: {})",
            optimal_threads, optimal_clients, optimal_threads * optimal_clients);
    println!("Maximum throughput: {:.2} req/s", max_throughput);
    if let Some(stats) = optimal_stats {
        println!("Error rate: {:.2}% ({} errors out of {} total requests)",
            stats.error_rate, stats.errors, stats.total_requests + stats.errors);
    }
    
    // Create a CSV output for easier analysis
    println!("\nCSV Format:");
    println!("threads,clients,throughput,requests,errors,error_rate%,p50_ms,p95_ms,p99_ms");
    
    for stats in &results {
        println!("{}", stats.as_csv_row());
    }
    
    // Add a summary about error rates
    println!("\nError Rate Analysis:");
    
    // Count configurations with high error rates
    let high_error_configs = results.iter().filter(|s| s.has_high_error_rate()).count();
    let total_configs = results.len();
    
    if high_error_configs > 0 {
        println!("{} out of {} tested configurations had high error rates (>5%).", 
                high_error_configs, total_configs);
        println!("Configurations with high error rates are marked with (!) in the results summary.");
        
        if let Some(highest_error) = results.iter().max_by(|a, b| a.error_rate.partial_cmp(&b.error_rate).unwrap()) {
            println!("The highest error rate was {:.2}% with {} threads and {} clients.",
                    highest_error.error_rate, highest_error.threads, highest_error.clients);
        }
    } else {
        println!("All tested configurations had acceptable error rates (<= 5%).");
    }
    
    // Add info about request timeout
    println!("\nRequest timeout was set to {} ms.", REQUEST_TIMEOUT_MS);
    
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
