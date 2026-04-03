//! End-to-end demo: generated counter metrics → Prometheus /metrics endpoint.
//!
//! This example simulates multiple pipeline nodes with consumer/producer
//! counters, collects them on a periodic tick, encodes as OTAP Arrow
//! delta batches, accumulates into cumulative state, and serves the
//! result as OpenMetrics text on HTTP.
//!
//! Run:
//!   cargo run -p otap-df-metrics-sdk --example prometheus_demo
//!
//! Then scrape:
//!   curl http://localhost:9464/metrics

#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use otap_df_config::SignalType;
use otap_df_config::policy::MetricLevel;
use otap_df_metrics_sdk::accumulator::{EntityKey, MetricIdentity};
use otap_df_metrics_sdk::dimension::Outcome;
use otap_df_metrics_sdk::prometheus::PrometheusExporter;
use otap_df_telemetry::self_metrics::generated::{
    NodeConsumerItems, NodeProducerItems, precomputed_schema,
};
use rand::Rng;
use rand::RngExt;
use tokio::net::TcpListener;

/// A simulated pipeline node with consumer and producer counters.
struct SimulatedNode {
    #[allow(dead_code)]
    name: &'static str,
    consumer: NodeConsumerItems,
    producer: NodeProducerItems,
}

impl SimulatedNode {
    fn new(name: &'static str, level: MetricLevel) -> Self {
        Self {
            name,
            consumer: NodeConsumerItems::new(level),
            producer: NodeProducerItems::new(level),
        }
    }

    /// Simulate some work: random counter increments.
    fn simulate_traffic(&mut self, rng: &mut impl Rng) {
        let signals = [SignalType::Logs, SignalType::Metrics, SignalType::Traces];

        for &signal in &signals {
            // Most items succeed
            let success_count: u64 = rng.random_range(10..100);
            self.consumer
                .add(success_count, Outcome::Success, signal);
            self.producer
                .add(success_count.saturating_sub(rng.random_range(0..5)), Outcome::Success, signal);

            // Occasional failures
            if rng.random_bool(0.3) {
                let fail_count: u64 = rng.random_range(1..5);
                self.consumer
                    .add(fail_count, Outcome::Failure, signal);
            }

            // Rare refused
            if rng.random_bool(0.1) {
                self.consumer
                    .add(1, Outcome::Refused, signal);
            }
        }
    }
}

const SCHEMA_KEY: &str = "pipeline.node";

#[tokio::main]
async fn main() {
    let level = MetricLevel::Normal;
    let schema = precomputed_schema(level).expect("Normal level should produce a schema");
    let exporter = PrometheusExporter::new();
    exporter.register_schema(SCHEMA_KEY, schema.clone());

    // Create simulated pipeline nodes
    let mut nodes = vec![
        SimulatedNode::new("otlp_receiver", level),
        SimulatedNode::new("batch_processor", level),
        SimulatedNode::new("otlp_exporter", level),
        SimulatedNode::new("filter_processor", level),
    ];

    // In production, each node gets a distinct EntityKey from the
    // telemetry registry. For this demo, we use a single default key
    // and aggregate across all nodes.
    let identity = MetricIdentity {
        schema_key: SCHEMA_KEY,
        entity_key: EntityKey::default(),
    };

    let exporter_for_server = exporter.clone();

    // Start the HTTP server
    let listener = TcpListener::bind("0.0.0.0:9464")
        .await
        .expect("failed to bind");
    println!("Prometheus metrics available at http://localhost:9464/metrics");
    println!("Press Ctrl+C to stop.\n");

    let _server_handle = tokio::spawn(async move {
        axum::serve(listener, exporter_for_server.router())
            .await
            .expect("server error");
    });

    // Collection loop: simulate traffic and collect metrics every second
    let mut rng = rand::rng();
    let start_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    loop {
        // Simulate traffic on each node
        for node in &mut nodes {
            node.simulate_traffic(&mut rng);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        // Collect: snapshot all nodes, combine, encode, ingest
        let mut all_values = Vec::new();
        // The schema has 2 metrics (consumer, producer) × 9 points each = 18 per node.
        // But the precomputed schema is shared across all nodes — for this demo we
        // merge all nodes' counters into a single aggregate.
        let mut consumer_totals = vec![0u64; 9]; // outcome(3) × signal_type(3)
        let mut producer_totals = vec![0u64; 9];

        for node in &mut nodes {
            if let Some(snap) = node.consumer.snapshot() {
                for (i, v) in snap.iter().enumerate() {
                    consumer_totals[i] += v;
                }
            }
            if let Some(snap) = node.producer.snapshot() {
                for (i, v) in snap.iter().enumerate() {
                    producer_totals[i] += v;
                }
            }
            // Clear after snapshot (delta semantics)
            node.consumer.clear();
            node.producer.clear();
        }

        all_values.extend_from_slice(&consumer_totals);
        all_values.extend_from_slice(&producer_totals);

        // Encode delta batch and ingest
        let builder = schema.data_points_builder();
        match builder.build_int_values(start_time, now, &all_values) {
            Ok(dp_batch) => {
                if let Err(e) = exporter.ingest_delta(identity, &dp_batch) {
                    eprintln!("ingest error: {e}");
                }
            }
            Err(e) => eprintln!("encode error: {e}"),
        }

        // Print a tick
        let consumer_total: u64 = consumer_totals.iter().sum();
        let producer_total: u64 = producer_totals.iter().sum();
        println!(
            "Tick: consumed={consumer_total} produced={producer_total} ({} nodes)",
            nodes.len()
        );

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
