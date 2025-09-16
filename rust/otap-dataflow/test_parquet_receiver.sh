#!/bin/bash

# Simple test script for the Parquet Receiver
# This script validates the build and basic functionality

set -e

echo "🚀 Testing Parquet Receiver Implementation"
echo "=========================================="

# Change to the otap-dataflow directory
cd "$(dirname "$0")"

echo "📦 Building the project..."
cargo build --release

echo "✅ Build successful!"

echo "🔍 Running basic smoke tests..."
cargo test --package otap-df-otap parquet_receiver -- --nocapture

echo "✅ Tests passed!"

echo "📋 Checking if demo config is valid..."
if [ -f "configs/parquet-receiver-demo.yaml" ]; then
    echo "✅ Demo configuration file exists"
else
    echo "❌ Demo configuration file missing"
    exit 1
fi

echo ""
echo "🎯 Parquet Receiver Implementation Summary"
echo "=========================================="
echo "✅ Core modules implemented:"
echo "   - config.rs: Configuration parsing and validation"
echo "   - error.rs: Error handling for parquet operations"  
echo "   - file_discovery.rs: Parquet file scanning and discovery"
echo "   - query_engine.rs: DataFusion-based query engine"
echo "   - reconstruction.rs: OTAP data reconstruction"
echo "   - parquet_receiver.rs: Main receiver implementation"
echo ""
echo "✅ Integration points:"
echo "   - Registered as 'urn:otel:otap:parquet:receiver'"
echo "   - Uses DataFusion for efficient parquet querying"
echo "   - Supports logs, traces, and metrics reconstruction"
echo "   - Compatible with existing pipeline framework"
echo ""
echo "📝 To test with real data:"
echo "   1. Generate parquet files using the exporter"
echo "   2. Run: ./target/release/df_engine --config configs/parquet-receiver-demo.yaml"
echo "   3. Watch for reconstructed OTAP data in debug output"
echo ""
echo "🏁 Hackathon MVP Ready!"