#!/bin/bash
set -e

# Fake Logs to Parquet Streaming Example
# =====================================
# This script demonstrates how to use the otap-dataflow engine to generate
# fake log data and stream it to parquet files.

echo "🚀 Starting OTel-Arrow Fake Logs to Parquet Streaming Example"
echo "============================================================="

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ]; then
    echo "❌ Error: Please run this script from the rust/otap-dataflow directory"
    echo "   Expected to find Cargo.toml in current directory"
    exit 1
fi

# Create output directory
OUTPUT_DIR="./output_parquet_files"
echo "📁 Creating output directory: $OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

# Clean any existing output files for a fresh start
if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A $OUTPUT_DIR)" ]; then
    echo "🧹 Cleaning existing parquet files..."
    rm -rf "${OUTPUT_DIR:?}"/*
fi

echo ""
echo "⚙️  Configuration Summary:"
echo "   - Config file: configs/fake-logs-parquet-streaming.yaml"
echo "   - Output directory: $OUTPUT_DIR"
echo "   - Signal rate: 10 logs/second"
echo "   - Batch size: 100 logs per batch"
echo "   - File flush interval: 15 seconds"
echo "   - Target rows per file: 1000"
echo ""

echo "🔧 Building the dataflow engine..."
cargo build --bin df_engine

echo ""
echo "🎯 Starting the streaming pipeline..."
echo "   Press Ctrl+C to stop the pipeline"
echo "   You can monitor the output directory for new parquet files"
echo ""

# Function to handle cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping pipeline..."
    
    # List generated files
    if [ -d "$OUTPUT_DIR" ] && [ "$(ls -A $OUTPUT_DIR)" ]; then
        echo ""
        echo "📊 Generated files:"
        find "$OUTPUT_DIR" -name "*.parquet" -exec ls -lah {} \; | head -10
        
        total_files=$(find "$OUTPUT_DIR" -name "*.parquet" | wc -l)
        echo ""
        echo "📈 Summary: $total_files parquet files generated"
        
        echo ""
        echo "💡 Next steps:"
        echo "   - Explore the generated parquet files in: $OUTPUT_DIR"
        echo "   - Use the parquet-query-examples to query the data:"
        echo "     cd ../parquet-query-examples"
        echo "     cargo run --example query_logs"
        echo "   - Or use any parquet-compatible tool (DuckDB, Apache Arrow, etc.)"
    else
        echo "ℹ️  No parquet files were generated (pipeline may not have run long enough)"
    fi
    
    echo ""
    echo "✅ Example completed!"
}

# Set up cleanup trap
trap cleanup EXIT INT TERM

# Run the dataflow engine with our streaming config
echo "Starting pipeline with fake-logs-parquet-streaming.yaml..."
cargo run --bin df_engine -- --pipeline configs/fake-logs-parquet-streaming.yaml
