use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use arrow::datatypes::DataType;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    println!("Testing DataFusion schema inference with our parquet files...");
    
    let ctx = SessionContext::new();
    
    // Test 1: Try simplest possible case - no partition columns, just point to directory
    println!("\n=== Test 1: Minimal configuration ===");
    let table_url = ListingTableUrl::parse("file:///home/jmacd/src/otel/otel-arrow-tail-sampler/rust/otap-dataflow/output_parquet_files/log_attrs")?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet");
        
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options);
        
    match ListingTable::try_new(config) {
        Ok(table) => {
            println!("✅ SUCCESS: ListingTable created without partition columns");
            println!("Schema: {:?}", table.schema());
            
            ctx.register_table("test_table", Arc::new(table))?;
            let df = ctx.sql("SELECT COUNT(*) FROM test_table").await?;
            df.show().await?;
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
        }
    }
    
    // Test 2: Try with partition columns
    println!("\n=== Test 2: With partition columns ===");
    let table_url = ListingTableUrl::parse("file:///home/jmacd/src/otel/otel-arrow-tail-sampler/rust/otap-dataflow/output_parquet_files/log_attrs")?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet")
        .with_table_partition_cols(vec![
            ("_part_id".to_string(), DataType::Utf8),
        ]);
        
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options);
        
    match ListingTable::try_new(config) {
        Ok(table) => {
            println!("✅ SUCCESS: ListingTable created with partition columns");
            println!("Schema: {:?}", table.schema());
            
            ctx.deregister_table("test_table")?;
            ctx.register_table("test_table_partitioned", Arc::new(table))?;
            let df = ctx.sql("SELECT _part_id, COUNT(*) FROM test_table_partitioned GROUP BY _part_id").await?;
            df.show().await?;
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
        }
    }
    
    // Test 3: Try pointing to a single partition directory
    println!("\n=== Test 3: Single partition directory ===");
    let table_url = ListingTableUrl::parse("file:///home/jmacd/src/otel/otel-arrow-tail-sampler/rust/otap-dataflow/output_parquet_files/log_attrs/_part_id=d9607e81-41df-4fc4-90ba-705f73da592f")?;
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("parquet");
        
    let config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options);
        
    match ListingTable::try_new(config) {
        Ok(table) => {
            println!("✅ SUCCESS: ListingTable created for single partition");
            println!("Schema: {:?}", table.schema());
            
            ctx.deregister_table("test_table")?;
            ctx.register_table("test_single_partition", Arc::new(table))?;
            let df = ctx.sql("SELECT COUNT(*) FROM test_single_partition").await?;
            df.show().await?;
        }
        Err(e) => {
            println!("❌ FAILED: {}", e);
        }
    }
    
    Ok(())
}