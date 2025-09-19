# Streaming Separate Queries Strategy for DataFusion

**Status**: 📋 **IMPLEMENTATION GUIDE**  
**Date**: September 19, 2025

## Problem Statement

**Goal**: Stream through batches of records containing at most 65,536 distinct IDs, joining:
- **Primary table P**: 1 record per ID
- **Supplemental tables A, R, S**: N records per ID (A has highest row count)

**Constraints**:
- Query "A" is complex and pre-defined
- Need streaming retrieval, not full materialization
- Process in ID-based batches
- All tables share common "id" field for joining
- Avoid Cartesian products that could create massive result sets

**Key Insight**: Instead of joining all tables at once (creating N×M×K rows per ID), query each table separately using the same ID batch, then combine results in application logic.

## Core Strategy: Separate Queries with Temporary MemTable

### Overview

1. **Extract distinct IDs** from complex query A (batch of ≤65,536 IDs)
2. **Create MemTable** with Arrow array containing the ID batch
3. **Register temporary table** for efficient ID filtering
4. **Issue 3 separate queries** for tables P, R, S using the temporary table
5. **Unregister temporary table** and clean up
6. **Process results** independently or combine as needed

### Complete Implementation

```rust
use datafusion::prelude::*;
use datafusion::catalog::memory::MemTable;
use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use futures::stream::{Stream, StreamExt};

pub struct SeparateQueriesJoin {
    ctx: SessionContext,
    batch_size: usize,
    current_offset: usize,
    temp_table_counter: usize,
}

impl SeparateQueriesJoin {
    pub fn new(ctx: SessionContext, batch_size: usize) -> Self {
        Self {
            ctx,
            batch_size,
            current_offset: 0,
            temp_table_counter: 0,
        }
    }

    /// Main streaming function
    pub async fn stream_batches(&mut self) -> datafusion::error::Result<impl Stream<Item = datafusion::error::Result<SeparateQueryResults>> + '_> {
        Ok(futures::stream::unfold(self, |state| async move {
            match state.get_next_batch().await {
                Ok(Some(results)) => Some((Ok(results), state)),
                Ok(None) => None, // End of stream
                Err(e) => Some((Err(e), state)),
            }
        }))
    }

    /// Get the next batch of separate query results
    async fn get_next_batch(&mut self) -> datafusion::error::Result<Option<SeparateQueryResults>> {
        // Step 1: Extract distinct IDs from complex query A
        let ids = self.extract_id_batch().await?;
        if ids.is_empty() {
            return Ok(None);
        }

        // Step 2: Create temporary MemTable with ID batch
        let temp_table_name = format!("batch_ids_{}", self.temp_table_counter);
        self.create_temp_id_table(&ids, &temp_table_name).await?;

        // Step 3: Query each table separately using the temporary table
        let results = self.query_tables_separately(&temp_table_name).await?;

        // Step 4: Clean up temporary table
        self.ctx.deregister_table(&temp_table_name)?;
        
        self.current_offset += self.batch_size;
        self.temp_table_counter += 1;
        
        Ok(Some(results))
    }

    /// Step 1: Extract distinct IDs from complex query A
    async fn extract_id_batch(&self) -> datafusion::error::Result<Vec<i64>> {
        let query = format!(
            r#"
            SELECT DISTINCT id 
            FROM (
                {}  -- Your complex query A
            ) AS a
            ORDER BY id
            LIMIT {} OFFSET {}
            "#,
            self.get_complex_query_a(),
            self.batch_size,
            self.current_offset
        );

        let df = self.ctx.sql(&query).await?;
        let batches = df.collect().await?;
        
        let mut ids = Vec::new();
        for batch in batches {
            let id_array = batch.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| datafusion::error::DataFusionError::Internal(
                    "Expected Int64Array for ID column".into()
                ))?;
            
            for i in 0..id_array.len() {
                if !id_array.is_null(i) {
                    ids.push(id_array.value(i));
                }
            }
        }

        Ok(ids)
    }

    /// Step 2: Create MemTable with Arrow array of IDs
    async fn create_temp_id_table(&self, ids: &[i64], table_name: &str) -> datafusion::error::Result<()> {
        // Create Arrow array from IDs - very efficient!
        let id_array = Arc::new(Int64Array::from(ids.to_vec()));
        
        // Create schema for single ID column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
        ]));
        
        // Create RecordBatch with the IDs
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array])?;
        
        // Step 3: Register as temporary MemTable
        let mem_table = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
        self.ctx.register_table(table_name, mem_table)?;
        
        Ok(())
    }

    /// Step 4: Query each table separately to avoid Cartesian products
    async fn query_tables_separately(&self, temp_table_name: &str) -> datafusion::error::Result<SeparateQueryResults> {
        // Query P table (primary - should be 1 record per ID)
        let p_query = format!(
            "SELECT p.* FROM {} AS ids INNER JOIN P ON P.id = ids.id ORDER BY ids.id", 
            temp_table_name
        );
        
        // Query A table (your complex query results)  
        let a_query = format!(
            "SELECT a.* FROM {} AS ids INNER JOIN ({}) AS A ON A.id = ids.id ORDER BY ids.id, A.timestamp", 
            temp_table_name,
            self.get_complex_query_a()
        );
        
        // Query R table (supplemental)
        let r_query = format!(
            "SELECT r.* FROM {} AS ids INNER JOIN R ON R.id = ids.id ORDER BY ids.id", 
            temp_table_name
        );
        
        // Query S table (supplemental)
        let s_query = format!(
            "SELECT s.* FROM {} AS ids INNER JOIN S ON S.id = ids.id ORDER BY ids.id", 
            temp_table_name
        );

        // Execute all queries in parallel
        let (p_result, a_result, r_result, s_result) = futures::try_join!(
            self.ctx.sql(&p_query).and_then(|df| df.collect()),
            self.ctx.sql(&a_query).and_then(|df| df.collect()),
            self.ctx.sql(&r_query).and_then(|df| df.collect()),
            self.ctx.sql(&s_query).and_then(|df| df.collect())
        )?;

        Ok(SeparateQueryResults {
            p_batches: p_result,
            a_batches: a_result,
            r_batches: r_result,
            s_batches: s_result,
        })
    }

    /// Your complex query A - implement this with your actual query
    fn get_complex_query_a(&self) -> &str {
        r#"
        SELECT 
            id,
            timestamp_unix_nano as timestamp,
            severity_number,
            body,
            trace_id
        FROM logs 
        WHERE timestamp_unix_nano >= 1695024000000000000
        AND severity_number >= 17
        "#
    }
}

/// Results from separate queries - no Cartesian products!
#[derive(Debug)]
pub struct SeparateQueryResults {
    pub p_batches: Vec<RecordBatch>,  // Primary table results
    pub a_batches: Vec<RecordBatch>,  // Complex query A results  
    pub r_batches: Vec<RecordBatch>,  // R table results
    pub s_batches: Vec<RecordBatch>,  // S table results
}

impl SeparateQueryResults {
    /// Count total rows across all tables
    pub fn total_rows(&self) -> usize {
        self.p_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.a_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.r_batches.iter().map(|b| b.num_rows()).sum::<usize>() +
        self.s_batches.iter().map(|b| b.num_rows()).sum::<usize>()
    }

    /// Process results maintaining hierarchical relationship
    pub fn process_by_id(&self) -> datafusion::error::Result<()> {
        // Group results by ID and process hierarchically
        println!("P records: {}", self.p_batches.iter().map(|b| b.num_rows()).sum::<usize>());
        println!("A records: {}", self.a_batches.iter().map(|b| b.num_rows()).sum::<usize>());
        println!("R records: {}", self.r_batches.iter().map(|b| b.num_rows()).sum::<usize>());
        println!("S records: {}", self.s_batches.iter().map(|b| b.num_rows()).sum::<usize>());
        
        // Your processing logic here - each table's data is separate
        // No need to de-duplicate from Cartesian products!
        
        Ok(())
    }
}
```

## Alternative: Using DataFusion Logical Query Constructors

**Avoid SQL entirely by using DataFusion's DataFrame API:**

```rust
use datafusion::logical_expr::JoinType;

impl SeparateQueriesJoin {
    /// Alternative approach using DataFrame API instead of SQL
    async fn query_tables_with_dataframe_api(&self, temp_table_name: &str) -> datafusion::error::Result<SeparateQueryResults> {
        // Get the temporary ID table as DataFrame
        let ids_df = self.ctx.table(temp_table_name).await?;
        
        // Get table DataFrames
        let p_df = self.ctx.table("P").await?;
        let r_df = self.ctx.table("R").await?;  
        let s_df = self.ctx.table("S").await?;
        
        // Get complex query A as DataFrame
        let a_df = self.ctx.sql(&self.get_complex_query_a()).await?;
        
        // Perform joins using DataFrame API - no SQL parsing!
        let p_joined = ids_df.clone()
            .join(p_df, JoinType::Inner, &["id"], &["id"], None)?
            .sort(vec![col("id")])?;
            
        let a_joined = ids_df.clone()
            .join(a_df, JoinType::Inner, &["id"], &["id"], None)?
            .sort(vec![col("id"), col("timestamp")])?;
            
        let r_joined = ids_df.clone()
            .join(r_df, JoinType::Inner, &["id"], &["id"], None)?
            .sort(vec![col("id")])?;
            
        let s_joined = ids_df
            .join(s_df, JoinType::Inner, &["id"], &["id"], None)?
            .sort(vec![col("id")])?;

        // Collect results in parallel
        let (p_result, a_result, r_result, s_result) = futures::try_join!(
            p_joined.collect(),
            a_joined.collect(),
            r_joined.collect(),
            s_joined.collect()
        )?;

        Ok(SeparateQueryResults {
            p_batches: p_result,
            a_batches: a_result,
            r_batches: r_result,
            s_batches: s_result,
        })
    }
}
```

## Usage Example

```rust
#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    
    // Register your tables
    setup_tables(&ctx).await?;
    
    // Create streaming processor
    let mut processor = SeparateQueriesJoin::new(ctx, 65536);
    let mut stream = processor.stream_batches().await?;
    
    let mut batch_count = 0;
    let mut total_rows = 0;
    
    // Process each batch of separate query results
    while let Some(results_result) = stream.next().await {
        let results = results_result?;
        batch_count += 1;
        
        let batch_rows = results.total_rows();
        total_rows += batch_rows;
        
        println!("Batch {}: {} total rows across all tables", batch_count, batch_rows);
        println!("  P: {} rows, A: {} rows, R: {} rows, S: {} rows",
                 results.p_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                 results.a_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                 results.r_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                 results.s_batches.iter().map(|b| b.num_rows()).sum::<usize>());
        
        // Process the separate results
        results.process_by_id()?;
    }
    
    println!("Completed: {} batches, {} total rows", batch_count, total_rows);
    Ok(())
}

async fn setup_tables(ctx: &SessionContext) -> datafusion::error::Result<()> {
    // Register your actual tables here
    ctx.sql("CREATE TABLE P (id BIGINT, name VARCHAR, category VARCHAR)").await?;
    ctx.sql("CREATE TABLE R (id BIGINT, resource_type VARCHAR, allocation BIGINT)").await?;
    ctx.sql("CREATE TABLE S (id BIGINT, status VARCHAR, last_updated TIMESTAMP)").await?;
    
    // Register any tables used in your complex query A
    // ctx.register_table("logs", your_logs_table)?;
    
    Ok(())
}
```

## Key Benefits

1. **No Cartesian Products**: Each table queried separately avoids N×M×K explosion
2. **Efficient ID Filtering**: MemTable with Arrow array handles 65K IDs efficiently
3. **Memory Efficient**: Fixed batch sizes, temporary table cleanup
4. **Parallel Execution**: All queries execute concurrently
5. **Hierarchical Results**: Preserve natural table relationships
6. **Flexible Processing**: Handle each table's results independently

## Memory Usage

**For 65,536 Int64 IDs:**
- MemTable: ~512KB Arrow array (8 bytes × 65,536)
- Temporary registration: minimal overhead
- Query results: only actual data rows (no multiplication)

**Example with 1000 IDs:**
- A: 10,000 rows (10 avg per ID)
- R: 5,000 rows (5 avg per ID)  
- S: 2,000 rows (2 avg per ID)
- **Total**: 17,000 rows (manageable)
- **vs Cartesian**: 100,000,000 rows (too large!)

This approach provides efficient streaming with predictable memory usage and clean separation of concerns.