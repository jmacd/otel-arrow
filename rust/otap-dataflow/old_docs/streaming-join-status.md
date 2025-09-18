# Streaming Join Implementation Status

## Overview
We have implemented a direct object store streaming join for reconstructing OTAP records from Parquet files, bypassing DataFusion and using arrow-rs directly. The implementation uses cursor-based streaming to coordinate parent-child table joins.

## What We've Implemented

### Core Components
1. **DirectStreamingMerger** - Main streaming join coordinator
2. **StreamingCoordinator** - Orchestrates batch processing and OTAP conversion
3. **DirectParquetStreamReader** - Reads Parquet files in timestamp order
4. **IdMapper** - Handles ID normalization (UInt32 → UInt16) for OTAP batches
5. **FileDiscovery** - Stateless file discovery mechanism

### Key Features
- **Range-based cursor streaming**: Collects parent_ids in specific ranges [min_id, max_id] rather than just "≤ max_id"
- **Cursor position maintenance**: Tracks position in attribute files to avoid re-reading
- **Batch self-containment**: Each OTAP batch has matching log IDs and log_attrs parent_ids
- **ID normalization**: Converts UInt32 parent_ids to UInt16 for OTAP compatibility
- **Comprehensive diagnostics**: Added logging for parent_id ranges, cursor positions, and attribute sampling

## Current Problem

### Symptoms
- First ~19 log records have correct attributes
- Records #20+ have empty attributes
- Pipeline runs successfully but produces incomplete results

### Expected Behavior
```
LogRecord #0 -> Attributes: ios.app.state: active, android.app.state: created
LogRecord #1 -> Attributes: rpc.message.type: SENT, rpc.message.id: 42
...
LogRecord #19 -> Attributes: [some attributes]
LogRecord #20 -> Attributes: [should have attributes but empty]
```

### Actual Behavior
```
LogRecord #20+ -> Attributes: [empty]
```

## Implementation Details

### Streaming Join Logic
1. **Primary table scan**: Read logs batch, get parent_id range [min_id, max_id]
2. **Child table coordination**: Use cursor-based streaming to collect all log_attrs where `min_id ≤ parent_id ≤ max_id`
3. **Batch assembly**: Combine logs + log_attrs into single OTAP batch
4. **ID normalization**: Convert UInt32 parent_ids to UInt16 for OTAP
5. **Cursor advancement**: Maintain position in attribute files for next iteration

### Key Methods
- `DirectStreamingMerger::read_next_merge_batch()` - Main coordination
- `DirectStreamingMerger::read_log_attrs_with_cursor_range()` - Range-based attribute collection
- `StreamingCoordinator::batch_to_otap()` - OTAP conversion
- `IdMapper::normalize_ids()` - ID space conversion

### File Structure
```
parquet_receiver/
├── mod.rs                          # Entry point (DataFusion removed)
├── streaming_coordinator.rs        # Main orchestration
├── direct_streaming_merger.rs      # Core streaming join logic
├── direct_stream_reader.rs         # Parquet file reader
├── id_mapping.rs                   # UInt32 → UInt16 conversion
└── file_discovery.rs              # Stateless file discovery
```

## Diagnostics Added
- Parent_id range logging: `🔍 Primary ID range: X to Y`
- Cursor position tracking: `📦 Processing buffered records from cursor position Z`
- Range extraction results: `🎯 Range extraction: found N matching records`
- Attribute sampling: `🔍 log_attrs parent_id samples from N rows`
- OTAP conversion tracking: `⚙️ Converting batch to OTAP`

## Investigation Status

### What's Working
✅ File discovery and ordering
✅ Range-based cursor streaming logic
✅ Parent_id range calculation
✅ ID normalization
✅ OTAP batch construction
✅ First ~19 records have correct attributes

### What's Not Working
❌ Attributes missing for records #20+
❌ Debug logs not showing in output (indicating possible code path issue)
❌ Complete parent_id coverage in later batches

## Next Steps for Debugging

1. **Verify code path**: Ensure our streaming coordinator is actually being called
2. **Check ID correlation**: Verify parent_id mapping between logs and log_attrs
3. **Examine cursor state**: Check if cursor position is correctly maintained across batches
4. **Validate range logic**: Confirm range [min_id, max_id] collection is working
5. **Test attribute file coverage**: Verify all parent_ids have corresponding attribute records

## Test Environment
- Files: `output_parquet_files/{logs,log_attrs}/_part_id={uuid}/part-*.parquet`
- Test script: `test_parquet_receiver.sh`
- Debug output: `RUST_LOG=debug timeout 15 ./test_parquet_receiver.sh 2>OUT 1>OUT`
- Parquet inspection: `./catparquet.sh` (uses duckdb)

## Key Hypothesis
The streaming join logic is working correctly for the first batch (parent_ids 0-99) but failing to maintain proper cursor state or parent_id correlation for subsequent batches (100-199, 200-299, etc.). The issue likely lies in either:

1. **Cursor management**: Buffer extraction not maintaining correct position
2. **ID normalization**: Mismatch between UInt32 parent_ids and UInt16 normalized IDs
3. **Range coverage**: Gap in parent_id ranges being collected
4. **Batch boundaries**: Incorrect splitting of parent_id ranges across OTAP batches

The fact that exactly the first ~19-20 records work suggests a batch boundary issue where the first small batch works but subsequent batches fail.