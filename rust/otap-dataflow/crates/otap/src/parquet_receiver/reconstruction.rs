// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP data reconstruction from parquet RecordBatch results
//!
//! This module handles converting DataFusion query results (RecordBatch) back into
//! OTAP data structures using otel-arrow-rust utilities.

use crate::parquet_receiver::{
    config::SignalType,
    error::ParquetReceiverError,
    query_engine::QueryResult,
};
use arrow::record_batch::RecordBatch;
use otel_arrow_rust::otap::{Logs, Metrics, OtapArrowRecords, OtapBatchStore, Traces};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// OTAP data reconstructor that converts DataFusion query results into OTAP records
pub struct OtapReconstructor {
    /// Optional batch size limit for processing
    batch_size_limit: Option<usize>,
}

impl OtapReconstructor {
    /// Create a new OTAP reconstructor
    pub fn new(batch_size_limit: Option<usize>) -> Self {
        Self { batch_size_limit }
    }

    /// Reconstruct OTAP data from DataFusion query results
    pub fn reconstruct_otap_data(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        match query_result.signal_type {
            SignalType::Logs => self.reconstruct_logs(query_result),
            SignalType::Traces => self.reconstruct_traces(query_result),
            SignalType::Metrics => self.reconstruct_metrics(query_result),
        }
    }

    /// Reconstruct logs data from query results
    fn reconstruct_logs(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        log::debug!("🔄 Starting logs reconstruction for partition: {}", query_result.partition_id);
        
        let mut logs = Logs::default();

        // Set the main logs data
        if !query_result.main_records.is_empty() {
            log::debug!("📊 Processing {} main log record batches", query_result.main_records.len());
            
            // Log input schemas before combining
            for (idx, batch) in query_result.main_records.iter().enumerate() {
                log::debug!("🔍 Input batch {} schema ({} rows):", idx, batch.num_rows());
                for field in batch.schema().fields() {
                    log::debug!("  '{}' -> {:?}", field.name(), field.data_type());
                }
            }
            
            let combined_logs = self.combine_record_batches(&query_result.main_records)?;
            if let Some(batch) = combined_logs {
                log::debug!("✅ Combined logs batch: {} rows, {} columns", 
                           batch.num_rows(), batch.num_columns());
                           
                // Log combined schema before OTAP conversion
                log::debug!("🎯 Final combined logs schema:");
                for field in batch.schema().fields() {
                    log::debug!("  '{}' -> {:?}", field.name(), field.data_type());
                }
                
                // Set the batch in OTAP logs
                logs.set(ArrowPayloadType::Logs, batch);
                log::debug!("✅ Successfully set logs batch in OTAP");
            }
        } else {
            log::debug!("⚠️ No main logs records found");
        }

        // Set related attribute data
        log::debug!("🔍 Processing {} related record types", query_result.related_records.len());
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                log::debug!("📊 Processing {} records of type '{}'", records.len(), file_type);
                
                // Log input schemas for related records
                for (idx, batch) in records.iter().enumerate() {
                    log::debug!("🔍 {} batch {} schema ({} rows):", file_type, idx, batch.num_rows());
                    for field in batch.schema().fields() {
                        log::debug!("  '{}' -> {:?}", field.name(), field.data_type());
                    }
                }
                
                let combined_records = self.combine_record_batches(&records)?;
                if let Some(batch) = combined_records {
                    let payload_type = file_type_to_payload_type(&file_type)?;
                    log::debug!("✅ Combined {} batch: {} rows, {} columns", 
                               file_type, batch.num_rows(), batch.num_columns());
                               
                    // Log final schema before OTAP conversion
                    log::debug!("🎯 Final {} schema:", file_type);
                    for field in batch.schema().fields() {
                        log::debug!("  '{}' -> {:?}", field.name(), field.data_type());
                    }
                    
                    // Set the batch in OTAP with payload type logging
                    log::debug!("🔄 Setting {} batch with payload type: {:?}", file_type, payload_type);
                    logs.set(payload_type, batch);
                    log::debug!("✅ Successfully set {} batch in OTAP", file_type);
                } else {
                    log::debug!("⚠️ No combined batch for file type '{}'", file_type);
                }
            } else {
                log::debug!("⚠️ Empty records for file type '{}'", file_type);
            }
        }

        let result = OtapArrowRecords::Logs(logs);
        
        log::debug!("🎉 Logs reconstruction complete - batch length: {}", result.batch_length());
        Ok(result)
    }

    /// Reconstruct traces data from query results  
    fn reconstruct_traces(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut traces = Traces::default();

        // Set the main spans data
        if !query_result.main_records.is_empty() {
            let combined_spans = self.combine_record_batches(&query_result.main_records)?;
            if let Some(batch) = combined_spans {
                traces.set(ArrowPayloadType::Spans, batch);
            }
        }

        // Set related attribute and event data
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                let combined_records = self.combine_record_batches(&records)?;
                if let Some(batch) = combined_records {
                    let payload_type = file_type_to_payload_type(&file_type)?;
                    traces.set(payload_type, batch);
                }
            }
        }

        Ok(OtapArrowRecords::Traces(traces))
    }

    /// Reconstruct metrics data from query results
    fn reconstruct_metrics(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut metrics = Metrics::default();

        // Set the main metrics data
        if !query_result.main_records.is_empty() {
            let combined_metrics = self.combine_record_batches(&query_result.main_records)?;
            if let Some(batch) = combined_metrics {
                metrics.set(ArrowPayloadType::UnivariateMetrics, batch);
            }
        }

        // Set related data points and attributes
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                let combined_records = self.combine_record_batches(&records)?;
                if let Some(batch) = combined_records {
                    let payload_type = file_type_to_payload_type(&file_type)?;
                    metrics.set(payload_type, batch);
                }
            }
        }

        Ok(OtapArrowRecords::Metrics(metrics))
    }

    /// Combine multiple RecordBatches into a single batch
    /// This handles cases where DataFusion returns multiple batches for a single table
    fn combine_record_batches(
        &self,
        batches: &[RecordBatch],
    ) -> Result<Option<RecordBatch>, ParquetReceiverError> {
        match batches.len() {
            0 => Ok(None),
            1 => {
                let batch = &batches[0];
                // Apply batch size limit if configured
                if let Some(limit) = self.batch_size_limit {
                    if batch.num_rows() > limit {
                        let limited_batch = batch.slice(0, limit);
                        Ok(Some(limited_batch))
                    } else {
                        Ok(Some(batch.clone()))
                    }
                } else {
                    Ok(Some(batch.clone()))
                }
            }
            _ => {
                // Log schemas of all batches before combining
                log::debug!("🔄 Combining {} batches:", batches.len());
                for (idx, batch) in batches.iter().enumerate() {
                    log::debug!("  Batch {} schema ({} rows):", idx, batch.num_rows());
                    for field in batch.schema().fields() {
                        log::debug!("    '{}' -> {:?}", field.name(), field.data_type());
                    }
                }
                
                // Combine multiple batches using Arrow's concat_batches
                let schema = batches[0].schema();
                let combined = arrow::compute::concat_batches(&schema, batches)
                    .map_err(|e| {
                        log::error!("❌ Failed to combine batches: {}", e);
                        log::error!("   Reference schema (batch 0):");
                        for field in schema.fields() {
                            log::error!("     '{}' -> {:?}", field.name(), field.data_type());
                        }
                        ParquetReceiverError::Arrow(e)
                    })?;
                
                // Apply batch size limit if configured
                if let Some(limit) = self.batch_size_limit {
                    if combined.num_rows() > limit {
                        let limited_batch = combined.slice(0, limit);
                        Ok(Some(limited_batch))
                    } else {
                        Ok(Some(combined))
                    }
                } else {
                    Ok(Some(combined))
                }
            }
        }
    }
}

impl Default for OtapReconstructor {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Convert file type string to ArrowPayloadType
fn file_type_to_payload_type(file_type: &str) -> Result<ArrowPayloadType, ParquetReceiverError> {
    let payload_type = match file_type {
        // Logs related
        "logs" => ArrowPayloadType::Logs,
        "log_attrs" => ArrowPayloadType::LogAttrs,

        // Traces related  
        "spans" => ArrowPayloadType::Spans,
        "span_attrs" => ArrowPayloadType::SpanAttrs,
        "span_events" => ArrowPayloadType::SpanEvents,
        "span_event_attrs" => ArrowPayloadType::SpanEventAttrs,
        "span_links" => ArrowPayloadType::SpanLinks,
        "span_link_attrs" => ArrowPayloadType::SpanLinkAttrs,

        // Metrics related
        "univariate_metrics" => ArrowPayloadType::UnivariateMetrics,
        "number_data_points" => ArrowPayloadType::NumberDataPoints,
        "number_dp_attrs" => ArrowPayloadType::NumberDpAttrs,
        "histogram_data_points" => ArrowPayloadType::HistogramDataPoints,
        "histogram_dp_attrs" => ArrowPayloadType::HistogramDpAttrs,

        // Common across signals
        "resource_attrs" => ArrowPayloadType::ResourceAttrs,
        "scope_attrs" => ArrowPayloadType::ScopeAttrs,

        // Add more mappings as needed
        _ => {
            return Err(ParquetReceiverError::Reconstruction(format!(
                "Unknown file type: {}",
                file_type
            )))
        }
    };

    Ok(payload_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("test1"), Some("test2"), Some("test3")]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_file_type_to_payload_type() {
        assert_eq!(
            file_type_to_payload_type("logs").unwrap(),
            ArrowPayloadType::Logs
        );
        assert_eq!(
            file_type_to_payload_type("log_attrs").unwrap(),
            ArrowPayloadType::LogAttrs
        );
        assert_eq!(
            file_type_to_payload_type("resource_attrs").unwrap(),
            ArrowPayloadType::ResourceAttrs
        );

        // Test unknown type
        assert!(file_type_to_payload_type("unknown_type").is_err());
    }

    #[test]
    fn test_reconstructor_creation() {
        let _reconstructor = OtapReconstructor::new(Some(1000));
        let _default_reconstructor = OtapReconstructor::default();
        assert!(true);
    }

    #[test]
    fn test_reconstruct_logs() {
        let reconstructor = OtapReconstructor::new(None);
        let batch = create_test_record_batch();

        let query_result = QueryResult {
            main_records: vec![batch],
            related_records: HashMap::new(),
            partition_id: Uuid::new_v4(),
            signal_type: SignalType::Logs,
        };

        let result = reconstructor.reconstruct_otap_data(query_result).unwrap();
        
        match result {
            OtapArrowRecords::Logs(_) => assert!(true),
            _ => panic!("Expected logs records"),
        }
    }
}