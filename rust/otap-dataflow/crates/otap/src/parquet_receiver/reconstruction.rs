// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP data reconstruction from DataFusion query results
//!
//! This module handles converting DataFusion RecordBatch results back to the 
//! OtapArrowRecords format that can be sent through the pipeline.

use crate::parquet_receiver::{
    config::SignalType,
    error::ParquetReceiverError,
    query_engine::QueryResult,
};
use arrow::record_batch::RecordBatch;
use otel_arrow_rust::otap::{Logs, Metrics, OtapArrowRecords, Traces};
use otel_arrow_rust::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// OTAP data reconstructor
pub struct OtapReconstructor {
    // Configuration for reconstruction options
    _batch_size_limit: Option<usize>,
}

impl OtapReconstructor {
    /// Create a new OTAP reconstructor
    pub fn new(batch_size_limit: Option<usize>) -> Self {
        Self {
            _batch_size_limit: batch_size_limit,
        }
    }

    /// Reconstruct OTAP data from query results
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
        let mut otap_records = OtapArrowRecords::Logs(Logs::default());

        // Set the main logs data
        if !query_result.main_records.is_empty() {
            // Combine all main record batches into one
            let combined_logs = combine_record_batches(&query_result.main_records)?;
            otap_records.set(ArrowPayloadType::Logs, combined_logs);
        }

        // Set related attribute data
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                let combined_records = combine_record_batches(&records)?;
                let payload_type = file_type_to_payload_type(&file_type)?;
                otap_records.set(payload_type, combined_records);
            }
        }

        Ok(otap_records)
    }

    /// Reconstruct traces data from query results  
    fn reconstruct_traces(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut otap_records = OtapArrowRecords::Traces(Traces::default());

        // Set the main spans data
        if !query_result.main_records.is_empty() {
            let combined_spans = combine_record_batches(&query_result.main_records)?;
            otap_records.set(ArrowPayloadType::Spans, combined_spans);
        }

        // Set related attribute and event data
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                let combined_records = combine_record_batches(&records)?;
                let payload_type = file_type_to_payload_type(&file_type)?;
                otap_records.set(payload_type, combined_records);
            }
        }

        Ok(otap_records)
    }

    /// Reconstruct metrics data from query results
    fn reconstruct_metrics(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        let mut otap_records = OtapArrowRecords::Metrics(Metrics::default());

        // Set the main metrics data
        if !query_result.main_records.is_empty() {
            let combined_metrics = combine_record_batches(&query_result.main_records)?;
            otap_records.set(ArrowPayloadType::UnivariateMetrics, combined_metrics);
        }

        // Set related data points and attributes
        for (file_type, records) in query_result.related_records {
            if !records.is_empty() {
                let combined_records = combine_record_batches(&records)?;
                let payload_type = file_type_to_payload_type(&file_type)?;
                otap_records.set(payload_type, combined_records);
            }
        }

        Ok(otap_records)
    }

    /// Reconstruct data with simplified approach (just combine tables without complex joins)
    /// This is the hackathon MVP approach
    pub fn reconstruct_simple(
        &self,
        query_result: QueryResult,
    ) -> Result<OtapArrowRecords, ParquetReceiverError> {
        match query_result.signal_type {
            SignalType::Logs => {
                let mut otap_records = OtapArrowRecords::Logs(Logs::default());

                // Set main logs data if present
                if !query_result.main_records.is_empty() {
                    let main_batch = combine_record_batches(&query_result.main_records)?;
                    otap_records.set(ArrowPayloadType::Logs, main_batch);
                }

                // Set each related table as separate payload types
                for (file_type, record_batches) in query_result.related_records {
                    if !record_batches.is_empty() {
                        let combined_batch = combine_record_batches(&record_batches)?;
                        let payload_type = file_type_to_payload_type(&file_type)?;
                        otap_records.set(payload_type, combined_batch);
                    }
                }

                Ok(otap_records)
            }
            SignalType::Traces => {
                // Similar pattern for traces
                let mut otap_records = OtapArrowRecords::Traces(Traces::default());

                if !query_result.main_records.is_empty() {
                    let main_batch = combine_record_batches(&query_result.main_records)?;
                    otap_records.set(ArrowPayloadType::Spans, main_batch);
                }

                for (file_type, record_batches) in query_result.related_records {
                    if !record_batches.is_empty() {
                        let combined_batch = combine_record_batches(&record_batches)?;
                        let payload_type = file_type_to_payload_type(&file_type)?;
                        otap_records.set(payload_type, combined_batch);
                    }
                }

                Ok(otap_records)
            }
            SignalType::Metrics => {
                // Similar pattern for metrics
                let mut otap_records = OtapArrowRecords::Metrics(Metrics::default());

                if !query_result.main_records.is_empty() {
                    let main_batch = combine_record_batches(&query_result.main_records)?;
                    otap_records.set(ArrowPayloadType::UnivariateMetrics, main_batch);
                }

                for (file_type, record_batches) in query_result.related_records {
                    if !record_batches.is_empty() {
                        let combined_batch = combine_record_batches(&record_batches)?;
                        let payload_type = file_type_to_payload_type(&file_type)?;
                        otap_records.set(payload_type, combined_batch);
                    }
                }

                Ok(otap_records)
            }
        }
    }
}

impl Default for OtapReconstructor {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Combine multiple record batches into a single batch
fn combine_record_batches(
    record_batches: &[RecordBatch],
) -> Result<RecordBatch, ParquetReceiverError> {
    if record_batches.is_empty() {
        return Err(ParquetReceiverError::Reconstruction(
            "Cannot combine empty record batches".to_string(),
        ));
    }

    if record_batches.len() == 1 {
        return Ok(record_batches[0].clone());
    }

    // Use Arrow's concat_batches to combine multiple batches
    let schema = record_batches[0].schema();
    arrow::compute::concat_batches(&schema, record_batches).map_err(|e| {
        ParquetReceiverError::Reconstruction(format!("Failed to combine record batches: {}", e))
    })
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
    fn test_combine_record_batches() {
        let batch1 = create_test_record_batch();
        let batch2 = create_test_record_batch();

        let combined = combine_record_batches(&[batch1, batch2]).unwrap();
        assert_eq!(combined.num_rows(), 6); // 3 + 3 rows
        assert_eq!(combined.num_columns(), 2);
    }

    #[test]
    fn test_combine_single_batch() {
        let batch = create_test_record_batch();
        let combined = combine_record_batches(&[batch.clone()]).unwrap();
        assert_eq!(combined.num_rows(), batch.num_rows());
        assert_eq!(combined.num_columns(), batch.num_columns());
    }

    #[test]
    fn test_combine_empty_batches() {
        let result = combine_record_batches(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_reconstructor_creation() {
        let _reconstructor = OtapReconstructor::new(Some(1000));
        // Basic creation test
        assert!(true);

        let _default_reconstructor = OtapReconstructor::default();
        assert!(true);
    }

    #[test]
    fn test_reconstruct_simple_logs() {
        let reconstructor = OtapReconstructor::new(None);
        let batch = create_test_record_batch();

        let query_result = QueryResult {
            main_records: vec![batch],
            related_records: HashMap::new(),
            partition_id: Uuid::new_v4(),
            signal_type: SignalType::Logs,
        };

        let _result = reconstructor.reconstruct_simple(query_result).unwrap();
        
        // Verify we get back an OtapArrowRecords with logs data
        // This is a basic test - in a real scenario we'd check the actual data content
        assert!(true);
    }
}