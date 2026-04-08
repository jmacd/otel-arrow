//! Assembly helper for combining precomputed + runtime tables into OtapPdata.

use arrow::array::RecordBatch;

use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::otap::Metrics;
use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

/// Assemble a complete OTAP metrics payload from the tables.
///
/// - `metrics_batch`: precomputed metrics table (one row per metric).
/// - `attrs_batch`: precomputed attributes table (dimension attrs per data point).
/// - `ndp_batch`: optional NumberDataPoints table (counter/gauge values).
/// - `hdp_batch`: optional HistogramDataPoints table (Mmsc values).
pub fn assemble_metrics_payload(
    metrics_batch: &RecordBatch,
    attrs_batch: &RecordBatch,
    ndp_batch: Option<RecordBatch>,
    hdp_batch: Option<RecordBatch>,
) -> Result<OtapArrowRecords, otap_df_pdata::error::Error> {
    let mut records = OtapArrowRecords::Metrics(Metrics::default());

    if metrics_batch.num_rows() > 0 {
        records.set(ArrowPayloadType::UnivariateMetrics, metrics_batch.clone())?;
    }

    if let Some(ndp) = ndp_batch {
        if ndp.num_rows() > 0 {
            records.set(ArrowPayloadType::NumberDataPoints, ndp)?;
        }
    }

    if let Some(hdp) = hdp_batch {
        if hdp.num_rows() > 0 {
            records.set(ArrowPayloadType::HistogramDataPoints, hdp)?;
        }
    }

    if attrs_batch.num_rows() > 0 {
        records.set(ArrowPayloadType::NumberDpAttrs, attrs_batch.clone())?;
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::self_metrics::precomputed::{CounterMetricDef, PrecomputedMetricSchema};

    #[test]
    fn assemble_roundtrip() {
        let metrics = [CounterMetricDef {
            name: "test.counter",
            unit: "{item}",
            description: "A test counter",
            num_points: 3,
            point_attributes: &[
                ("outcome", "success"),
                ("outcome", "failure"),
                ("outcome", "refused"),
            ],
            attrs_per_point: 1,
        }];

        let schema = PrecomputedMetricSchema::new(&metrics).expect("should build schema");
        let builder = schema.data_points_builder();

        let dp_batch = builder
            .build_int_values(1_000_000_000, 2_000_000_000, &[100, 5, 2])
            .expect("should build data points");

        let records =
            assemble_metrics_payload(schema.metrics_batch(), schema.attrs_batch(), Some(dp_batch), None)
                .expect("should assemble");

        match &records {
            OtapArrowRecords::Metrics(_) => {}
            _ => panic!("expected Metrics variant"),
        }
    }
}
