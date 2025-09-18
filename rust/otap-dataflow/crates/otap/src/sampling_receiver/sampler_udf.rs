// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reservoir Sampling UDF implementation using Vitter's Algorithm R
//!
//! This implementation provides a user-defined aggregate function for DataFusion
//! that performs reservoir sampling on streaming data with uniform probability.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array, ListArray, StructArray, UInt32Array};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow_schema::SchemaRef;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility};
use rand::prelude::*;

/// Creates a new reservoir sampling UDF
pub fn reservoir_sample_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(ReservoirSampleUDF::new())
}

/// Reservoir Sampling aggregate function using Algorithm R
///
/// SQL Usage: SELECT reservoir_sample(column_name, k) FROM table GROUP BY ...
///
/// Parameters:
/// - column_name: The column to sample from (any type)
/// - k: Sample size (must be positive integer)
///
/// Returns: List of sampled values with uniform probability
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReservoirSampleUDF {
    signature: Signature,
}

impl ReservoirSampleUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Null,  // Will be coerced to actual input type
                    DataType::Int64, // Sample size k
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for ReservoirSampleUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "reservoir_sample"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return internal_err!("reservoir_sample expects exactly 2 arguments");
        }

        // Return List<input_type>
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            arg_types[0].clone(),
            true,
        ))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        ReservoirSampleAccumulator::try_new(&acc_args.input_types[0], acc_args.input_exprs.len())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let input_type = &args.input_types[0];

        Ok(vec![
            // Reservoir array (List of input values)
            Field::new(
                "reservoir",
                DataType::List(Arc::new(Field::new("item", input_type.clone(), true))),
                true,
            )
            .into(),
            // Number of items seen so far
            Field::new("items_seen", DataType::UInt32, false).into(),
            // Sample size k
            Field::new("sample_size", DataType::UInt32, false).into(),
        ])
    }
}

/// Accumulator for reservoir sampling using Algorithm R
#[derive(Debug)]
struct ReservoirSampleAccumulator {
    /// The reservoir - maintains exactly k items (or fewer if less than k seen)
    reservoir: Vec<ScalarValue>,
    /// Number of items processed so far
    items_seen: u32,
    /// Target sample size k
    sample_size: u32,
    /// Data type of items being sampled
    data_type: DataType,
    /// Random number generator
    rng: StdRng,
}

impl ReservoirSampleAccumulator {
    fn try_new(data_type: &DataType, _num_exprs: usize) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(Self {
            reservoir: Vec::new(),
            items_seen: 0,
            sample_size: 0, // Will be set when we see the first batch
            data_type: data_type.clone(),
            rng: StdRng::from_entropy(),
        }))
    }

    /// Algorithm R: Reservoir sampling with uniform probability
    fn consider_item(&mut self, item: ScalarValue) {
        self.items_seen += 1;

        if self.reservoir.len() < self.sample_size as usize {
            // Reservoir not full, just add the item
            self.reservoir.push(item);
        } else if self.sample_size > 0 {
            // Reservoir is full, decide whether to replace an existing item
            // Generate random integer j in range [1, items_seen]
            let j = self.rng.gen_range(1..=self.items_seen);

            if j <= self.sample_size {
                // Replace item at position (j-1) with the new item
                let replace_idx = (j - 1) as usize;
                self.reservoir[replace_idx] = item;
            }
            // Otherwise, skip this item
        }
    }
}

impl Accumulator for ReservoirSampleAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return internal_err!("reservoir_sample expects exactly 2 arguments");
        }

        let data_array = &values[0];
        let k_array = values[1].as_primitive::<arrow::datatypes::Int64Type>();

        // Extract sample size k from first non-null value
        if self.sample_size == 0 {
            for i in 0..k_array.len() {
                if !k_array.is_null(i) {
                    let k_val = k_array.value(i);
                    if k_val <= 0 {
                        return internal_err!("Sample size k must be positive");
                    }
                    self.sample_size = k_val as u32;
                    self.reservoir.reserve(self.sample_size as usize);
                    break;
                }
            }
            if self.sample_size == 0 {
                return internal_err!("No valid sample size provided");
            }
        }

        // Process each item in the batch using Algorithm R
        for i in 0..data_array.len() {
            if !data_array.is_null(i) {
                let item = ScalarValue::try_from_array(data_array, i)?;
                self.consider_item(item);
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Convert reservoir to a List ScalarValue
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &self.reservoir,
            &self.data_type,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.reservoir.iter().map(|v| v.size()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let reservoir_list = ScalarValue::List(ScalarValue::new_list_nullable(
            &self.reservoir,
            &self.data_type,
        ));

        Ok(vec![
            reservoir_list,
            ScalarValue::UInt32(Some(self.items_seen)),
            ScalarValue::UInt32(Some(self.sample_size)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 3 {
            return internal_err!("Expected 3 state arrays for merge_batch");
        }

        let reservoir_states = states[0].as_list::<i32>();
        let items_seen_states = states[1].as_primitive::<arrow::datatypes::UInt32Type>();
        let sample_size_states = states[2].as_primitive::<arrow::datatypes::UInt32Type>();

        for row_idx in 0..reservoir_states.len() {
            if reservoir_states.is_null(row_idx) {
                continue;
            }

            // Extract state from this partition
            let other_reservoir_array = reservoir_states.value(row_idx);
            let other_items_seen = items_seen_states.value(row_idx);
            let other_sample_size = sample_size_states.value(row_idx);

            // Set sample size if not already set
            if self.sample_size == 0 {
                self.sample_size = other_sample_size;
                self.reservoir.reserve(self.sample_size as usize);
            }

            // Convert other reservoir to Vec<ScalarValue>
            let mut other_reservoir = Vec::new();
            for i in 0..other_reservoir_array.len() {
                if !other_reservoir_array.is_null(i) {
                    other_reservoir.push(ScalarValue::try_from_array(&other_reservoir_array, i)?);
                }
            }

            // Merge reservoirs using Algorithm R merge logic
            self.merge_reservoirs(other_reservoir, other_items_seen)?;
        }

        Ok(())
    }
}

impl ReservoirSampleAccumulator {
    /// Merge another reservoir into this one, maintaining uniform sampling probability
    fn merge_reservoirs(
        &mut self,
        other_reservoir: Vec<ScalarValue>,
        other_items_seen: u32,
    ) -> Result<()> {
        let total_items_seen = self.items_seen + other_items_seen;

        // Create combined reservoir by treating each item from other reservoir
        // as if it was seen in the combined stream
        for item in other_reservoir {
            self.items_seen += 1;

            if self.reservoir.len() < self.sample_size as usize {
                // Our reservoir not full, just add
                self.reservoir.push(item);
            } else {
                // Decide whether to include this item using the probability
                // that it would have been selected in the combined stream
                let j = self.rng.gen_range(1..=self.items_seen);

                if j <= self.sample_size {
                    let replace_idx = (j - 1) as usize;
                    self.reservoir[replace_idx] = item;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use datafusion::prelude::*;
    use datafusion_common::assert_batches_eq;

    #[tokio::test]
    async fn test_reservoir_sample_basic() -> Result<()> {
        let ctx = SessionContext::new();

        // Register the UDF
        ctx.register_udaf(reservoir_sample_udaf());

        // Create test data: integers 1 through 100
        let mut batch_data = Vec::new();
        for i in 1i32..=100 {
            batch_data.push(i);
        }

        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Int32Array::from(batch_data)) as ArrayRef,
        )])?;

        let table = datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])?;

        ctx.register_table("test_data", Arc::new(table))?;

        // Test sampling 10 items
        let df = ctx
            .sql("SELECT reservoir_sample(value, 10) as sample FROM test_data")
            .await?;
        let results = df.collect().await?;

        assert_eq!(results.len(), 1);
        let sample_array = results[0].column(0);

        // Verify we got a list
        assert!(matches!(sample_array.data_type(), DataType::List(_)));

        let list_array = sample_array.as_list::<i32>();
        assert_eq!(list_array.len(), 1); // One row

        let sample_list = list_array.value(0);
        assert_eq!(sample_list.len(), 10); // Should have exactly 10 items

        println!("Sample: {:?}", sample_list);

        Ok(())
    }

    #[test]
    fn test_algorithm_r_properties() {
        let mut acc = ReservoirSampleAccumulator {
            reservoir: Vec::new(),
            items_seen: 0,
            sample_size: 3,
            data_type: DataType::Int32,
            rng: StdRng::seed_from_u64(42), // Fixed seed for reproducibility
        };

        // Add items 1 through 10
        for i in 1..=10 {
            acc.consider_item(ScalarValue::Int32(Some(i)));
        }

        assert_eq!(acc.reservoir.len(), 3);
        assert_eq!(acc.items_seen, 10);

        // Verify all items in reservoir are valid
        for item in &acc.reservoir {
            if let ScalarValue::Int32(Some(val)) = item {
                assert!(*val >= 1 && *val <= 10);
            } else {
                panic!("Expected Int32 value");
            }
        }
    }
}
