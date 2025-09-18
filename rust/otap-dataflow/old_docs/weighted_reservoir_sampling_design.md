# Design Document: Weighted Reservoir Sampling UDAF for DataFusion

## 1. Objective

This document outlines the design for a User-Defined Aggregate Function (UDAF) in DataFusion to perform **weighted reservoir sampling**. The function will process a stream of items of unknown length, each with an associated weight, and produce a fixed-size sample of `k` items.

A key requirement is that the function must also output an **adjusted weight** for each sampled item, which serves as an unbiased estimate of its importance within the overall population.

## 2. Background and Motivation

Standard reservoir sampling (like Vitter's Algorithm R) treats all items with uniform probability. However, in many analytical scenarios (e.g., telemetry, log analysis, transaction monitoring), items have varying importance. Weighted sampling ensures that more "important" items have a higher chance of being included in the final sample, providing a more representative view of the data.

The output of adjusted weights is crucial for performing accurate downstream calculations (like weighted averages or sums) on the sampled data.

## 3. SQL Interface and Usage

The UDAF will be exposed to users through a simple and intuitive SQL interface. It will accept three arguments:

1. The column to be sampled.
2. The column containing the weight for each item.
3. The desired sample size, `k`.

### Example SQL Query

```sql
SELECT
    group_column,
    weighted_reservoir_sample(value_column, weight_column, 50) AS sample
FROM
    my_table
GROUP BY
    group_column;
```

## 4. UDF Signature and Schema

### 4.1. Function Signature

The function will be registered with the following signature:

- `weighted_reservoir_sample(value: ANY, weight: FLOAT64, k: INT64)`

The `value` argument can be of any data type, which will be preserved in the output. The `weight` is expected to be a `Float64`, and `k` is an integer representing the sample size.

### 4.2. Return Type and Schema

The function will return a `List` of `Struct`s. This structure is necessary to bundle each sampled value with its corresponding adjusted weight.

The schema of the returned `List` will be:

```text
List<Struct<
    value: T,
    adjusted_weight: Float64
>>
```

Where `T` is the data type of the input `value_column`.

This will be implemented in the `AggregateUDFImpl::return_type` method by dynamically constructing the `Struct` and `List` types based on the function's input arguments.

## 5. Core Implementation (`AggregateUDFImpl`)

The implementation will be centered around two main components: the `WeightedReservoirSampleUDF` struct and the `WeightedReservoirAccumulator`.

### 5.1. `WeightedReservoirSampleUDF` Struct

This struct implements the `AggregateUDFImpl` trait and serves as the main entry point for the UDAF. It is responsible for:

- Defining the function's `name` ("weighted_reservoir_sample").
- Defining the `signature` as described in section 4.1.
- Defining the `return_type` as described in section 4.2.
- Acting as a factory for creating `WeightedReservoirAccumulator` instances.

### 5.2. `WeightedReservoirAccumulator`

This struct implements the `Accumulator` trait and contains the core state and logic for the sampling algorithm.

#### 5.2.1. State Management

The accumulator's state must be sufficient to manage the weighted selection process and support distributed merging. A recommended approach (based on algorithms like A-Res) is to use a min-heap.

- **`reservoir`**: A `BinaryHeap` of size `k` storing tuples of `(key, value)`. The `key` is a randomly generated number derived from the item's weight (e.g., `key = rand(0,1)^(1/weight)`), and `value` is the item itself. The heap is ordered by the `key`, allowing for efficient O(log k) replacement of the item with the smallest key.
- **`sample_size`**: The integer `k`.
- **`items_seen`**: A counter for the total number of items processed.
- **`total_weight`**: The sum of weights of all items seen so far. This may be needed for calculating adjusted weights.
- **`rng`**: A random number generator instance.

#### 5.2.2. Core Logic

- **`update_batch`**: For each row in an input batch, this method will:
    1. Generate a random `key` based on the item's weight.
    2. If the reservoir (heap) is not yet full, push the `(key, value)` pair.
    3. If the reservoir is full, compare the new `key` with the smallest key in the heap (the heap's top element). If the new key is larger, pop the smallest element and push the new one.

- **`merge_batch`**: This method is critical for distributed execution. It combines the state of two accumulators. The logic involves:
    1. Taking the union of the two reservoirs.
    2. Selecting the `k` items with the highest keys from the combined set to form the new merged reservoir.
    3. Summing other state variables like `items_seen` and `total_weight`.

- **`evaluate`**: This finalizes the aggregation. It will:
    1. Iterate through the `k` items in the final reservoir.
    2. Calculate the `adjusted_weight` for each item based on the specific algorithm's formula (which may involve `total_weight`, `k`, and the item's original key).
    3. Construct a `StructArray` with two columns: one for the values and one for the calculated adjusted weights.
    4. Return the result as a `ScalarValue` of type `List<Struct>`.

## 6. Conclusion

This design provides a robust and efficient way to implement weighted reservoir sampling within DataFusion. It leverages DataFusion's existing aggregation infrastructure to support distributed execution, handles complex data types gracefully, and exposes a simple, powerful interface to the end-user.

## 7. Implementation Pointers for Rust Developers

This section provides concrete code signatures and `use` statements to guide the implementation in Rust.

### 7.1. Necessary `use` Statements

```rust
use std::any::Any;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Float64Type};
use arrow_array::builder::{Float64Builder, ListBuilder, StructBuilder};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use rand::prelude::*;
```

### 7.2. `AggregateUDFImpl` Implementation

```rust
#[derive(Debug, Clone)]
pub struct WeightedReservoirSampleUDF {
    signature: Signature,
}

impl WeightedReservoirSampleUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Null,     // Placeholder for value type
                    DataType::Float64,  // Weight
                    DataType::Int64,    // Sample size k
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for WeightedReservoirSampleUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "weighted_reservoir_sample"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let struct_fields = Fields::from(vec![
            Field::new("value", arg_types[0].clone(), true),
            Field::new("adjusted_weight", DataType::Float64, false),
        ]);
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields),
            true,
        ))))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        // Create and return the accumulator instance
        Ok(Box::new(WeightedReservoirAccumulator::new(&acc_args.input_types[0])))
    }
}
```

### 7.3. `Accumulator` Implementation

```rust
#[derive(Debug)]
struct WeightedReservoirAccumulator {
    reservoir: BinaryHeap<Reverse<(f64, ScalarValue)>>,
    sample_size: u32,
    items_seen: u64,
    total_weight: f64,
    rng: StdRng,
    data_type: DataType,
}

impl WeightedReservoirAccumulator {
    pub fn new(data_type: &DataType) -> Self {
        Self {
            reservoir: BinaryHeap::new(),
            sample_size: 0,
            items_seen: 0,
            total_weight: 0.0,
            rng: StdRng::from_entropy(),
            data_type: data_type.clone(),
        }
    }
}

impl Accumulator for WeightedReservoirAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize state for merging: reservoir, items_seen, total_weight, etc.
        unimplemented!()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Core logic for processing a batch of items and weights
        unimplemented!()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Logic to merge state from another accumulator
        unimplemented!()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Finalize and return the List<Struct<...>> result
        unimplemented!()
    }

    fn size(&self) -> usize {
        // Return the memory size of the accumulator's state
        std::mem::size_of_val(self) + self.reservoir.capacity() * std::mem::size_of::<(f64, ScalarValue)>()
    }
}
```
