# Bottom-K Formula Refinement Summary

## Current Status: September 19, 2025

### Formula Correction Implemented

We have implemented the corrected Bottom-K weighted estimator formula:

```
a(i) = w(i) × exp(w(i) × r_{k+1})
```

Where:
- `r_{k+1} = -ln(R_k)` (rank calculation)
- `w(i) = 1.0` for uniform-weight (counting) case
- `R_k` is the normalized randomness value

### Implementation Details

**Location**: `bucket_manager.go`, `calculateBottomKThreshold` method

**Current Implementation**:
```go
// For uniform-weight (counting) case, w(i) = 1.0 (incoming adjusted count)
incomingWeight := 1.0

// Convert randomness value to [0,1) range as per OTEP 235
normalizedRandomness := float64(minRandomnessValueOfKeptTraces) / float64(maxRandomness)

// Calculate rank: r_{k+1} = -ln(normalized_randomness)
rank := -math.Log(normalizedRandomness) / incomingWeight

// Calculate adjustment factor using Bottom-K estimator formula
adjustmentFactor := incomingWeight * math.Exp(rank) * float64(k)

// The sampling probability is the inverse of the adjustment factor
samplingProbability := 1.0 / adjustmentFactor
```

### User's Pseudocode Reference

The user provided working pseudocode demonstrating the expected behavior:

```go
// the weight is adjusted count for this example
incomingWeight := incomingAdjustedCount

rank := -math.Log(vals[outSize]) / incomingWeight

// For the uniform-input weight case (i.e.,
// unweighted counts) we multiply the sample
totalSum += incomingWeight * math.Exp(rank) * float64(outSize)
```

### Test Failures and Issues

**Current Problems**:

1. **Probability Direction Issue**: 
   - Lower randomness values should produce higher sampling probability
   - Current implementation produces lower probability for lower randomness

2. **Adjustment Count Range**:
   - Tests expect adjustment factors > 1
   - Some calculations produce values < 1 (e.g., 0.685)

3. **Formula Mismatch**: 
   - Tests appear to expect formula pattern: `Weight / (1 - exp(Weight * Rank))`
   - This differs from our current implementation

**Test Output Examples**:
```
Expected adjustment count 1.5, got 0.685
```

### Next Steps for Refinement

1. **Mathematical Analysis**: 
   - Review Bottom-K paper for correct probability relationship
   - Understand why lower randomness should give higher probability

2. **Test Expectation Analysis**:
   - Examine test mathematical expectations more thoroughly
   - Determine if test formula `Weight / (1 - exp(Weight * Rank))` is correct

3. **Implementation Adjustment**:
   - Modify calculation to match both user pseudocode intent and test requirements
   - Ensure formula produces expected adjustment factor ranges (> 1)

4. **Validation**:
   - Verify implementation matches Bottom-K theoretical behavior
   - Ensure OTEP 235 compliance with threshold calculations

### Key Questions to Resolve

1. **Rank Calculation**: Is `rank := -math.Log(normalizedRandomness) / incomingWeight` correct?
2. **Probability Relationship**: Why should lower randomness give higher probability?
3. **Adjustment Factor**: Should the final calculation be `1/adjustmentFactor` or something else?
4. **Test Formula**: Is the test's expected formula `Weight / (1 - exp(Weight * Rank))` the correct Bottom-K estimator?

### Implementation Status

- **✅ Formula Structure**: Basic weighted estimator structure implemented
- **✅ Configuration**: Proper OTEP 235 randomness value extraction
- **✅ Integration**: Connected to bucket manager and threshold calculation
- **❌ Mathematical Correctness**: Probability relationship needs adjustment
- **❌ Test Compatibility**: Current implementation fails test expectations
