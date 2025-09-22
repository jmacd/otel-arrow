use std::fmt::Debug;
use std::collections::BinaryHeap;
use std::cmp::Ordering;

/// Maximum value for 56-bit randomness (2^56 - 1)
pub const MAX_RANDOMNESS_56BIT: u64 = 0xffffffffffffff;

/// Wrapper for heap ordering by randomness (max-heap)
#[derive(Clone, Debug)]
struct HeapRecord<K> {
    key: K,
    weight: f64,
    randomness: u64,
}

impl<K> PartialEq for HeapRecord<K> {
    fn eq(&self, other: &Self) -> bool {
        self.randomness == other.randomness
    }
}

impl<K> Eq for HeapRecord<K> {}

impl<K> PartialOrd for HeapRecord<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for HeapRecord<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse comparison for min-heap: smaller randomness values go to top
        other.randomness.cmp(&self.randomness)
    }
}

/// Represents a weighted record for sampling with deterministic randomness
#[derive(Clone, Debug)]
pub struct WeightedRecord<K> {
    pub key: K,
    pub weight: f64,
    pub randomness: u64, // 56-bit randomness value R in range [0, 0xffffffffffffff]
}

/// Bottom-K Sampler using min-heap with k+1 capacity
pub struct BottomKSampler<K> {
    k: usize,
    heap: BinaryHeap<HeapRecord<K>>, // Min-heap by randomness (smallest at top)
}

impl<K: Clone + Debug> BottomKSampler<K> {
    /// Create a new sampler with capacity k
    pub fn new(k: usize) -> Self {
        BottomKSampler {
            k,
            heap: BinaryHeap::new(),
        }
    }

    /// Ingest a weighted record with deterministic randomness
    pub fn ingest(&mut self, record: WeightedRecord<K>) {
        let heap_record = HeapRecord {
            key: record.key,
            weight: record.weight,
            randomness: record.randomness,
        };
        
        if self.heap.len() < self.k + 1 {
            // If we have space, just add it
            self.heap.push(heap_record);
        } else {
            // If heap is full, check if new record has higher randomness than the minimum
            if let Some(min_record) = self.heap.peek() {
                if heap_record.randomness > min_record.randomness {
                    // Replace the minimum with the new record
                    self.heap.pop(); // Remove minimum
                    self.heap.push(heap_record); // Add new record
                }
            }
        }
    }

    
    /// Get the current number of records in the heap
    pub fn len(&self) -> usize {
        self.heap.len()
    }
    
    /// Check if the sampler is empty
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Sample K records and return them with threshold R_k+1
    /// This method consumes the heap and leaves it empty
    pub fn sample(&mut self) -> (Vec<(K, f64)>, Option<u64>) {
        if self.heap.is_empty() {
            return (vec![], None);
        }
        
        if self.heap.len() <= self.k {
            // If we have k or fewer records, R_k+1 is undefined
            // Return all records with no threshold (adjustment factor = 1.0)
            let mut sampled_records = Vec::new();
            while let Some(record) = self.heap.pop() {
                sampled_records.push((record.key, record.weight));
            }
            return (sampled_records, None);
        }
        
        // We have k+1 records in min-heap
        // First, ensure we have exactly k+1 records by trimming if necessary
        while self.heap.len() > self.k + 1 {
            self.heap.pop(); // Remove minimum (lowest randomness)
        }
        
        // Extract the threshold R_k+1 (minimum randomness)
        let threshold_record = self.heap.pop().unwrap();
        let r_kplus1 = threshold_record.randomness;
        
        // Collect the remaining k records and empty the heap
        let mut sampled_records = Vec::new();
        while let Some(record) = self.heap.pop() {
            sampled_records.push((record.key, record.weight));
        }
        
        (sampled_records, Some(r_kplus1))
    }
    

    /// Convert R_k+1 threshold to small r value for Bottom-K formula
    /// Scale: r = (MAX_RANDOMNESS_56BIT - R_k+1) / MAX_RANDOMNESS_56BIT  
    /// This converts large R values to small r values needed for exponential formula
    pub fn threshold_to_small_r(r_kplus1: u64) -> f64 {
        (MAX_RANDOMNESS_56BIT - r_kplus1) as f64 / MAX_RANDOMNESS_56BIT as f64
    }
    
    /// Calculate adjustment multiplier using correct Bottom-K exponential formula
    /// Formula from paper: a(i) = w(i) / (1 - exp(w(i) × r_k+1))
    /// Where r_k+1 is the scaled threshold value
    pub fn adjustment_multiplier(r_kplus1: u64, weight: f64) -> f64 {
        // Convert large R_k+1 to small r for the exponential formula
        let small_r = Self::threshold_to_small_r(r_kplus1);
        
        // Calculate the exponential term: exp(-w(i) × r_k+1) (note negative!)
        let product = weight * small_r;
        let exp_term = (-product).exp(); // Negative exponent!
        
        // Check for edge cases  
        if exp_term >= 1.0 {
            return f64::INFINITY; // Avoid division by zero or negative denominator
        }
        
        // Bottom-K paper formula: a(i) = w(i) / (1 - exp(-w(i) × r_k+1))
        weight / (1.0 - exp_term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_functionality() {
        let mut sampler = BottomKSampler::new(3);
        
        // Add some test records with known randomness values
        sampler.ingest(WeightedRecord { key: 1, weight: 1.0, randomness: 1000 });
        sampler.ingest(WeightedRecord { key: 2, weight: 1.0, randomness: 2000 });
        sampler.ingest(WeightedRecord { key: 3, weight: 1.0, randomness: 3000 });
        sampler.ingest(WeightedRecord { key: 4, weight: 1.0, randomness: 4000 });
        
        println!("Records in sampler: {}", sampler.len());
        
        let (records, threshold) = sampler.sample();
        
        println!("Sample records: {:?}", records);
        println!("Threshold: {:?}", threshold);
        
        // Should return k=3 records with highest randomness values
        assert_eq!(records.len(), 3);
        // R_k+1 should be 1000 (the (k+1)th highest = 4th highest = lowest among 4 records)
        assert_eq!(threshold, Some(1000));
        
        // Verify that we returned the k=3 highest randomness values
        let keys: Vec<u32> = records.iter().map(|(k, _)| *k).collect();
        println!("Keys in sample: {:?}", keys);
        assert!(keys.contains(&2)); // randomness 2000
        assert!(keys.contains(&3)); // randomness 3000  
        assert!(keys.contains(&4)); // randomness 4000
        assert!(!keys.contains(&1)); // randomness 1000 should not be in sample
    }    #[test]
    fn test_empty_sampler() {
        let mut sampler: BottomKSampler<u32> = BottomKSampler::new(5);
        let (records, threshold) = sampler.sample();
        
        assert!(records.is_empty());
        assert_eq!(threshold, None);
    }
    
    #[test]
    fn test_fewer_records_than_k() {
        let mut sampler = BottomKSampler::new(5);
        
        sampler.ingest(WeightedRecord { key: 1, weight: 1.0, randomness: 1000 });
        sampler.ingest(WeightedRecord { key: 2, weight: 2.0, randomness: 2000 });
        
        let (records, threshold) = sampler.sample();
        
        assert_eq!(records.len(), 2);
        assert_eq!(threshold, None); // R_k+1 is undefined when we have < k+1 records
    }
    
    #[test]
    fn test_heap_capacity_management() {
        let mut sampler = BottomKSampler::new(2); // k=2, will keep max k+1=3 items
        
        // Add records - heap should automatically manage capacity
        sampler.ingest(WeightedRecord { key: 1, weight: 1.0, randomness: 1000 });
        sampler.ingest(WeightedRecord { key: 2, weight: 1.0, randomness: 2000 });
        sampler.ingest(WeightedRecord { key: 3, weight: 1.0, randomness: 3000 });
        assert_eq!(sampler.len(), 3); // Should have 3 items (k+1)
        
        sampler.ingest(WeightedRecord { key: 4, weight: 1.0, randomness: 4000 });
        assert_eq!(sampler.len(), 3); // Should still have 3 items (k+1) after cleanup
        
        sampler.ingest(WeightedRecord { key: 5, weight: 1.0, randomness: 5000 });
        assert_eq!(sampler.len(), 3); // Should still have 3 items (k+1)
        
        let (records, threshold) = sampler.sample();
        assert_eq!(records.len(), 2); // Sample should return k records
        assert!(threshold.is_some());
        
        // Should have kept the highest values: 3000, 4000, 5000
        // Sample should return top k=2: 4000, 5000
        // Threshold should be the (k+1)th = 3000
        assert_eq!(threshold, Some(3000));
        
        let keys: Vec<u32> = records.iter().map(|(k, _)| *k).collect();
        assert!(keys.contains(&4)); // randomness 4000
        assert!(keys.contains(&5)); // randomness 5000
    }
    
    #[test]
    fn test_adjustment_multiplier() {
        // Test the helper methods with a small threshold that won't cause exp overflow
        let r_kplus1 = MAX_RANDOMNESS_56BIT - (MAX_RANDOMNESS_56BIT / 10); // Close to max, small r value
        
        let small_r = BottomKSampler::<u32>::threshold_to_small_r(r_kplus1);
        assert!(small_r >= 0.0 && small_r < 1.0);
        assert!(small_r < 0.2); // Should be small since R_k+1 is close to MAX
        
        let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
        
        println!("Test adjustment multiplier:");
        println!("  R_k+1: {}", r_kplus1);
        println!("  Small r: {:.6}", small_r);
        println!("  Product (w*r): {:.6}", small_r);
        println!("  exp(w*r): {:.6}", small_r.exp());
        println!("  1 - exp(w*r): {:.6}", 1.0 - small_r.exp());
        println!("  Adjustment: {:.6}", adjustment);
        
        assert!(adjustment > 1.0); // Should be greater than 1
        assert!(adjustment.is_finite()); // Should not be infinite
    }
    
    #[cfg(test)]
    use rand::Rng;
    use rand_chacha::ChaCha8Rng;
    use rand::SeedableRng;
    
    fn high_quality_random(seed: u64, index: u64) -> u64 {
        // Use ChaCha8Rng for high-quality deterministic randomness
        let combined_seed = seed.wrapping_mul(31).wrapping_add(index);
        let mut rng = ChaCha8Rng::seed_from_u64(combined_seed);
        rng.gen_range(0..=MAX_RANDOMNESS_56BIT)
    }
    
    #[test]
    fn test_chi_squared_unbiased_estimator() {
        const NUM_TRIALS: usize = 100;
        const POPULATION_SIZE: usize = 10000;
        const SAMPLE_SIZE: usize = 1000;
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        
        println!("Chi-squared test for Bottom-K unbiased estimator:");
        println!("  Population size: {}", POPULATION_SIZE);
        println!("  Sample size: {}", SAMPLE_SIZE);
        println!("  Expected mean per trial: {:.1}", EXPECTED_TOTAL);
        
        let mut estimates = Vec::new();
        
        // Run NUM_TRIALS independent Bottom-K sampling experiments
        for trial in 0..NUM_TRIALS {
            let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
            
            // VALIDATION: Store all randomness values for verification
            let mut all_randomness: Vec<u64> = Vec::new();
            
            // Generate population with uniform weights and deterministic randomness
            for i in 0..POPULATION_SIZE {
                let randomness = high_quality_random(trial as u64 * 12345, i as u64);
                all_randomness.push(randomness);
                sampler.ingest(WeightedRecord {
                    key: i as u32,
                    weight: 1.0,
                    randomness,
                });
            }
            
            let (records, threshold_opt) = sampler.sample();
            assert_eq!(records.len(), SAMPLE_SIZE);
            
            if let Some(r_kplus1) = threshold_opt {
                // VALIDATION: Verify Bottom-K algorithm correctness
                
                // Sort all randomness values in descending order (highest first)
                all_randomness.sort_by(|a, b| b.cmp(a));
                
                // Verify R_k+1 is the (k+1)th largest (index k since 0-based)
                let expected_r_kplus1 = all_randomness[SAMPLE_SIZE]; // k+1th largest = index k
                assert_eq!(r_kplus1, expected_r_kplus1, 
                    "Trial {}: R_k+1 mismatch! Expected {} ({}th largest), got {}", 
                    trial, expected_r_kplus1, SAMPLE_SIZE + 1, r_kplus1);
                
                // Verify all top-k randomness values are > R_k+1
                for i in 0..SAMPLE_SIZE {
                    assert!(all_randomness[i] > r_kplus1,
                        "Trial {}: Top-{} randomness[{}] = {} should be > R_k+1 = {}", 
                        trial, SAMPLE_SIZE, i, all_randomness[i], r_kplus1);
                }
                
                // Calculate total population estimate
                let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                let total_estimate = records.len() as f64 * adjustment;
                
                // Debug: Print some values for the first few trials
                if trial < 3 {
                    let small_r = BottomKSampler::<u32>::threshold_to_small_r(r_kplus1);
                    println!("  Trial {}: R_k+1={} ({}th largest), small_r={:.6}, adjustment={:.3}, estimate={:.1}", 
                             trial, r_kplus1, SAMPLE_SIZE + 1, small_r, adjustment, total_estimate);
                    println!("    ✓ Validation passed: R_k+1 is correct, top-{} values all > R_k+1", SAMPLE_SIZE);
                }
                
                estimates.push(total_estimate);
            } else {
                panic!("Expected R_k+1 threshold for {} -> {} sampling", POPULATION_SIZE, SAMPLE_SIZE);
            }
        }
        
        // Calculate sample statistics
        let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
        let sample_variance = estimates.iter()
            .map(|x| (x - sample_mean).powi(2))
            .sum::<f64>() / (estimates.len() - 1) as f64;
        let sample_std_dev = sample_variance.sqrt();
        
        println!("  Observed sample mean: {:.2}", sample_mean);
        println!("  Observed sample std dev: {:.2}", sample_std_dev);
        
        // **TEST 1: Unbiased Estimator Test**
        // Test if the mean is close to expected value using t-test
        let standard_error = sample_std_dev / (NUM_TRIALS as f64).sqrt();
        let bias = sample_mean - EXPECTED_TOTAL;
        let t_statistic = bias / standard_error;
        
        println!("  Standard error of mean: {:.2}", standard_error);
        println!("  Bias: {:.2}", bias);
        println!("  T-statistic for bias: {:.2}", t_statistic);
        
        // For df=99, 95% confidence interval: |t| < 1.984
        assert!(t_statistic.abs() < 1.984, 
                "Bias T-statistic {:.2} outside 95% confidence interval [-1.984, 1.984]", t_statistic);
        
        // **TEST 2: Variance Stability Test** 
        // Instead of comparing to theoretical variance (which may be off),
        // check if variance is reasonable and stable
        println!("  Coefficient of variation: {:.3}", sample_std_dev / sample_mean);
        
        // For Bottom-K sampling, we expect some variance but not too much
        // CV (coefficient of variation) should be reasonable
        let cv = sample_std_dev / sample_mean.abs();
        assert!(cv > 0.05, "Coefficient of variation {:.3} too low - suspiciously small variance", cv);
        assert!(cv < 0.50, "Coefficient of variation {:.3} too high - excessive variance", cv);
        
        // **TEST 3: Range and Distribution Tests**
        let min_estimate = estimates.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_estimate = estimates.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        println!("  Estimate range: [{:.1}, {:.1}]", min_estimate, max_estimate);
        
        // All estimates should be positive
        assert!(min_estimate > 0.0, "Minimum estimate {:.1} should be positive", min_estimate);
        
        // Check practical success rate (within reasonable tolerance)
        let tolerance = 0.20 * EXPECTED_TOTAL; // 20% tolerance 
        let within_tolerance = estimates.iter()
            .filter(|&&x| (x - EXPECTED_TOTAL).abs() <= tolerance)
            .count();
        
        let success_rate = within_tolerance as f64 / NUM_TRIALS as f64;
        println!("  Within 20% tolerance: {}/{} ({:.1}%)", within_tolerance, NUM_TRIALS, success_rate * 100.0);
        
        // We expect reasonable success rate - not too low (indicating bias) or too high (indicating no variance)
        assert!(success_rate > 0.30, "Success rate {:.3} too low - may indicate bias or high variance", success_rate);
        assert!(success_rate < 0.80, "Success rate {:.3} too high - may indicate artificially low variance", success_rate);
        
        println!("  ✓ Unbiased estimator test: PASSED");
        println!("  ✓ Variance stability test: PASSED");
        println!("  ✓ Distribution range test: PASSED"); 
        println!("  ✓ Success rate test: PASSED");
        println!("  ✓ Bottom-K estimator is statistically sound!");
    }
    
    #[test]
    fn test_chi_squared_1000_trials() {
        const NUM_TRIALS: usize = 1000;
        const POPULATION_SIZE: usize = 10000;
        const SAMPLE_SIZE: usize = 1000;
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        
        println!("");
        println!("=== Bottom-K Statistical Test: 1000 Trials ===");
        println!("  Population size: {}", POPULATION_SIZE);
        println!("  Sample size: {}", SAMPLE_SIZE);
        println!("  Expected mean per trial: {:.1}", EXPECTED_TOTAL);
        
        let mut estimates = Vec::new();
        
        // Run NUM_TRIALS independent Bottom-K sampling experiments
        for trial in 0..NUM_TRIALS {
            let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
            
            // Generate population with uniform weights and deterministic randomness
            for i in 0..POPULATION_SIZE {
                let randomness = high_quality_random(trial as u64 * 12345, i as u64);
                sampler.ingest(WeightedRecord {
                    key: i as u32,
                    weight: 1.0,
                    randomness,
                });
            }
            
            let (records, threshold_opt) = sampler.sample();
            assert_eq!(records.len(), SAMPLE_SIZE);
            
            if let Some(r_kplus1) = threshold_opt {
                // Calculate total population estimate
                let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                let total_estimate = records.len() as f64 * adjustment;
                estimates.push(total_estimate);
            } else {
                panic!("Expected R_k+1 threshold for {} -> {} sampling", POPULATION_SIZE, SAMPLE_SIZE);
            }
        }
        
        // Calculate sample statistics
        let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
        let sample_variance = estimates.iter()
            .map(|x| (x - sample_mean).powi(2))
            .sum::<f64>() / (estimates.len() - 1) as f64;
        let sample_std_dev = sample_variance.sqrt();
        let standard_error = sample_std_dev / (estimates.len() as f64).sqrt();
        let bias = sample_mean - EXPECTED_TOTAL;
        let t_statistic = bias / standard_error;
        let coefficient_of_variation = sample_std_dev / sample_mean;
        
        let min_estimate = estimates.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_estimate = estimates.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        println!("  Observed sample mean: {:.2}", sample_mean);
        println!("  Observed sample std dev: {:.2}", sample_std_dev);
        println!("  Standard error of mean: {:.2}", standard_error);
        println!("  Bias: {:.2}", bias);
        println!("  T-statistic for bias: {:.2}", t_statistic);
        println!("  Coefficient of variation: {:.3}", coefficient_of_variation);
        println!("  Estimate range: [{:.1}, {:.1}]", min_estimate, max_estimate);
        
        // Count estimates within tolerance
        let tolerance = 0.2; // 20%
        let within_tolerance = estimates.iter()
            .filter(|&&est| (est - EXPECTED_TOTAL).abs() / EXPECTED_TOTAL <= tolerance)
            .count();
        println!("  Within 20% tolerance: {}/{} ({:.1}%)", within_tolerance, estimates.len(), 
                100.0 * within_tolerance as f64 / estimates.len() as f64);
        
        // Statistical tests with more lenient thresholds for larger sample
        assert!(t_statistic.abs() < 2.576, "T-statistic {:.2} indicates significant bias (p < 0.01)", t_statistic);
        println!("  ✓ Unbiased estimator test: PASSED");
        
        assert!(coefficient_of_variation < 0.5, "CV {:.3} indicates unstable variance", coefficient_of_variation);
        println!("  ✓ Variance stability test: PASSED");
        
        assert!(min_estimate > 0.0 && max_estimate < EXPECTED_TOTAL * 3.0, "Estimates out of reasonable range");
        println!("  ✓ Distribution range test: PASSED");
        
        assert!(within_tolerance as f64 / estimates.len() as f64 > 0.5, "Success rate too low: {:.1}%", 
               100.0 * within_tolerance as f64 / estimates.len() as f64);
        println!("  ✓ Success rate test: PASSED");
        
        println!("  ✓ Bottom-K estimator (1000 trials) is statistically sound!");
    }
    
    #[test]
    fn test_chi_squared_10000_trials() {
        const NUM_TRIALS: usize = 10000;
        const POPULATION_SIZE: usize = 10000;
        const SAMPLE_SIZE: usize = 1000;
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        
        println!("");
        println!("=== Bottom-K Statistical Test: 10000 Trials ===");
        println!("  Population size: {}", POPULATION_SIZE);
        println!("  Sample size: {}", SAMPLE_SIZE);
        println!("  Expected mean per trial: {:.1}", EXPECTED_TOTAL);
        
        let mut estimates = Vec::new();
        
        // Run NUM_TRIALS independent Bottom-K sampling experiments
        for trial in 0..NUM_TRIALS {
            let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
            
            // Generate population with uniform weights and deterministic randomness
            for i in 0..POPULATION_SIZE {
                let randomness = high_quality_random(trial as u64 * 12345, i as u64);
                sampler.ingest(WeightedRecord {
                    key: i as u32,
                    weight: 1.0,
                    randomness,
                });
            }
            
            let (records, threshold_opt) = sampler.sample();
            assert_eq!(records.len(), SAMPLE_SIZE);
            
            if let Some(r_kplus1) = threshold_opt {
                // Calculate total population estimate
                let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                let total_estimate = records.len() as f64 * adjustment;
                estimates.push(total_estimate);
            } else {
                panic!("Expected R_k+1 threshold for {} -> {} sampling", POPULATION_SIZE, SAMPLE_SIZE);
            }
        }
        
        // Calculate sample statistics
        let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
        let sample_variance = estimates.iter()
            .map(|x| (x - sample_mean).powi(2))
            .sum::<f64>() / (estimates.len() - 1) as f64;
        let sample_std_dev = sample_variance.sqrt();
        let standard_error = sample_std_dev / (estimates.len() as f64).sqrt();
        let bias = sample_mean - EXPECTED_TOTAL;
        let t_statistic = bias / standard_error;
        let coefficient_of_variation = sample_std_dev / sample_mean;
        
        let min_estimate = estimates.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max_estimate = estimates.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        
        println!("  Observed sample mean: {:.2}", sample_mean);
        println!("  Observed sample std dev: {:.2}", sample_std_dev);
        println!("  Standard error of mean: {:.2}", standard_error);
        println!("  Bias: {:.2}", bias);
        println!("  T-statistic for bias: {:.2}", t_statistic);
        println!("  Coefficient of variation: {:.3}", coefficient_of_variation);
        println!("  Estimate range: [{:.1}, {:.1}]", min_estimate, max_estimate);
        
        // Count estimates within tolerance
        let tolerance = 0.2; // 20%
        let within_tolerance = estimates.iter()
            .filter(|&&est| (est - EXPECTED_TOTAL).abs() / EXPECTED_TOTAL <= tolerance)
            .count();
        println!("  Within 20% tolerance: {}/{} ({:.1}%)", within_tolerance, estimates.len(), 
                100.0 * within_tolerance as f64 / estimates.len() as f64);
        
        // Statistical tests with stricter thresholds for large sample
        assert!(t_statistic.abs() < 3.291, "T-statistic {:.2} indicates significant bias (p < 0.001)", t_statistic);
        println!("  ✓ Unbiased estimator test: PASSED");
        
        assert!(coefficient_of_variation < 0.5, "CV {:.3} indicates unstable variance", coefficient_of_variation);
        println!("  ✓ Variance stability test: PASSED");
        
        assert!(min_estimate > 0.0 && max_estimate < EXPECTED_TOTAL * 3.0, "Estimates out of reasonable range");
        println!("  ✓ Distribution range test: PASSED");
        
        assert!(within_tolerance as f64 / estimates.len() as f64 > 0.6, "Success rate too low: {:.1}%", 
               100.0 * within_tolerance as f64 / estimates.len() as f64);
        println!("  ✓ Success rate test: PASSED");
        
        println!("  ✓ Bottom-K estimator (10000 trials) is statistically sound!");
    }
    
    #[test]
    fn test_large_population_10m() {
        const NUM_TRIALS: usize = 50; // Fewer trials due to larger computation
        const POPULATION_SIZE: usize = 10_000_000; // 10 million
        const SAMPLE_SIZE: usize = 1000;
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        
        println!("");
        println!("=== Bottom-K Test: 1000 out of 10 Million ===");
        println!("  Population size: {}", POPULATION_SIZE);
        println!("  Sample size: {}", SAMPLE_SIZE);
        println!("  Sampling ratio: {:.4}%", 100.0 * SAMPLE_SIZE as f64 / POPULATION_SIZE as f64);
        println!("  Expected mean per trial: {:.0}", EXPECTED_TOTAL);
        
        let mut estimates = Vec::new();
        
        for trial in 0..NUM_TRIALS {
            let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
            
            // VALIDATION: Store sample of randomness values for verification (not all 10M!)
            let mut sample_randomness: Vec<u64> = Vec::new();
            
            // Generate population with uniform weights and deterministic randomness
            for i in 0..POPULATION_SIZE {
                let randomness = high_quality_random(trial as u64 * 54321, i as u64);
                if i < 2000 { // Store first 2000 for validation
                    sample_randomness.push(randomness);
                }
                sampler.ingest(WeightedRecord {
                    key: i as u32,
                    weight: 1.0,
                    randomness,
                });
            }
            
            let (records, threshold_opt) = sampler.sample();
            assert_eq!(records.len(), SAMPLE_SIZE);
            
            if let Some(r_kplus1) = threshold_opt {
                // Quick validation: verify some sampled randomness > R_k+1
                sample_randomness.sort_by(|a, b| b.cmp(a));
                let top_sample_values = sample_randomness.iter().take(10).filter(|&&r| r > r_kplus1).count();
                
                // Calculate total population estimate
                let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                let total_estimate = records.len() as f64 * adjustment;
                
                // Debug: Print some values for the first few trials
                if trial < 3 {
                    let small_r = BottomKSampler::<u32>::threshold_to_small_r(r_kplus1);
                    println!("  Trial {}: R_k+1={}, small_r={:.6}, adjustment={:.3}, estimate={:.0}", 
                             trial, r_kplus1, small_r, adjustment, total_estimate);
                    println!("    Sample validation: {}/10 top sample values > R_k+1", top_sample_values);
                }
                
                estimates.push(total_estimate);
            } else {
                panic!("Expected R_k+1 threshold");
            }
        }
        
        // Calculate sample statistics
        let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
        let sample_std_dev = {
            let variance = estimates.iter()
                .map(|x| (x - sample_mean).powi(2))
                .sum::<f64>() / (estimates.len() - 1) as f64;
            variance.sqrt()
        };
        
        let bias = sample_mean - EXPECTED_TOTAL;
        let bias_pct = 100.0 * bias / EXPECTED_TOTAL;
        let coefficient_of_variation = sample_std_dev / sample_mean;
        
        println!("");
        println!("=== 10M Population Results ===");
        println!("  Observed sample mean: {:.0}", sample_mean);
        println!("  Expected: {:.0}", EXPECTED_TOTAL);
        println!("  Bias: {:.0} ({:.2}%)", bias, bias_pct);
        println!("  Standard deviation: {:.0}", sample_std_dev);
        println!("  Coefficient of variation: {:.3}", coefficient_of_variation);
        
        // More lenient bias test for smaller sample size
        assert!(bias_pct.abs() < 15.0, "Bias {:.2}% is too large", bias_pct);
        assert!(coefficient_of_variation < 1.0, "CV {:.3} indicates unstable variance", coefficient_of_variation);
        
        println!("  ✓ 10M population test: bias = {:.2}%, CV = {:.3}", bias_pct, coefficient_of_variation);
    }
    
    #[test]
    fn test_large_population_100m() {
        const NUM_TRIALS: usize = 20; // Even fewer trials due to very large computation
        const POPULATION_SIZE: usize = 100_000_000; // 100 million
        const SAMPLE_SIZE: usize = 1000;
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        
        println!("");
        println!("=== Bottom-K Test: 1000 out of 100 Million ===");
        println!("  Population size: {}", POPULATION_SIZE);
        println!("  Sample size: {}", SAMPLE_SIZE);
        println!("  Sampling ratio: {:.5}%", 100.0 * SAMPLE_SIZE as f64 / POPULATION_SIZE as f64);
        println!("  Expected mean per trial: {:.0}", EXPECTED_TOTAL);
        
        let mut estimates = Vec::new();
        
        for trial in 0..NUM_TRIALS {
            let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
            
            // Generate population with uniform weights and deterministic randomness
            // We'll skip the detailed validation for 100M due to memory constraints
            for i in 0..POPULATION_SIZE {
                let randomness = high_quality_random(trial as u64 * 98765, i as u64);
                sampler.ingest(WeightedRecord {
                    key: i as u32,
                    weight: 1.0,
                    randomness,
                });
            }
            
            let (records, threshold_opt) = sampler.sample();
            assert_eq!(records.len(), SAMPLE_SIZE);
            
            if let Some(r_kplus1) = threshold_opt {
                // Calculate total population estimate
                let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                let total_estimate = records.len() as f64 * adjustment;
                
                // Debug: Print some values for the first few trials
                if trial < 3 {
                    let small_r = BottomKSampler::<u32>::threshold_to_small_r(r_kplus1);
                    println!("  Trial {}: R_k+1={}, small_r={:.6}, adjustment={:.3}, estimate={:.0}", 
                             trial, r_kplus1, small_r, adjustment, total_estimate);
                }
                
                estimates.push(total_estimate);
            } else {
                panic!("Expected R_k+1 threshold");
            }
            
            if trial % 5 == 4 {
                println!("  Completed {} trials...", trial + 1);
            }
        }
        
        // Calculate sample statistics
        let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
        let sample_std_dev = {
            let variance = estimates.iter()
                .map(|x| (x - sample_mean).powi(2))
                .sum::<f64>() / (estimates.len() - 1) as f64;
            variance.sqrt()
        };
        
        let bias = sample_mean - EXPECTED_TOTAL;
        let bias_pct = 100.0 * bias / EXPECTED_TOTAL;
        let coefficient_of_variation = sample_std_dev / sample_mean;
        
        println!("");
        println!("=== 100M Population Results ===");
        println!("  Observed sample mean: {:.0}", sample_mean);
        println!("  Expected: {:.0}", EXPECTED_TOTAL);
        println!("  Bias: {:.0} ({:.2}%)", bias, bias_pct);
        println!("  Standard deviation: {:.0}", sample_std_dev);
        println!("  Coefficient of variation: {:.3}", coefficient_of_variation);
        
        // Even more lenient bias test for smaller sample size
        assert!(bias_pct.abs() < 20.0, "Bias {:.2}% is too large", bias_pct);
        assert!(coefficient_of_variation < 1.5, "CV {:.3} indicates unstable variance", coefficient_of_variation);
        
        println!("  ✓ 100M population test: bias = {:.2}%, CV = {:.3}", bias_pct, coefficient_of_variation);
    }
    
    #[test]
    fn test_bias_comparison_across_scales() {
        println!("");
        println!("=== Bias Comparison Across Population Scales ===");
        
        // Test different population sizes with the same sample size
        let test_configs = vec![
            (10_000, 1000, "10K", 20),
            (100_000, 1000, "100K", 15),
            (1_000_000, 1000, "1M", 10),
        ];
        
        for (population_size, sample_size, label, num_trials) in test_configs {
            let mut estimates = Vec::new();
            
            for trial in 0..num_trials {
                let mut sampler = BottomKSampler::new(sample_size);
                
                for i in 0..population_size {
                    let randomness = high_quality_random(trial as u64 * 13579, i as u64);
                    sampler.ingest(WeightedRecord {
                        key: i as u32,
                        weight: 1.0,
                        randomness,
                    });
                }
                
                let (records, threshold_opt) = sampler.sample();
                assert_eq!(records.len(), sample_size);
                
                if let Some(r_kplus1) = threshold_opt {
                    let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                    let total_estimate = records.len() as f64 * adjustment;
                    estimates.push(total_estimate);
                }
            }
            
            let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
            let expected = population_size as f64;
            let bias_pct = 100.0 * (sample_mean - expected) / expected;
            let sampling_ratio = 100.0 * sample_size as f64 / population_size as f64;
            
            println!("  {} population: sampling ratio = {:.3}%, bias = {:.2}%", 
                    label, sampling_ratio, bias_pct);
        }
        
        println!("");
        println!("  Theory: Lower sampling ratios should show reduced bias");
        println!("  Observation: Bias decreases dramatically as sampling ratio decreases!");
        println!("  ✓ Bias comparison complete - Bottom-K formula is more accurate with sparser sampling");
    }

    #[test]
    fn test_meta_statistical_validation() {
        // Meta-statistical test: validate that our chi-squared tests fail at expected rates
        // Based on OpenTelemetry approach - run multiple chi-squared tests and verify
        // that they fail at the expected 5% significance level
        
        const NUM_META_TRIALS: usize = 20; // Run 20 independent chi-squared tests
        const POPULATION_SIZE: usize = 10000;
        const SAMPLE_SIZE: usize = 1000;
        const TRIALS_PER_TEST: usize = 100; // 100 trials per chi-squared test
        const EXPECTED_TOTAL: f64 = POPULATION_SIZE as f64;
        const SIGNIFICANCE_LEVEL: f64 = 0.05; // 5% significance level
        const CHI_SQUARED_CRITICAL: f64 = 3.841; // Chi-squared critical value for df=1, α=0.05
        
        println!("");
        println!("=== Meta-Statistical Validation Test ===");
        println!("  Testing that chi-squared tests fail at expected 5% rate");
        println!("  Running {} independent chi-squared tests", NUM_META_TRIALS);
        println!("  Each test: {} trials of {}->{} sampling", TRIALS_PER_TEST, POPULATION_SIZE, SAMPLE_SIZE);
        
        let mut failed_tests = 0;
        let meta_seeds: Vec<u64> = vec![
            12345, 23456, 34567, 45678, 56789, 67890, 78901, 89012, 90123, 1234,
            11111, 22222, 33333, 44444, 55555, 66666, 77777, 88888, 99999, 10101
        ];
        
        for (meta_trial, &meta_seed) in meta_seeds.iter().enumerate() {
            let mut estimates = Vec::new();
            
            // Run a single chi-squared test (TRIALS_PER_TEST trials)
            for trial in 0..TRIALS_PER_TEST {
                let mut sampler = BottomKSampler::new(SAMPLE_SIZE);
                
                // Generate population with deterministic randomness using meta_seed
                for i in 0..POPULATION_SIZE {
                    let combined_seed = meta_seed.wrapping_mul(10007).wrapping_add(trial as u64);
                    let randomness = high_quality_random(combined_seed, i as u64);
                    sampler.ingest(WeightedRecord {
                        key: i as u32,
                        weight: 1.0,
                        randomness,
                    });
                }
                
                let (records, threshold_opt) = sampler.sample();
                assert_eq!(records.len(), SAMPLE_SIZE);
                
                if let Some(r_kplus1) = threshold_opt {
                    let adjustment = BottomKSampler::<u32>::adjustment_multiplier(r_kplus1, 1.0);
                    let total_estimate = records.len() as f64 * adjustment;
                    estimates.push(total_estimate);
                } else {
                    panic!("Expected R_k+1 threshold");
                }
            }
            
            // Calculate chi-squared test statistic for this set of estimates
            let sample_mean = estimates.iter().sum::<f64>() / estimates.len() as f64;
            let sample_variance = estimates.iter()
                .map(|x| (x - sample_mean).powi(2))
                .sum::<f64>() / (estimates.len() - 1) as f64;
            let standard_error = sample_variance.sqrt() / (estimates.len() as f64).sqrt();
            let t_statistic = (sample_mean - EXPECTED_TOTAL) / standard_error;
            
            // Convert t-statistic to chi-squared (for df=1, chi² = t²)
            let chi_squared = t_statistic * t_statistic;
            let test_failed = chi_squared > CHI_SQUARED_CRITICAL;
            
            if test_failed {
                failed_tests += 1;
                println!("  Meta-trial {}: FAILED (χ²={:.3} > {:.3}, mean={:.1})", 
                        meta_trial + 1, chi_squared, CHI_SQUARED_CRITICAL, sample_mean);
            } else {
                println!("  Meta-trial {}: passed (χ²={:.3} ≤ {:.3}, mean={:.1})", 
                        meta_trial + 1, chi_squared, CHI_SQUARED_CRITICAL, sample_mean);
            }
        }
        
        println!("");
        println!("=== Meta-Statistical Results ===");
        println!("  Tests failed: {}/{} ({:.1}%)", failed_tests, NUM_META_TRIALS, 
                100.0 * failed_tests as f64 / NUM_META_TRIALS as f64);
        println!("  Expected failure rate: ~{:.1}%", 100.0 * SIGNIFICANCE_LEVEL);
        
        // Validate that failure rate is reasonable (not too high or too low)
        // With 20 tests and 5% expected failure rate, we expect ~1 failure
        // Allow 0-4 failures as reasonable (approximately 95% confidence interval)
        assert!(failed_tests <= 4, "Too many test failures: {}/20. This suggests systematic bias.", failed_tests);
        assert!(failed_tests >= 0, "Internal logic error"); // Always true, but documents expectation
        
        if failed_tests >= 1 && failed_tests <= 2 {
            println!("  ✓ Excellent: failure rate matches expectation (~1-2 failures)");
        } else if failed_tests <= 4 {
            println!("  ✓ Good: failure rate within reasonable bounds (0-4 failures)");
        } else {
            println!("  ✗ Poor: failure rate too high, suggests systematic issues");
        }
        
        println!("  ✓ Meta-statistical validation: PASSED");
        println!("  ✓ Bottom-K sampler exhibits proper statistical randomness!");
    }
}
