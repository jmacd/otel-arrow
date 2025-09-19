//! Main SamplingReceiver implementation
//!
//! This module contains the main receiver implementation that integrates with
//! the OTAP pipeline engine and provides DataFusion-powered query processing.

use crate::sampling_receiver::{
    config::Config,
    error::{Result, SamplingReceiverError},
    query_engine::DataFusionQueryEngine,
    otap_reconstructor::OtapReconstructor,
};
use crate::{OTAP_RECEIVER_FACTORIES, pdata::OtapPdata};
use async_trait::async_trait;
use linkme::distributed_slice;
use log::{debug, error, info};
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::{
    ReceiverFactory, config::ReceiverConfig, context::PipelineContext, error::Error, node::NodeId,
    receiver::ReceiverWrapper, shared::receiver as shared,
};
use std::sync::Arc;
use tokio::time::{interval, Duration};

/// URN for the Sampling Receiver
pub const SAMPLING_RECEIVER_URN: &str = "urn:otel:otap:sampling:receiver";

/// Main SamplingReceiver implementation
pub struct SamplingReceiver {
    /// Configuration for this receiver
    config: Config,
    /// DataFusion query engine (lazily initialized)
    query_engine: Option<DataFusionQueryEngine>,
    /// OTAP record reconstructor (lazily initialized)
    otap_reconstructor: Option<OtapReconstructor>,
}

impl SamplingReceiver {
    /// Create a new SamplingReceiver with the given configuration
    pub fn new(config: Config) -> Result<Self> {
        // Validate configuration
        config.validate()?;
        
        info!(
            "Created SamplingReceiver with base_uri: {}, window_granularity: {:?}",
            config.base_uri, config.temporal.window_granularity
        );

        // Defer DataFusion query engine creation until first use
        Ok(Self { 
            config, 
            query_engine: None,
            otap_reconstructor: None,
        })
    }

    /// Initialize the DataFusion query engine if not already done
    async fn ensure_query_engine(&mut self) -> Result<&DataFusionQueryEngine> {
        if self.query_engine.is_none() {
            let query_engine = DataFusionQueryEngine::new(self.config.clone()).await?;
            self.query_engine = Some(query_engine);
        }
        Ok(self.query_engine.as_ref().unwrap())
    }

    /// Initialize the OTAP reconstructor if not already done
    async fn ensure_otap_reconstructor(&mut self) -> Result<&OtapReconstructor> {
        if self.otap_reconstructor.is_none() {
            let _query_engine = self.ensure_query_engine().await?; // Ensure query engine exists
            // Create a new Arc from query engine for the reconstructor
            let query_engine = Arc::new(DataFusionQueryEngine::new(self.config.clone()).await?);
            let reconstructor = OtapReconstructor::new(
                Arc::clone(&query_engine),
                &self.config,
            );
            self.otap_reconstructor = Some(reconstructor);
        }
        Ok(self.otap_reconstructor.as_ref().unwrap())
    }

    /// Create a SamplingReceiver from configuration  
    fn from_config(
        _pipeline: PipelineContext,
        config: &serde_json::Value,
    ) -> std::result::Result<SamplingReceiver, otap_df_config::error::Error> {
        let config: Config = serde_json::from_value(config.clone())
            .map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
                error: format!("Failed to parse sampling receiver config: {}", e),
            })?;
        
        SamplingReceiver::new(config)
            .map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
                error: format!("Failed to create sampling receiver: {}", e),
            })
    }

    /// Process a time window using DataFusion queries
    async fn process_time_window(
        &mut self,
        window_start_ns: i64,
        window_end_ns: i64,
    ) -> Result<Vec<OtapPdata>> {
        debug!(
            "Processing time window: {} to {}",
            window_start_ns, window_end_ns
        );

        // Get the query with substituted parameters
        let query = self.config.get_query_with_window(window_start_ns, window_end_ns);
        
        debug!("Executing query: {}", query);

        // TODO: Implement DataFusion query execution
        // This is a placeholder that will be implemented in Phase 2
        let results = self.execute_datafusion_query(&query).await?;
        
        Ok(results)
    }

    /// Execute a DataFusion query and convert results to OTAP data
    async fn execute_datafusion_query(&mut self, query: &str) -> Result<Vec<OtapPdata>> {
        debug!("Executing DataFusion query: {}", query);
        
        // Ensure query engine is initialized
        let query_engine = self.ensure_query_engine().await?;
        
        // Execute the query using the DataFusion engine
        let query_results = query_engine.execute_query(query).await?;
        
        // Get OTAP reconstructor and process results
        let reconstructor = self.ensure_otap_reconstructor().await?;
        let _related_data = reconstructor.get_related_data().await?; // This will be implemented
        
        // Convert query results to OTAP records using the reconstructor (now needs mutable access)
        let otap_records = {
            let mut_reconstructor = self.otap_reconstructor.as_mut()
                .ok_or_else(|| SamplingReceiverError::ReconstructionError {
                    message: "Reconstructor not initialized".to_string(),
                })?;
            mut_reconstructor.reconstruct_from_stream(query_results).await?
        };
        
        info!("Successfully reconstructed {} OTAP records", otap_records.len());
        
        Ok(otap_records)
    }

    /// Discover time range from parquet files
    async fn discover_data_time_range(&mut self) -> Result<(i64, i64)> {
        let query_engine = self.ensure_query_engine().await?;
        
        // Query to find min and max timestamps across all data
        let discovery_query = r#"
        SELECT 
            MIN(time_unix_nano) as min_time,
            MAX(time_unix_nano) as max_time
        FROM logs
        "#;
        
        debug!("Discovering data time range with query: {}", discovery_query);
        let results = query_engine.execute_query(discovery_query).await?;
        
        if results.is_empty() {
            error!("No data found in parquet files");
            return Err(SamplingReceiverError::config_error(
                "No data found in parquet files"
            ));
        }
        
        // Extract min and max timestamps
        let batch = &results[0];
        let min_col = batch.column_by_name("min_time").unwrap();
        let max_col = batch.column_by_name("max_time").unwrap();
        
        // Cast to timestamp and convert to nanoseconds
        let min_time_ns = arrow::compute::cast(min_col, &arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond, None
        ))?;
        let max_time_ns = arrow::compute::cast(max_col, &arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond, None
        ))?;
        
        let min_array = min_time_ns.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();
        let max_array = max_time_ns.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();
        
        let min_time = min_array.value(0);
        let max_time = max_array.value(0);
        
        info!("Discovered data time range: {} to {} ns", min_time, max_time);
        Ok((min_time, max_time))
    }

    /// Calculate all time windows to process based on actual data range
    async fn calculate_all_windows(&mut self) -> Result<Vec<(i64, i64)>> {
        let (min_time_ns, max_time_ns) = self.discover_data_time_range().await?;
        let window_duration_ns = self.config.temporal.window_granularity.as_nanos() as i64;

        // Align min time to window boundary (round down to nearest minute)
        let first_window_start = (min_time_ns / window_duration_ns) * window_duration_ns;
        
        // Calculate all windows from first to last
        let mut windows = Vec::new();
        let mut current_start = first_window_start;
        
        while current_start <= max_time_ns {
            let current_end = current_start + window_duration_ns;
            windows.push((current_start, current_end));
            current_start = current_end;
        }
        
        info!("Generated {} time windows to process", windows.len());
        Ok(windows)
    }

    /// Process a single time interval - now processes all data chronologically
    async fn process_interval(&mut self, effect_handler: &shared::EffectHandler<OtapPdata>) -> Result<()> {
        let windows = self.calculate_all_windows().await?;
        
        info!("Processing {} time windows sequentially", windows.len());
        
        for (i, (window_start_ns, window_end_ns)) in windows.iter().enumerate() {
            info!("Processing window {}/{}: {} to {}", i + 1, windows.len(), window_start_ns, window_end_ns);
            let records = self.process_time_window(*window_start_ns, *window_end_ns).await?;
            
            // Send records downstream
            for record in records {
                if let Err(e) = effect_handler.send_message(record).await {
                    error!("Failed to send OTAP record downstream: {}", e);
                }
            }
        }
        
        Ok(())
    }
}

#[async_trait]
impl shared::Receiver<OtapPdata> for SamplingReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl_msg_recv: shared::ControlChannel<OtapPdata>,
        effect_handler: shared::EffectHandler<OtapPdata>,
    ) -> std::result::Result<(), Error> {
        info!("Starting SamplingReceiver");

        let mut polling_timer = interval(Duration::from_secs(
            self.config.temporal.window_granularity.as_secs()
        ));

        loop {
            tokio::select! {
                // Handle control messages
                ctrl_msg = ctrl_msg_recv.recv() => {
                    match ctrl_msg {
                        Ok(msg) => {
                            debug!("Received control message: {:?}", msg);
                            match msg {
                                otap_df_engine::control::NodeControlMsg::Shutdown { .. } => {
                                    info!("Shutdown requested, stopping SamplingReceiver");
                                    break;
                                }
                                _ => {
                                    debug!("Received other control message");
                                }
                            }
                        }
                        Err(_) => {
                            info!("Control channel closed, shutting down SamplingReceiver");
                            break;
                        }
                    }
                }
                // Handle periodic processing
                _ = polling_timer.tick() => {
                    match self.process_interval(&effect_handler).await {
                        Ok(_) => {
                            debug!("Successfully processed interval");
                        }
                        Err(e) => {
                            error!("Error processing interval: {}", e);
                            // Continue processing - don't exit on errors
                        }
                    }
                }
            }
        }

        info!("SamplingReceiver shutdown complete");
        Ok(())
    }
}

/// Register the SamplingReceiver factory using distributed_slice
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static SAMPLING_RECEIVER_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SAMPLING_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig| {
        Ok(ReceiverWrapper::shared(
            SamplingReceiver::from_config(pipeline, &node_config.config)?,
            node,
            node_config,
            receiver_config,
        ))
    },
};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use url::Url;

    fn create_test_config() -> Config {
        let temp_dir = tempdir().unwrap();
        let mut config = Config::default();
        config.base_uri = Url::from_file_path(temp_dir.path()).unwrap();
        config
    }

    #[test]
    fn test_sampling_receiver_creation() {
        let config = create_test_config();
        
        let receiver = SamplingReceiver::new(config);
        assert!(receiver.is_ok());
    }

    #[test]
    fn test_time_window_calculation() {
        let config = create_test_config();
        let receiver = SamplingReceiver::new(config).unwrap();
        
        let current_time_ns = 1000000000000i64; // Some test time
        let windows = receiver.calculate_time_windows(current_time_ns);
        
        assert!(!windows.is_empty());
        let (start, end) = windows[0];
        assert!(start < end);
        assert!(end - start > 0);
    }

    #[test]
    fn test_query_substitution() {
        let config = create_test_config();
        let receiver = SamplingReceiver::new(config).unwrap();
        
        let query = receiver.config.get_query_with_window(1000000, 2000000);
        assert!(query.contains("1000000"));
        assert!(query.contains("2000000"));
    }
}