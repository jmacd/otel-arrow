//! Main SamplingReceiver implementation
//!
//! This module contains the main receiver implementation that integrates with
//! the OTAP pipeline engine and provides DataFusion-powered query processing.

use crate::sampling_receiver::{
    config::Config,
    error::Result,
    query_engine::DataFusionQueryEngine,
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
        Ok(Self { config, query_engine: None })
    }

    /// Initialize the DataFusion query engine if not already done
    async fn ensure_query_engine(&mut self) -> Result<&DataFusionQueryEngine> {
        if self.query_engine.is_none() {
            info!("Initializing DataFusion query engine");
            self.query_engine = Some(DataFusionQueryEngine::new(self.config.clone()).await?);
        }
        Ok(self.query_engine.as_ref().unwrap())
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
        let record_batches = query_engine.execute_query(query).await?;
        
        // TODO: Convert RecordBatch results to OtapPdata
        // For now, return empty results but log what we got
        let total_rows: usize = record_batches.iter().map(|batch| batch.num_rows()).sum();
        info!("DataFusion query returned {} batches with {} total rows", record_batches.len(), total_rows);
        
        // TODO: Implement OTAP reconstruction in the next task
        Ok(vec![])
    }

    /// Calculate time windows based on configuration
    fn calculate_time_windows(&self, current_time_ns: i64) -> Vec<(i64, i64)> {
        let window_duration_ns = self.config.temporal.window_granularity.as_nanos() as i64;
        let processing_delay_ns = self.config.temporal.processing_delay.as_nanos() as i64;

        // Calculate the latest window we can safely process
        let latest_processable_time = current_time_ns - processing_delay_ns;
        
        // Align to window boundaries
        let window_start = (latest_processable_time / window_duration_ns) * window_duration_ns;
        let window_end = window_start + window_duration_ns;

        // For now, return a single window - later we'll support multiple windows
        vec![(window_start, window_end)]
    }

    /// Process a single time interval
    async fn process_interval(&mut self, effect_handler: &shared::EffectHandler<OtapPdata>) -> Result<()> {
        let current_time_ns = chrono::Utc::now().timestamp_nanos_opt()
            .unwrap_or_default();
        
        let windows = self.calculate_time_windows(current_time_ns);
        
        for (window_start_ns, window_end_ns) in windows {
            let records = self.process_time_window(window_start_ns, window_end_ns).await?;
            
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