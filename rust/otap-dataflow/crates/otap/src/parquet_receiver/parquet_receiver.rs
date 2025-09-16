//! Main ParquetReceiver implementation
//!
//! This module contains the main receiver implementation that integrates with
//! the OTAP pipeline engine.

use crate::parquet_receiver::{
    config::Config,
    error::ParquetReceiverError,
    file_discovery::FileDiscovery,
    query_engine::ParquetQueryEngine,
    reconstruction::OtapReconstructor,
};
use crate::{pdata::OtapPdata, OTAP_RECEIVER_FACTORIES};
use async_trait::async_trait;
use linkme::distributed_slice;
use log::{debug, error, info};
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::{
    config::ReceiverConfig,
    context::PipelineContext,
    error::Error,
    node::NodeId,
    receiver::ReceiverWrapper,
    shared::receiver as shared,
    ReceiverFactory,
};
use serde_json::Value;
use std::sync::Arc;
use tokio::time::interval;

/// URN for the Parquet Receiver
pub const PARQUET_RECEIVER_URN: &str = "urn:otel:otap:parquet:receiver";

/// Main ParquetReceiver implementation
pub struct ParquetReceiver {
    /// Configuration for this receiver
    config: Config,
    /// File discovery component
    file_discoverer: FileDiscovery,
    /// Query engine for parquet operations  
    query_engine: ParquetQueryEngine,
    /// OTAP data reconstructor
    reconstructor: OtapReconstructor,
}

impl ParquetReceiver {
    /// Create a new ParquetReceiver with the given configuration
    pub fn new(config: Config) -> Result<Self, ParquetReceiverError> {
        debug!("Creating ParquetReceiver with config: {:?}", config);

        let file_discoverer = FileDiscovery::new(
            config.base_uri.clone().into(),
            config.processing_options.min_file_age,
        );
        let query_engine = ParquetQueryEngine::new();
        let reconstructor = OtapReconstructor::new(Some(config.processing_options.batch_size));

        Ok(Self {
            config,
            file_discoverer,
            query_engine,
            reconstructor,
        })
    }

    /// Create a new ParquetReceiver from configuration value
    pub fn from_config(
        _pipeline: PipelineContext,
        config: &Value,
    ) -> Result<Self, otap_df_config::error::Error> {
        let config: Config = serde_json::from_value(config.clone()).map_err(|e| {
            otap_df_config::error::Error::InvalidUserConfig {
                error: format!("Invalid ParquetReceiver configuration: {}", e),
            }
        })?;

        Self::new(config).map_err(|e| otap_df_config::error::Error::InvalidUserConfig {
            error: format!("Failed to create ParquetReceiver: {}", e),
        })
    }
}

#[async_trait]
impl shared::Receiver<OtapPdata> for ParquetReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl_msg_recv: shared::ControlChannel<OtapPdata>,
        effect_handler: shared::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        debug!("🚀 ParquetReceiver::start() method called - entering main loop");
        info!(
            "Starting ParquetReceiver with base_uri: {}, signal_types: {:?}, polling_interval: {:?}",
            self.config.base_uri, self.config.signal_types, self.config.polling_interval
        );

        debug!("🔍 DEBUG: ParquetReceiver start() method called - creating polling interval");

        // Create the polling interval inside the async context where Tokio runtime is available
        let mut polling_timer = interval(self.config.polling_interval);

        debug!("🔍 DEBUG: ParquetReceiver polling timer created successfully");

        debug!("🔍 DEBUG: ParquetReceiver entering main loop");

        loop {
            tokio::select! {
                // Handle control messages
                ctrl_msg = ctrl_msg_recv.recv() => {
                    match ctrl_msg {
                        Ok(msg) => {
                            debug!("Received control message: {:?}", msg);
                            match msg {
                                otap_df_engine::control::NodeControlMsg::Shutdown { .. } => {
                                    info!("Shutdown requested, stopping ParquetReceiver");
                                    break;
                                }
                                _ => {
                                    debug!("Ignoring unsupported control message");
                                }
                            }
                        }
                        Err(e) => {
                            error!("Control channel error: {}, shutting down ParquetReceiver", e);
                            break;
                        }
                    }
                }
                // Polling for new files
                _ = polling_timer.tick() => {
                    debug!("⏰ Polling timer tick - scanning for new parquet files...");

                    // Discover new files for each configured signal type
                    for signal_type in &self.config.signal_types {
                        debug!("🔍 Polling for new {:?} files...", signal_type);
                        match self.file_discoverer.discover_new_files(&[signal_type.clone()]) {
                            Ok(files) => {
                                if !files.is_empty() {
                                    info!("📁 Found {} new {:?} files to process:", files.len(), signal_type);
                                    for file in &files {
                                        info!("📄 Processing file: {}", file.path.display());
                                    }

                                    // For now, just process each file individually
                                    for file in &files {
                                        // Query a single file to get RecordBatch
                                        match self.query_engine.query_file(&file).await {
                                            Ok(query_result) => {
                                                // Reconstruct OTAP data
                                                match self.reconstructor.reconstruct_otap_data(query_result) {
                                                    Ok(otap_records) => {
                                                        // Convert to OtapPdata and send downstream
                                                        let pdata = OtapPdata::new_todo_context(otap_records.into());

                                                        debug!("Sending reconstructed data downstream for signal: {:?}", signal_type);

                                                        // Send the data downstream
                                                        if let Err(e) = effect_handler.send_message(pdata).await {
                                                            error!("Failed to send data downstream: {}", e);
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to reconstruct OTAP data: {}", e);
                                                        continue;
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to query parquet file: {}", e);
                                                continue;
                                            }
                                        }

                                        // Mark file as processed
                                        self.file_discoverer.mark_processed(&file.path);
                                    }
                                } else {
                                    debug!("No new {:?} files found", signal_type);
                                }
                            }
                            Err(e) => {
                                error!("Failed to discover {:?} files: {}", signal_type, e);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        info!("ParquetReceiver shutdown complete");
        Ok(())
    }
}

/// Register the ParquetReceiver factory using distributed_slice
#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static PARQUET_RECEIVER_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: PARQUET_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig| {
        Ok(ReceiverWrapper::shared(
            ParquetReceiver::from_config(pipeline, &node_config.config)?,
            node,
            node_config,
            receiver_config,
        ))
    },
};