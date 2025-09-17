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
    streaming_coordinator::{StreamingConfig, StreamingCoordinator},
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
    /// Streaming coordinator for multi-stream processing
    streaming_coordinator: StreamingCoordinator,
}

impl ParquetReceiver {
    /// Create a new ParquetReceiver with the given configuration
    pub fn new(config: Config) -> Result<Self, ParquetReceiverError> {
        let file_discoverer = FileDiscovery::new(
            config.base_uri.clone().into(),
            config.processing_options.min_file_age,
        );
        let query_engine = ParquetQueryEngine::new();
        let reconstructor = OtapReconstructor::new(Some(config.processing_options.batch_size));

        // Configure streaming coordinator
        let streaming_config = StreamingConfig {
            base_directory: config.base_uri.clone().into(),
            primary_batch_size: config.processing_options.batch_size,
        };
        let streaming_coordinator = StreamingCoordinator::new(streaming_config);

        Ok(Self {
            config,
            file_discoverer,
            query_engine,
            reconstructor,
            streaming_coordinator,
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

                                    // Process partition using streaming coordinator
                                    match self.streaming_coordinator.process_partition(&files[0].partition_id, &signal_type).await {
                                        Ok(streaming_batches) => {
                                            info!("� Streaming coordinator found {} batches for partition {}",
                                                streaming_batches.len(), files[0].partition_id);

                                            // Process each streaming batch
                                            for streaming_batch in streaming_batches {
                                                log::debug!("📊 Processing streaming batch: {} primary records, max_id: {}",
                                                           streaming_batch.primary_batch.num_rows(),
                                                           streaming_batch.max_primary_id);

                                                // Convert streaming batch to OTAP with ID mapping
                                                match self.streaming_coordinator.batch_to_otap(streaming_batch) {
                                                    Ok(otap_records) => {
                                                        log::debug!("🎯 OTAP reconstruction successful - batch length: {}", otap_records.batch_length());

                                                        // Convert to OtapPdata and send downstream
                                                        let pdata = OtapPdata::new_todo_context(otap_records.into());

                                                        info!("🚀 Sending reconstructed OTAP data downstream for signal: {:?}", signal_type);

                                                        // Send the data downstream
                                                        if let Err(e) = effect_handler.send_message(pdata).await {
                                                            error!("Failed to send data downstream: {}", e);
                                                        } else {
                                                            log::debug!("✅ Data successfully sent downstream");
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to convert streaming batch to OTAP: {}", e);
                                                        continue;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("Failed to query parquet partition: {}", e);
                                            continue;
                                        }
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
