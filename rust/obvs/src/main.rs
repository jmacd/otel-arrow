// This example begins with a copy of ../beaubourg/src/examples/thread_per_core_engine_example.rs

use beaubourg::{
    engine::Engine,
    engine::thread_per_core,
    task::labels::ProcessLabels,
};

mod common;
mod exporter;
mod processor;
mod receiver;

use crate::{
    exporter::TestExporterFactory,
    processor::TestProcessorFactory,
    receiver::TestReceiverFactory,
};

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

fn main() -> Result<(), anyhow::Error> {
    init()?;

    let mut engine = thread_per_core::Engine::new(
        TestReceiverFactory::default(),
        TestProcessorFactory::default(),
        TestExporterFactory::default(),
    );
    engine.run(ProcessLabels::new("test"), "config.yaml")?;

    Ok(())
}

/// Initializes the collector
fn init() -> Result<(), anyhow::Error> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    Ok(())
}
