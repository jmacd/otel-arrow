// This example begins with a copy of ../beaubourg/src/examples/thread_per_core_engine_example.rs

use beaubourg::{engine::Engine, task::labels::ProcessLabels};
use engine::thread_per_core;
use mimalloc_rust::GlobalMiMalloc;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::{exporter::TestExporterFactory, processor::TestProcessorFactory, receiver::TestReceiverFactory};

mod exporter;
mod processor;
mod receiver;

#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

fn main() -> Result<()> {
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
fn init() -> Result<()> {
    let subscriber = FmtSubscriber::builder().with_max_level(Level::INFO).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    Ok(())
}
