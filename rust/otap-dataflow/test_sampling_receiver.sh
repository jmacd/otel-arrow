#!/bin/bash

# Enable detailed DataFusion logging to debug schema inference
export RUST_LOG="otap_df_otap::sampling_receiver=debug,datafusion=debug,datafusion_core=debug,datafusion_physical_plan=debug"

# This is OK:
#export RUST_LOG=otap_df_engine=debug;otap_df_config=debug;otap_df_controller=debug;otal_df_otap=debug

cargo build --bin df_engine && \
timeout 10s ./target/debug/df_engine --num-cores 1 --pipeline configs/sampling-receiver-demo.yaml
