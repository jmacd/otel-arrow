#!/bin/bash

# This is pretty noisy
#export RUST_LOG=debug

# This is OK:
export RUST_LOG=otap_df_engine=debug,otap_df_config=debug,otap_df_controller=debug,otap_df_otap=debug

cargo run --bin df_engine -- --num-cores 1 --pipeline configs/fake-logs-parquet-streaming.yaml
