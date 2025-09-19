#!/bin/bash

# Enable comprehensive DataFusion logging to debug the hanging queries
export RUST_LOG=debug

# This is OK:
#export RUST_LOG=otap_df_engine=debug;otap_df_config=debug;otap_df_controller=debug;otal_df_otap=debug

cargo build --bin df_engine && \
timeout 30s ./target/debug/df_engine --num-cores 1 --pipeline configs/sampling-receiver-demo.yaml
