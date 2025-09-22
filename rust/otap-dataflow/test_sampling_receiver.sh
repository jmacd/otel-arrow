#!/bin/bash

# Enable application logging to debug the query results and OTLP encoding
export RUST_LOG=otap_df_engine=debug,otap_df_config=debug,otap_df_controller=debug,otap_df_otap=debug,otel_arrow_rust=debug

# This is OK:
#export RUST_LOG=otap_df_engine=debug;otap_df_config=debug;otap_df_controller=debug;otal_df_otap=debug

cargo build --bin df_engine && \
timeout 300s ./target/debug/df_engine --num-cores 1 --pipeline configs/sampling-receiver-demo.yaml
