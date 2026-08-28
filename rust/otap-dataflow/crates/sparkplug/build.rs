// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Build script for the standalone Sparkplug protobuf schema.

use std::path::PathBuf;

fn main() {
    println!("cargo::rerun-if-changed=proto/sparkplug.proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("failed to locate bundled protoc binary");

    let mut config = prost_build::Config::new();
    let _ = config.protoc_executable(protoc);
    let _ = config.disable_comments(["."]);
    let _ = config.format(false);
    let _ = config.include_file("sparkplug.rs");

    let proto = PathBuf::from("proto").join("sparkplug.proto");
    let includes = [PathBuf::from("proto")];

    config
        .compile_protos(&[proto], &includes)
        .expect("failed to compile Sparkplug protobuf schema");
}
