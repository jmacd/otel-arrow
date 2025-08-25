// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use tokio::time::Instant;

/// Context for OTAP requests
#[derive(Clone, Debug)]
pub struct Context {
    msg_id: usize,
    deadline: Instant,
    // return-to information
}

impl Default for Context {
    fn default() -> Self {
        Self {
            msg_id: 0,
            deadline: Instant::now(), // @@@
        }
    }
}
