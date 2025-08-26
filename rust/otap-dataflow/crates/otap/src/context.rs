// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use tokio::time::Instant;

/// Context for OTAP requests
#[derive(Clone, Debug)]
pub struct Context {
    msg_id: usize,
    deadline: Instant,

    // return-to information
    reply_count: usize,
}

/// Context for OTAP responses
#[derive(Clone, Debug)]
pub struct ReturnContext {
    // routing
    // msg_id: usize,
    pub(crate) message: String,
    pub(crate) failure: bool,
    pub(crate) permanent: bool,
    pub(crate) code: Option<tonic::Code>,

    // partial: the OTLP partial-success failure
    pub(crate) rejected: Option<i64>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            msg_id: 0,
            deadline: Instant::now(), // @@@
            reply_count: 0,
        }
    }
}
