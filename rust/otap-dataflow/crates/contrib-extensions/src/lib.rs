// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP Contrib extensions.
//!
//! Each extension is gated behind an opt-in feature and registers itself into
//! the OTAP pipeline factory's extension slice via `linkme` when its feature is
//! enabled.

#[cfg(feature = "azure-identity-auth-extension")]
pub mod azure_identity_auth;

#[cfg(feature = "k8s-service-account-token-auth-extension")]
pub mod k8s_service_account_token_auth;

#[cfg(feature = "oauth2-client-auth-extension")]
pub mod oauth2_client_auth;

#[cfg(feature = "mqtt-client-extension")]
pub mod mqtt_client;

#[cfg(feature = "mqtt-server-extension")]
pub mod mqtt_server;

pub mod common;
