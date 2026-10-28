// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Node-facing bindings compiled from pdata context declarations.

use super::propagation::CompiledHeaderPropagationPolicy;
use crate::capability::auth::{AuthorizedIdentity, ClaimValue};
use crate::pdata_context::{ContextMaterializationPlan, Metadata};
use http::HeaderMap;
use otel_arrow_dfe_config::context::authorized_identity::AuthorizedIdentityPolicy;
use otel_arrow_dfe_config::context::transport_headers::{TransportHeaders, ValueKind};
pub use otel_arrow_dfe_config::context::transport_policy::CaptureStats;
use otel_arrow_dfe_config::context::transport_policy::CompiledHeaderCapturePolicy;
use otel_arrow_dfe_config::node::NodeKind;
use std::net::SocketAddr;
use std::sync::Arc;

/// A borrowed authorized identity claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimRef<'a> {
    /// One claim value.
    One(&'a str),
    /// Multiple claim values.
    Many(&'a [String]),
}

impl<'a> ClaimRef<'a> {
    /// Returns the number of values in the claim.
    #[must_use]
    pub fn len(self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(values) => values.len(),
        }
    }

    /// Returns whether the claim contains no values.
    #[must_use]
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    /// Returns whether the claim is explicitly multi-valued.
    #[must_use]
    pub fn is_many(self) -> bool {
        matches!(self, Self::Many(_))
    }

    /// Iterates over the claim's values.
    pub fn values(self) -> impl Iterator<Item = &'a str> {
        let one = match self {
            Self::One(value) => Some(value),
            Self::Many(_) => None,
        };
        let many = match self {
            Self::One(_) => &[][..],
            Self::Many(values) => values,
        };
        one.into_iter().chain(many.iter().map(String::as_str))
    }
}

/// Provides claims to an arrival binding without coupling it to one identity type.
pub trait Claims {
    /// Returns the named claim when it is present.
    fn claim(&self, name: &str) -> Option<ClaimRef<'_>>;
}

impl Claims for AuthorizedIdentity {
    fn claim(&self, name: &str) -> Option<ClaimRef<'_>> {
        AuthorizedIdentity::claim(self, name).map(|value| match value {
            ClaimValue::One(value) => ClaimRef::One(value),
            ClaimValue::Many(values) => ClaimRef::Many(values),
        })
    }
}

/// Transport headers presented to an arrival binding.
#[derive(Debug, Clone, Copy)]
pub enum ArrivalHeaders<'a> {
    /// Protocol-neutral decoded header pairs.
    Pairs(&'a [(&'a str, &'a [u8])]),
    /// Native HTTP headers, captured without building an intermediate pair vector.
    Http(&'a HeaderMap),
}

/// Request metadata observed by a receiver.
#[derive(Clone, Copy)]
pub struct Arrival<'a> {
    /// Inbound transport headers.
    pub headers: ArrivalHeaders<'a>,
    /// Verified identity produced by the receiver's authorization layer.
    pub identity: Option<&'a dyn Claims>,
    /// Socket peer observed by the receiver.
    pub peer: Option<SocketAddr>,
}

/// Forms message metadata at a receiver boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrivalBinding {
    header_capture: Option<CompiledHeaderCapturePolicy>,
    authorized_identity_capture: Option<AuthorizedIdentityPolicy>,
    materialization: ContextMaterializationPlan,
}

impl ArrivalBinding {
    /// Forms complete metadata from one receiver arrival.
    #[must_use]
    pub fn form(&self, arrival: &Arrival<'_>) -> (Metadata, Option<CaptureStats>) {
        let mut metadata = Metadata::default();
        let capture_stats = self.header_capture.as_ref().and_then(|policy| {
            let mut captured = TransportHeaders::new();
            let stats = match arrival.headers {
                ArrivalHeaders::Pairs(pairs) => policy.capture_from_pairs(
                    pairs.iter().map(|(name, value)| (*name, *value)),
                    &mut captured,
                ),
                ArrivalHeaders::Http(headers) => {
                    policy.capture_from_http_headers(headers, &mut captured)
                }
            };
            if !captured.is_empty() {
                metadata.set_transport_headers(captured);
            }
            stats
        });
        if let (Some(policy), Some(identity)) =
            (&self.authorized_identity_capture, arrival.identity)
        {
            metadata.capture_claims(policy, identity);
        }
        if let Some(peer) = arrival.peer {
            metadata.set_peer_addr(peer);
        }
        metadata.materialize_context(&self.materialization);
        (metadata, capture_stats)
    }

    pub(crate) fn header_capture_policy(&self) -> Option<&CompiledHeaderCapturePolicy> {
        self.header_capture.as_ref()
    }

    pub(crate) fn authorized_identity_policy(&self) -> Option<&AuthorizedIdentityPolicy> {
        self.authorized_identity_capture.as_ref()
    }

    pub(crate) fn set_capture_policy(&mut self, policy: Option<CompiledHeaderCapturePolicy>) {
        self.header_capture = policy;
    }

    pub(crate) fn set_authorized_identity_policy(
        &mut self,
        policy: Option<AuthorizedIdentityPolicy>,
    ) {
        self.authorized_identity_capture = policy;
    }

    pub(crate) fn set_materialization_plan(&mut self, plan: ContextMaterializationPlan) {
        self.materialization = plan;
    }

    pub(crate) fn materialization_plan(&self) -> &ContextMaterializationPlan {
        &self.materialization
    }
}

/// A propagated header value borrowed from message metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropagatedValue<'a> {
    kind: ValueKind,
    bytes: &'a [u8],
}

impl<'a> PropagatedValue<'a> {
    /// Returns whether the value is text or binary.
    #[must_use]
    pub const fn kind(self) -> ValueKind {
        self.kind
    }

    /// Returns the raw value bytes.
    #[must_use]
    pub const fn bytes(self) -> &'a [u8] {
        self.bytes
    }
}

/// Emits transport headers selected for an exporter boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkBinding {
    policy: CompiledHeaderPropagationPolicy,
}

impl SinkBinding {
    /// Emits all primitive and materialized headers selected by the binding.
    pub fn propagate(&self, metadata: &Metadata, mut emit: impl FnMut(&str, PropagatedValue<'_>)) {
        if let Some(headers) = metadata.transport_headers() {
            for header in self
                .policy
                .propagate_materialized(headers, metadata.materialized_context())
            {
                emit(
                    header.header_name,
                    PropagatedValue {
                        kind: header.value_kind,
                        bytes: header.value,
                    },
                );
            }
        } else {
            for header in self
                .policy
                .propagate_materialized_only(metadata.materialized_context())
            {
                emit(
                    header.header_name,
                    PropagatedValue {
                        kind: header.value_kind,
                        bytes: header.value,
                    },
                );
            }
        }
    }

    pub(crate) const fn policy(&self) -> &CompiledHeaderPropagationPolicy {
        &self.policy
    }
}

/// Runtime pdata context bindings compiled for one node.
///
/// The binding operations are added incrementally while this type keeps their
/// underlying compiled policies private.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeBindings {
    arrival: Option<ArrivalBinding>,
    sink: Option<SinkBinding>,
}

impl NodeBindings {
    pub(crate) fn set_materialization_plan(
        bindings: &mut Option<Arc<Self>>,
        plan: ContextMaterializationPlan,
    ) {
        if bindings
            .as_ref()
            .and_then(|bindings| bindings.arrival())
            .is_some_and(|arrival| arrival.materialization == plan)
        {
            return;
        }
        Self::receiver_mut(bindings).set_materialization_plan(plan);
    }

    // Compatibility setters use copy-on-write so changing one handler's policy
    // does not modify other handlers or the compiler's shared snapshot.
    pub(crate) fn receiver_mut(bindings: &mut Option<Arc<Self>>) -> &mut ArrivalBinding {
        let bindings = bindings.get_or_insert_with(|| {
            Arc::new(Self::new(
                NodeKind::Receiver,
                None,
                None,
                None,
                ContextMaterializationPlan::default(),
            ))
        });
        Arc::make_mut(bindings)
            .arrival
            .as_mut()
            .expect("receiver must have an arrival binding")
    }

    pub(crate) fn set_propagation_policy(
        bindings: &mut Option<Arc<Self>>,
        policy: Option<CompiledHeaderPropagationPolicy>,
    ) {
        let bindings = bindings.get_or_insert_with(|| {
            Arc::new(Self::new(
                NodeKind::Exporter,
                None,
                None,
                None,
                ContextMaterializationPlan::default(),
            ))
        });
        Arc::make_mut(bindings).sink = policy.map(|policy| SinkBinding { policy });
    }

    pub(super) fn new(
        kind: NodeKind,
        header_capture: Option<CompiledHeaderCapturePolicy>,
        header_propagation: Option<CompiledHeaderPropagationPolicy>,
        authorized_identity_capture: Option<AuthorizedIdentityPolicy>,
        materialization: ContextMaterializationPlan,
    ) -> Self {
        match kind {
            NodeKind::Receiver => Self {
                arrival: Some(ArrivalBinding {
                    header_capture,
                    authorized_identity_capture,
                    materialization,
                }),
                sink: None,
            },
            NodeKind::Exporter => Self {
                arrival: None,
                sink: header_propagation.map(|policy| SinkBinding { policy }),
            },
            NodeKind::Processor => Self {
                arrival: None,
                sink: None,
            },
        }
    }

    /// Returns the receiver arrival binding, when this node is a receiver.
    #[must_use]
    pub const fn arrival(&self) -> Option<&ArrivalBinding> {
        self.arrival.as_ref()
    }

    /// Returns the exporter sink binding, when propagation is configured.
    #[must_use]
    pub const fn sink(&self) -> Option<&SinkBinding> {
        self.sink.as_ref()
    }

    pub(crate) fn header_capture_policy(&self) -> Option<&CompiledHeaderCapturePolicy> {
        self.arrival.as_ref()?.header_capture_policy()
    }

    pub(crate) fn header_propagation_policy(&self) -> Option<&CompiledHeaderPropagationPolicy> {
        Some(self.sink.as_ref()?.policy())
    }

    pub(crate) fn authorized_identity_policy(&self) -> Option<&AuthorizedIdentityPolicy> {
        self.arrival.as_ref()?.authorized_identity_policy()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.arrival.as_ref().is_none_or(|arrival| {
            arrival.header_capture.is_none() && arrival.authorized_identity_capture.is_none()
        }) && self.sink.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pdata_context::PreparedHeaderPropagationPolicy;
    use otel_arrow_dfe_config::context::ContextEntryName;
    use otel_arrow_dfe_config::context::policy::{
        ContextEntryDeclaration, ContextEntryDefinition, ContextEntryPart, ContextRandomnessKind,
        ContextScope,
    };
    use otel_arrow_dfe_config::context::transport_headers::TransportHeader;
    use otel_arrow_dfe_config::context::transport_policy::{
        CaptureDefaults, CaptureRule, HeaderCapturePolicy, HeaderPropagationPolicy,
    };

    fn name(value: &str) -> ContextEntryName {
        ContextEntryName::try_from(value).expect("valid context entry name")
    }

    fn capture_policy() -> CompiledHeaderCapturePolicy {
        HeaderCapturePolicy::new(
            CaptureDefaults::default(),
            vec![CaptureRule {
                match_names: vec![name("x-tenant")],
                store_as: Some(name("tenant")),
                sensitive: false,
                value_kind: None,
            }],
        )
        .compile(|_| true)
    }

    /// Scenario: local and shared receivers replace and clear policies after installing bindings.
    /// Guarantees: old accessors, arrival formation, and the send hook share state without changing clones.
    #[test]
    fn receiver_compatibility_setters_update_the_arrival_binding() {
        macro_rules! check {
            ($mode:ident) => {{
                let identity_policy: AuthorizedIdentityPolicy =
                    serde_json::from_value(serde_json::json!([
                        {"claim": "sub", "store_as": "subject"}
                    ]))
                    .expect("identity policy");
                let bindings = Arc::new(NodeBindings::new(
                    NodeKind::Receiver,
                    Some(capture_policy()),
                    None,
                    Some(identity_policy.clone()),
                    ContextMaterializationPlan::default(),
                ));
                let (ctrl_tx, _ctrl_rx) = crate::control::runtime_ctrl_msg_channel(1);
                let (_metrics_rx, reporter) =
                    otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(1);
                let mut handler = crate::$mode::receiver::EffectHandler::<Metadata>::new(
                    crate::testing::test_node("receiver"),
                    Default::default(),
                    None,
                    ctrl_tx,
                    reporter,
                    crate::testing::test_pipeline_runtime_services(),
                );
                handler.set_node_bindings(Some(Arc::clone(&bindings)));
                let cloned = handler.clone();
                let identity = AuthorizedIdentity::new().with_subject("alice");
                let pairs = [("x-tenant", b"acme".as_slice())];
                let arrival = Arrival {
                    headers: ArrivalHeaders::Pairs(&pairs),
                    identity: Some(&identity),
                    peer: Some("127.0.0.1:4317".parse().expect("peer")),
                };
                let original = cloned.form_context(&arrival).0;
                assert!(original.transport_headers().is_some());
                assert!(original.authorized_identity_entries().is_some());

                handler.set_capture_policy(None);
                handler.set_authorized_identity_policy(None);
                assert!(handler.capture_policy().is_none());
                assert!(handler.authorized_identity_policy().is_none());
                let cleared = handler.form_context(&arrival).0;
                assert!(cleared.transport_headers().is_none());
                assert!(cleared.authorized_identity_entries().is_none());
                assert_eq!(cleared.peer_addr(), arrival.peer);
                assert_eq!(cloned.form_context(&arrival).0, original);
                assert!(bindings.header_capture_policy().is_some());
                assert!(bindings.authorized_identity_policy().is_some());

                handler.set_capture_policy(Some(capture_policy()));
                handler.set_authorized_identity_policy(Some(identity_policy));
                assert_eq!(handler.form_context(&arrival).0, original);

                let plan = ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
                    scope: ContextScope::Engine,
                    name: name("request"),
                    definition: ContextEntryDefinition(vec![ContextEntryPart::Constant {
                        name: name("route"),
                        value: "primary".to_owned(),
                    }]),
                }])
                .expect("materialization plan");
                handler.set_context_materializer(plan, Metadata::materialize_context);
                let formed = handler.form_context(&arrival).0;
                let mut sent = original.clone();
                handler.materialize_context(&mut sent);
                assert_ne!(formed, original);
                assert_eq!(sent, formed);
                assert_eq!(cloned.form_context(&arrival).0, original);
            }};
        }
        check!(local);
        check!(shared);
    }

    /// Scenario: local and shared exporters replace and clear propagation on installed bindings.
    /// Guarantees: the legacy accessor and sink observe the same policy while clones remain unchanged.
    #[test]
    fn exporter_compatibility_setters_update_the_sink_binding() {
        macro_rules! check {
            ($mode:ident) => {{
                let config = serde_json::from_value(serde_json::json!({
                    "default": {"selector": {"type": "all_captured"}}
                }))
                .expect("propagation policy");
                let policy = PreparedHeaderPropagationPolicy::compile(config, &[])
                    .expect("prepared policy")
                    .bind(&ContextMaterializationPlan::default())
                    .expect("bound policy");
                let bindings = Arc::new(NodeBindings::new(
                    NodeKind::Exporter,
                    None,
                    Some(policy.clone()),
                    None,
                    ContextMaterializationPlan::default(),
                ));
                let (_metrics_rx, reporter) =
                    otel_arrow_dfe_telemetry::reporter::MetricsReporter::create_new_and_receiver(1);
                let mut handler = crate::$mode::exporter::EffectHandler::<Metadata>::new(
                    crate::testing::test_node("exporter"),
                    reporter,
                    crate::testing::test_pipeline_runtime_services(),
                );
                handler.set_node_bindings(Some(Arc::clone(&bindings)));
                let cloned = handler.clone();
                let mut headers = TransportHeaders::new();
                headers.push(TransportHeader::text(name("tenant"), b"acme".to_vec()));
                let mut metadata = Metadata::default();
                metadata.set_transport_headers(headers);
                let emitted = |sink: &SinkBinding| {
                    let mut values = Vec::new();
                    sink.propagate(&metadata, |name, value| {
                        values.push((name.to_owned(), value.bytes().to_vec()));
                    });
                    values
                };
                assert_eq!(
                    emitted(cloned.sink().expect("original sink")),
                    vec![("tenant".to_owned(), b"acme".to_vec())],
                );

                handler.set_propagation_policy(Some(CompiledHeaderPropagationPolicy::default()));
                assert_eq!(
                    handler.propagation_policy(),
                    Some(&CompiledHeaderPropagationPolicy::default()),
                );
                assert!(emitted(handler.sink().expect("replacement sink")).is_empty());
                handler.set_propagation_policy(None);
                assert!(handler.propagation_policy().is_none());
                assert!(handler.sink().is_none());
                assert_eq!(cloned.propagation_policy(), Some(&policy));
                assert_eq!(bindings.header_propagation_policy(), Some(&policy));
                assert_eq!(
                    emitted(cloned.sink().expect("unchanged sink")),
                    vec![("tenant".to_owned(), b"acme".to_vec())],
                );
            }};
        }
        check!(local);
        check!(shared);
    }

    /// Scenario: an authorized identity contains one single-valued and one multi-valued claim.
    /// Guarantees: the generic claims view borrows both shapes without allocation or flattening.
    #[test]
    fn authorized_identity_implements_claims_view() {
        let identity = AuthorizedIdentity::new()
            .with_claim_str("sub", "alice")
            .with_claim_values("groups", ["operators", "developers"]);

        assert_eq!(
            Claims::claim(&identity, "sub"),
            Some(ClaimRef::One("alice"))
        );
        assert_eq!(
            Claims::claim(&identity, "groups"),
            Some(ClaimRef::Many(&[
                "operators".to_owned(),
                "developers".to_owned()
            ]))
        );
        assert_eq!(Claims::claim(&identity, "missing"), None);
    }

    /// Scenario: an arrival contains headers, verified claims, a peer, and constant context.
    /// Guarantees: the binding produces exactly the same metadata as the legacy operations.
    #[test]
    fn arrival_binding_matches_legacy_metadata_formation() {
        let identity_policy: AuthorizedIdentityPolicy = serde_json::from_value(serde_json::json!([
            {"claim": "sub", "store_as": "subject"}
        ]))
        .expect("valid identity policy");
        let identity = AuthorizedIdentity::new().with_subject("alice");
        let materialization = ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
            scope: ContextScope::Engine,
            name: name("request"),
            definition: ContextEntryDefinition(vec![ContextEntryPart::Constant {
                name: name("route"),
                value: "primary".to_owned(),
            }]),
        }])
        .expect("materialization plan");
        let capture = capture_policy();
        let binding = ArrivalBinding {
            header_capture: Some(capture.clone()),
            authorized_identity_capture: Some(identity_policy.clone()),
            materialization: materialization.clone(),
        };
        let peer = "127.0.0.1:4317".parse().expect("peer address");
        let pairs = [("X-Tenant", b"acme".as_slice())];

        let (actual, stats) = binding.form(&Arrival {
            headers: ArrivalHeaders::Pairs(&pairs),
            identity: Some(&identity),
            peer: Some(peer),
        });

        let mut expected = Metadata::default();
        let mut captured = TransportHeaders::new();
        let expected_stats = capture.capture_from_pairs(pairs.into_iter(), &mut captured);
        expected.set_transport_headers(captured);
        expected.capture_authorized_identity(&identity_policy, &identity);
        expected.set_peer_addr(peer);
        expected.materialize_context(&materialization);
        assert_eq!(stats, expected_stats);
        assert_eq!(actual, expected);
    }

    /// Scenario: an arrival binding materializes a generated UUID before the legacy send hook.
    /// Guarantees: repeating the same plan does not regenerate or replace the arrival value.
    #[test]
    fn arrival_materialization_is_idempotent() {
        let materialization = ContextMaterializationPlan::compile(&[ContextEntryDeclaration {
            scope: ContextScope::Engine,
            name: name("request"),
            definition: ContextEntryDefinition(vec![ContextEntryPart::Randomness {
                name: name("id"),
                value: ContextRandomnessKind::Uuid7,
            }]),
        }])
        .expect("materialization plan");
        let binding = ArrivalBinding {
            header_capture: None,
            authorized_identity_capture: None,
            materialization: materialization.clone(),
        };
        let (actual, stats) = binding.form(&Arrival {
            headers: ArrivalHeaders::Pairs(&[]),
            identity: None,
            peer: None,
        });
        assert_eq!(stats, None);

        let mut repeated = actual.clone();
        repeated.materialize_context(&materialization);
        assert_eq!(repeated, actual);
    }

    /// Scenario: an HTTP arrival is captured through the native HeaderMap path.
    /// Guarantees: the binding produces the same headers and capture statistics as direct capture.
    #[test]
    fn arrival_binding_matches_native_http_capture() {
        let capture = capture_policy();
        let binding = ArrivalBinding {
            header_capture: Some(capture.clone()),
            authorized_identity_capture: None,
            materialization: ContextMaterializationPlan::default(),
        };
        let mut headers = HeaderMap::new();
        _ = headers.insert("x-tenant", "acme".parse().expect("header value"));

        let (actual, stats) = binding.form(&Arrival {
            headers: ArrivalHeaders::Http(&headers),
            identity: None,
            peer: None,
        });

        let mut expected_headers = TransportHeaders::new();
        let expected_stats = capture.capture_from_http_headers(&headers, &mut expected_headers);
        let mut expected = Metadata::default();
        expected.set_transport_headers(expected_headers);
        assert_eq!(stats, expected_stats);
        assert_eq!(actual, expected);
    }

    /// Scenario: a sink propagates captured text and binary headers with retained wire names.
    /// Guarantees: callback output exactly matches the existing compiled propagation iterator.
    #[test]
    fn sink_binding_matches_compiled_propagation() {
        let config: HeaderPropagationPolicy = serde_json::from_value(serde_json::json!({
            "default": {
                "selector": {"type": "all_captured"},
                "name": "preserve"
            }
        }))
        .expect("valid propagation policy");
        let prepared =
            PreparedHeaderPropagationPolicy::compile(config, &[]).expect("prepared policy");
        let policy = prepared
            .bind(&ContextMaterializationPlan::default())
            .expect("compiled policy");
        let sink = SinkBinding {
            policy: policy.clone(),
        };
        let mut headers = TransportHeaders::new();
        headers.push(TransportHeader::captured(
            name("tenant"),
            "X-Tenant",
            true,
            ValueKind::Text,
            b"acme".as_slice(),
        ));
        headers.push(TransportHeader::captured(
            name("trace-bin"),
            "Trace-Bin",
            true,
            ValueKind::Binary,
            b"\x01\x02".as_slice(),
        ));
        let mut metadata = Metadata::default();
        metadata.set_transport_headers(headers);

        let mut actual = Vec::new();
        sink.propagate(&metadata, |name, value| {
            actual.push((name.to_owned(), value.kind(), value.bytes().to_vec()));
        });
        let expected = policy
            .propagate(metadata.transport_headers().expect("captured headers"))
            .map(|header| {
                (
                    header.header_name.to_owned(),
                    header.value_kind,
                    header.value.to_vec(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
