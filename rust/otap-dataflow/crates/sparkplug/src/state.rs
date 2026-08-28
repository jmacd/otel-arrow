// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use crate::context::{DeathOrigin, ResolvedAlias, SignalType, SparkplugDecodeContext};
use crate::payload::SparkplugPayload;
use crate::pb;
use crate::topic::{LifecycleMessageType, LifecycleTopic, SparkplugMessageType};

/// Sparkplug timestamps represented as milliseconds since the Unix epoch.
pub type Timestamp = u64;

/// Errors returned by the Sparkplug state machine.
#[allow(variant_size_differences)]
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SparkplugStateError {
    /// The payload referenced an alias that has not been defined in this session.
    #[error("rebirth is needed to resolve alias {alias}")]
    RebirthNeeded {
        /// The unresolved alias.
        alias: u64,
    },
    /// The caller attempted to use a non-lifecycle topic where lifecycle/data was required.
    #[error("message type {0} is not a lifecycle or data message")]
    NotLifecycleOrData(SparkplugMessageType),
}

/// One tracked Sparkplug metric.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    /// The Sparkplug metric name.
    pub name: String,
    /// The first timestamp associated with the metric definition.
    pub start_timestamp: Timestamp,
    /// The most recent metric timestamp.
    pub timestamp: Timestamp,
    /// The Sparkplug metadata description, when provided at definition time.
    pub description: String,
    /// The most recent decoded metric value.
    pub value: Option<MetricValue>,
}

/// The supported Sparkplug metric value kinds this crate preserves.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// A Sparkplug `int_value`.
    Int(u32),
    /// A Sparkplug `long_value`.
    Long(u64),
    /// A Sparkplug `float_value`.
    Float(f32),
    /// A Sparkplug `double_value`.
    Double(f64),
    /// A Sparkplug `boolean_value`.
    Boolean(bool),
    /// A Sparkplug `string_value`.
    String(String),
    /// A Sparkplug `bytes_value`.
    Bytes(Vec<u8>),
    /// An unsupported but non-panicking placeholder for complex value kinds.
    Unsupported(UnsupportedMetricValue),
}

impl MetricValue {
    fn from_proto(metric: &pb::payload::Metric) -> Option<Self> {
        if metric.is_null.unwrap_or(false) {
            return None;
        }

        match metric.value.as_ref()? {
            pb::payload::metric::Value::IntValue(value) => Some(Self::Int(*value)),
            pb::payload::metric::Value::LongValue(value) => Some(Self::Long(*value)),
            pb::payload::metric::Value::FloatValue(value) => Some(Self::Float(*value)),
            pb::payload::metric::Value::DoubleValue(value) => Some(Self::Double(*value)),
            pb::payload::metric::Value::BooleanValue(value) => Some(Self::Boolean(*value)),
            pb::payload::metric::Value::StringValue(value) => Some(Self::String(value.clone())),
            pb::payload::metric::Value::BytesValue(value) => Some(Self::Bytes(value.clone())),
            pb::payload::metric::Value::DatasetValue(_) => {
                Some(Self::Unsupported(UnsupportedMetricValue::Dataset))
            }
            pb::payload::metric::Value::TemplateValue(_) => {
                Some(Self::Unsupported(UnsupportedMetricValue::Template))
            }
            pb::payload::metric::Value::ExtensionValue(_) => {
                Some(Self::Unsupported(UnsupportedMetricValue::Extension))
            }
        }
    }

    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Int(value) => Some(u64::from(*value)),
            Self::Long(value) => Some(*value),
            _ => None,
        }
    }
}

/// Unsupported Sparkplug metric value categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsupportedMetricValue {
    /// A Sparkplug `dataset_value`.
    Dataset,
    /// A Sparkplug `template_value`.
    Template,
    /// A Sparkplug `extension_value`.
    Extension,
}

/// Top-level Sparkplug session state keyed by group identifier.
#[derive(Debug, Default, Clone)]
pub struct SparkplugState {
    groups: HashMap<String, GroupState>,
}

impl SparkplugState {
    /// Creates an empty Sparkplug state table.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the group state, creating it on first access.
    pub fn group(&mut self, group_id: impl Into<String>) -> &mut GroupState {
        let key = group_id.into();
        self.groups.entry(key).or_default()
    }

    /// Returns the group state when it already exists.
    #[must_use]
    pub fn group_ref(&self, group_id: &str) -> Option<&GroupState> {
        self.groups.get(group_id)
    }

    /// Routes and applies one lifecycle or data message.
    pub fn visit_message(
        &mut self,
        topic: &LifecycleTopic,
        payload: &SparkplugPayload,
        observed_at: Timestamp,
    ) -> Result<VisitOutcome, SparkplugStateError> {
        match topic.message_type() {
            LifecycleMessageType::NBirth
            | LifecycleMessageType::NDeath
            | LifecycleMessageType::NData => {
                let group = self.group(topic.group_id().to_owned());
                let edge = group.edge_node(topic.edge_node_id().to_owned());
                edge.store
                    .visit_lifecycle_or_data(topic.message_type(), payload, observed_at)?;

                let cascaded_device_deaths = if topic.message_type() == LifecycleMessageType::NDeath {
                    edge.cascade_node_death(topic.group_id(), topic.edge_node_id(), observed_at)
                } else {
                    Vec::new()
                };

                Ok(VisitOutcome {
                    cascaded_device_deaths,
                })
            }
            LifecycleMessageType::DBirth
            | LifecycleMessageType::DDeath
            | LifecycleMessageType::DData => {
                let group = self.group(topic.group_id().to_owned());
                let edge = group.edge_node(topic.edge_node_id().to_owned());
                let device = edge.device(topic.device_id().unwrap_or_default().to_owned());
                device
                    .store
                    .visit_lifecycle_or_data(topic.message_type(), payload, observed_at)?;
                Ok(VisitOutcome {
                    cascaded_device_deaths: Vec::new(),
                })
            }
        }
    }

    /// Builds immutable decode context for one lifecycle or data payload.
    pub fn classify_decode_context(
        &self,
        topic: &LifecycleTopic,
        payload: &SparkplugPayload,
        death_origin: DeathOrigin,
    ) -> Result<SparkplugDecodeContext, SparkplugStateError> {
        let store = self.store_for(topic);
        let mut resolved_aliases = Vec::new();
        let mut seen_aliases = HashSet::new();
        let mut b_d_seq = None;

        for metric in payload.metrics() {
            let alias = metric.alias.unwrap_or_default();
            let resolved_name = resolve_metric_name(store, metric)?;

            if alias != 0 {
                let inserted = seen_aliases.insert(alias);
                if inserted {
                    resolved_aliases.push(ResolvedAlias {
                        alias,
                        name: resolved_name.clone(),
                    });
                }
            }

            if resolved_name == "bdSeq" {
                b_d_seq = MetricValue::from_proto(metric).and_then(|value| value.as_u64()).or(b_d_seq);
            }
        }

        if b_d_seq.is_none() {
            b_d_seq = store.and_then(Store::b_d_seq);
        }

        let message_type = topic.message_type().into();

        Ok(SparkplugDecodeContext {
            group_id: topic.group_id().to_owned(),
            edge_node_id: topic.edge_node_id().to_owned(),
            device_id: topic.device_id().map(str::to_owned),
            message_type,
            signal: SignalType::for_message_type(message_type),
            b_d_seq,
            resolved_aliases,
            death_origin: if topic.message_type().is_death() {
                death_origin
            } else {
                DeathOrigin::Unknown
            },
        })
    }

    fn store_for(&self, topic: &LifecycleTopic) -> Option<&Store> {
        let group = self.group_ref(topic.group_id())?;
        let edge = group.edge_node_ref(topic.edge_node_id())?;

        if topic.message_type().is_device_message() {
            topic
                .device_id()
                .and_then(|device_id| edge.device_ref(device_id))
                .map(|device| &device.store)
        } else {
            Some(&edge.store)
        }
    }

}

/// Builds immutable decode context for one lifecycle or data payload.
pub fn classify_decode_context(
    state: &SparkplugState,
    topic: &LifecycleTopic,
    payload: &SparkplugPayload,
    death_origin: DeathOrigin,
) -> Result<SparkplugDecodeContext, SparkplugStateError> {
    state.classify_decode_context(topic, payload, death_origin)
}

/// The result of visiting one routed Sparkplug message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisitOutcome {
    /// Devices implicitly marked dead by an NDEATH cascade.
    pub cascaded_device_deaths: Vec<CascadedDeviceDeath>,
}

/// One device affected by an edge-node death cascade.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CascadedDeviceDeath {
    /// The group containing the device.
    pub group_id: String,
    /// The edge node containing the device.
    pub edge_node_id: String,
    /// The affected device identifier.
    pub device_id: String,
}

/// Sparkplug group state keyed by edge-node identifier.
#[derive(Debug, Default, Clone)]
pub struct GroupState {
    edge_nodes: HashMap<String, EdgeNodeState>,
}

impl GroupState {
    /// Returns the edge node state, creating it on first access.
    pub fn edge_node(&mut self, edge_node_id: impl Into<String>) -> &mut EdgeNodeState {
        let key = edge_node_id.into();
        self.edge_nodes.entry(key).or_default()
    }

    /// Returns the edge node state when it already exists.
    #[must_use]
    pub fn edge_node_ref(&self, edge_node_id: &str) -> Option<&EdgeNodeState> {
        self.edge_nodes.get(edge_node_id)
    }
}

/// Sparkplug edge-node state keyed by device identifier.
#[derive(Debug, Clone, Default)]
pub struct EdgeNodeState {
    /// The node-level metric store.
    pub store: Store,
    devices: HashMap<String, DeviceState>,
}

impl EdgeNodeState {
    /// Returns the device state, creating it on first access.
    pub fn device(&mut self, device_id: impl Into<String>) -> &mut DeviceState {
        let key = device_id.into();
        self.devices.entry(key).or_default()
    }

    /// Returns the device state when it already exists.
    #[must_use]
    pub fn device_ref(&self, device_id: &str) -> Option<&DeviceState> {
        self.devices.get(device_id)
    }

    /// Marks every tracked device under this edge node offline.
    pub fn cascade_node_death(
        &mut self,
        group_id: &str,
        edge_node_id: &str,
        observed_at: Timestamp,
    ) -> Vec<CascadedDeviceDeath> {
        let mut affected = Vec::with_capacity(self.devices.len());

        for (device_id, device) in &mut self.devices {
            device.store.mark_dead(observed_at);
            affected.push(CascadedDeviceDeath {
                group_id: group_id.to_owned(),
                edge_node_id: edge_node_id.to_owned(),
                device_id: device_id.clone(),
            });
        }

        affected
    }
}

/// Sparkplug device state.
#[derive(Debug, Clone, Default)]
pub struct DeviceState {
    /// The device-level metric store.
    pub store: Store,
}

/// One Sparkplug metric store for a node or device.
#[derive(Debug, Clone, Default)]
pub struct Store {
    name_map: HashMap<String, usize>,
    alias_map: HashMap<u64, usize>,
    metrics: Vec<Metric>,
    birth_time: Option<Timestamp>,
    last_time: Option<Timestamp>,
    online: bool,
    b_d_seq: Option<u64>,
}

impl Store {
    /// Returns the first nonzero birth timestamp seen for this entity.
    #[must_use]
    pub fn birth_time(&self) -> Option<Timestamp> {
        self.birth_time
    }

    /// Returns the most recent observed visit time.
    #[must_use]
    pub fn last_time(&self) -> Option<Timestamp> {
        self.last_time
    }

    /// Returns true when the entity is currently marked online.
    #[must_use]
    pub fn is_online(&self) -> bool {
        self.online
    }

    /// Returns the current birth/death sequence number when known.
    #[must_use]
    pub fn b_d_seq(&self) -> Option<u64> {
        self.b_d_seq
    }

    /// Returns the tracked metrics in insertion order.
    #[must_use]
    pub fn metrics(&self) -> &[Metric] {
        &self.metrics
    }

    /// Returns a tracked metric by name.
    #[must_use]
    pub fn metric_by_name(&self, name: &str) -> Option<&Metric> {
        self.name_map
            .get(name)
            .and_then(|index| self.metrics.get(*index))
    }

    /// Returns a tracked metric by alias.
    #[must_use]
    pub fn metric_by_alias(&self, alias: u64) -> Option<&Metric> {
        self.alias_map
            .get(&alias)
            .and_then(|index| self.metrics.get(*index))
    }

    /// Defines or resolves a metric by name or alias.
    pub fn define(
        &mut self,
        name: &str,
        alias: u64,
        timestamp: Timestamp,
        description: &str,
    ) -> Option<&mut Metric> {
        if name.is_empty() {
            let index = *self.alias_map.get(&alias)?;
            return self.metrics.get_mut(index);
        }

        if let Some(index) = self.name_map.get(name).copied() {
            return self.metrics.get_mut(index);
        }

        let index = self.metrics.len();
        self.metrics.push(Metric {
            name: name.to_owned(),
            start_timestamp: timestamp,
            timestamp,
            description: description.to_owned(),
            value: None,
        });
        _ = self.name_map.insert(name.to_owned(), index);

        if alias != 0 {
            _ = self.alias_map.insert(alias, index);
        }

        self.metrics.get_mut(index)
    }

    /// Applies a Sparkplug lifecycle or data payload to this store.
    pub fn visit_lifecycle_or_data(
        &mut self,
        message_type: LifecycleMessageType,
        payload: &SparkplugPayload,
        observed_at: Timestamp,
    ) -> Result<(), SparkplugStateError> {
        self.last_time = Some(observed_at);

        if message_type.is_birth() && self.birth_time.is_none() && payload.timestamp() != 0 {
            self.birth_time = Some(payload.timestamp());
        }

        self.online = !message_type.is_death();

        for metric in payload.metrics() {
            let alias = metric.alias.unwrap_or_default();
            let name = metric.name.as_deref().unwrap_or_default();
            let timestamp = metric.timestamp.unwrap_or_default();
            let description = metric
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.description.as_deref())
                .unwrap_or_default();

            let value = MetricValue::from_proto(metric);
            let tracked = match self.define(name, alias, timestamp, description) {
                Some(metric_ref) => metric_ref,
                None => return Err(SparkplugStateError::RebirthNeeded { alias }),
            };

            tracked.timestamp = timestamp;
            tracked.value = value;

            if tracked.name == "bdSeq" {
                self.b_d_seq = tracked.value.as_ref().and_then(MetricValue::as_u64);
            }
        }

        Ok(())
    }

    fn mark_dead(&mut self, observed_at: Timestamp) {
        self.last_time = Some(observed_at);
        self.online = false;
    }
}

fn resolve_metric_name(
    store: Option<&Store>,
    metric: &pb::payload::Metric,
) -> Result<String, SparkplugStateError> {
    let name = metric.name.as_deref().unwrap_or_default();
    if !name.is_empty() {
        return Ok(name.to_owned());
    }

    let alias = metric.alias.unwrap_or_default();
    match store.and_then(|item| item.metric_by_alias(alias)) {
        Some(metric_ref) => Ok(metric_ref.name.clone()),
        None => Err(SparkplugStateError::RebirthNeeded { alias }),
    }
}
