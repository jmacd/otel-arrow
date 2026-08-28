// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Formatter};

/// The fixed Sparkplug namespace prefix for non-STATE topics.
pub const SPARKPLUG_NAMESPACE: &str = "spBv1.0";

/// All Sparkplug message types supported by topic parsing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SparkplugMessageType {
    /// Birth certificate for MQTT edge nodes.
    NBirth,
    /// Death certificate for MQTT edge nodes.
    NDeath,
    /// Birth certificate for devices.
    DBirth,
    /// Death certificate for devices.
    DDeath,
    /// Node data message.
    NData,
    /// Device data message.
    DData,
    /// Node command message.
    NCmd,
    /// Device command message.
    DCmd,
    /// Primary Host STATE message.
    State,
}

impl SparkplugMessageType {
    /// Parses a Sparkplug message type from its topic spelling.
    pub fn parse(value: &str) -> Result<Self, TopicParseError> {
        match value {
            "NBIRTH" => Ok(Self::NBirth),
            "NDEATH" => Ok(Self::NDeath),
            "DBIRTH" => Ok(Self::DBirth),
            "DDEATH" => Ok(Self::DDeath),
            "NDATA" => Ok(Self::NData),
            "DDATA" => Ok(Self::DData),
            "NCMD" => Ok(Self::NCmd),
            "DCMD" => Ok(Self::DCmd),
            "STATE" => Ok(Self::State),
            other => Err(TopicParseError::UnknownMessageType(other.to_owned())),
        }
    }

    /// Returns true when the message type is a birth.
    #[must_use]
    pub const fn is_birth(self) -> bool {
        matches!(self, Self::NBirth | Self::DBirth)
    }

    /// Returns true when the message type is a death.
    #[must_use]
    pub const fn is_death(self) -> bool {
        matches!(self, Self::NDeath | Self::DDeath)
    }
}

impl Display for SparkplugMessageType {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NBirth => "NBIRTH",
            Self::NDeath => "NDEATH",
            Self::DBirth => "DBIRTH",
            Self::DDeath => "DDEATH",
            Self::NData => "NDATA",
            Self::DData => "DDATA",
            Self::NCmd => "NCMD",
            Self::DCmd => "DCMD",
            Self::State => "STATE",
        })
    }
}

/// Sparkplug STATE topic profile variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StateTopicProfile {
    /// Sparkplug 2.2 `STATE/{host_id}`.
    Sparkplug22,
    /// Sparkplug 3.0 `spBv1.0/STATE/{host_id}`.
    Sparkplug30,
}

/// Sparkplug lifecycle and data message types only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleMessageType {
    /// Edge-node birth.
    NBirth,
    /// Edge-node death.
    NDeath,
    /// Device birth.
    DBirth,
    /// Device death.
    DDeath,
    /// Edge-node data.
    NData,
    /// Device data.
    DData,
}

impl LifecycleMessageType {
    /// Returns true when the message type is a birth.
    #[must_use]
    pub const fn is_birth(self) -> bool {
        matches!(self, Self::NBirth | Self::DBirth)
    }

    /// Returns true when the message type is a death.
    #[must_use]
    pub const fn is_death(self) -> bool {
        matches!(self, Self::NDeath | Self::DDeath)
    }

    /// Returns true when the topic addresses a device.
    #[must_use]
    pub const fn is_device_message(self) -> bool {
        matches!(self, Self::DBirth | Self::DDeath | Self::DData)
    }
}

impl From<LifecycleMessageType> for SparkplugMessageType {
    fn from(value: LifecycleMessageType) -> Self {
        match value {
            LifecycleMessageType::NBirth => Self::NBirth,
            LifecycleMessageType::NDeath => Self::NDeath,
            LifecycleMessageType::DBirth => Self::DBirth,
            LifecycleMessageType::DDeath => Self::DDeath,
            LifecycleMessageType::NData => Self::NData,
            LifecycleMessageType::DData => Self::DData,
        }
    }
}

impl TryFrom<SparkplugMessageType> for LifecycleMessageType {
    type Error = SparkplugMessageType;

    fn try_from(value: SparkplugMessageType) -> Result<Self, Self::Error> {
        match value {
            SparkplugMessageType::NBirth => Ok(Self::NBirth),
            SparkplugMessageType::NDeath => Ok(Self::NDeath),
            SparkplugMessageType::DBirth => Ok(Self::DBirth),
            SparkplugMessageType::DDeath => Ok(Self::DDeath),
            SparkplugMessageType::NData => Ok(Self::NData),
            SparkplugMessageType::DData => Ok(Self::DData),
            SparkplugMessageType::NCmd
            | SparkplugMessageType::DCmd
            | SparkplugMessageType::State => Err(value),
        }
    }
}

/// A parsed Sparkplug topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Topic {
    /// A Sparkplug message under the `spBv1.0` namespace.
    Message {
        /// The Sparkplug group identifier.
        group_id: String,
        /// The Sparkplug message type.
        message_type: SparkplugMessageType,
        /// The edge-node identifier.
        edge_node_id: String,
        /// The optional device identifier.
        device_id: Option<String>,
    },
    /// A Primary Host STATE topic.
    State {
        /// Which Sparkplug STATE topic profile was used.
        profile: StateTopicProfile,
        /// The Primary Host identifier.
        host_id: String,
    },
}

impl Topic {
    /// Parses a Sparkplug topic string.
    pub fn parse(topic: &str) -> Result<Self, TopicParseError> {
        let elements: Vec<&str> = topic.split('/').collect();

        if elements.len() == 2 && elements[0] == "STATE" {
            if elements[1].is_empty() {
                return Err(TopicParseError::EmptySegment("host_id"));
            }

            return Ok(Self::State {
                profile: StateTopicProfile::Sparkplug22,
                host_id: elements[1].to_owned(),
            });
        }

        if elements.len() == 3 && elements[0] == SPARKPLUG_NAMESPACE && elements[1] == "STATE" {
            if elements[2].is_empty() {
                return Err(TopicParseError::EmptySegment("host_id"));
            }

            return Ok(Self::State {
                profile: StateTopicProfile::Sparkplug30,
                host_id: elements[2].to_owned(),
            });
        }

        if elements.first().copied() != Some(SPARKPLUG_NAMESPACE) {
            return Err(TopicParseError::InvalidNamespace(
                elements.first().copied().unwrap_or_default().to_owned(),
            ));
        }

        if elements.len() != 4 && elements.len() != 5 {
            return Err(TopicParseError::InvalidElementCount(elements.len()));
        }

        if elements[1].is_empty() {
            return Err(TopicParseError::EmptySegment("group_id"));
        }
        if elements[2].is_empty() {
            return Err(TopicParseError::EmptySegment("message_type"));
        }
        if elements[3].is_empty() {
            return Err(TopicParseError::EmptySegment("edge_node_id"));
        }

        let message_type = SparkplugMessageType::parse(elements[2])?;
        if message_type == SparkplugMessageType::State {
            return Err(TopicParseError::InvalidStateShape(topic.to_owned()));
        }

        let device_id = if elements.len() == 5 {
            if elements[4].is_empty() {
                return Err(TopicParseError::EmptySegment("device_id"));
            }
            Some(elements[4].to_owned())
        } else {
            None
        };

        let expects_device = matches!(
            message_type,
            SparkplugMessageType::DBirth
                | SparkplugMessageType::DDeath
                | SparkplugMessageType::DData
                | SparkplugMessageType::DCmd
        );
        let expects_node = matches!(
            message_type,
            SparkplugMessageType::NBirth
                | SparkplugMessageType::NDeath
                | SparkplugMessageType::NData
                | SparkplugMessageType::NCmd
        );

        if expects_device && device_id.is_none() {
            return Err(TopicParseError::MissingDeviceId(message_type));
        }
        if expects_node && device_id.is_some() {
            return Err(TopicParseError::UnexpectedDeviceId(message_type));
        }

        Ok(Self::Message {
            group_id: elements[1].to_owned(),
            message_type,
            edge_node_id: elements[3].to_owned(),
            device_id,
        })
    }

    /// Returns the message type for any parsed topic.
    #[must_use]
    pub fn message_type(&self) -> SparkplugMessageType {
        match self {
            Self::Message { message_type, .. } => *message_type,
            Self::State { .. } => SparkplugMessageType::State,
        }
    }

    /// Converts the topic into a lifecycle/data topic when possible.
    pub fn as_lifecycle_topic(&self) -> Result<LifecycleTopic, SparkplugMessageType> {
        match self {
            Self::Message {
                group_id,
                message_type,
                edge_node_id,
                device_id,
            } => Ok(LifecycleTopic {
                group_id: group_id.clone(),
                edge_node_id: edge_node_id.clone(),
                device_id: device_id.clone(),
                message_type: LifecycleMessageType::try_from(*message_type)?,
            }),
            Self::State { .. } => Err(SparkplugMessageType::State),
        }
    }
}

impl Display for Topic {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Message {
                group_id,
                message_type,
                edge_node_id,
                device_id,
            } => {
                write!(
                    formatter,
                    "{SPARKPLUG_NAMESPACE}/{group_id}/{message_type}/{edge_node_id}"
                )?;
                if let Some(device_id) = device_id {
                    write!(formatter, "/{device_id}")?;
                }
                Ok(())
            }
            Self::State { profile, host_id } => match profile {
                StateTopicProfile::Sparkplug22 => write!(formatter, "STATE/{host_id}"),
                StateTopicProfile::Sparkplug30 => {
                    write!(formatter, "{SPARKPLUG_NAMESPACE}/STATE/{host_id}")
                }
            },
        }
    }
}

/// A parsed lifecycle or data topic with invalid states excluded.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LifecycleTopic {
    group_id: String,
    edge_node_id: String,
    device_id: Option<String>,
    message_type: LifecycleMessageType,
}

impl LifecycleTopic {
    /// Returns the group identifier.
    #[must_use]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the edge-node identifier.
    #[must_use]
    pub fn edge_node_id(&self) -> &str {
        &self.edge_node_id
    }

    /// Returns the optional device identifier.
    #[must_use]
    pub fn device_id(&self) -> Option<&str> {
        self.device_id.as_deref()
    }

    /// Returns the lifecycle/data message type.
    #[must_use]
    pub fn message_type(&self) -> LifecycleMessageType {
        self.message_type
    }
}

/// Topic parsing errors.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TopicParseError {
    /// The topic has the wrong number of path elements.
    #[error("invalid topic element count: {0}")]
    InvalidElementCount(usize),
    /// The Sparkplug namespace prefix was missing or wrong.
    #[error("invalid Sparkplug namespace: {0}")]
    InvalidNamespace(String),
    /// One required path segment was empty.
    #[error("empty topic segment: {0}")]
    EmptySegment(&'static str),
    /// The message type token was not recognized.
    #[error("unknown Sparkplug message type: {0}")]
    UnknownMessageType(String),
    /// A device-level message omitted its device identifier.
    #[error("missing device id for message type {0}")]
    MissingDeviceId(SparkplugMessageType),
    /// A node-level message unexpectedly included a device identifier.
    #[error("unexpected device id for message type {0}")]
    UnexpectedDeviceId(SparkplugMessageType),
    /// A STATE topic used an invalid path shape.
    #[error("invalid STATE topic shape: {0}")]
    InvalidStateShape(String),
}
