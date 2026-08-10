// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compile-time and request-time failures, and the startup diagnostics that
//! explain what reachability analysis removed.
//!
//! Compilation reports every problem it can rather than stopping at the first,
//! so that a misconfigured engine explains itself in one pass.

/// A configuration that cannot be compiled.
#[derive(Debug, thiserror::Error)]
#[error("metadata configuration is invalid")]
pub struct CompileError {
    /// Every problem found, in the order the passes found them.
    pub problems: Vec<CompileProblem>,
}

impl CompileError {
    pub(crate) fn new(problems: Vec<CompileProblem>) -> Self {
        Self { problems }
    }
}

/// One reason a configuration cannot be compiled.
#[derive(Debug, thiserror::Error)]
pub enum CompileProblem {
    /// Two declarations claim the same name within one namespace.
    #[error("duplicate {namespace} named `{name}`")]
    DuplicateName {
        /// What kind of declaration collided.
        namespace: &'static str,
        /// The name declared twice.
        name: String,
    },

    /// A token lists the same key twice, so its key sequence is ambiguous.
    #[error("token `{token}` produces key `{key}` more than once")]
    DuplicateKeyInToken {
        /// The token that lists the key twice.
        token: String,
        /// The key listed twice.
        key: String,
    },

    /// A consumer lists one token more than once. This API models a token as
    /// one descriptor producer at a consumer; duplicating it would silently
    /// collapse two intended descriptor occurrences into one match.
    #[error("consumer `{consumer}` declares token `{token}` more than once")]
    DuplicateTokenRequirement {
        /// The consumer with the duplicate declaration.
        consumer: String,
        /// The token listed twice.
        token: String,
    },

    /// A transport-header wire name's `-bin` suffix disagrees with the field's
    /// declared value kind. The existing engine preserves this distinction, and
    /// the compiler must not turn arbitrary binary bytes into OTLP text.
    #[error(
        "transport header `{name}` implies {implied} values, but key `{key}` is declared as \
         {declared}"
    )]
    TransportHeaderKindMismatch {
        /// The logical key the extractor produces.
        key: String,
        /// The configured wire name.
        name: String,
        /// The kind implied by the wire name.
        implied: &'static str,
        /// The kind declared for the key.
        declared: &'static str,
    },

    /// A condition's key sequence is not the key set of any token its consumer
    /// declares. Envoy matches a descriptor by its whole key sequence, so a
    /// condition can only describe a descriptor its consumer actually produces.
    #[error(
        "condition {branch} of set `{condition_set}` names keys {keys:?}, but consumer \
         `{consumer}` declares no token producing exactly those keys{hint}"
    )]
    NoTokenForCondition {
        /// The condition set that declared the condition.
        condition_set: String,
        /// The condition's position within its set.
        branch: usize,
        /// The consumer whose declared tokens were searched.
        consumer: String,
        /// The keys the condition named.
        keys: Vec<String>,
        /// What the nearest declared token would have needed, when there is one.
        hint: String,
    },

    /// A consumer reads or bags a key that none of the tokens it declares can
    /// supply.
    #[error(
        "consumer `{consumer}` reads key `{key}`, but declares no token that produces it; \
         add the token to the consumer's required or optional list"
    )]
    KeyNotSupplied {
        /// The consumer that cannot be served.
        consumer: String,
        /// The key nothing it declares produces.
        key: String,
    },

    /// A bare carrier key is produced by several tokens declared by one
    /// consumer. The caller must preserve provenance with `token:key`.
    #[error(
        "consumer `{consumer}` reads ambiguous key `{key}` from tokens {tokens:?}; \
         use a token-qualified MetadataFieldId"
    )]
    AmbiguousField {
        /// The consumer that requested the bare key.
        consumer: String,
        /// The ambiguous key.
        key: String,
        /// Tokens that can supply it.
        tokens: Vec<String>,
    },

    /// A token-qualified carrier field is not available at the consumer.
    #[error("consumer `{consumer}` does not declare token `{token}` producing key `{key}`")]
    QualifiedFieldNotSupplied {
        /// The consumer that requested the field.
        consumer: String,
        /// The token named by the field.
        token: String,
        /// The key named by the field.
        key: String,
    },

    /// A condition names one of its token's keys twice.
    #[error("condition {branch} of set `{condition_set}` names key `{key}` more than once")]
    DuplicateKeyInCondition {
        /// The condition set that declared the condition.
        condition_set: String,
        /// The condition's position within its set.
        branch: usize,
        /// The key named twice.
        key: String,
    },

    /// A condition tests a key by value that may hold several values at once.
    /// Equality is not defined against a repeated value, and Envoy's descriptor
    /// entry holds exactly one value.
    #[error(
        "condition {branch} of set `{condition_set}` tests repeated key `{key}` by value; \
         a value-matched key must resolve to a single value"
    )]
    ValueMatchOnRepeatedKey {
        /// The condition set that declared the condition.
        condition_set: String,
        /// The condition's position within its set.
        branch: usize,
        /// The repeated key.
        key: String,
    },

    /// Two entries can select the same descriptor from one token. Envoy emits
    /// one descriptor per producer, not one per matching rule, so selecting
    /// both would invent multi-dispatch. The configuration must make the
    /// selection unambiguous.
    #[error(
        "conditions {first} and {second} of set `{condition_set}` both match token `{token}`; \
         make their predicates disjoint"
    )]
    AmbiguousConditions {
        /// The condition set that declared both entries.
        condition_set: String,
        /// The earlier overlapping entry.
        first: usize,
        /// The later overlapping entry.
        second: usize,
        /// The descriptor token that makes the entries overlap.
        token: String,
    },

    /// A declared dimension exceeds its bound. Growing state without bound in
    /// response to configuration is the failure mode [`Limits`](crate::Limits) exists to
    /// prevent.
    #[error("{dimension} is {declared}, which exceeds the limit of {limit}")]
    LimitExceeded {
        /// The dimension that overflowed.
        dimension: &'static str,
        /// What the configuration asked for.
        declared: usize,
        /// The bound from [`Limits`](crate::Limits).
        limit: usize,
    },

    /// The fixed part of the packed layout does not fit, before a single value
    /// byte has been added.
    #[error("the packed context header needs {needed} bytes, which exceeds the limit of {limit}")]
    ContextHeaderTooLarge {
        /// The fixed portion of the layout.
        needed: usize,
        /// The bound from [`Limits`](crate::Limits).
        limit: usize,
    },
}

impl CompileProblem {
    pub(crate) fn limit(dimension: &'static str, declared: usize, limit: usize) -> Self {
        Self::LimitExceeded {
            dimension,
            declared,
            limit,
        }
    }
}

/// Something the compiler removed, or that a caller most likely did not intend.
///
/// Warnings never fail compilation. The caller logs them once at startup, which
/// is how an operator learns that a header they configured is being dropped
/// because nothing downstream can observe it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompileWarning {
    /// No surviving token produces this key, so it is never extracted and never
    /// occupies a byte of any context.
    UnobservedKey {
        /// The key that was dropped.
        key: String,
    },

    /// This key is named only as a wildcard, so its value decides nothing. It is
    /// still extracted, because its token cannot resolve without it, but its
    /// bytes are not carried.
    GatingOnlyKey {
        /// The key whose value is not carried.
        key: String,
        /// A token that cannot resolve without it.
        token: String,
    },

    /// No condition names this token, no consumer requires it, and none of its
    /// keys are retained, so it is never resolved.
    UnobservedToken {
        /// The token that was dropped.
        token: String,
    },

    /// This extractor belongs only to tokens that were dropped, so it never
    /// runs. This is the warning that tells an operator a captured header is
    /// going nowhere.
    UnobservedExtractor {
        /// The key the extractor would have produced.
        key: String,
        /// A short description of where the value would have come from.
        source: String,
    },

    /// No producer can reach this consumer, so its condition sets are never
    /// evaluated and its reads always see an unset context.
    UnreachableConsumer {
        /// The consumer that nothing reaches.
        consumer: String,
    },

    /// This key is only ever tested by value, never read or bagged, so its
    /// values are dictionary-encoded and the bytes are not retained.
    MatchOnlyKey {
        /// The key whose bytes are not retained.
        key: String,
    },
}

impl std::fmt::Display for CompileWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnobservedKey { key } => write!(
                f,
                "metadata key `{key}` is dropped: no surviving token produces it"
            ),
            Self::GatingOnlyKey { key, token } => write!(
                f,
                "metadata key `{key}` is never read or tested; it is extracted only because token \
                 `{token}` cannot resolve without it"
            ),
            Self::UnobservedToken { token } => write!(
                f,
                "metadata token `{token}` is dropped: no condition names it, no node requires it, \
                 and none of its keys are retained"
            ),
            Self::UnobservedExtractor { key, source } => write!(
                f,
                "metadata extractor for `{key}` from {source} is dropped: it belongs only to \
                 tokens that nothing observes"
            ),
            Self::UnreachableConsumer { consumer } => write!(
                f,
                "metadata consumer `{consumer}` is unreachable: no producer feeds it"
            ),
            Self::MatchOnlyKey { key } => write!(
                f,
                "metadata key `{key}` is matched but never read, so its value is compiled to a \
                 symbol and its bytes are not carried"
            ),
        }
    }
}

/// A request whose metadata cannot be packed.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// A value exceeded [`Limits::value_bytes`](crate::Limits::value_bytes), or the request offered more
    /// repetitions of a key than [`Limits::values_per_key`](crate::Limits::values_per_key) allows.
    #[error("metadata value for key `{key}` exceeds the configured limit")]
    ValueTooLarge {
        /// The key whose value was rejected.
        key: String,
    },

    /// The packed context would exceed [`Limits::context_bytes`](crate::Limits::context_bytes).
    #[error("packed metadata context needs {needed} bytes, which exceeds the limit of {limit}")]
    ContextTooLarge {
        /// The size the context would have occupied.
        needed: usize,
        /// The bound from [`Limits`](crate::Limits).
        limit: usize,
    },

    /// The request offered a value for a key an extractor declared as
    /// non-repeating, and the extractor rejects repetition.
    #[error("metadata key `{key}` was offered more than once and repetition is rejected")]
    UnexpectedRepetition {
        /// The key offered more than once.
        key: String,
    },

    /// A key declared as text received bytes that are not valid UTF-8.
    #[error("metadata text value for key `{key}` is not valid UTF-8")]
    InvalidTextValue {
        /// The key whose text value was rejected.
        key: String,
    },

    /// The request's staged values exceed reusable scratch capacity.
    #[error("metadata scratch needs {needed} bytes, which exceeds the configured limit of {limit}")]
    ScratchTooLarge {
        /// Bytes staged for this request.
        needed: usize,
        /// The configured scratch bound.
        limit: usize,
    },
}
