// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The tenant compiler and the packed context it produces.
//!
//! [`TenantRegistry::compile`] turns the engine-scoped [`TenantPolicy`] into a
//! frozen registry: names are interned to dense ids, header names are
//! lowercased once, and every limit is checked. After that no request-time
//! operation looks at a name.
//!
//! [`TenantRegistry::resolve`] turns a request into a [`TenantContext`]: one
//! allocation holding the declared values, addressed by key id. Because the
//! layout is positional and fixed by the registry rather than by arrival
//! order, two requests carrying equal values produce byte-equal contexts.
//!
//! # Layout
//!
//! A context is a single `Arc<[u8]>`:
//!
//! ```text
//! 0..8    generation   u64  identity of the registry that produced this
//! 8..16   tokens       u64  bit per resolved token
//! 16..24  present      u64  bit per key holding a value
//! 24..26  key_count    u16  number of end offsets that follow
//! 26..28  reserved     u16  zero
//! 28..    ends         u32 * key_count, end offset of each key's value
//! then    values       concatenated in key order
//! ```
//!
//! Reading a key is a bit test and two `u32` loads, with no search and no
//! comparison against a name.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::tenant::{ExtractorSource, TenantPolicy};

/// Maximum number of distinct keys one engine may declare.
///
/// The bound is 64 because token membership and value presence are each
/// carried as one `u64` bitmask.
pub const MAX_KEYS: usize = 64;

/// Maximum number of tokens one engine may declare, bounded for the same
/// reason as [`MAX_KEYS`].
pub const MAX_TOKENS: usize = 64;

/// Maximum total size, in bytes, of the values one request may carry.
///
/// Counted across every key after resolution. A request exceeding it is
/// reported rather than truncated, so a context is never a partial identity.
pub const MAX_CONTEXT_BYTES: usize = u16::MAX as usize;

const HEADER_BYTES: usize = 28;
const OFFSET_GENERATION: usize = 0;
const OFFSET_TOKENS: usize = 8;
const OFFSET_PRESENT: usize = 16;
const OFFSET_KEY_COUNT: usize = 24;

/// Dense identifier of a declared key. Also the key's slot in a context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyId(u16);

impl KeyId {
    /// Returns the key's index, which is its slot in a [`TenantContext`].
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Dense identifier of a declared token.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TokenId(u16);

impl TokenId {
    /// Returns the token's index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Why a request could not be resolved into a context.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolveError {
    /// The declared values of this request exceed [`MAX_CONTEXT_BYTES`].
    ///
    /// Reported rather than truncated: a caller that silently shortened a
    /// value would hand downstream conditions an identity that matches the
    /// wrong rule.
    #[error("tenant values total {bytes} bytes, exceeding the {limit} byte per-request budget")]
    ValuesTooLarge {
        /// Total size of the values that would have been stored.
        bytes: usize,
        /// The budget that was exceeded.
        limit: usize,
    },
}

/// The request-scoped material a registry may read.
///
/// This trait is the seam between the compiler and the protocols that feed
/// it. Keeping resolution defined against it means the compiler depends on no
/// protocol crate, and that a receiver is the only place a wire name is read.
pub trait RequestSource {
    /// Returns the value of a transport header.
    ///
    /// `name` is lowercase ASCII, having been normalized when the registry was
    /// compiled. Implementations must match case-insensitively.
    fn transport_header(&self, name: &str) -> Option<Cow<'_, [u8]>>;
}

/// What a key reads.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CompiledSource {
    /// A transport header, stored lowercased.
    TransportHeader(Box<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyDef {
    name: Box<str>,
    source: CompiledSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TokenDef {
    name: Box<str>,
    /// Bit per key this token requires. All must be present for the token to
    /// resolve.
    key_mask: u64,
}

/// The frozen result of compiling [`TenantPolicy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantRegistry {
    generation: u64,
    keys: Box<[KeyDef]>,
    tokens: Box<[TokenDef]>,
    key_by_name: HashMap<Box<str>, KeyId>,
    token_by_name: HashMap<Box<str>, TokenId>,
}

impl TenantRegistry {
    /// Compiles a declaration, returning every error it contains rather than
    /// the first.
    ///
    /// # Errors
    ///
    /// Returns the list of configuration errors when the declaration cannot be
    /// compiled.
    pub fn compile(policy: &TenantPolicy) -> Result<Self, Vec<String>> {
        let mut errors = Vec::new();
        let mut keys: Vec<KeyDef> = Vec::new();
        let mut key_by_name: HashMap<Box<str>, KeyId> = HashMap::new();
        let mut header_owner: HashMap<Box<str>, Box<str>> = HashMap::new();
        let mut tokens: Vec<TokenDef> = Vec::new();
        let mut token_by_name: HashMap<Box<str>, TokenId> = HashMap::new();

        if policy.tokens.len() > MAX_TOKENS {
            errors.push(format!(
                "{} tokens declared, exceeding the maximum of {MAX_TOKENS}",
                policy.tokens.len()
            ));
        }

        for (token_name, spec) in &policy.tokens {
            if let Err(error) = validate_name("token", token_name) {
                errors.push(error);
                continue;
            }
            if spec.extractors.is_empty() {
                errors.push(format!(
                    "token `{token_name}` declares no extractor, so it can never resolve"
                ));
                continue;
            }

            let mut key_mask = 0u64;
            for extractor in &spec.extractors {
                let key_name = extractor.key.as_ref();
                if let Err(error) = validate_name("key", key_name) {
                    errors.push(format!("token `{token_name}`: {error}"));
                    continue;
                }

                let source = match &extractor.source {
                    ExtractorSource::TransportHeader(header) => {
                        match normalize_header_name(header) {
                            Ok(normalized) => CompiledSource::TransportHeader(normalized),
                            Err(error) => {
                                errors.push(format!(
                                    "token `{token_name}`, key `{key_name}`: {error}"
                                ));
                                continue;
                            }
                        }
                    }
                };

                let key_id = match key_by_name.get(key_name) {
                    Some(existing) => {
                        // A key is the name of one fact. Two declarations of
                        // the same key must agree about where that fact comes
                        // from, or the value a consumer reads would depend on
                        // which token happened to resolve.
                        if keys[existing.index()].source != source {
                            errors.push(format!(
                                "key `{key_name}` is declared with two different sources; a key names one fact and must have one source"
                            ));
                            continue;
                        }
                        *existing
                    }
                    None => {
                        if keys.len() >= MAX_KEYS {
                            errors.push(format!(
                                "more than {MAX_KEYS} distinct keys declared, starting at `{key_name}`"
                            ));
                            continue;
                        }
                        let CompiledSource::TransportHeader(header) = &source;
                        // One header, one key. Two names for one fact is a
                        // configuration smell, and allowing it would make the
                        // header index ambiguous for no gain.
                        if let Some(owner) = header_owner.get(header)
                            && owner.as_ref() != key_name
                        {
                            errors.push(format!(
                                "header `{header}` is already bound to key `{owner}` and cannot also fill key `{key_name}`"
                            ));
                            continue;
                        }
                        let _ = header_owner.insert(header.clone(), key_name.into());

                        let id = KeyId(u16::try_from(keys.len()).unwrap_or(u16::MAX));
                        keys.push(KeyDef {
                            name: key_name.into(),
                            source,
                        });
                        let _ = key_by_name.insert(key_name.into(), id);
                        id
                    }
                };

                let bit = 1u64 << key_id.index();
                if key_mask & bit != 0 {
                    errors.push(format!(
                        "token `{token_name}` declares key `{key_name}` more than once"
                    ));
                    continue;
                }
                key_mask |= bit;
            }

            if key_mask == 0 {
                // Every extractor of this token was rejected above; the errors
                // are already recorded, and a token with no key must not be
                // registered as one that always resolves.
                continue;
            }
            if tokens.len() < MAX_TOKENS {
                let id = TokenId(u16::try_from(tokens.len()).unwrap_or(u16::MAX));
                tokens.push(TokenDef {
                    name: token_name.as_ref().into(),
                    key_mask,
                });
                let _ = token_by_name.insert(token_name.as_ref().into(), id);
            }
        }

        if !errors.is_empty() {
            return Err(errors);
        }

        let generation = generation_of(&keys, &tokens);
        Ok(Self {
            generation,
            keys: keys.into_boxed_slice(),
            tokens: tokens.into_boxed_slice(),
            key_by_name,
            token_by_name,
        })
    }

    /// Returns a registry declaring nothing, which resolves every request to
    /// an empty context.
    #[must_use]
    pub fn empty() -> Self {
        Self::compile(&TenantPolicy::default()).expect("an empty declaration always compiles")
    }

    /// Returns the identity of this registry's declaration.
    ///
    /// A [`TenantContext`] records the generation it was built under, so a
    /// context cannot be read by slot against a registry that assigned those
    /// slots to different keys.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns true when nothing is declared.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Number of declared keys, which is the number of slots in a context.
    #[must_use]
    pub const fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// Number of declared tokens.
    #[must_use]
    pub const fn token_count(&self) -> usize {
        self.tokens.len()
    }

    /// Resolves the id of a declared key. Consumers do this once at startup
    /// and hold the id, so no request-time read touches a name.
    #[must_use]
    pub fn key_id(&self, name: &str) -> Option<KeyId> {
        self.key_by_name.get(name).copied()
    }

    /// Resolves the id of a declared token.
    #[must_use]
    pub fn token_id(&self, name: &str) -> Option<TokenId> {
        self.token_by_name.get(name).copied()
    }

    /// Returns the declared name of a key.
    #[must_use]
    pub fn key_name(&self, key: KeyId) -> Option<&str> {
        self.keys.get(key.index()).map(|def| def.name.as_ref())
    }

    /// Returns the declared name of a token.
    #[must_use]
    pub fn token_name(&self, token: TokenId) -> Option<&str> {
        self.tokens.get(token.index()).map(|def| def.name.as_ref())
    }

    /// Resolves a request into a context.
    ///
    /// A token contributes its values only if every one of its extractors
    /// resolved, so a request that satisfies no token yields an empty context
    /// rather than a partial identity.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::ValuesTooLarge`] when the values that would be
    /// stored exceed [`MAX_CONTEXT_BYTES`].
    pub fn resolve<S: RequestSource + ?Sized>(
        &self,
        source: &S,
    ) -> Result<TenantContext, ResolveError> {
        let key_count = self.keys.len();
        if key_count == 0 {
            return Ok(TenantContext::empty());
        }

        // First pass measures. The source is queried again below to write,
        // which avoids a scratch allocation for the borrowed values.
        let mut lengths = [0u32; MAX_KEYS];
        let mut present = 0u64;
        for (index, key) in self.keys.iter().enumerate() {
            let CompiledSource::TransportHeader(header) = &key.source;
            if let Some(value) = source.transport_header(header) {
                present |= 1u64 << index;
                lengths[index] = u32::try_from(value.len()).unwrap_or(u32::MAX);
            }
        }

        let mut tokens = 0u64;
        let mut required = 0u64;
        for (index, token) in self.tokens.iter().enumerate() {
            if present & token.key_mask == token.key_mask {
                tokens |= 1u64 << index;
                required |= token.key_mask;
            }
        }

        let stored = present & required;
        if stored == 0 {
            return Ok(TenantContext::empty());
        }

        let mut total: usize = 0;
        for (index, length) in lengths.iter().enumerate().take(key_count) {
            if stored & (1u64 << index) != 0 {
                total = total.saturating_add(*length as usize);
            }
        }
        if total > MAX_CONTEXT_BYTES {
            return Err(ResolveError::ValuesTooLarge {
                bytes: total,
                limit: MAX_CONTEXT_BYTES,
            });
        }

        let ends_at = HEADER_BYTES;
        let values_at = ends_at + 4 * key_count;
        let mut buffer = vec![0u8; values_at + total];
        buffer[OFFSET_GENERATION..OFFSET_GENERATION + 8]
            .copy_from_slice(&self.generation.to_le_bytes());
        buffer[OFFSET_TOKENS..OFFSET_TOKENS + 8].copy_from_slice(&tokens.to_le_bytes());
        buffer[OFFSET_PRESENT..OFFSET_PRESENT + 8].copy_from_slice(&stored.to_le_bytes());
        buffer[OFFSET_KEY_COUNT..OFFSET_KEY_COUNT + 2]
            .copy_from_slice(&u16::try_from(key_count).unwrap_or(u16::MAX).to_le_bytes());

        let mut end = 0usize;
        for (index, key) in self.keys.iter().enumerate() {
            if stored & (1u64 << index) != 0 {
                let CompiledSource::TransportHeader(header) = &key.source;
                if let Some(value) = source.transport_header(header) {
                    // Clamped to what the measuring pass reserved, so a source
                    // that answers inconsistently cannot overrun the buffer.
                    let length = value.len().min(lengths[index] as usize);
                    let at = values_at + end;
                    buffer[at..at + length].copy_from_slice(&value[..length]);
                    end += length;
                }
            }
            let at = ends_at + 4 * index;
            buffer[at..at + 4]
                .copy_from_slice(&u32::try_from(end).unwrap_or(u32::MAX).to_le_bytes());
        }

        Ok(TenantContext(Some(Arc::from(buffer.into_boxed_slice()))))
    }
}

/// The resolved, request-scoped identity, packed into one allocation.
///
/// Cloning shares the allocation, so carrying a context through a pipeline
/// costs a reference count.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TenantContext(Option<Arc<[u8]>>);

impl TenantContext {
    /// Returns a context carrying nothing.
    #[must_use]
    pub const fn empty() -> Self {
        Self(None)
    }

    /// Returns true when no token resolved.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// Returns the generation of the registry that produced this context, or
    /// `None` when it is empty.
    #[must_use]
    pub fn generation(&self) -> Option<u64> {
        self.0
            .as_deref()
            .map(|data| read_u64(data, OFFSET_GENERATION))
    }

    /// Returns true when the named token resolved for this request.
    #[must_use]
    pub fn has_token(&self, token: TokenId) -> bool {
        let Some(data) = self.0.as_deref() else {
            return false;
        };
        read_u64(data, OFFSET_TOKENS) & (1u64 << token.index()) != 0
    }

    /// Returns the value stored for a key, or `None` when the key holds no
    /// value on this request.
    ///
    /// The read is a bit test and two `u32` loads. It is the caller's
    /// responsibility to pass a [`KeyId`] from the registry that produced this
    /// context; [`Self::generation`] identifies which one that was.
    #[must_use]
    pub fn value(&self, key: KeyId) -> Option<&[u8]> {
        let data = self.0.as_deref()?;
        let index = key.index();
        let key_count = usize::from(read_u16(data, OFFSET_KEY_COUNT));
        if index >= key_count {
            return None;
        }
        if read_u64(data, OFFSET_PRESENT) & (1u64 << index) == 0 {
            return None;
        }
        let ends_at = HEADER_BYTES;
        let values_at = ends_at + 4 * key_count;
        let end = read_u32(data, ends_at + 4 * index) as usize;
        let start = if index == 0 {
            0
        } else {
            read_u32(data, ends_at + 4 * (index - 1)) as usize
        };
        data.get(values_at + start..values_at + end)
    }

    /// Returns the packed bytes, for callers that persist or transport a
    /// context verbatim.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_deref().unwrap_or(&[])
    }
}

impl fmt::Debug for TenantContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_deref() {
            None => f.write_str("TenantContext(empty)"),
            Some(data) => f
                .debug_struct("TenantContext")
                .field("generation", &read_u64(data, OFFSET_GENERATION))
                .field(
                    "tokens",
                    &format_args!("{:#x}", read_u64(data, OFFSET_TOKENS)),
                )
                .field(
                    "present",
                    &format_args!("{:#x}", read_u64(data, OFFSET_PRESENT)),
                )
                .field("bytes", &data.len())
                .finish(),
        }
    }
}

fn read_u16(data: &[u8], at: usize) -> u16 {
    let mut bytes = [0u8; 2];
    bytes.copy_from_slice(&data[at..at + 2]);
    u16::from_le_bytes(bytes)
}

fn read_u32(data: &[u8], at: usize) -> u32 {
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&data[at..at + 4]);
    u32::from_le_bytes(bytes)
}

fn read_u64(data: &[u8], at: usize) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&data[at..at + 8]);
    u64::from_le_bytes(bytes)
}

/// FNV-1a over the compiled declaration.
///
/// Written out rather than taken from `DefaultHasher` so that the value
/// depends only on the declaration, and not on a standard library detail that
/// is explicitly allowed to change.
fn generation_of(keys: &[KeyDef], tokens: &[TokenDef]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET_BASIS;
    let mix = |hash: &mut u64, bytes: &[u8]| {
        for byte in bytes {
            *hash ^= u64::from(*byte);
            *hash = hash.wrapping_mul(PRIME);
        }
    };
    for key in keys {
        mix(&mut hash, key.name.as_bytes());
        mix(&mut hash, b"\x00");
        let CompiledSource::TransportHeader(header) = &key.source;
        mix(&mut hash, b"header:");
        mix(&mut hash, header.as_bytes());
        mix(&mut hash, b"\x00");
    }
    mix(&mut hash, b"\x01");
    for token in tokens {
        mix(&mut hash, token.name.as_bytes());
        mix(&mut hash, b"\x00");
        mix(&mut hash, &token.key_mask.to_le_bytes());
    }
    hash
}

/// Key and token names share the component id charset used elsewhere in the
/// engine, so a name is safe to use in a URN, a metric label, or a log field.
fn validate_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{kind} name must not be empty"));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')))
    {
        return Err(format!(
            "{kind} name `{name}` contains `{bad}`; only ASCII letters, digits, `_`, `-` and `.` are allowed"
        ));
    }
    Ok(())
}

/// Lowercases a header name and rejects anything that is not an HTTP field
/// name, since a name that cannot appear on the wire can never match.
fn normalize_header_name(name: &str) -> Result<Box<str>, String> {
    if name.is_empty() {
        return Err("transport_header must not be empty".to_owned());
    }
    if let Some(bad) = name.chars().find(|c| !is_header_name_char(*c)) {
        return Err(format!(
            "transport_header `{name}` contains `{bad}`, which is not valid in an HTTP field name"
        ));
    }
    Ok(name.to_ascii_lowercase().into_boxed_str())
}

/// The `token` production of RFC 9110 section 5.6.2.
fn is_header_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(
            c,
            '!' | '#'
                | '$'
                | '%'
                | '&'
                | '\''
                | '*'
                | '+'
                | '-'
                | '.'
                | '^'
                | '_'
                | '`'
                | '|'
                | '~'
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::{Extractor, TenantTokenSpec};

    /// A request made of header pairs, standing in for the protocol sources
    /// that later steps introduce.
    struct Headers(Vec<(String, Vec<u8>)>);

    impl Headers {
        fn new(pairs: &[(&str, &str)]) -> Self {
            Self(
                pairs
                    .iter()
                    .map(|(name, value)| ((*name).to_owned(), value.as_bytes().to_vec()))
                    .collect(),
            )
        }
    }

    impl RequestSource for Headers {
        fn transport_header(&self, name: &str) -> Option<Cow<'_, [u8]>> {
            self.0
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| Cow::Borrowed(value.as_slice()))
        }
    }

    fn header_extractor(key: &'static str, header: &str) -> Extractor {
        Extractor {
            key: Cow::Borrowed(key),
            source: ExtractorSource::TransportHeader(header.to_owned()),
        }
    }

    fn policy(tokens: &[(&'static str, &[Extractor])]) -> TenantPolicy {
        let mut policy = TenantPolicy::default();
        for (name, extractors) in tokens {
            let _ = policy.tokens.insert(
                Cow::Borrowed(name),
                TenantTokenSpec {
                    extractors: extractors.to_vec(),
                },
            );
        }
        policy
    }

    fn compile(tokens: &[(&'static str, &[Extractor])]) -> TenantRegistry {
        TenantRegistry::compile(&policy(tokens)).expect("declaration should compile")
    }

    /// Scenario: a two-extractor token is offered a request carrying only one
    /// of its two headers.
    /// Guarantees: the token does not resolve and neither of its keys holds a
    /// value, so no consumer can act on a half-populated identity.
    #[test]
    fn token_resolves_only_when_every_extractor_resolves() {
        let registry = compile(&[(
            "edge",
            &[
                header_extractor("tenant_id", "x-tenant-id"),
                header_extractor("project_id", "x-project-id"),
            ],
        )]);
        let tenant = registry.key_id("tenant_id").expect("key declared");
        let edge = registry.token_id("edge").expect("token declared");

        let partial = registry
            .resolve(&Headers::new(&[("x-tenant-id", "acme")]))
            .expect("within budget");
        assert!(partial.is_empty());
        assert!(!partial.has_token(edge));
        assert_eq!(partial.value(tenant), None);

        let complete = registry
            .resolve(&Headers::new(&[
                ("x-tenant-id", "acme"),
                ("x-project-id", "p1"),
            ]))
            .expect("within budget");
        assert!(complete.has_token(edge));
        assert_eq!(complete.value(tenant), Some(b"acme".as_slice()));
    }

    /// Scenario: one key belongs to two tokens, and a request satisfies only
    /// the token that requires nothing else.
    /// Guarantees: the shared key is stored because a token containing it
    /// resolved, and only the resolved token is reported present.
    #[test]
    fn shared_key_is_stored_when_any_containing_token_resolves() {
        let registry = compile(&[
            ("tenant", &[header_extractor("tenant_id", "x-tenant-id")]),
            (
                "edge",
                &[
                    header_extractor("tenant_id", "x-tenant-id"),
                    header_extractor("project_id", "x-project-id"),
                ],
            ),
        ]);
        let tenant_id = registry.key_id("tenant_id").expect("key declared");
        let project_id = registry.key_id("project_id").expect("key declared");
        let tenant = registry.token_id("tenant").expect("token declared");
        let edge = registry.token_id("edge").expect("token declared");

        let context = registry
            .resolve(&Headers::new(&[("x-tenant-id", "acme")]))
            .expect("within budget");
        assert!(context.has_token(tenant));
        assert!(!context.has_token(edge));
        assert_eq!(context.value(tenant_id), Some(b"acme".as_slice()));
        assert_eq!(context.value(project_id), None);
    }

    /// Scenario: two requests carry the same declared values, one with the
    /// headers reversed and differently cased, plus an undeclared header.
    /// Guarantees: both produce byte-identical contexts, because the layout is
    /// fixed by the registry and undeclared input contributes nothing.
    #[test]
    fn layout_is_positional_and_ignores_undeclared_input() {
        let registry = compile(&[(
            "edge",
            &[
                header_extractor("tenant_id", "x-tenant-id"),
                header_extractor("project_id", "x-project-id"),
            ],
        )]);

        let first = registry
            .resolve(&Headers::new(&[
                ("x-tenant-id", "acme"),
                ("x-project-id", "p1"),
            ]))
            .expect("within budget");
        let second = registry
            .resolve(&Headers::new(&[
                ("authorization", "Bearer secret"),
                ("X-Project-Id", "p1"),
                ("X-TENANT-ID", "acme"),
            ]))
            .expect("within budget");

        assert_eq!(first, second);
        assert_eq!(first.as_bytes(), second.as_bytes());
    }

    /// Scenario: three keys resolve with values of differing length, one of
    /// them empty.
    /// Guarantees: every key reads back byte-exactly by slot, and a
    /// zero-length value stays distinguishable from an absent one.
    #[test]
    fn values_read_back_exactly_by_slot() {
        let registry = compile(&[(
            "edge",
            &[
                header_extractor("a", "x-a"),
                header_extractor("b", "x-b"),
                header_extractor("c", "x-c"),
            ],
        )]);
        let (a, b, c) = (
            registry.key_id("a").expect("declared"),
            registry.key_id("b").expect("declared"),
            registry.key_id("c").expect("declared"),
        );

        let context = registry
            .resolve(&Headers::new(&[
                ("x-a", ""),
                ("x-b", "value-b"),
                ("x-c", "cc"),
            ]))
            .expect("within budget");

        assert_eq!(context.value(a), Some(b"".as_slice()));
        assert_eq!(context.value(b), Some(b"value-b".as_slice()));
        assert_eq!(context.value(c), Some(b"cc".as_slice()));
    }

    /// Scenario: a declaration is compiled twice, and compared against one
    /// that binds the same key name to a different header.
    /// Guarantees: the generation is stable for an unchanged declaration and
    /// changes when slot meaning changes, so a context cannot be read by slot
    /// against a registry that assigned those slots differently.
    #[test]
    fn generation_identifies_the_declaration() {
        let one = compile(&[("t", &[header_extractor("tenant_id", "x-tenant-id")])]);
        let same = compile(&[("t", &[header_extractor("tenant_id", "x-tenant-id")])]);
        let different = compile(&[("t", &[header_extractor("tenant_id", "x-customer-id")])]);

        assert_eq!(one.generation(), same.generation());
        assert_ne!(one.generation(), different.generation());

        let context = one
            .resolve(&Headers::new(&[("x-tenant-id", "acme")]))
            .expect("within budget");
        assert_eq!(context.generation(), Some(one.generation()));
    }

    /// Scenario: a request carries declared values totalling more than the
    /// per-request budget.
    /// Guarantees: resolution reports the overflow instead of returning a
    /// context holding a truncated value, which would be an identity that
    /// matches the wrong rule.
    #[test]
    fn oversized_values_are_reported_not_truncated() {
        let registry = compile(&[("t", &[header_extractor("big", "x-big")])]);
        let oversized = "v".repeat(MAX_CONTEXT_BYTES + 1);

        let error = registry
            .resolve(&Headers::new(&[("x-big", &oversized)]))
            .expect_err("should exceed the budget");
        assert_eq!(
            error,
            ResolveError::ValuesTooLarge {
                bytes: MAX_CONTEXT_BYTES + 1,
                limit: MAX_CONTEXT_BYTES,
            }
        );
    }

    /// Scenario: nothing is declared.
    /// Guarantees: the registry compiles, and every request resolves to an
    /// empty context, so the feature costs nothing when unconfigured.
    #[test]
    fn empty_declaration_resolves_to_an_empty_context() {
        let registry = TenantRegistry::empty();
        assert!(registry.is_empty());
        assert_eq!(registry.key_count(), 0);

        let context = registry
            .resolve(&Headers::new(&[("x-tenant-id", "acme")]))
            .expect("within budget");
        assert!(context.is_empty());
        assert_eq!(context.generation(), None);
    }

    /// Scenario: one key is declared against two different headers in two
    /// tokens.
    /// Guarantees: compilation fails, because a consumer reading that key
    /// would otherwise get a value determined by which token happened to
    /// resolve.
    #[test]
    fn rejects_one_key_with_two_sources() {
        let errors = TenantRegistry::compile(&policy(&[
            ("a", &[header_extractor("tenant_id", "x-tenant-id")]),
            ("b", &[header_extractor("tenant_id", "x-customer-id")]),
        ]))
        .expect_err("should not compile");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("two different sources")),
            "unexpected errors: {errors:?}"
        );
    }

    /// Scenario: one header is declared as the source of two different keys,
    /// spelled with different capitalization.
    /// Guarantees: compilation fails, keeping one wire fact addressable under
    /// exactly one name.
    #[test]
    fn rejects_one_header_filling_two_keys() {
        let errors = TenantRegistry::compile(&policy(&[(
            "t",
            &[
                header_extractor("tenant_id", "x-tenant-id"),
                header_extractor("customer_id", "X-Tenant-Id"),
            ],
        )]))
        .expect_err("should not compile");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("already bound to key")),
            "unexpected errors: {errors:?}"
        );
    }

    /// Scenario: a declaration contains an empty token, a token name with a
    /// disallowed character, and a header name that is not an HTTP field name.
    /// Guarantees: all three are reported together, so an operator fixes a
    /// declaration in one pass rather than one error per restart.
    #[test]
    fn reports_every_declaration_error_at_once() {
        let mut spec = policy(&[
            ("empty", &[]),
            ("bad key", &[header_extractor("tenant_id", "x-tenant-id")]),
        ]);
        let _ = spec.tokens.insert(
            Cow::Borrowed("bad_header"),
            TenantTokenSpec {
                extractors: vec![header_extractor("other_id", "x tenant id")],
            },
        );

        let errors = TenantRegistry::compile(&spec).expect_err("should not compile");
        assert!(
            errors.iter().any(|e| e.contains("declares no extractor")),
            "unexpected errors: {errors:?}"
        );
        assert!(
            errors.iter().any(|e| e.contains("token name `bad key`")),
            "unexpected errors: {errors:?}"
        );
        assert!(
            errors
                .iter()
                .any(|e| e.contains("not valid in an HTTP field name")),
            "unexpected errors: {errors:?}"
        );
    }

    /// Scenario: a declaration names more keys than a context has slots for.
    /// Guarantees: the limit is enforced at startup rather than silently
    /// dropping the keys that do not fit.
    #[test]
    fn rejects_more_keys_than_slots() {
        let extractors: Vec<Extractor> = (0..=MAX_KEYS)
            .map(|index| Extractor {
                key: Cow::Owned(format!("k{index}")),
                source: ExtractorSource::TransportHeader(format!("x-k{index}")),
            })
            .collect();
        let mut spec = TenantPolicy::default();
        let _ = spec
            .tokens
            .insert(Cow::Borrowed("t"), TenantTokenSpec { extractors });

        let errors = TenantRegistry::compile(&spec).expect_err("should not compile");
        assert!(
            errors
                .iter()
                .any(|error| error.contains("distinct keys declared")),
            "unexpected errors: {errors:?}"
        );
    }

    /// Scenario: a declaration is parsed from the YAML an operator writes.
    /// Guarantees: the documented shape deserializes, so the configuration in
    /// `docs/tenant-context.md` is the configuration the engine accepts.
    #[test]
    fn documented_yaml_shape_deserializes() {
        let policy: TenantPolicy = serde_yaml::from_str(
            r"
tokens:
  edge:
    extractors:
      - key: tenant_id
        transport_header: x-tenant-id
      - key: project_id
        transport_header: x-project-id
",
        )
        .expect("documented shape should parse");

        let registry = TenantRegistry::compile(&policy).expect("should compile");
        assert_eq!(registry.key_count(), 2);
        assert_eq!(registry.token_count(), 1);
        assert!(registry.token_id("edge").is_some());
        assert_eq!(
            registry.key_name(registry.key_id("tenant_id").expect("declared")),
            Some("tenant_id")
        );
    }
}
