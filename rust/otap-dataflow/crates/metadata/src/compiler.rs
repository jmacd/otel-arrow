// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! The compiler: declarations in, one immutable epoch out.
//!
//! Callers declare keys, extractors, tokens, and what each consumer wants, and
//! receive dense identifiers straight away so they can hold on to them.
//! [`MetadataCompiler::compile`] then runs the passes in the only order they can
//! run in, because each one needs the whole picture the previous one produced:
//!
//! 1. **Validate.** Every problem is collected rather than the first one
//!    returned, so a misconfigured engine explains itself in one pass.
//! 2. **Reachability.** Drop what nothing observes, and work out what each
//!    producer's downstream can observe.
//! 3. **Intern.** Key names become identifiers, wire names become one
//!    case-insensitive dispatch table, and every literal any condition tests
//!    becomes a symbol in its key's dictionary.
//! 4. **Signatures and PairSlots.** Drop each condition's wildcards, share what
//!    is left, and give every (token, signature) pair a word.
//! 5. **Branch tables.** Turn each condition into one entry in a dense table
//!    indexed by that word.
//! 6. **Retention.** Decide which keys' bytes travel.
//! 7. **Layout and plans.** Fix the epoch's byte layout, then give each producer
//!    the subset of the work it actually has to do.

use crate::branch_table::BranchTableBuilder;
use crate::compiled::{
    CompileReport, CompiledExtractor, CompiledMetadata, CompiledSymbolSlot, CompiledToken,
    CompiledValueSlot, EXACT_FOLDING, HEADER_FOLDING, PeerAddressExtractor, TokenRequirements,
};
use crate::condition::{CompiledConditionSet, Condition, KeyPredicate, Range, TableParticipant};
use crate::declaration::{
    ConditionSetDeclaration, Declarations, ExtractorDeclaration, KeyDeclaration, MetadataField,
    ReadDeclaration, Requirement, RequirementDeclaration, SiteDeclaration, TokenDeclaration,
};
use crate::dictionary::{ValueDictionary, ValueDictionaryBuilder};
use crate::error::{CompileError, CompileProblem};
use crate::ids::{
    ConditionSetId, ConsumerId, Epoch, ExtractorId, KeyId, MetadataFieldId, PairSlotId, ProducerId,
    SymbolSlotId, TokenId, ValueSlotId,
};
use crate::layout::ContextLayout;
use crate::limits::Limits;
use crate::name_index::NameIndexBuilder;
use crate::pair_slot::{PairSlotBuilder, field_bits};
use crate::plan::ExtractionPlan;
use crate::reachability::{self, Reachability};
use crate::signature::SignatureTable;
use crate::source::{ExtractorSource, ValueKind};
use hashbrown::{HashMap, HashSet};

/// Declares a metadata configuration and compiles it into one epoch.
#[derive(Debug)]
pub struct MetadataCompiler {
    limits: Limits,
    epoch: Epoch,
    declarations: Declarations,
    problems: Vec<CompileProblem>,
    taken_names: HashSet<(&'static str, Box<str>)>,
}

impl MetadataCompiler {
    /// Starts a compiler for one epoch.
    #[must_use]
    pub fn new(epoch: Epoch, limits: Limits) -> Self {
        Self {
            limits: limits.clamped(),
            epoch,
            declarations: Declarations::default(),
            problems: Vec::new(),
            taken_names: HashSet::new(),
        }
    }

    /// Declares a qualified metadata field and the kind of value it holds.
    pub fn declare_key(&mut self, name: &str, value_kind: ValueKind) -> KeyId {
        self.claim_name("key", name);
        let id = KeyId::from_index(self.declarations.keys.len());
        self.declarations.keys.push(KeyDeclaration {
            name: name.into(),
            value_kind,
        });
        id
    }

    /// Declares one rule that produces a value for a key.
    pub fn declare_extractor(&mut self, key: KeyId, source: ExtractorSource) -> ExtractorId {
        let id = ExtractorId::from_index(self.declarations.extractors.len());
        self.declarations
            .extractors
            .push(ExtractorDeclaration { key, source });
        id
    }

    /// Declares a token: a group of extractors that resolves all-or-nothing.
    pub fn declare_token(&mut self, name: &str, extractors: &[ExtractorId]) -> TokenId {
        self.claim_name("token", name);
        let id = TokenId::from_index(self.declarations.tokens.len());
        self.declarations.tokens.push(TokenDeclaration {
            name: name.into(),
            extractors: extractors.to_vec(),
        });
        id
    }

    /// Declares a site that builds contexts, such as a receiver.
    pub fn declare_producer(&mut self, name: &str) -> ProducerId {
        self.claim_name("producer", name);
        let id = ProducerId::from_index(self.declarations.producers.len());
        self.declarations
            .producers
            .push(SiteDeclaration { name: name.into() });
        id
    }

    /// Declares a site that observes contexts, such as a router or an exporter.
    pub fn declare_consumer(&mut self, name: &str) -> ConsumerId {
        self.claim_name("consumer", name);
        let id = ConsumerId::from_index(self.declarations.consumers.len());
        self.declarations
            .consumers
            .push(SiteDeclaration { name: name.into() });
        id
    }

    /// Declares that contexts this producer builds can arrive at this consumer.
    ///
    /// This is the pipeline graph, reduced to the only thing the compiler needs
    /// from it. A caller that declares no edges at all is taken to mean that
    /// every producer reaches every consumer.
    pub fn declare_reachable(&mut self, producer: ProducerId, consumer: ConsumerId) {
        self.declarations.reachable.push((producer, consumer));
    }

    /// Declares a node's admission contract for a token.
    pub fn declare_token_requirement(
        &mut self,
        consumer: ConsumerId,
        token: TokenId,
        requirement: Requirement,
    ) {
        self.declarations.requirements.push(RequirementDeclaration {
            consumer,
            token,
            requirement,
        });
    }

    /// Declares that a consumer reads a key's value, so its bytes must travel.
    ///
    /// A bare key is accepted only when one token declared by `consumer`
    /// produces it. Use [`MetadataFieldId`] to select a value explicitly when
    /// several tokens reuse the key.
    pub fn declare_read(&mut self, consumer: ConsumerId, field: impl Into<MetadataField>) {
        self.declarations.reads.push(ReadDeclaration {
            consumer,
            field: field.into(),
        });
    }

    /// Declares one consumer's ordered descriptor-map entries.
    ///
    /// A condition names a key sequence, not a token. At compilation the
    /// sequence is bound to every token this consumer declared that produces
    /// exactly those keys, matching Envoy's descriptor-map semantics.
    pub fn declare_condition_set(
        &mut self,
        consumer: ConsumerId,
        name: &str,
        conditions: Vec<Condition>,
    ) -> ConditionSetId {
        self.claim_name("condition set", name);
        let id = ConditionSetId::from_index(self.declarations.condition_sets.len());
        self.declarations
            .condition_sets
            .push(ConditionSetDeclaration {
                name: name.into(),
                consumer,
                conditions,
            });
        id
    }

    /// Compiles everything declared into one immutable epoch.
    pub fn compile(mut self) -> Result<(CompiledMetadata, CompileReport), CompileError> {
        self.check_dimensions();
        self.check_tokens();
        self.check_extractors();
        self.check_requirements();
        self.check_conditions();
        if !self.problems.is_empty() {
            return Err(CompileError::new(self.problems));
        }

        let mut reach = reachability::analyze(&self.declarations);
        let warnings = std::mem::take(&mut reach.warnings);
        let mut build = Build::new(&self.declarations, &self.limits, &reach);
        build.intern_dictionaries();
        build.build_matching();
        build.build_retention();
        build.validate_layout();
        if !build.problems.is_empty() {
            return Err(CompileError::new(build.problems));
        }

        let report = CompileReport { warnings };
        Ok((
            build.finish(self.epoch, self.limits, &self.declarations),
            report,
        ))
    }

    fn claim_name(&mut self, namespace: &'static str, name: &str) {
        if !self.taken_names.insert((namespace, name.into())) {
            self.problems.push(CompileProblem::DuplicateName {
                namespace,
                name: name.to_owned(),
            });
        }
    }

    /// Rejects a configuration that would grow compiled state past its bounds.
    fn check_dimensions(&mut self) {
        let declarations = &self.declarations;
        let checks: [(&'static str, usize, usize); 7] = [
            ("keys", declarations.keys.len(), self.limits.keys),
            (
                "extractors",
                declarations.extractors.len(),
                self.limits.extractors,
            ),
            ("tokens", declarations.tokens.len(), self.limits.tokens),
            (
                "condition sets",
                declarations.condition_sets.len(),
                self.limits.condition_sets,
            ),
            (
                "producers",
                declarations.producers.len(),
                self.limits.producers,
            ),
            (
                "consumers",
                declarations.consumers.len(),
                self.limits.consumers,
            ),
            (
                "value slots",
                declarations.reads.len(),
                self.limits.value_slots,
            ),
        ];
        for (dimension, declared, limit) in checks {
            if declared > limit {
                self.problems
                    .push(CompileProblem::limit(dimension, declared, limit));
            }
        }
    }

    /// A token's key sequence must be unambiguous, because a condition names it.
    fn check_tokens(&mut self) {
        for token in &self.declarations.tokens {
            if token.extractors.len() > self.limits.keys_per_token {
                self.problems.push(CompileProblem::limit(
                    "keys per token",
                    token.extractors.len(),
                    self.limits.keys_per_token,
                ));
            }

            let mut seen = HashSet::new();
            for &extractor in &token.extractors {
                let key = self.declarations.extractors[extractor.index()].key;
                if !seen.insert(key) {
                    self.problems.push(CompileProblem::DuplicateKeyInToken {
                        token: token.name.to_string(),
                        key: self.declarations.key_name(key).to_owned(),
                    });
                }
            }
        }
    }

    /// A token represents one descriptor producer at one consumer. Supporting
    /// duplicate occurrences would require a separate consumer-token instance
    /// identifier to preserve Envoy's two-descriptor cardinality; reject them
    /// rather than silently merge them.
    fn check_requirements(&mut self) {
        let mut seen = HashSet::new();
        for requirement in &self.declarations.requirements {
            if !seen.insert((requirement.consumer, requirement.token)) {
                self.problems
                    .push(CompileProblem::DuplicateTokenRequirement {
                        consumer: self.declarations.consumers[requirement.consumer.index()]
                            .name
                            .to_string(),
                        token: self.declarations.token_name(requirement.token).to_owned(),
                    });
            }
        }
    }

    /// Validates source-specific bounds and compatibility rules that the
    /// generic declaration shapes cannot express.
    fn check_extractors(&mut self) {
        for extractor in &self.declarations.extractors {
            let ExtractorSource::TransportHeader(source) = &extractor.source else {
                continue;
            };

            let name_limit = self.limits.names_per_extractor.min(u8::MAX as usize + 1);
            if source.names.len() > name_limit {
                self.problems.push(CompileProblem::limit(
                    "transport header names per extractor",
                    source.names.len(),
                    name_limit,
                ));
            }

            let declared = self.declarations.keys[extractor.key.index()].value_kind;
            for name in &source.names {
                let implied = ValueKind::implied_by_name(name);
                if implied != declared {
                    self.problems
                        .push(CompileProblem::TransportHeaderKindMismatch {
                            key: self.declarations.key_name(extractor.key).to_owned(),
                            name: name.to_owned(),
                            implied: implied.name(),
                            declared: declared.name(),
                        });
                }
            }
        }
    }

    /// A condition names the whole key sequence of one of its consumer's
    /// declared tokens, as Envoy requires, and it must not test a key that can
    /// hold several values at once.
    fn check_conditions(&mut self) {
        for set in &self.declarations.condition_sets {
            if set.conditions.len() > self.limits.branches_per_condition_set {
                self.problems.push(CompileProblem::limit(
                    "conditions per set",
                    set.conditions.len(),
                    self.limits.branches_per_condition_set,
                ));
            }

            for (branch, condition) in set.conditions.iter().enumerate() {
                let mut named = HashSet::new();
                for entry in &condition.entries {
                    if !named.insert(entry.key) {
                        self.problems.push(CompileProblem::DuplicateKeyInCondition {
                            condition_set: set.name.to_string(),
                            branch,
                            key: self.declarations.key_name(entry.key).to_owned(),
                        });
                    }
                }

                let key_sequence: Vec<KeyId> =
                    condition.entries.iter().map(|entry| entry.key).collect();
                let candidates: Vec<TokenId> = self
                    .declarations
                    .consumer_tokens(set.consumer)
                    .filter(|&token| {
                        token_has_exact_key_sequence(&self.declarations, token, &key_sequence)
                    })
                    .collect();
                if candidates.is_empty() {
                    self.problems.push(CompileProblem::NoTokenForCondition {
                        condition_set: set.name.to_string(),
                        branch,
                        consumer: self.declarations.consumers[set.consumer.index()]
                            .name
                            .to_string(),
                        keys: condition
                            .entries
                            .iter()
                            .map(|entry| self.declarations.key_name(entry.key).to_owned())
                            .collect(),
                        hint: token_sequence_hint(&self.declarations, set.consumer),
                    });
                    continue;
                }

                for entry in &condition.entries {
                    if !matches!(entry.predicate, KeyPredicate::Equals(_)) {
                        continue;
                    }
                    if candidates.iter().any(|&token| {
                        self.declarations
                            .extractor_for(token, entry.key)
                            .is_some_and(|extractor| {
                                self.declarations.extractors[extractor.index()]
                                    .source
                                    .repetition()
                                    .is_repeated()
                            })
                    }) {
                        self.problems.push(CompileProblem::ValueMatchOnRepeatedKey {
                            condition_set: set.name.to_string(),
                            branch,
                            key: self.declarations.key_name(entry.key).to_owned(),
                        });
                    }
                }
            }

            let overlaps = overlapping_entries(&self.declarations, set);
            for (first, second, token) in overlaps {
                self.problems.push(CompileProblem::AmbiguousConditions {
                    condition_set: set.name.to_string(),
                    first,
                    second,
                    token: self.declarations.token_name(token).to_owned(),
                });
            }
        }

        let read_fields: Vec<(ConsumerId, MetadataField)> = self
            .declarations
            .reads
            .iter()
            .map(|read| (read.consumer, read.field))
            .collect();
        for (consumer, field) in read_fields {
            self.check_field_supplied(consumer, field);
        }
    }

    fn check_field_supplied(&mut self, consumer: ConsumerId, field: MetadataField) {
        if let Err(problem) = resolve_field(&self.declarations, consumer, field) {
            self.problems.push(problem);
        }
    }
}

fn resolve_field(
    declarations: &Declarations,
    consumer: ConsumerId,
    field: MetadataField,
) -> Result<MetadataFieldId, CompileProblem> {
    let consumer_name = || declarations.consumers[consumer.index()].name.to_string();
    match field {
        MetadataField::Field(field) => {
            if declarations
                .consumer_tokens(consumer)
                .any(|token| token == field.token())
                && declarations
                    .extractor_for(field.token(), field.key())
                    .is_some()
            {
                Ok(field)
            } else {
                Err(CompileProblem::QualifiedFieldNotSupplied {
                    consumer: consumer_name(),
                    token: declarations.token_name(field.token()).to_owned(),
                    key: declarations.key_name(field.key()).to_owned(),
                })
            }
        }
        MetadataField::Key(key) => {
            let candidates: Vec<TokenId> = declarations
                .consumer_tokens(consumer)
                .filter(|&token| declarations.extractor_for(token, key).is_some())
                .collect();
            match candidates.as_slice() {
                [] => Err(CompileProblem::KeyNotSupplied {
                    consumer: consumer_name(),
                    key: declarations.key_name(key).to_owned(),
                }),
                [token] => Ok(MetadataFieldId::new(*token, key)),
                _ => Err(CompileProblem::AmbiguousField {
                    consumer: consumer_name(),
                    key: declarations.key_name(key).to_owned(),
                    tokens: candidates
                        .iter()
                        .map(|&token| declarations.token_name(token).to_owned())
                        .collect(),
                }),
            }
        }
    }
}

/// Returns whether `token` produces exactly `keys`, in that order.
///
/// Envoy's descriptor match compares each entry's key positionally. Extractor
/// order is therefore part of a token's descriptor shape, and condition entry
/// order must agree with it.
fn token_has_exact_key_sequence(
    declarations: &Declarations,
    token: TokenId,
    keys: &[KeyId],
) -> bool {
    declarations.token_keys(token).eq(keys.iter().copied())
}

fn token_sequence_hint(declarations: &Declarations, consumer: ConsumerId) -> String {
    let Some(token) = declarations.consumer_tokens(consumer).next() else {
        return "; declare a required or optional token for this consumer".to_owned();
    };
    let keys: Vec<&str> = declarations
        .token_keys(token)
        .map(|key| declarations.key_name(key))
        .collect();
    format!(
        "; for example token `{}` has key sequence {keys:?}",
        declarations.token_name(token)
    )
}

/// Returns every pair of entries that can select the same descriptor.
///
/// The entries first have to bind to a common consumer-declared token, which
/// proves their ordered key sequence is identical. They overlap exactly when
/// every key is wildcarded by at least one entry or both entries demand the
/// same literal. Such an overlap would turn one Envoy descriptor into two
/// limiter applications, so compilation rejects it.
fn overlapping_entries(
    declarations: &Declarations,
    set: &ConditionSetDeclaration,
) -> Vec<(usize, usize, TokenId)> {
    let candidates: Vec<Vec<TokenId>> = set
        .conditions
        .iter()
        .map(|condition| condition_candidate_tokens(declarations, set.consumer, condition))
        .collect();
    let mut overlaps = Vec::new();
    for first in 0..set.conditions.len() {
        for second in first + 1..set.conditions.len() {
            if !conditions_overlap(&set.conditions[first], &set.conditions[second]) {
                continue;
            }
            if let Some(token) = candidates[first]
                .iter()
                .find(|token| candidates[second].contains(token))
            {
                overlaps.push((first, second, *token));
            }
        }
    }
    overlaps
}

fn condition_candidate_tokens(
    declarations: &Declarations,
    consumer: ConsumerId,
    condition: &Condition,
) -> Vec<TokenId> {
    let keys: Vec<KeyId> = condition.entries.iter().map(|entry| entry.key).collect();
    declarations
        .consumer_tokens(consumer)
        .filter(|&token| token_has_exact_key_sequence(declarations, token, &keys))
        .collect()
}

fn conditions_overlap(first: &Condition, second: &Condition) -> bool {
    first
        .entries
        .iter()
        .zip(&second.entries)
        .all(|(left, right)| {
            debug_assert_eq!(left.key, right.key);
            match (&left.predicate, &right.predicate) {
                (KeyPredicate::Equals(left), KeyPredicate::Equals(right)) => left == right,
                _ => true,
            }
        })
}

/// The state the compilation passes share.
struct Build<'a> {
    declarations: &'a Declarations,
    limits: &'a Limits,
    reach: &'a Reachability,
    problems: Vec<CompileProblem>,

    /// Condition sets worth compiling, which are those a producer can reach.
    live_sets: Vec<ConditionSetId>,
    live_token_set: HashSet<TokenId>,

    key_dictionaries: Vec<Option<u16>>,
    dictionaries: Vec<ValueDictionaryBuilder>,

    signatures: SignatureTable,
    pair_slots: PairSlotBuilder,
    symbol_slots: Vec<CompiledSymbolSlot>,
    symbol_slot_index: HashMap<ExtractorId, SymbolSlotId>,
    symbol_bits: u32,

    condition_sets: Vec<CompiledConditionSet>,
    participants: Vec<TableParticipant>,
    tables: BranchTableBuilder,

    value_slots: Vec<CompiledValueSlot>,
    field_value_slots: Vec<(MetadataFieldId, ValueSlotId)>,
}

impl<'a> Build<'a> {
    fn new(declarations: &'a Declarations, limits: &'a Limits, reach: &'a Reachability) -> Self {
        let reachable_consumers: HashSet<ConsumerId> = reach
            .per_producer
            .iter()
            .flat_map(|producer| producer.consumers.iter().copied())
            .collect();
        let live_sets = declarations
            .condition_sets
            .iter()
            .enumerate()
            .filter(|(_, set)| reachable_consumers.contains(&set.consumer))
            .map(|(index, _)| ConditionSetId::from_index(index))
            .collect();

        Self {
            declarations,
            limits,
            reach,
            problems: Vec::new(),
            live_sets,
            live_token_set: reach.live_tokens.iter().copied().collect(),
            key_dictionaries: vec![None; declarations.keys.len()],
            dictionaries: Vec::new(),
            signatures: SignatureTable::default(),
            pair_slots: PairSlotBuilder::default(),
            symbol_slots: Vec::new(),
            symbol_slot_index: HashMap::new(),
            symbol_bits: 0,
            condition_sets: Vec::new(),
            participants: Vec::new(),
            tables: BranchTableBuilder::default(),
            value_slots: Vec::new(),
            field_value_slots: Vec::new(),
        }
    }

    /// Interns every literal any live condition tests, one dictionary per key.
    fn intern_dictionaries(&mut self) {
        for &set_id in &self.live_sets {
            let set = &self.declarations.condition_sets[set_id.index()];
            for condition in &set.conditions {
                for entry in &condition.entries {
                    let KeyPredicate::Equals(literal) = &entry.predicate else {
                        continue;
                    };
                    let dictionary = match self.key_dictionaries[entry.key.index()] {
                        Some(existing) => existing,
                        None => {
                            let index = self.dictionaries.len() as u16;
                            self.dictionaries.push(ValueDictionaryBuilder::new());
                            self.key_dictionaries[entry.key.index()] = Some(index);
                            index
                        }
                    };
                    let _ = self.dictionaries[dictionary as usize].intern(literal);
                }
            }
        }

        for dictionary in &self.dictionaries {
            if dictionary.len() + 2 > self.limits.dictionary_entries_per_key {
                self.problems.push(CompileProblem::limit(
                    "dictionary entries per key",
                    dictionary.len() + 2,
                    self.limits.dictionary_entries_per_key,
                ));
            }
            if dictionary.widest_literal() > self.limits.literal_bytes {
                self.problems.push(CompileProblem::limit(
                    "literal bytes",
                    dictionary.widest_literal(),
                    self.limits.literal_bytes,
                ));
            }
        }
    }

    /// Derives signatures, allocates PairSlots, and fills the branch tables.
    fn build_matching(&mut self) {
        for index in 0..self.declarations.condition_sets.len() {
            let set_id = ConditionSetId::from_index(index);
            let set = &self.declarations.condition_sets[index];
            let live = self.live_sets.contains(&set_id);
            let participants_start = self.participants.len() as u32;

            if live {
                self.compile_conditions(set);
                let token_order: HashMap<TokenId, usize> = self
                    .declarations
                    .consumer_tokens(set.consumer)
                    .enumerate()
                    .map(|(position, token)| (token, position))
                    .collect();
                self.participants[participants_start as usize..].sort_by_key(|participant| {
                    token_order[&self.pair_slots.slots[participant.pair_slot.index()].token]
                });
            }

            self.condition_sets.push(CompiledConditionSet {
                name: set.name.clone(),
                consumer: set.consumer,
                branches: set.conditions.len(),
                participants: Range {
                    start: participants_start,
                    end: self.participants.len() as u32,
                },
            });
        }

        if self.pair_slots.len() > self.limits.pair_slots {
            self.problems.push(CompileProblem::limit(
                "pair slots",
                self.pair_slots.len(),
                self.limits.pair_slots,
            ));
        }
        if self.signatures.len() > self.limits.signatures {
            self.problems.push(CompileProblem::limit(
                "signatures",
                self.signatures.len(),
                self.limits.signatures,
            ));
        }
        if self.tables.total_entries() > self.limits.branch_table_entries {
            self.problems.push(CompileProblem::limit(
                "branch table entries",
                self.tables.total_entries(),
                self.limits.branch_table_entries,
            ));
        }
    }

    /// Turns one set's conditions into table entries.
    fn compile_conditions(&mut self, set: &ConditionSetDeclaration) {
        // One table per (set, PairSlot), created when the first condition needs
        // it, so a set only pays for the slots it actually consults.
        let mut tables: HashMap<PairSlotId, u32> = HashMap::new();

        for (branch, condition) in set.conditions.iter().enumerate() {
            let candidates = condition_candidate_tokens(self.declarations, set.consumer, condition);
            for token in candidates {
                if !self.live_token_set.contains(&token) {
                    continue;
                }
                let Some((slot_id, word)) = self.locate(token, condition) else {
                    continue;
                };

                let offset = if let Some(&offset) = tables.get(&slot_id) {
                    offset
                } else {
                    let Some(len) = self.pair_slots.slots[slot_id.index()].table_len() else {
                        self.problems.push(CompileProblem::limit(
                            "branch table entries",
                            usize::MAX,
                            self.limits.branch_table_entries,
                        ));
                        continue;
                    };
                    let Some(total) = self.tables.total_entries().checked_add(len) else {
                        self.problems.push(CompileProblem::limit(
                            "branch table entries",
                            usize::MAX,
                            self.limits.branch_table_entries,
                        ));
                        continue;
                    };
                    if total > self.limits.branch_table_entries {
                        self.problems.push(CompileProblem::limit(
                            "branch table entries",
                            total,
                            self.limits.branch_table_entries,
                        ));
                        continue;
                    }
                    let offset = self.tables.reserve(len);
                    self.participants.push(TableParticipant {
                        pair_slot: slot_id,
                        table_offset: offset,
                    });
                    let _ = tables.insert(slot_id, offset);
                    offset
                };

                self.tables.set(offset, word, (branch + 1) as u8);
            }
        }
    }

    /// Returns the PairSlot a condition reads and the word that satisfies it.
    fn locate(&mut self, token: TokenId, condition: &Condition) -> Option<(PairSlotId, u64)> {
        let constrained: Vec<KeyId> = condition
            .entries
            .iter()
            .filter(|entry| matches!(entry.predicate, KeyPredicate::Equals(_)))
            .map(|entry| entry.key)
            .collect();
        let signature_id = self.signatures.intern(&constrained);
        let keys = self.signatures.get(signature_id).keys().to_vec();

        let mut symbol_slots = Vec::with_capacity(keys.len());
        let mut widths = Vec::with_capacity(keys.len());
        for &key in &keys {
            let extractor = self.declarations.extractor_for(token, key)?;
            symbol_slots.push(self.symbol_slot(extractor)?);
            widths.push(field_bits(self.cardinality(key)));
        }

        let bits: u32 = widths.iter().sum();
        if bits > self.limits.pair_slot_bits {
            self.problems.push(CompileProblem::limit(
                "pair slot bits",
                bits as usize,
                self.limits.pair_slot_bits as usize,
            ));
            return None;
        }

        let slot_id = self
            .pair_slots
            .intern(token, signature_id, &symbol_slots, &widths);

        // The word a condition demands is its literals, each shifted into the
        // field the slot gave its key.
        let mut word = 0u64;
        let mut shift = 0;
        for (&key, &field_width) in keys.iter().zip(&widths) {
            let literal = condition
                .entries
                .iter()
                .find(|entry| entry.key == key)
                .and_then(|entry| match &entry.predicate {
                    KeyPredicate::Equals(literal) => Some(literal.as_slice()),
                    KeyPredicate::Any => None,
                })?;
            let dictionary = self.key_dictionaries[key.index()]?;
            let symbol = self.dictionaries[dictionary as usize].intern(literal);
            word |= symbol.as_word() << shift;
            shift += field_width;
        }
        Some((slot_id, word))
    }

    /// Returns where an extractor's value is encoded in the packed symbol field.
    fn symbol_slot(&mut self, extractor: ExtractorId) -> Option<SymbolSlotId> {
        if let Some(&existing) = self.symbol_slot_index.get(&extractor) {
            return Some(existing);
        }
        let key = self.declarations.extractors[extractor.index()].key;
        let dictionary = self.key_dictionaries[key.index()]?;
        let bits = field_bits(self.cardinality(key));
        let id = SymbolSlotId::from_index(self.symbol_slots.len());
        self.symbol_slots.push(CompiledSymbolSlot {
            extractor,
            dictionary,
            bit_offset: self.symbol_bits,
            bits,
        });
        self.symbol_bits += bits;
        let _ = self.symbol_slot_index.insert(extractor, id);
        Some(id)
    }

    fn cardinality(&self, key: KeyId) -> usize {
        match self.key_dictionaries[key.index()] {
            Some(dictionary) => self.dictionaries[dictionary as usize].len() + 2,
            None => 2,
        }
    }

    /// Gives every requested token-qualified field a slot in the packed
    /// context. One slot has one extractor; there is no declaration-order
    /// fallback between transport metadata and trusted identity.
    fn build_retention(&mut self) {
        for field in self.retained_fields() {
            let Some(extractor) = self.declarations.extractor_for(field.token(), field.key())
            else {
                continue;
            };
            let source = &self.declarations.extractors[extractor.index()].source;
            let id = ValueSlotId::from_index(self.value_slots.len());
            self.value_slots.push(CompiledValueSlot {
                field,
                key: field.key(),
                token: field.token(),
                extractor,
                repeated: source.repetition().is_repeated(),
                value_kind: self.declarations.keys[field.key().index()].value_kind,
            });
            self.field_value_slots.push((field, id));
        }

        if self.value_slots.len() > self.limits.value_slots {
            self.problems.push(CompileProblem::limit(
                "value slots",
                self.value_slots.len(),
                self.limits.value_slots,
            ));
        }
    }

    fn retained_fields(&self) -> Vec<MetadataFieldId> {
        let live_consumers: HashSet<ConsumerId> = self
            .reach
            .per_producer
            .iter()
            .flat_map(|producer| producer.consumers.iter().copied())
            .collect();
        let mut fields = Vec::new();
        let mut seen = HashSet::new();

        for read in &self.declarations.reads {
            if live_consumers.contains(&read.consumer)
                && let Ok(field) = resolve_field(self.declarations, read.consumer, read.field)
                && seen.insert(field)
            {
                fields.push(field);
            }
        }
        fields
    }

    /// Rejects an epoch whose fixed header alone cannot fit the packed-context
    /// bound. Checking before request time keeps an impossible configuration
    /// from failing only after a receiver has staged data.
    fn validate_layout(&mut self) {
        let layout = self.layout();
        if layout.data_offset > self.limits.context_bytes {
            self.problems.push(CompileProblem::ContextHeaderTooLarge {
                needed: layout.data_offset,
                limit: self.limits.context_bytes,
            });
        }
    }

    fn layout(&self) -> ContextLayout {
        let token_bits = self
            .reach
            .live_tokens
            .iter()
            .map(|token| token.index() + 1)
            .max()
            .unwrap_or(0);
        ContextLayout::new(token_bits, self.symbol_bits, self.value_slots.len())
    }

    /// Assembles the epoch: the dispatch tables, the layout, and one extraction
    /// plan per producer.
    fn finish(self, epoch: Epoch, limits: Limits, declarations: &Declarations) -> CompiledMetadata {
        let live_extractors: HashSet<ExtractorId> =
            self.reach.live_extractors.iter().copied().collect();

        let mut headers = NameIndexBuilder::new(HEADER_FOLDING);
        let mut claims = NameIndexBuilder::new(EXACT_FOLDING);
        let mut derived = NameIndexBuilder::new(EXACT_FOLDING);
        let mut peer_address = Vec::new();
        let mut extractors = Vec::with_capacity(declarations.extractors.len());

        for (index, declared) in declarations.extractors.iter().enumerate() {
            let id = ExtractorId::from_index(index);
            let live = live_extractors.contains(&id);
            let mut value_limit = limits.value_bytes;

            match &declared.source {
                ExtractorSource::TransportHeader(source) => {
                    value_limit = source
                        .max_value_bytes
                        .unwrap_or(limits.value_bytes)
                        .min(limits.value_bytes);
                    if live {
                        for name in &source.names {
                            headers.insert(name, id);
                        }
                    }
                }
                ExtractorSource::AuthorizedClaim(source) => {
                    if live {
                        claims.insert(&source.claim, id);
                    }
                }
                ExtractorSource::Derived(source) => {
                    if live {
                        derived.insert(&source.name, id);
                    }
                }
                ExtractorSource::PeerAddress(part) => {
                    if live {
                        peer_address.push(PeerAddressExtractor {
                            extractor: id,
                            part: *part,
                        });
                    }
                }
            }

            extractors.push(CompiledExtractor {
                key: declared.key,
                repetition: declared.source.repetition(),
                value_limit,
            });
        }

        let mut token_extractors = Vec::new();
        let tokens = declarations
            .tokens
            .iter()
            .map(|token| {
                let start = token_extractors.len() as u32;
                token_extractors.extend_from_slice(&token.extractors);
                CompiledToken {
                    extractors: Range {
                        start,
                        end: token_extractors.len() as u32,
                    },
                }
            })
            .collect::<Vec<_>>();

        let layout = self.layout();

        let mut requirements =
            vec![TokenRequirements::default(); declarations.consumers.len()].into_boxed_slice();
        for declared in &declarations.requirements {
            let entry = &mut requirements[declared.consumer.index()];
            let bit = 1u64 << declared.token.index();
            match declared.requirement {
                Requirement::Required => entry.required |= bit,
                Requirement::Optional => entry.optional |= bit,
            }
        }

        let plans = self.build_plans(declarations);
        let dictionaries: Vec<ValueDictionary> = self
            .dictionaries
            .into_iter()
            .map(ValueDictionaryBuilder::build)
            .collect();

        CompiledMetadata {
            epoch,
            limits,
            key_names: declarations
                .keys
                .iter()
                .map(|key| key.name.clone())
                .collect(),
            key_value_kinds: declarations.keys.iter().map(|key| key.value_kind).collect(),
            token_names: declarations
                .tokens
                .iter()
                .map(|token| token.name.clone())
                .collect(),
            token_requirements: requirements,
            extractors: extractors.into_boxed_slice(),
            header_names: headers.build(),
            claim_names: claims.build(),
            derived_names: derived.build(),
            peer_address_extractors: peer_address.into_boxed_slice(),
            tokens: tokens.into_boxed_slice(),
            token_extractors: token_extractors.into_boxed_slice(),
            symbol_slots: self.symbol_slots.into_boxed_slice(),
            dictionaries: dictionaries.into_boxed_slice(),
            signature_count: self.signatures.len(),
            pair_slots: self.pair_slots.slots.into_boxed_slice(),
            pair_slot_fields: self.pair_slots.fields.into_boxed_slice(),
            condition_sets: self.condition_sets.into_boxed_slice(),
            participants: self.participants.into_boxed_slice(),
            tables: self.tables.build(),
            value_slots: self.value_slots.into_boxed_slice(),
            field_value_slots: self.field_value_slots.into_boxed_slice(),
            layout,
            plans,
        }
    }

    /// Restricts the epoch's work to what each producer's downstream observes.
    fn build_plans(&self, declarations: &Declarations) -> Box<[ExtractionPlan]> {
        self.reach
            .per_producer
            .iter()
            .map(|reach| {
                let tokens: HashSet<TokenId> = reach.tokens.iter().copied().collect();

                let symbol_slots: Vec<SymbolSlotId> = self
                    .symbol_slots
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| reach.extractors.contains(&slot.extractor))
                    .map(|(index, _)| SymbolSlotId::from_index(index))
                    .collect();

                let value_slots: Vec<ValueSlotId> = self
                    .value_slots
                    .iter()
                    .enumerate()
                    .filter(|(_, slot)| tokens.contains(&slot.token))
                    .map(|(index, _)| ValueSlotId::from_index(index))
                    .collect();

                ExtractionPlan::new(
                    declarations.producers[reach.producer.index()].name.clone(),
                    declarations.extractors.len(),
                    &reach.extractors,
                    &reach.tokens,
                    symbol_slots.into_boxed_slice(),
                    value_slots.into_boxed_slice(),
                )
            })
            .collect()
    }
}
