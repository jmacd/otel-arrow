// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Reachability: what survives, and for whom.
//!
//! Nothing in a metadata configuration is worth compiling unless something can
//! observe it, and the cheapest work is the work that is never done. This pass
//! answers two questions.
//!
//! *What does anything observe?* A key survives if a reachable consumer reads,
//! bags or tests it. A token survives when a reachable consumer declares it
//! required or optional. The consumer declaration is the only binding rule:
//! values do not float in from unrelated tokens that happen to produce the same
//! key. An extractor survives if a surviving token holds it. Everything else is
//! dropped and reported, so an operator who configured a header that goes
//! nowhere hears about it at startup rather than wondering why it never arrives.
//!
//! *What does this producer's downstream observe?* Restricting the first answer
//! to the consumers a producer can reach gives that producer's extraction plan.
//! Layout is untouched by this: contexts carry extractor symbols, not condition
//! answers, so condition sets do not occupy context slots at all.

use crate::condition::KeyPredicate;
use crate::declaration::Declarations;
use crate::error::CompileWarning;
use crate::ids::{ConsumerId, ExtractorId, ProducerId, TokenId};

/// What survives compilation, and why.
#[derive(Debug)]
pub(crate) struct Reachability {
    /// Tokens worth resolving.
    pub(crate) live_tokens: Vec<TokenId>,
    /// Extractors worth running.
    pub(crate) live_extractors: Vec<ExtractorId>,
    /// What each producer's downstream can observe.
    pub(crate) per_producer: Vec<ProducerReach>,
    /// What was dropped, for the caller to log at startup.
    pub(crate) warnings: Vec<CompileWarning>,
}

/// The consumers and tokens one producer's downstream can observe.
#[derive(Debug)]
pub(crate) struct ProducerReach {
    pub(crate) producer: ProducerId,
    pub(crate) consumers: Vec<ConsumerId>,
    pub(crate) tokens: Vec<TokenId>,
    pub(crate) extractors: Vec<ExtractorId>,
}

/// A dense set over dense identifiers.
#[derive(Debug)]
struct IdSet {
    present: Vec<bool>,
}

impl IdSet {
    fn new(len: usize) -> Self {
        Self {
            present: vec![false; len],
        }
    }

    fn insert(&mut self, index: usize) -> bool {
        let added = !self.present[index];
        self.present[index] = true;
        added
    }

    fn contains(&self, index: usize) -> bool {
        self.present[index]
    }
}

/// Runs both levels of pruning over the declarations.
pub(crate) fn analyze(declarations: &Declarations) -> Reachability {
    let mut warnings = Vec::new();

    let live_consumers = reachable_consumers(declarations, &mut warnings);
    let read_keys = observed_keys_for(declarations, |consumer| {
        live_consumers.contains(consumer.index())
    });
    let matched = value_matched_keys_for(declarations, |consumer| {
        live_consumers.contains(consumer.index())
    });
    let live_tokens = observed_tokens(declarations, |consumer| {
        live_consumers.contains(consumer.index())
    });
    let live_extractors = extractors_of(declarations, &live_tokens);

    report_drops(
        declarations,
        &read_keys,
        &matched,
        &live_tokens,
        &live_extractors,
        &mut warnings,
    );

    let per_producer = declarations
        .producers
        .iter()
        .enumerate()
        .map(|(index, _)| {
            let producer = ProducerId::from_index(index);
            let consumers = consumers_reached(declarations, producer, &live_consumers);
            let reached: IdSet = {
                let mut set = IdSet::new(declarations.consumers.len());
                for consumer in &consumers {
                    let _ = set.insert(consumer.index());
                }
                set
            };
            let tokens =
                observed_tokens(declarations, |consumer| reached.contains(consumer.index()));
            let extractors = extractors_of(declarations, &tokens);
            ProducerReach {
                producer,
                consumers,
                tokens: to_ids(&tokens, TokenId::from_index),
                extractors: to_ids(&extractors, ExtractorId::from_index),
            }
        })
        .collect();

    Reachability {
        live_tokens: to_ids(&live_tokens, TokenId::from_index),
        live_extractors: to_ids(&live_extractors, ExtractorId::from_index),
        per_producer,
        warnings,
    }
}

fn observed_keys_for(
    declarations: &Declarations,
    mut wanted: impl FnMut(ConsumerId) -> bool,
) -> IdSet {
    let mut keys = IdSet::new(declarations.keys.len());
    for read in &declarations.reads {
        if wanted(read.consumer) {
            let _ = keys.insert(read.field.key().index());
        }
    }
    for bag in &declarations.bags {
        if wanted(bag.consumer) {
            for field in &bag.fields {
                let _ = keys.insert(field.key().index());
            }
        }
    }
    keys
}

/// Keys some condition tests by value, and which therefore need a dictionary.
fn value_matched_keys_for(
    declarations: &Declarations,
    mut wanted: impl FnMut(ConsumerId) -> bool,
) -> IdSet {
    let mut keys = IdSet::new(declarations.keys.len());
    for set in &declarations.condition_sets {
        if !wanted(set.consumer) {
            continue;
        }
        for condition in &set.conditions {
            for entry in &condition.entries {
                if matches!(entry.predicate, KeyPredicate::Equals(_)) {
                    let _ = keys.insert(entry.key.index());
                }
            }
        }
    }
    keys
}

/// Tokens worth resolving for the consumers `wanted` accepts.
fn observed_tokens(
    declarations: &Declarations,
    mut wanted: impl FnMut(ConsumerId) -> bool,
) -> IdSet {
    let mut tokens = IdSet::new(declarations.tokens.len());

    // Required and optional declarations are the consumer's descriptor list.
    // Conditions bind only to that list, never to an ambient token.
    for requirement in &declarations.requirements {
        if wanted(requirement.consumer) {
            let _ = tokens.insert(requirement.token.index());
        }
    }

    tokens
}

fn extractors_of(declarations: &Declarations, tokens: &IdSet) -> IdSet {
    let mut extractors = IdSet::new(declarations.extractors.len());
    for (index, token) in declarations.tokens.iter().enumerate() {
        if tokens.contains(index) {
            for &extractor in &token.extractors {
                let _ = extractors.insert(extractor.index());
            }
        }
    }
    extractors
}

/// Consumers some producer can reach.
fn reachable_consumers(declarations: &Declarations, warnings: &mut Vec<CompileWarning>) -> IdSet {
    let mut consumers = IdSet::new(declarations.consumers.len());
    if declarations.reachable.is_empty() {
        for index in 0..declarations.consumers.len() {
            let _ = consumers.insert(index);
        }
        return consumers;
    }

    for &(_, consumer) in &declarations.reachable {
        let _ = consumers.insert(consumer.index());
    }
    for (index, consumer) in declarations.consumers.iter().enumerate() {
        if !consumers.contains(index) {
            warnings.push(CompileWarning::UnreachableConsumer {
                consumer: consumer.name.to_string(),
            });
        }
    }
    consumers
}

fn consumers_reached(
    declarations: &Declarations,
    producer: ProducerId,
    live: &IdSet,
) -> Vec<ConsumerId> {
    if declarations.reachable.is_empty() {
        return (0..declarations.consumers.len())
            .map(ConsumerId::from_index)
            .collect();
    }

    let mut reached: Vec<ConsumerId> = declarations
        .reachable
        .iter()
        .filter(|&&(from, consumer)| from == producer && live.contains(consumer.index()))
        .map(|&(_, consumer)| consumer)
        .collect();
    reached.sort_unstable();
    reached.dedup();
    reached
}

fn report_drops(
    declarations: &Declarations,
    retained_keys: &IdSet,
    matched_keys: &IdSet,
    live_tokens: &IdSet,
    live_extractors: &IdSet,
    warnings: &mut Vec<CompileWarning>,
) {
    for (index, key) in declarations.keys.iter().enumerate() {
        if retained_keys.contains(index) {
            continue;
        }
        // A key whose tokens all went away is genuinely gone. A key that only
        // ever appears as a wildcard is still extracted, because its token
        // cannot resolve without it; only its bytes are dropped.
        let gating_token = (0..declarations.tokens.len()).find(|&token| {
            live_tokens.contains(token)
                && declarations
                    .token_keys(TokenId::from_index(token))
                    .any(|produced| produced.index() == index)
        });
        match (gating_token, matched_keys.contains(index)) {
            (None, _) => warnings.push(CompileWarning::UnobservedKey {
                key: key.name.to_string(),
            }),
            (Some(_), true) => warnings.push(CompileWarning::MatchOnlyKey {
                key: key.name.to_string(),
            }),
            (Some(token), false) => warnings.push(CompileWarning::GatingOnlyKey {
                key: key.name.to_string(),
                token: declarations.tokens[token].name.to_string(),
            }),
        }
    }
    for (index, token) in declarations.tokens.iter().enumerate() {
        if !live_tokens.contains(index) {
            warnings.push(CompileWarning::UnobservedToken {
                token: token.name.to_string(),
            });
        }
    }
    for (index, extractor) in declarations.extractors.iter().enumerate() {
        if !live_extractors.contains(index) {
            warnings.push(CompileWarning::UnobservedExtractor {
                key: declarations.key_name(extractor.key).to_string(),
                source: extractor.source.describe(),
            });
        }
    }
}

fn to_ids<T>(set: &IdSet, build: impl Fn(usize) -> T) -> Vec<T> {
    set.present
        .iter()
        .enumerate()
        .filter(|&(_, &present)| present)
        .map(|(index, _)| build(index))
        .collect()
}
