// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Prototype walkthrough for pdata context entries.
//!
//! `cargo run -p otap-df-pdata --release --example pdata_context`
//!
//! The example follows the same order the engine would: read
//! `policies.context`, compile it, bind two nodes to handles, then push
//! messages through and show what each node sees.

#![allow(clippy::print_stdout)]

use std::collections::HashMap;
use std::time::Instant;

use otap_df_pdata::context::{
    ContextBuilder, ContextCompiler, ContextPolicy, ContextRecord, ContextRef, ContextSchema,
    EntryHandle, ValueKind,
};
use std::sync::Arc;

/// The `policies.context` section, taken from RFC 0004.
const POLICY_YAML: &str = r#"
entries:
  # A product user is a claim and a header together, but only in
  # production. Two dimensions, one condition.
  product_user:
    - type: authorized_identity
      name: customer_id
    - type: transport_header
      name: workspace_id
    - type: transport_header_match
      name: xyz_environment
      value: production

  # The same claim on its own. One dimension, shares the claim's
  # source slot with product_user.
  product_account:
    - type: authorized_identity
      name: customer_id

  # Network information.
  origin_address:
    - type: network_info
      name: peer_socket_addr

  # A compile-time constant, resolved once at finalize.
  receiver:
    type: constant
    value: otlp-http-json

  # Generated per message.
  idempotency:
    - type: randomness
      value: uuid7

required_in:
  - node: receiver0
    entry: product_account
optional_in:
  - node: receiver0
    entry: product_user
"#;

/// One simulated inbound message.
struct Message {
    label: &'static str,
    peer: &'static str,
    claims: &'static [(&'static str, &'static str)],
    headers: &'static [(&'static str, &'static str)],
}

const MESSAGES: &[Message] = &[
    Message {
        label: "acme, production, full context",
        peer: "10.1.2.3:44120",
        claims: &[("customer_id", "acme"), ("unused-claim", "ignored")],
        headers: &[
            ("Workspace-Id", "ws-77"),
            ("workspace_id", "ws-77"),
            ("xyz_environment", "production"),
            ("x-request-id", "abc123"),
            ("user-agent", "otel/1.0"),
        ],
    },
    Message {
        label: "acme, staging: condition fails",
        peer: "10.1.2.4:51002",
        claims: &[("customer_id", "acme")],
        headers: &[("workspace_id", "ws-77"), ("xyz_environment", "staging")],
    },
    Message {
        label: "globex, production",
        peer: "10.9.9.9:33001",
        claims: &[("customer_id", "globex")],
        headers: &[("workspace_id", "ws-01"), ("xyz_environment", "production")],
    },
    Message {
        label: "unauthenticated: no claim at all",
        peer: "10.0.0.1:1234",
        claims: &[],
        headers: &[("xyz_environment", "production")],
    },
];

fn main() {
    let policy: ContextPolicy = serde_yaml::from_str(POLICY_YAML).expect("policy parses");

    // ---- configuration time -------------------------------------------
    let mut compiler = ContextCompiler::new();
    compiler.declare_policy(&policy).expect("policy compiles");
    let schema = compiler.finalize().expect("schema finalizes");

    print_tables(&schema);

    // A node binds once and keeps the handle. From here on it never
    // sees a name again.
    let route_on = schema.entry("product_account").expect("bound");
    let batch_on = schema.entry("product_user").expect("bound");
    let workspace_of = match schema.resolve("product_user:workspace_id").expect("bound") {
        ContextRef::Dim(handle) => handle,
        ContextRef::Entry(_) => unreachable!("reference names a component"),
    };
    let required_at_receiver0 = schema.entry("product_account").expect("bound");

    println!("== node bindings ==");
    println!(
        "  router  -> entry #{} `{}`",
        route_on.index(),
        schema.entry_name(route_on)
    );
    println!(
        "  batcher -> entry #{} `{}` ({} dimensions)",
        batch_on.index(),
        schema.entry_name(batch_on),
        schema.entry_arity(batch_on)
    );
    println!(
        "  exporter-> dimension `{}` of `{}`\n",
        schema.dim_name(workspace_of),
        schema.entry_name(workspace_of.entry())
    );

    // A router's table is keyed by the precomputed entry hash. Lookup
    // costs one load from the record plus one small map probe.
    // A router's table is keyed by the precomputed entry hash, but the
    // hash is only a bucket: the stored key is compared on hit. Entry
    // values are attacker-influenced (headers, claims), so a 64-bit
    // collision must not be allowed to misroute. This mirrors
    // `EntityRegistry`, whose signature map hashes for the bucket and
    // then compares attribute sets structurally.
    let mut routes: HashMap<u64, (Vec<u8>, &str)> = HashMap::new();
    for (tenant, destination) in [("acme", "dedicated"), ("globex", "shared")] {
        let mut probe = ContextBuilder::new(Arc::clone(&schema));
        let _kept = probe.set_claim("customer_id", tenant);
        let record = probe.build();
        let hash = record.hash(route_on).expect("probe resolves");
        let key = record.key(route_on).expect("probe resolves").to_vec();
        let previous = routes.insert(hash, (key, destination));
        assert!(previous.is_none(), "distinct tenants hash apart");
    }

    // ---- message time -------------------------------------------------
    // One builder per receiver, reused for every message.
    let mut builder = ContextBuilder::new(Arc::clone(&schema));

    println!("== messages ==");
    for message in MESSAGES {
        let record = build_record(&mut builder, message);

        println!("  {}", message.label);
        println!("    encoded record: {} bytes", record.byte_len());
        for (name, key) in record.iter() {
            println!("      {name:<15} = {}", render(key));
        }
        for handle in schema.entry_handles() {
            if !record.is_present(handle) {
                println!("      {:<15} = <absent>", schema.entry_name(handle));
            }
        }

        let routed = record.hash(route_on).and_then(|hash| {
            routes
                .get(&hash)
                .filter(|(key, _)| Some(key.as_slice()) == record.key(route_on))
                .map(|(_, destination)| *destination)
        });
        match routed {
            Some(destination) => println!("    route      -> {destination}"),
            None => println!("    route      -> <default>"),
        }
        match record.hash(batch_on) {
            Some(hash) => println!("    batch key  -> {hash:#018x}"),
            None => println!("    batch key  -> <not batchable by product_user>"),
        }
        match record.dim_typed(workspace_of) {
            Some((kind, value)) => println!(
                "    propagate  -> workspace_id: {} ({})",
                render(value),
                kind.label()
            ),
            None => println!("    propagate  -> <nothing>"),
        }
        if !record.is_present(required_at_receiver0) {
            println!(
                "    POLICY     -> `{}` is required_in receiver0 and is missing",
                schema.entry_name(required_at_receiver0)
            );
        }
        println!();
    }

    demo_typed_equality(&schema);
    measure(&schema);
}

/// Shows what the value kind buys: keys that are byte-identical but
/// semantically different do not collide.
fn demo_typed_equality(schema: &Arc<ContextSchema>) {
    let product_user = schema.entry("product_user").expect("bound");
    let product_account = schema.entry("product_account").expect("bound");
    let mut builder = ContextBuilder::new(Arc::clone(schema));

    let as_text = {
        let _kept = builder.set_claim("customer_id", "acme");
        let _kept = builder.set_header_text("xyz_environment", "production");
        let _kept = builder.set_header("workspace_id", b"ws-77", ValueKind::Text);
        builder.build()
    };
    let as_binary = {
        let _kept = builder.set_claim("customer_id", "acme");
        let _kept = builder.set_header_text("xyz_environment", "production");
        let _kept = builder.set_header("workspace_id", b"ws-77", ValueKind::Binary);
        builder.build()
    };
    let single = {
        let _kept = builder.set_claim("customer_id", "acme");
        builder.build()
    };
    let multi = {
        let _kept = builder.set_claim_values("customer_id", ["ac", "me"]);
        builder.build()
    };

    println!("== typed equality ==");
    println!("  same bytes, different kind (text vs binary header):");
    report(&as_text, product_user, schema);
    report(&as_binary, product_user, schema);
    println!(
        "    keys equal as bytes: {}, records equal: {}",
        as_text.key(product_user) == as_binary.key(product_user),
        as_text.entry_eq(product_user, &as_binary, product_user)
    );

    println!("  single value \"acme\" vs multi-value [\"ac\", \"me\"]:");
    report(&single, product_account, schema);
    report(&multi, product_account, schema);
    println!(
        "    records equal: {}",
        single.entry_eq(product_account, &multi, product_account)
    );
    if let Some(values) = multi.dim_list(
        schema
            .dim_handles(product_account)
            .next()
            .expect("one dimension"),
    ) {
        println!("    multi decodes back to {values:?}");
    }
    println!();
}

fn report(record: &ContextRecord, handle: EntryHandle, schema: &ContextSchema) {
    let kinds: Vec<&str> = schema
        .dim_handles(handle)
        .filter_map(|dim| record.dim_typed(dim).map(|(kind, _)| kind.label()))
        .collect();
    println!(
        "    {:<15} key {:<14} kinds {:<18} hash {:#018x}",
        schema.entry_name(handle),
        record
            .key(handle)
            .map_or_else(|| "<absent>".to_owned(), render),
        format!("{kinds:?}"),
        record.hash(handle).unwrap_or(0)
    );
}

fn build_record(builder: &mut ContextBuilder, message: &Message) -> ContextRecord {
    offer(builder, message);
    builder.build()
}

/// Offers everything the receiver has. The source table decides what is
/// interesting; nothing upstream needs to know.
fn offer(builder: &mut ContextBuilder, message: &Message) {
    if !message.peer.is_empty() {
        let _kept = builder.set_network("peer_socket_addr", message.peer);
    }
    for (name, value) in message.claims {
        let _kept = builder.set_claim(name, value);
    }
    for (name, value) in message.headers {
        let _kept = builder.set_header_text(name, value);
    }
}

fn print_tables(schema: &ContextSchema) {
    println!("== source table ({} slots) ==", schema.source_count());
    println!("  only values some entry reads get a slot; everything else");
    println!("  is dropped by a single failed probe at the door\n");
    for (slot, source) in schema.source_table() {
        println!("  [{slot}] {:<20} {}", source.kind.label(), source.name);
    }

    println!("\n== entry table ({} entries) ==", schema.entry_count());
    for handle in schema.entry_handles() {
        let dims: Vec<&str> = schema
            .dim_handles(handle)
            .map(|dim| schema.dim_name(dim))
            .collect();
        println!(
            "  [{}] {:<16} dimensions {:?}",
            handle.index(),
            schema.entry_name(handle),
            dims
        );
    }

    let layout = schema.layout();
    println!("\n== record layout (fixed at finalize) ==");
    println!("  presence bitmap  @ 0 .. {} bytes", layout.hash_off);
    println!(
        "  entry hashes     @ {} ({} x u64)",
        layout.hash_off, layout.n_entries
    );
    println!(
        "  dimension index  @ {} ({} x 8B)",
        layout.dim_off, layout.n_dims
    );
    println!(
        "  dimension kinds  @ {} ({} x u8)",
        layout.kind_off, layout.n_dims
    );
    println!("  key data         @ {}", layout.data_off);
    println!("  fixed header     = {} bytes\n", layout.header_len());
}

/// Renders a key for display: text when printable, hex otherwise.
fn render(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) if text.chars().all(|c| !c.is_control()) => format!("{text:?}"),
        _ => format!("0x{}", hex::encode(bytes)),
    }
}

/// The same tenant model without the per-message generator or the
/// constant, which is what a routing or batching pipeline actually
/// needs. Isolates the cost of `uuid7` from the cost of the mechanism.
const LEAN_YAML: &str = r#"
entries:
  product_user:
    - type: authorized_identity
      name: customer_id
    - type: transport_header
      name: workspace_id
    - type: transport_header_match
      name: xyz_environment
      value: production
  product_account:
    - type: authorized_identity
      name: customer_id
"#;

fn measure(schema: &Arc<ContextSchema>) {
    println!("== cost ==");
    bench("full schema (5 entries, includes uuid7)", schema);

    let lean: ContextPolicy = serde_yaml::from_str(LEAN_YAML).expect("policy parses");
    let mut compiler = ContextCompiler::new();
    compiler.declare_policy(&lean).expect("policy compiles");
    let lean = compiler.finalize().expect("schema finalizes");
    bench("lean schema (2 entries, no generator)", &lean);
}

fn bench(label: &str, schema: &Arc<ContextSchema>) {
    let handle = schema.entry("product_user").expect("bound");
    let message = &MESSAGES[0];
    let rounds = 500_000;

    let mut builder = ContextBuilder::new(Arc::clone(schema));
    let mut sink = 0u64;

    // Warm up, then measure the steady state.
    for _ in 0..1000 {
        sink ^= build_record(&mut builder, message)
            .hash(handle)
            .unwrap_or(0);
    }

    let start = Instant::now();
    for _ in 0..rounds {
        let record = build_record(&mut builder, message);
        sink ^= record.hash(handle).unwrap_or(0);
    }
    let build_ns = start.elapsed().as_nanos() as f64 / f64::from(rounds);

    let record = build_record(&mut builder, message);
    let start = Instant::now();
    for _ in 0..rounds {
        sink ^= record.hash(handle).unwrap_or(0);
        sink ^= record.key(handle).map_or(0, |k| k.len() as u64);
    }
    let lookup_ns = start.elapsed().as_nanos() as f64 / f64::from(rounds);

    // Ingest only: the per-arriving-value hash probe, no record built.
    let start = Instant::now();
    for _ in 0..rounds {
        offer(&mut builder, message);
        builder.reset();
    }
    let ingest_ns = start.elapsed().as_nanos() as f64 / f64::from(rounds);

    // Seal only: entry evaluation plus the single record allocation.
    let start = Instant::now();
    for _ in 0..rounds {
        sink ^= builder.build().byte_len() as u64;
    }
    let seal_ns = start.elapsed().as_nanos() as f64 / f64::from(rounds);

    // Rotating messages defeats the record memo, so this is the
    // worst case: every message re-evaluates and re-encodes.
    let start = Instant::now();
    for round in 0..rounds {
        let message = &MESSAGES[round as usize % MESSAGES.len()];
        let record = build_record(&mut builder, message);
        sink ^= record.hash(handle).unwrap_or(0) ^ record.byte_len() as u64;
    }
    let varying_ns = start.elapsed().as_nanos() as f64 / f64::from(rounds);

    let memo = if builder.is_memoizing() {
        "memo hits"
    } else {
        "memo disabled by generator"
    };

    println!("  {label}");
    println!("    ingest, 8 values offered:  {ingest_ns:6.1} ns");
    println!("    seal,   evaluate + encode: {seal_ns:6.1} ns");
    println!("    build,  repeated message:  {build_ns:6.1} ns/message  ({memo})");
    println!("    build,  rotating messages: {varying_ns:6.1} ns/message  (memo misses)");
    println!("    lookup, hash + key:        {lookup_ns:6.2} ns/call");
    println!("    (checksum {sink:#x})");
}
