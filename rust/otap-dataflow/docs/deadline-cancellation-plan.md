# Engine plan: deadlines, cancellation, and Ack/Nack propagation

This document outlines a staged plan to add end-to-end deadline and cancellation handling to the OTAP dataflow engine and nodes (receivers, processors, exporters), plus the intended use of Ack/Nack for reliable delivery and backpressure.

The concrete motivating path is: OTLP/OTAP Receiver → Rate Limiter → OTLP/OTAP Exporter.

## Background and current state

- Tonic behavior
  - Deadlines: No automatic forward propagation of request deadlines. The `grpc-timeout` header is parsed server-side when configured, but per-call deadlines on outgoing client requests must be set explicitly.
  - Cancellation: When a server timeout or client cancellation occurs, futures are dropped and cancellation propagates backward through async tasks.

- Engine capabilities
  - Control messages include `Shutdown { deadline: Duration }` and the message channel drains pdata until the deadline.
  - `NodeControlMsg` defines `Ack { id }` and `Nack { id, reason }`. The `RetryProcessor` consumes these to implement reliable delivery (ack removes from pending, nack schedules retry).

- Gaps
  - No correlation ID carried with pdata across nodes.
  - No upstream route from a node’s effect handler to deliver Ack/Nack back to the owner node (engine routing missing).
  - No deadline context attached to pdata; nodes can’t know remaining time budget.
  - No cancellation token in-flight to allow early abort inside processors.
  - OTLP/OTAP receivers and exporters have TODOs for deadline handling; no server timeout or per-call client timeout usage.

## Goals

- Represent per-message deadline and optional cancellation in the dataflow.
- Allow nodes to emit Ack/Nack to the owning upstream node (e.g., retry processor), enabling reliable delivery and backpressure.
- Keep typical processors simple; only nodes that care (rate limiter, exporter, retry) need to inspect context.
- Preserve current behavior where IDs/deadlines aren’t provided (best-effort delivery still works).

## Proposed design

### Core types (new)

Introduce a minimal envelope for pdata, with small helper traits so most nodes can remain generic:

```rust
pub struct RequestCtx<T> {
    pub data: T,
    pub deadline: Option<std::time::Instant>,
    pub id: Option<u64>,
    // Optional: a cooperative cancellation token to cancel scheduled/delayed work
    pub cancel: Option<tokio_util::sync::CancellationToken>,
}

pub trait HasDeadline {
    fn deadline(&self) -> Option<std::time::Instant>;
}

pub trait HasMessageId {
    fn message_id(&self) -> Option<u64>;
}

impl<T> HasDeadline for RequestCtx<T> {
    fn deadline(&self) -> Option<std::time::Instant> { self.deadline }
}
impl<T> HasMessageId for RequestCtx<T> {
    fn message_id(&self) -> Option<u64> { self.id }
}
```

Location: either the engine crate or a small shared crate re-used by nodes. Keeping it in engine keeps the surface small initially.

### Receivers (OTLP/OTAP)

- Parse inbound deadline:
  - If `grpc-timeout` is present, set `deadline = now + parsed_duration`.
  - Optionally, configure `Server::timeout(...)` for a default upper bound.
- Create a `CancellationToken` tied to request lifetime; when the RPC future is dropped (timeout/cancel), the token cancels.
- Wrap decoded pdata as `RequestCtx<PData> { data, deadline, id: None, cancel: Some(token) }` and forward.

### RetryProcessor (owner of IDs)

- When forwarding a pdata downstream, stamp a unique `id` and register a routing entry `id → owner` in the engine runtime.
- Forward `RequestCtx` (preserve existing `deadline`/`cancel` from upstream if present, set/overwrite the `id`).
- Continue to handle `NodeControlMsg::Ack/Nack` as today.

### Rate limiter processor

- Accept pdata implementing `HasDeadline` (i.e., `RequestCtx<T>`). It does not need to inspect `data`.
- Compute remaining = `deadline - now` (if any). Estimate token delay.
  - If `delay > remaining`: do not queue; emit `Nack(id, "deadline exceeded by rate limit")` if an id exists; otherwise drop/log.
  - Else: schedule delayed send (spawn task with `tokio::sleep(delay)`), and also select on `cancel.cancelled()` to abort early. On cancel, emit `Nack(id, "cancelled")` if id exists.

### Exporters (OTLP/OTAP)

- Read remaining budget from `HasDeadline`; wrap the client call with `tokio::time::timeout(remaining, client.export(req))`.
- On success → `Ack(id)` (if present). On timeout or error → `Nack(id, reason)`.
- If no `id`, behave best-effort (log only) to preserve non-retry pipelines.

### Effect handler and runtime routing (engine)

- Add effect handler helpers:
  - `ack(id: u64)` and `nack(id: u64, reason: impl Into<String>)` that send `PipelineControlMsg::UpstreamAck/UpstreamNack`.
- Add two new pipeline control messages:
  - `UpstreamAck { id: u64 }` and `UpstreamNack { id: u64, reason: String }`.
- Runtime routing table:
  - When a retry processor stamps an `id`, it registers `id → control_sender_of_owner`.
  - On `UpstreamAck/Nack`, the engine looks up the owner and delivers `NodeControlMsg::Ack/Nack` to that node’s control channel.
  - Cleanup mapping when the message lifecycle is done (Ack or terminal drop).

### Cancellation propagation (optional but recommended)

- Carry `CancellationToken` in `RequestCtx` from receivers.
- Processors that schedule delayed/long work select on `token.cancelled()` to stop promptly.
- Exporters don’t need the token if they already timebox via `timeout(..)`; token is still useful for local work before the RPC.

## Phased implementation plan

### Phase 1: Core plumbing and routing

- Add `RequestCtx<T>` and traits in engine.
- Add `EffectHandler::ack/nack` and `PipelineControlMsg::{UpstreamAck, UpstreamNack}`.
- Add id routing map in runtime; deliver `NodeControlMsg::Ack/Nack` to owners.
- Tests: routing works; unknown id handling; cleanup.

### Phase 2: Integrate RetryProcessor

- Stamp ids and forward `RequestCtx` with `id` set; register ids.
- Tests: Ack removes pending; Nack reschedules; end-to-end through engine routing.

### Phase 3: Node adoption

- Receivers: wrap inbound as `RequestCtx`, attach deadline from `grpc-timeout` and cancel token.
- Exporters: apply per-call timeout using remaining deadline; emit Ack/Nack when `id` exists.
- Rate limiter: read `HasDeadline`, delay or Nack appropriately; respect `cancel`.
- Tests: end-to-end path Receiver → RateLimiter → Exporter with success, timeout, and cancellation cases.

### Phase 4: Optional enhancements

- Add a pass-through “context-preserving” helper for processors that transform only `data`.
- Metrics/telemetry on deadlines hit, nacks, retries.

## Open questions

1. Where should `RequestCtx` live? Engine vs a new shared crate (`otap-df-context`)?
2. Message ID scope: must ids be globally unique across pipelines/processes, or only within a runtime instance? (A simple per-runtime counter is easiest.)
3. Branching: when a message is fanned out to multiple downstreams, do we stamp multiple ids or reuse one id with fanout semantics? Who owns Ack aggregation? (Initial scope: assume linear path.)
4. Batching/splitting: how to maintain ids and deadlines when a processor merges/splits messages? (Proposed: copy id to all splits; merging produces a new id owned by the merger.)
5. Security/trust: prevent a node from forging Ack/Nack for foreign ids. (Engine could validate that only downstream of the owner can signal; initial scope: single-tenant trusted nodes.)
6. Deadlines source of truth: prefer `grpc-timeout` vs server-wide `Server::timeout`? How to combine if both present? (Proposed: use the shorter.)
7. What to do when no id is present but downstream hits deadline? Log-only vs a separate control signal for “best-effort failed”? (Initial scope: log-only.)
8. What happens on cancellation during a delayed rate-limit? Prefer Nack vs silent drop? (Proposed: Nack if id present, else silent drop.)
9. Cleanup policy for id routing entries on engine restart/failover; do we need persistence? (Out of scope for now.)
10. Testing approach for cancellation paths that rely on future drop; rely on `CancellationToken` and timeboxed sleeps.

## API sketch (selected)

Effect handler additions:

```rust
impl<PData> EffectHandler<PData> {
    pub async fn ack(&self, id: u64) -> Result<(), Error<PData>> { /* send UpstreamAck */ }
    pub async fn nack(&self, id: u64, reason: impl Into<String>) -> Result<(), Error<PData>> { /* send UpstreamNack */ }
}
```

Exporter usage pattern:

```rust
if let Some(dl) = pdata.deadline() {
    let rem = dl.saturating_duration_since(Instant::now());
    let res = tokio::time::timeout(rem, client.export(req)).await;
    match (res, pdata.message_id()) {
        (Ok(Ok(_)), Some(id)) => effect_handler.ack(id).await?,
        (Ok(Err(e)), Some(id)) => effect_handler.nack(id, e.to_string()).await?,
        (Err(_), Some(id)) => effect_handler.nack(id, "export timeout").await?,
        _ => { /* best-effort */ }
    }
}
```

Rate limiter decision:

```rust
let rem = pdata.deadline().and_then(|dl| dl.checked_duration_since(Instant::now()));
if rem.map_or(false, |r| r < estimated_delay) {
    if let Some(id) = pdata.message_id() { effect_handler.nack(id, "deadline too close").await?; }
    return Ok(());
}
// else schedule send after estimated_delay with cancel token
```

## Risks and mitigations

- Complexity creep: Keep RequestCtx minimal; use traits to avoid leaking details into processors.
- Misrouted Ack/Nack: Strong tests around id routing; clear cleanup on Ack or terminal outcomes.
- Starvation with aggressive deadlines: Prefer Nack early in rate limiter rather than queuing.
- Back-compat: When no ids are present, exporters/loggers behave as today.

## Milestones

- M1: Core routing and effect handler APIs + unit tests.
- M2: RetryProcessor id stamping + routing integration + tests.
- M3: Receiver wrapping + Exporter timeouts + Rate limiter deadline support + e2e tests.
- M4: Optional: CancellationToken propagation; metrics; docs.

---

Authoring note: this plan intentionally scopes branching/aggregation to a later phase; if your pipeline uses fan-out today, we’ll document interim behavior and follow up with an aggregation-aware Ack model.
