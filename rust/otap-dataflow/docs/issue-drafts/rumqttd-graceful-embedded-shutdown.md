# Add a graceful, embeddable shutdown API to `Broker`

## Status

Draft, intended to be filed upstream at
<https://github.com/bytebeamio/rumqtt/issues>. Every claim about current
`rumqttd` behavior in this document was verified by reading the source of the
pinned version used as context for this request: `rumqttd` 0.20.0, commit
[`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`](https://github.com/bytebeamio/rumqtt/tree/c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74/rumqttd),
not inferred from documentation. File and function references below (for
example `rumqttd/src/server/broker.rs`) point at that commit.

This is not a new request. Upstream already tracks the same underlying gap in
[issue #771, "Feature: Shutdown"](https://github.com/bytebeamio/rumqtt/issues/771)
(opened December 2023, still open), and a contributor prototyped a partial
solution in the
[`rumqttd-shutdown` comparison branch](https://github.com/bytebeamio/rumqtt/compare/main...rumqttd-shutdown).
This document exists because that issue and its proof of concept, while
correctly identifying the core problem, leave several parts of the lifecycle
contract implicit or unresolved (router-thread shutdown, per-connection task
tracking, deadlines, idempotency, Drop behavior, and retained/will/session
semantics). It is written to be detailed enough to implement directly, and to
consolidate the existing discussion into one concrete, acceptance-testable
specification rather than replace it.

## Summary

`rumqttd::Broker` has no supported way to stop a broker that is already
running. `Broker::start()` blocks for the life of the process in normal
operation, spawns OS threads that the caller cannot join or cancel, and
provides no signal, handle, or callback that causes those threads to exit or
their listening sockets to close. The only way to stop an embedded broker
today is to terminate the process that embeds it.

This request asks for a supported, documented, cross-platform way to:

1. Stop accepting new MQTT connections.
2. Optionally drain already-connected clients within a bounded deadline.
3. Release every bound listener socket.
4. Stop every OS thread that `Broker::new`/`Broker::start` created.
5. Return a typed, aggregated result describing how shutdown went.

This is a general embedding-lifecycle request, not specific to any one
downstream project. It applies equally to a long-running service that embeds
`rumqttd` as one component among several, a plugin or dynamically loaded
library that must release its resources before unload, a test harness that
starts and stops brokers repeatedly in one process, and a supervisor that
performs config reload or rolling restart without a full process restart.

## Motivation

`rumqttd` is documented and marketed as an embeddable broker library, not
only a standalone binary. Embedding implies the host application controls the
component's lifecycle: start it when needed, stop it when the host is
reconfigured, tested, or shut down, and reclaim its resources afterward.
`rumqttd` 0.20.0 only supports the "start" half of that contract.

Concrete scenarios where this gap is blocking, not cosmetic:

- **Any process that embeds more than one long-running component.** A host
  that runs an MQTT broker alongside other subsystems (an HTTP server, a
  data-processing pipeline, other protocol listeners) needs every embedded
  component to participate in the same orderly shutdown sequence -- for
  example responding to `SIGTERM`/`SIGINT`/Windows console control events
  within a bounded deadline. A component that can only be stopped by killing
  the whole process breaks that contract for everyone else in the process.
- **Dynamically loaded or unloaded components.** Issue #771 already reports
  this directly: an application embedding `rumqttd` inside a loadable/
  unloadable Windows DLL that outlives the extension's own lifetime found
  the inability to clean up broker-owned threads and sockets a "showstopper"
  before the DLL could be unloaded. The same problem applies to any plugin
  system, not only DLLs: threads and bound sockets that outlive their owning
  module corrupt the host process's resource accounting.
- **Automated and repeated testing.** Issue #771 separately reports that
  "lack of shutdown causes tests to hang" when trying to use the embedded
  broker in integration tests. A broker that leaks its listener thread and
  bound port for the remainder of the test process's life forces every test
  suite either to isolate each broker-starting test in its own child process
  or to share a small number of long-lived broker fixtures and design tests
  around never needing an independent instance -- both of which are
  workarounds for a missing library feature, not reasonable test design
  choices on their own.
- **Configuration reload and rolling restart.** `Config` (listener addresses,
  TLS material, `RouterConfig` limits) is consumed once at `Broker::new`/
  `Broker::start` and cannot be changed in place. The only way to apply a
  changed configuration is to construct a new `Broker`, which requires the
  old one's listener sockets to be released first; today they never are
  within the same process.
- **Freeing a listening port for reuse.** Any workflow that expects "stop
  the broker" to make its port immediately available to a different process
  or a differently configured broker cannot rely on `rumqttd` for that,
  because the underlying `TcpListener` is never dropped.
- **Multi-tenant or multi-instance hosts.** A process that creates and
  retires many logical brokers over its lifetime (for example, one per
  tenant, or one per test case, or one per dynamically provisioned
  workload) accumulates threads and sockets without bound, since nothing
  it does causes a `Broker`'s resources to be released.

None of these scenarios require killing the host process to work around a
library limitation. They require the library to support the lifecycle it
already advertises as a feature ("embeddable").

## Current behavior: blocking topology and absence of a stop API

This section states verified facts about the pinned commit, not
interpretation.

### `Broker::new` immediately spawns the router thread

`Broker::new(config)` (`rumqttd/src/server/broker.rs`) constructs a `Router`
and calls `router.spawn()` (`rumqttd/src/router/routing.rs`) before
`Broker::new` returns. `Router::spawn` starts a named OS thread
(`thread::Builder::new().name(format!("router-{}", self.id))`) running
`self.run(0)`, which is:

```rust
fn run(&mut self, count: usize) -> Result<(), RouterError> {
    match count {
        0 => loop {
            self.run_inner()?;
        },
        // ...
    }
}
```

`run_inner` blocks on `self.router_rx.recv()` (a `flume::Receiver`) whenever
there is no ready connection to service. This loop has no exit condition
other than `router_rx.recv()` returning an error, which only happens once
every clone of the corresponding `router_tx` sender has been dropped. In
practice that condition is unreachable from application code: `Broker` itself
holds a clone for its own lifetime, every accepted remote connection's
`remote(...)` task holds a clone for its lifetime, and every `Broker::link`,
`Broker::meters`, and `Broker::alerts` handle holds a clone. There is no
public API to close the sending side deliberately, and no `Event` variant the
router treats as a shutdown request.

### `Broker::start` is blocking and owns thread creation for every listener

`Broker::start(&mut self) -> Result<(), Error>` (`rumqttd/src/server/broker.rs`)
spawns, depending on configuration:

- one `timer` thread if `config.metrics` is set,
- one bridge thread if `config.bridge` is set,
- one thread per configured `v4` listener, each running its own
  `tokio::runtime::Builder::new_current_thread()` runtime that calls
  `Server::start(LinkType::Remote)`,
- one thread per configured `v5` listener, same shape,
- one thread per configured `ws` listener (behind the `websocket` feature),
  same shape,
- one `Metrics` thread if `config.prometheus` is set,
- one `Console` thread if `config.console` is set.

After spawning all of the above, `start()` ends with:

```rust
// in ideal case, where server doesn't crash, join() will never resolve
// we still try to join threads so that we don't return from function
// unless everything crashes.
server_thread_handles.into_iter().for_each(|handle| {
    // join() might panic in case the thread panics
    // we just ignore it
    let _ = handle.join();
});

Ok(())
```

The comment is accurate and is the point of this request: `start()` is
documented, in its own source, to normally never return. There is no
parameter, handle, channel, or flag anywhere in this path that a caller can
use to make it return early.

### Each listener's accept loop has no cancellation point

`Server::start` (same file) is:

```rust
pub async fn start(&mut self, link_type: LinkType) -> Result<(), Error> {
    let listener = TcpListener::bind(&self.config.listen).await?;
    // ...
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok((s, r)) => (s, r),
            Err(e) => { error!(error=?e, "Unable to accept socket."); continue; }
        };
        // ... TLS accept, then:
        task::spawn(remote(config, tenant_id, router_tx, network, protocol, /* ... */));
        time::sleep(delay).await;
    }
}
```

This is an unconditional `loop` around `listener.accept().await` with no
`select!`, no cancellation token, and no shutdown signal of any kind. The
`TcpListener` is a local variable owned by this function; it is only dropped,
and the port only released, if this function returns -- which it cannot do
except by an unhandled I/O panic path, since every branch of the match either
`continue`s or proceeds to spawn a connection and loop again. Each accepted
connection is handed to `tokio::task::spawn(remote(...))` and neither the
returned `JoinHandle` nor any other reference to that task is retained
anywhere; `Server`/`Broker` cannot enumerate, signal, or await any
already-accepted connection.

### No public shutdown API exists anywhere in this path

Confirmed for the pinned commit: `Broker` exposes `new`, `start`, `link`,
`meters`, and `alerts`. None of these, and no other public item in the crate,
provides a way to stop the router thread, close a listener's bound socket,
cancel an in-flight `accept().await`, signal a spawned per-connection task, or
cause `Broker::start` to return under any application-controlled condition.
Dropping every `Broker` value the application holds does not stop any of
these threads either, for the reasons given above.

### Prior art: issue #771 and the `rumqttd-shutdown` proof of concept

The linked proof-of-concept branch adds a `BrokerHandle` obtained from
`Broker::start`, backed by a shared `tokio_util::sync::CancellationToken`:
each listener/timer/bridge/console thread's `block_on` is changed from
awaiting its task directly to a `select!` between that task and
`token.cancelled()`, and `BrokerHandle::stop(self)` cancels the token. This is
a reasonable
starting point and this request deliberately reuses its shape rather than
proposing an unrelated design, but as its own review thread on issue #771
already surfaces, it has several gaps this document treats as required, not
optional, follow-up work:

- It never signals the **router thread**. `Router::spawn`'s loop has no
  cancellation branch at all in the PoC; only the listener/timer/bridge/
  console threads race the token.
- It does not track or signal **already-accepted per-connection tasks**.
  Cancelling a listener's `select!` stops new accepts and drops that
  listener's `TcpListener`, but every `remote(...)` task already spawned by
  `task::spawn` keeps running independently.
- `stop(self)` **returns immediately** with no way to learn when shutdown has
  actually finished, no deadline, and no aggregated result; a caller cannot
  distinguish "shutdown requested" from "shutdown complete."
- It provides only an abort-style stop, with no drain phase for in-flight
  MQTT exchanges.
- Because `stop` consumes `self`, a single `BrokerHandle` cannot be observed
  by more than one part of an application without an additional wrapper.

## Requested lifecycle semantics

Model the broker's life as an explicit sequence with observable transitions:
**Created** (`Broker::new` returned; router thread running) -> **Running**
(`start` has spawned listener/auxiliary threads and is accepting connections)
-> **Draining** (no longer accepting new connections; existing clients being
given a bounded opportunity to finish in-flight work) -> **Stopped** (every
broker-owned thread has exited; every listener socket is closed; the router
is no longer processing events).

Two distinct caller-facing operations should exist:

- **Graceful shutdown**: stop accepting new connections first, allow
  in-flight protocol exchanges to complete up to a caller-supplied deadline,
  then close remaining connections and stop every thread.
- **Immediate abort**: stop everything as soon as possible without waiting
  for in-flight work, for callers that need the fastest possible resource
  release (for example, a panicking test harness's cleanup path).

`Broker::start`, or its replacement, must return control to the caller under
normal operation once shutdown completes, rather than only on an unhandled
crash. A shutdown request must be usable from a different thread or task than
the one that called `start`, since in an embedding application the code that
decides "stop now" (a signal handler, a supervisor, a test's teardown code)
is very rarely the same call frame that started the broker.

## Proposed API options

The exact shape is intentionally left open for maintainer discussion; three
options are sketched below in increasing order of API disruption. They are
not mutually exclusive as a delivery sequence (for example, Option A first,
Option B later as a larger follow-up).

### Option A: extend the existing `BrokerHandle` proof of concept

```rust
pub struct BrokerHandle { /* ... */ }

impl BrokerHandle {
    /// Request immediate shutdown; does not wait for completion.
    pub fn abort(&self);

    /// Request graceful shutdown: stop accepting new connections, allow
    /// in-flight work up to `deadline`, then force-close anything left.
    /// Awaits full completion (every thread exited, every socket closed)
    /// and returns an aggregated, typed outcome.
    pub async fn shutdown(&self, deadline: Duration) -> ShutdownOutcome;

    /// True once shutdown has fully completed.
    pub fn is_stopped(&self) -> bool;
}

impl Clone for BrokerHandle { /* backed by Arc + CancellationToken */ }

impl Broker {
    // Unchanged signature; internally blocks until shutdown completes
    // instead of blocking forever, and returns once it does.
    pub fn start(&mut self) -> Result<(), Error>;

    // New: obtain a handle before or after calling start().
    pub fn handle(&self) -> BrokerHandle;
}
```

This keeps `start`'s existing signature and blocking behavior for callers who
never stop their broker, while making a `BrokerHandle` (cloneable, so
multiple owners can observe and await the same shutdown) the supported way to
request and observe shutdown.

### Option B: fully async, caller-owned task

```rust
pub struct BrokerTask { /* implements Future<Output = Result<(), Error>> */ }

impl Broker {
    /// Consumes self; caller decides how to drive the returned future
    /// (spawn it, or run it inside their own runtime task).
    pub fn run(self) -> (BrokerTask, BrokerHandle);
}
```

This better fits callers who already run a multi-threaded Tokio runtime and
would rather not have `rumqttd` spawn its own dedicated OS threads per
listener at all. It is a larger change (it changes where and how many
threads exist, not just how they stop) and is presented as a longer-term
alternative, not a precondition for solving this issue.

### Option C: minimal, fully additive

```rust
impl Broker {
    // Existing start() is untouched and remains documented as
    // "runs until the process exits; use start_with_shutdown for a
    // stoppable broker."
    pub fn start(&mut self) -> Result<(), Error>;

    // New, opt-in entry point with the lifecycle described above.
    pub fn start_with_shutdown(&mut self) -> Result<BrokerHandle, Error>;
}
```

This avoids any behavior change for existing callers of `start`, at the cost
of two parallel start paths to maintain.

The essential request is the guarantee (a supported way to stop and reclaim
resources), not any one of these specific names or signatures.

## Listener accept cancellation

Every `Server::start` accept loop (one instance per configured `v4`, `v5`,
and `ws` listener) must become cancellable:

- Race `listener.accept().await` against a cancellation signal (a
  `CancellationToken`, a `oneshot::Receiver`, or equivalent) using
  `tokio::select!`, matching the shape already prototyped in the
  `rumqttd-shutdown` branch.
- On cancellation, break out of the loop, let the `TcpListener` drop (which
  releases the bound port), and return so the owning thread's `block_on`
  completes and the thread becomes joinable.
- The existing `time::sleep(delay).await` between accepted connections must
  also race the cancellation signal; it currently sits inside the same loop
  and would otherwise add up to `next_connection_delay_ms` of extra latency
  to every shutdown.
- Define what happens to a TLS handshake or MQTT `CONNECT` wait that is
  already in progress when cancellation is requested. Recommend allowing it
  to finish within the overall shutdown deadline rather than aborting it
  mid-handshake, since a half-completed TLS handshake is more likely to
  confuse a well-behaved client than a short additional wait.
- After cancellation, the port must be immediately available for a new bind
  (by this process or another) without any artificial delay introduced by
  `rumqttd` itself. Normal TCP `TIME_WAIT` behavior is outside this request's
  control and is not a defect in the API being requested here.

## Router, link, and task ownership

- The router thread must gain its own defined stop path, not only the
  listener threads. The simplest option compatible with the router's
  existing design is a new `Event` variant (for example `Event::Shutdown`)
  that `run_inner`/`events` recognizes and that causes `run` to return `Ok(())`
  instead of looping forever; this reuses the router's existing single
  channel-driven control flow rather than introducing a second, parallel
  signaling mechanism specific to the router.
- Every accepted remote connection's `tokio::task::spawn(remote(...))` must
  be tracked by the owning `Server` (for example, in a `tokio::task::JoinSet`)
  so that graceful shutdown can (a) stop accepting new connections, (b)
  signal already-connected clients that the broker is shutting down, and (c)
  wait for those tasks up to the deadline before abandoning them. This is not
  unprecedented in the codebase: `Server` already tracks comparable
  per-client state today via `awaiting_will_handler: Arc<Mutex<HashMap<...>>>`,
  so the existing structure can be extended rather than redesigned.
- `Broker::link`, `Broker::meters`, and `Broker::alerts` all hand out
  `flume`-channel-backed handles keyed off the same `router_tx`. Once the
  router stops, calls on these handles (`LinkRx::recv`, `MetersLink::recv`,
  `AlertsLink::recv`) must observe a defined, documented outcome (for
  example, the existing `RecvError`/`Disconnected` variants) rather than
  hanging indefinitely, since these are exactly the handles an in-process
  embedding consumer polls in its own event loop and that consumer needs a
  way to notice the broker has stopped.
- The relative ordering between "router stops" and "listener threads stop"
  should be defined: recommend stopping listener accept loops and draining
  connected clients first, then stopping the router last, once no connection
  or listener still depends on it. This avoids connections observing a
  vanished router while they still expect to send or receive protocol
  traffic.

## Idempotency

Calling the shutdown/stop operation more than once, whether sequentially or
concurrently from different callers holding the same or a cloned handle,
must not panic, must not attempt to redo cleanup that already happened, and
must not race on partially-released resources. The PoC's `stop(self)`
enforces "at most once" only by consuming the handle, which prevents a
second call from the same owner but does not support more than one interested
party observing the same broker (for example, a signal handler and an
independent health-check task that both want to react to shutdown). Prefer a
design where the handle is `Clone` (backed by a shared `CancellationToken` or
equivalent) and the underlying stop operation is itself idempotent: a second
or later call, from any handle clone, returns the same terminal outcome (or
an explicit "already stopping"/"already stopped" status) without re-running
shutdown steps or double-closing already-closed resources.

## Deadlines

Graceful shutdown must accept a deadline as a first-class parameter of the
API, not be left to the caller to implement by racing the shutdown call
against their own timer. The caller cannot otherwise know which internal
step -- listener close, per-connection drain, router stop -- is still
pending, and therefore cannot safely apply their own timeout without risking
either premature abort mid-step or an indefinite hang.

- If graceful completion happens before the deadline, the outcome should
  reflect that with confirmation that every thread exited and every socket
  closed.
- If the deadline elapses first, the operation must escalate to forced
  abort of whatever remains (abort remaining connection tasks, stop the
  router unconditionally) and return a typed outcome identifying that the
  deadline was hit and, if practical, which stage(s) had not completed
  (for example, `ShutdownOutcome::TimedOut { pending_connections: usize }`),
  rather than silently returning as if graceful completion succeeded.
- An indefinite wait (no deadline) should remain available for callers that
  want to block until graceful shutdown fully completes regardless of how
  long that takes, such as a test's teardown code.

## Draining

Before any hard stop, "drain" should have this meaning:

1. Stop accepting new TCP connections on every configured listener.
2. Stop admitting new `Event::Connect` requests at the router.
3. Allow already-connected clients to finish in-flight, already-started QoS 1
   acknowledgement exchanges and already-buffered outbound `Forward`
   delivery, up to the deadline.
4. Close any connections still open once draining ends, using a defined,
   protocol-appropriate disconnect rather than an abrupt socket close: for
   MQTT 5 clients, a `DISCONNECT` packet with a reason code such as "Server
   shutting down" (`0x8B`, "Server busy," or a similarly appropriate reason
   already defined by the MQTT 5 specification) so a well-behaved client can
   distinguish an intentional, orderly shutdown from an unexpected network
   failure and apply its own reconnect/backoff policy accordingly; for MQTT
   3.1.1 clients, which have no disconnect reason codes, a plain connection
   close is the only option and remains acceptable.

Immediate abort (skipping steps 2 to 4's waiting behavior) must remain
available for callers that explicitly want the fastest possible resource
release, but graceful drain-then-stop should be the documented, recommended
default for production use. This mirrors a "stop accepting new work, then
drain, then stop" pattern that is already a common lifecycle contract for
long-running network servers embedded inside a larger host application, and
is not specific to any one embedding project.

## Retained messages, will messages, and persistent session behavior

Shutdown must have a clearly documented effect, or explicitly no effect, on
each of the following. None of these currently have documented behavior at
all, since no shutdown path exists today:

- **Retained messages.** These live only in `rumqttd`'s in-memory commit log
  and retained-message state; `rumqttd` has no built-in cross-process
  persistence. Stopping a `Broker` does not, by itself, need to add any
  persistence guarantee it does not already have; the requirement here is
  only that the documentation state plainly that retained messages do not
  survive process exit today, and confirm whether they are expected to
  survive an in-process stop-then-start of a *new* `Broker` within the same
  process (they should not, unless the application explicitly captures and
  restores that state itself, since a new `Broker` has a new, empty
  `Router`).
- **Last Will and Testament (LWT) messages.** For clients still connected at
  the moment shutdown is requested, define whether an intentional, orderly
  broker shutdown fires each connected client's configured Will message (as
  if every connection had failed), or whether Wills are suppressed for an
  orderly shutdown and reserved for genuine, unexpected connection loss.
  These are different events for downstream subscribers -- "the broker is
  restarting" is not the same fact as "this specific client's connection
  failed" -- and firing every configured Will on every graceful restart
  could itself create a burst of Will publishes that surprises subscribers
  who are used to Wills meaning "unexpected client loss." Recommend making
  this an explicit, documented, and ideally configurable choice rather than
  an accidental side effect of whichever code path happens to run first.
- **Persistent (non-clean) sessions for clients not connected at shutdown
  time.** This state lives in the router's in-memory `Graveyard`. A shutdown
  operation must not silently discard this state if the same process is
  expected to resume serving those sessions afterward (for example, a
  config-reload flow that stops listeners, applies new configuration, and
  restarts them against the same router). This should be a documented,
  tested invariant -- either "session state survives shutdown of listeners
  while the router keeps running" or "a full `Broker` stop discards session
  state, and callers who need it preserved must keep the router alive across
  a listener-only restart" -- not an implementation detail nobody has
  verified.

## Error propagation

Errors encountered while shutting down must be collected and returned to the
caller as a typed, aggregated result, not only logged and discarded as the
current code already does at every relevant call site (for example, each
listener's error branch today only calls `error!(error=?e, "Server error - V4")`
and continues, and the final thread-join loop explicitly discards any panic
via `let _ = handle.join();`, per its own comment "join() might panic in case
the thread panics, we just ignore it"). At minimum, the shutdown result
should be able to report, per broker-owned thread or task:

- a clean exit,
- a panic (without letting that panic propagate out of or poison the
  shutdown call itself; one panicking thread must not prevent the rest from
  being collected and reported),
- an I/O error encountered while closing a listener or connection, and
- "still running when the deadline elapsed" (see Deadlines above).

A panic inside any broker-owned thread, or inside any tracked per-connection
task, during shutdown must not hang or poison the overall shutdown operation;
it should be captured (analogous to `std::thread::JoinHandle::join()`
returning `Err`) and included in the aggregated result instead of being
silently swallowed.

## Drop behavior

Confirmed for the pinned commit: dropping every `Broker` value an application
holds today does nothing to the router thread, any listener thread, or any
bound socket, because `Broker` holds only a cheap `Sender` clone and an
`Arc<Config>`; none of its fields have a `Drop` impl that touches the
spawned threads. This is undocumented and, based on the discussion in
issue #771, was surprising enough to be reported as a blocking problem by
more than one embedder before anyone confirmed it was working as
implemented rather than working as intended.

This request asks that Drop behavior be an explicit, documented, and tested
part of the new lifecycle API, along one of these two lines:

- **Cancel-on-drop**: dropping a not-yet-stopped shutdown handle triggers the
  same signal as an explicit immediate abort, on a best-effort, non-blocking
  basis (since `Drop::drop` cannot `.await` or block on a runtime). This
  favors "resources get released even if the embedding application forgets
  to call shutdown explicitly" over "shutdown always requires an explicit
  call."
- **Leak-on-drop, documented**: dropping the handle without an explicit
  shutdown call intentionally leaves the broker running, matching today's
  behavior, but this must be stated plainly in the type's documentation
  (for example, "Dropping `BrokerHandle` does not stop the broker; call
  `shutdown()` or `abort()` explicitly") so embedding applications can rely
  on the documented behavior instead of discovering it empirically the way
  the DLL-unload report in issue #771 had to.

Either choice is acceptable as long as it is deliberate, documented, and
covered by a test; the current, silent "neither documented nor tested" state
is the actual defect being reported here.

## Compatibility

`rumqttd` is pre-1.0 (0.20.0 at the time of this request), and the existing
`rumqttd-shutdown` proof of concept already demonstrates that maintainers
have been willing to change `Broker::start`'s return type to solve this
problem, so a breaking change to gain correct lifecycle behavior should be
acceptable in principle.

An additive-first delivery path (Option C above, or Option A's variant that
keeps `start`'s existing signature) avoids forcing an immediate migration:
existing callers who never stop their broker keep working unchanged, while
new or updated callers opt in to the lifecycle-aware entry point to get
graceful shutdown. Any new internal `Event` variant (for example
`Event::Shutdown`) is additive to a private enum and has no effect on wire
compatibility. No change to the MQTT wire protocol is requested beyond the
already wire-legal, optional use of an MQTT 5 `DISCONNECT` reason code during
graceful drain described above, which does not apply to MQTT 3.1.1 clients at
all.

## Non-goals

- Live hot-reload of TLS material, listener addresses, or `RouterConfig`
  limits while a broker keeps running. Stopping one `Broker` and starting a
  new one with updated `Config` is a sufficient substitute once this request
  is resolved, and is explicitly one of the scenarios this request is meant
  to unblock.
- Broker clustering or multi-node coordinated shutdown; `rumqttd`'s cluster
  support is a separate, largely unimplemented area at the time of this
  writing and is out of scope here.
- Persisting retained messages, sessions, or the commit log across a full
  process restart. This request is about releasing in-process resources
  (threads, sockets) on demand, not about adding durability the library does
  not otherwise have.
- Guaranteeing delivery of any message enqueued after shutdown was
  requested; this request only defines what happens to work already in
  flight when shutdown begins.
- Changing QoS acknowledgement sequencing, PUBACK timing relative to commit
  log writes, or commit-log retention/eviction behavior. These are real,
  separately reported gaps but are independent of the shutdown lifecycle
  and should be tracked as their own issues.
- Prescribing one specific signal-handling library or process-supervisor
  integration (for example, a particular `SIGTERM` handler crate). This
  request only asks for the library-level primitive; wiring it to a
  particular host application's signal handling is the embedder's
  responsibility.

## Tests

Tests should run on both Linux and Windows in CI. `rumqttd` is already pure
Rust and builds on both platforms without the native-toolchain workarounds
some other embedded brokers require, but socket- and thread-teardown timing
can differ meaningfully between the two (for example, `TIME_WAIT` and
`SO_REUSEADDR`-equivalent behavior), so lifecycle correctness should not be
assumed to be platform-independent without running on both.

- Repeated start/stop of a `Broker` on an ephemeral loopback port within a
  single test process. This is the direct regression test for the behavior
  reported in issue #771 ("lack of shutdown causes tests to hang") and
  should assert that a new `Broker` can bind the same port immediately after
  the previous one's shutdown completes.
- Graceful shutdown with zero active connections completes promptly and
  reports full success.
- Graceful shutdown while a QoS 1 exchange is in flight: verify the client
  observes a clean, protocol-defined disconnect (not a connection reset) and
  that the deadline is honored.
- Graceful shutdown whose deadline elapses with a connection still open:
  verify escalation to forced abort and a typed outcome identifying the
  timeout, not a silent "success."
- Idempotency: calling the stop/shutdown operation twice, including
  concurrently from two clones of the same handle, does not panic and
  returns a consistent terminal result both times.
- Calling stop/shutdown from a different thread or task than the one that
  called `start`/`run`.
- Dropping the shutdown handle without calling stop/shutdown explicitly:
  verify whichever documented Drop behavior (cancel-on-drop or
  documented-leak) was chosen.
- After shutdown completes, verify every broker-owned thread has actually
  exited (for example, via a thread-name-based check or an internal test
  hook) and every configured listener's port is free for a new bind, not
  merely that the API call returned.
- A persistent (non-clean) session test verifying the documented behavior
  for session state across shutdown, per
  [Retained messages, will messages, and persistent session behavior](#retained-messages-will-messages-and-persistent-session-behavior).
- A Will-message test verifying the documented behavior (fired or
  suppressed) for still-connected clients during an orderly shutdown.
- An error-propagation test that induces a panic in one tracked
  per-connection task during shutdown and verifies the aggregated shutdown
  result reports it without hanging or panicking the caller.

## Acceptance criteria

- `Broker::start` (or its replacement entry point) returns under normal,
  non-crashing operation once shutdown completes; it does not require an
  unhandled error to return.
- A documented, public API exists to request both immediate abort and
  graceful, deadline-bounded shutdown.
- After shutdown completes: the router thread has exited, every listener
  thread has exited, every listener's bound socket is closed and its port is
  immediately available for a new bind, and every tracked per-connection
  task has finished or been aborted.
- Shutdown can be requested from a thread or task other than the one that
  called `start`.
- Calling the shutdown/stop operation more than once, including
  concurrently, is safe and idempotent.
- Graceful shutdown accepts an explicit deadline; exceeding it produces a
  typed, non-silent outcome and still results in full resource release.
- Documented, tested behavior exists for retained messages, Will messages,
  and persistent-session state across a shutdown.
- Errors and panics encountered during shutdown are aggregated into a typed
  result rather than only logged and discarded.
- Documented, tested Drop behavior exists for the shutdown handle, whichever
  of cancel-on-drop or documented-leak is chosen.
- A repeated start/stop test within a single process, on both Linux and
  Windows, passes without leaking threads or ports.
- Existing callers of today's `Broker::start` who never call the new
  shutdown API observe no behavior change.

## Related work

- <https://github.com/bytebeamio/rumqtt/issues/771> -- the existing tracking
  issue for this gap; this document is intended as a detailed, consolidated
  specification building on that discussion, not a duplicate of it.
- <https://github.com/bytebeamio/rumqtt/compare/main...rumqttd-shutdown> --
  the existing proof-of-concept branch this document builds on and extends.
- <https://github.com/bytebeamio/rumqtt/tree/c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74/rumqttd>
  -- the pinned source this request's technical claims were verified
  against.
