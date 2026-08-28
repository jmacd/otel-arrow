<!--
Upstream-ready issue draft for microsoft/rust-mqtt-client.
Grounded in pinned commit 032c3ee282f425c19f5130d11cb7ad16a7525cfa.
Copy the body below the horizontal rule into a new GitHub issue on
microsoft/rust-mqtt-client. Not intended to be filed against this repository.
-->

# Issue draft: pluggable TLS backend and no-TLS build for ms-mqtt-client

- Target repository: `microsoft/rust-mqtt-client`
- Grounding commit: `032c3ee282f425c19f5130d11cb7ad16a7525cfa`
- Suggested labels: `enhancement`, `breaking-change`, `tls`

---

## Title

Support a pluggable TLS backend (rustls with an ambient `CryptoProvider`) and a
true no-TLS build; stop hard-depending on OpenSSL

## Summary

`ms-mqtt-client` currently depends unconditionally on `openssl` and
`tokio-openssl`, and its public TLS configuration type is an OpenSSL type.
Every consumer of this crate -- including ones that never use TLS, or that
need to standardize on a specific cryptography provider for compliance or
portability reasons -- links OpenSSL and is bound to OpenSSL's API and trust
model. This issue requests:

1. A true no-TLS (plaintext-only) build with zero cryptography dependencies.
2. A `rustls`-backed TLS implementation that works with whatever
   `rustls::crypto::CryptoProvider` the host application has installed as the
   process default, rather than pulling in and building against a specific
   crypto backend itself.
3. Keeping the existing OpenSSL backend available as an opt-in, non-default
   feature for existing consumers who want to keep using it.

## Current behavior (as of `032c3ee2`)

### Cargo.toml unconditionally depends on OpenSSL

`Cargo.toml` declares both OpenSSL crates as plain, non-optional dependencies:

```toml
# >= 0.10.72 to clear known RustSec advisories (verified via `cargo deny check advisories`).
openssl = { version = "0.10.72", default-features = false }
...
tokio-openssl = { version = "0.6", default-features = false }
```

Neither entry sets `optional = true`. **Setting `default-features = false` on
`openssl` does not make the dependency optional** -- it only disables that
crate's own Cargo features (for example, `vendored`); the crate itself is
still compiled and linked into every build of `ms-mqtt-client`, including
builds that never construct a `TlsConfig`. There is no Cargo feature that
gates `openssl` or `tokio-openssl` out of the dependency graph; `[features]`
only defines `websockets`, `__integration`, `__fuzzing`, and `__network`, none
of which touch TLS.

### The public TLS API exposes OpenSSL types directly

`src/transport.rs` defines the only public TLS configuration type as a
newtype over an OpenSSL builder:

```rust
use openssl::{
    pkey::{PKey, Private},
    ssl::{SslConnector, SslConnectorBuilder, SslMethod, SslVersion},
    x509::X509,
};
...
/// Represents the configuration for a TLS connection.
pub struct TlsConfig(pub(crate) SslConnectorBuilder);

impl TlsConfig {
    pub fn new(
        client_cert: Option<(X509, PKey<Private>, Vec<X509>)>,
        ca_trust_bundle: Vec<X509>,
    ) -> io::Result<Self> { ... }

    pub fn from_pem(
        client_cert: Option<(&[u8], &[u8])>,
        ca_trust_bundle: &[u8],
    ) -> io::Result<Self> { ... }
}

impl From<SslConnectorBuilder> for TlsConfig {
    fn from(connector: SslConnectorBuilder) -> Self { ... }
}
```

`TlsConfig::new` takes `openssl::x509::X509` and
`openssl::pkey::PKey<Private>` for certificates and keys, and the `From`
impl accepts an `openssl::ssl::SslConnectorBuilder` directly. Any application
that wants a custom trust store, a hardware-backed key, or a
non-default cipher/version policy must construct these OpenSSL types itself,
which means every consumer of `ConnectionTransportType::Tls`,
`ProxyEndpoint::Https`, and the `wss` `Ws` variant is coupled to OpenSSL's
object model, not just its presence on disk.

### The TLS handshake itself is hard-wired to OpenSSL

`src/io/stream.rs` performs the handshake with `tokio_openssl::SslStream`:

```rust
use tokio_openssl::SslStream;
...
pub(crate) async fn tls_handshake<S>(
    stream: S,
    config: TlsConfig,
    hostname: &str,
) -> io::Result<SslStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let TlsConfig(connector) = config;
    let connector = connector.build().configure()?;
    let ssl = connector.into_ssl(hostname)?;
    ...
}
```

and `src/io/tokio_tls.rs` wraps a `tokio_openssl::SslStream<TransportStream>`
in the crate's internal `ReadableStream`/`WritableStream` traits. There is no
seam at which an alternate TLS implementation could be substituted; OpenSSL
is referenced by concrete type at every layer from the public API down to the
byte stream.

### Documented status

`doc/feature-support.md` records the current state plainly (status symbols
transcribed here as `[supported]`/`[not supported]` for ASCII compatibility):

> `MQTT over TLS` | `[supported]` | `OpenSSL TLS 1.2 or later, with custom CA
> trust and optional client certificates.`

and `doc/feature-support.md` also lists:

> `Application-supplied transport streams` | `[not supported]` | `The public
> API cannot use a caller-provided Tokio` `AsyncRead + AsyncWrite` `stream;
> Unix-domain sockets, QUIC, and other custom transports require library
> changes.`

So today there is exactly one TLS implementation, it is not swappable, and
applications cannot work around that by supplying their own transport.

### Net effect on the dependency graph

Because `openssl`/`tokio-openssl` are non-optional, `cargo tree` for *any*
build of `ms-mqtt-client` -- even one that only ever uses
`ConnectionTransportType::Tcp` -- includes OpenSSL and, transitively, the
`openssl-sys` build-time link to a system or vendored libssl/libcrypto.

## Motivation and use cases

This is a dependency-policy, portability, and integration request, not a
report of an OpenSSL defect:

- **Plaintext-only consumers should not pay for TLS.** Gateways, brokers, and
  test harnesses that only ever connect over `Tcp` (often to a local broker,
  or behind a transport that is already encrypted at a different layer)
  currently still build, link, and ship OpenSSL for no functional benefit.
  This affects binary size, build time (OpenSSL has an FFI/build-script
  surface that complicates cross-compilation, static linking, and
  reproducible builds), and the vulnerability-scanning/SBOM surface of every
  downstream binary.
- **Host applications standardize on a single crypto backend.** Many
  applications embedding this client already select and configure a specific
  `rustls::crypto::CryptoProvider` for the whole process (for example, one
  backed by a platform-native or FIPS-validated cryptographic library) so
  that all TLS connections in the process -- HTTP clients, gRPC channels, and
  MQTT -- share one implementation, one certificate/key handling path, and one
  set of algorithms to audit. An MQTT client that only speaks OpenSSL cannot
  participate in that model: it forces either a second, independently
  configured crypto stack to exist in the same process, or abandoning the
  host application's chosen provider entirely for MQTT traffic.
- **Portability and build-environment constraints.** OpenSSL requires either
  a system install (version and headers must match at build and run time) or
  the `vendored` build (a C compiler and additional build time). Pure-Rust
  TLS avoids both, which matters for cross-compiled, containerized, and
  embedded-adjacent targets.
- **Provider choice should belong to the application, not the library.**
  `rustls` was designed so that libraries depend on `rustls` and its trait
  objects, while the *application* selects and installs the concrete
  `CryptoProvider` (for example `rustls-symcrypt`, `aws-lc-rs`, or `ring`)
  once, ambiently, for the whole process. `ms-mqtt-client` should follow this
  pattern rather than making that choice on the application's behalf.

Concretely, this issue is scoped to support two additional first-class
outcomes that are impossible today:

1. `cargo build` (or `cargo build --no-default-features`) for a plaintext-only
   consumer produces zero OpenSSL crates in `cargo tree`.
2. `cargo build --features tls-rustls` for a consumer that has already called
   `rustls::crypto::CryptoProvider::install_default()` with, for example,
   `rustls-symcrypt`, produces a working TLS connection that uses that
   provider, with zero OpenSSL *and* zero `ring`/`aws-lc-rs` crates in
   `cargo tree`.

## Requested feature topology

Recommended shape (naming is a suggestion; maintainers should pick whatever
is most consistent with existing conventions such as `websockets`):

| Feature | Default | Effect |
| --- | --- | --- |
| *(none)* | -- | `ConnectionTransportType::Tcp` (and `Ws` without a `tls_config`) always available; no TLS support and no crypto dependency at all. |
| `tls-rustls` | off | Enables a `rustls`-backed `TlsConfig`/handshake path. Depends on `rustls` and `tokio-rustls` only; does **not** depend on `rustls`'s `ring` or `aws-lc-rs` default crypto-provider features (see below). Also implies whatever is needed for `ProxyEndpoint::Https` and `wss` to work over rustls. |
| `tls-openssl` | off | The current OpenSSL-backed `TlsConfig`/handshake path, preserved for existing consumers, gated behind an explicit opt-in feature instead of being unconditional. |
| `websockets` | off | Unchanged; `wss` additionally requires one of the `tls-*` features to construct a `TlsConfig`. |

Key requirements on this topology:

- **No default TLS backend.** Neither `tls-rustls` nor `tls-openssl` should be
  a default feature. A consumer that wants TLS must opt into a backend
  explicitly; a consumer that does not enable either gets a plaintext-only
  build with no crypto dependency, not a build that silently fails to
  compile because `TlsConfig` disappeared -- see "Migration and
  compatibility" for how `TlsConfig`/`ConnectionTransportType::Tls` behave
  when no backend is enabled.
- **Mutually additive, not mutually exclusive, at the Cargo level.** Enabling
  both `tls-rustls` and `tls-openssl` in the same build (for example because
  two crates in a workspace disagree, and Cargo unifies features across a
  build -- see "Feature unification") must still produce a working build. It
  is acceptable for the two backends to expose backend-specific `TlsConfig`
  constructors (for example `TlsConfig::from_rustls_client_config` alongside
  `TlsConfig::from_pem` staying OpenSSL-specific, or namespaced types --
  see "Backend-neutral public TLS configuration" below), as long as having
  both compiled in is never a compile error and never silently picks the
  wrong backend for a given `ConnectionTransportType`.
- **`tls-openssl` is not deprecated by this request.** It should remain
  fully supported for consumers already depending on OpenSSL-specific
  behavior (for example a corporate CA store already expressed as `X509`
  values, or FIPS-validated OpenSSL builds already qualified for a
  deployment).

## Zero OpenSSL in the no-TLS and rustls dependency trees

Today, `openssl = { version = "0.10.72", default-features = false }` and
`tokio-openssl = { version = "0.6", default-features = false }` are plain
dependencies. This request requires:

- Both become `optional = true`, gated by `tls-openssl` (directly, or via a
  Cargo feature that both `tls-openssl` and any dependent feature enable
  with `dep:openssl`/`dep:tokio-openssl` syntax, per the Rust 2021+ feature
  resolver).
- Any code path currently reached without an active `TlsConfig` (proxy
  `CONNECT` request formatting in `src/io/stream.rs` currently calls
  `openssl::base64::encode_block` for the `Proxy-Authorization: Basic`
  header -- this must move to a non-OpenSSL base64 encoder, or be
  feature-gated per backend, since it runs even for a plain HTTP proxy with
  no TLS involved at all) must not require OpenSSL.
- Acceptance is a `cargo tree` assertion, not just "the crate compiles": see
  "CI matrix and cargo-tree assertions" below.

## Disabling rustls's default crypto-provider features

`rustls` (and `tokio-rustls`) ship default Cargo features that pull in a
bundled crypto provider -- historically `ring`, and more recently `aws-lc-rs`
is also offered as a default-feature option depending on the `rustls` version
line in use. If `ms-mqtt-client` depends on `rustls` with default features
enabled, it silently reintroduces exactly the "one crypto backend forced on
every consumer" problem this issue is trying to solve, just with a different
backend.

Requirement: `rustls`/`tokio-rustls` must be depended on with
`default-features = false`, enabling only the non-crypto-provider default
features actually required (for example `tls12`, `logging`, `std`, or
whatever the pinned `rustls` version calls its always-needed capability
features), and explicitly *not* enabling `ring` or `aws-lc-rs`. This must be
verified in CI (see below) so a routine `cargo update` cannot silently
re-add a bundled provider through a dependency version bump.

## Ambient `CryptoProvider` semantics

With no bundled provider, `rustls::ClientConfig` construction requires a
`CryptoProvider` to be available. `rustls` supports installing one process-wide
default via `rustls::crypto::CryptoProvider::install_default()`, which any
number of libraries in the process can then pick up with
`CryptoProvider::get_default()` without each needing to be told which
provider to use.

Requested behavior:

- When building a `rustls`-backed `TlsConfig` (or the `rustls::ClientConfig`
  it wraps) without an explicit provider argument, `ms-mqtt-client` must use
  `rustls::crypto::CryptoProvider::get_default()` -- i.e. whatever the *host
  application* installed at process start -- rather than compiling in and
  defaulting to any specific provider itself.
- If the host application has not installed a default provider,
  `ms-mqtt-client` must fail clearly and early rather than panicking deep in
  `rustls` internals or silently doing nothing. The failure should surface
  through the crate's existing error types (see "Error taxonomy") with a
  message that names the actual problem, for example: `"no rustls
  CryptoProvider is installed for this process; call
  CryptoProvider::install_default() (e.g. from rustls-symcrypt, aws-lc-rs, or
  ring) before constructing a TLS transport"`. This should be an explicit,
  documented error variant (or a documented `io::Error` kind if the crate
  keeps its current io::Error-based TLS error surface), not a generic
  "operation failed" string.
- For applications that want to be explicit rather than ambient (multiple
  providers in one process, or an application that prefers not to rely on a
  global default), also accept an explicit
  `Arc<rustls::crypto::CryptoProvider>` at `TlsConfig` construction time as an
  alternative to the ambient path. Ambient-by-default with an explicit
  override is the recommended shape; see "Recommended design" for how this
  interacts with `TlsConfig` construction.

## `rustls-symcrypt` compatibility (Windows and Linux) without pulling `ring`/`aws-lc-rs`

`rustls-symcrypt` is a `CryptoProvider` implementation the host application
installs itself; it should need no special-casing in `ms-mqtt-client` beyond
correctly implementing the ambient-provider semantics above. This issue
specifically calls it out as an acceptance scenario because it is a common
case that is easy to get wrong:

- `ms-mqtt-client`'s own `Cargo.toml` must not depend on `ring` or
  `aws-lc-rs` (directly or via un-disabled `rustls` default features -- see
  above), because either would make the OS-specific
  `symcrypt`-backed provider one of *two or three* crypto backends compiled
  into the process, defeating the purpose of standardizing on it.
- The rustls integration must not assume a specific provider's supported
  cipher suites, key exchange groups, or signature schemes; it must build
  its `rustls::ClientConfig` (or equivalent) using whatever the installed
  `CryptoProvider` reports as supported, not a hard-coded list tuned for
  `ring`.
- Acceptance: a CI job builds and runs the network test suite (see below)
  against Mosquitto with `--features tls-rustls`, with the test process
  installing `rustls-symcrypt`'s default provider before connecting, on both
  a Windows and a Linux runner, asserting the resulting `cargo tree` contains
  neither `ring` nor `aws-lc-rs`.

## Backend-neutral public TLS configuration (or application-supplied transport)

The public `TlsConfig` type currently forces every caller through OpenSSL
types (`X509`, `PKey<Private>`, `SslConnectorBuilder`). With two backends,
the API needs one of:

- **(Recommended) Backend-neutral `TlsConfig` construction**, where the
  common cases -- CA trust bundle, optional client certificate/key, minimum
  TLS version -- are expressed in backend-neutral terms (PEM bytes,
  `Duration`/enum-style version selectors) at the public API surface, with
  backend selection happening either automatically (if only one `tls-*`
  feature is enabled) or via an explicit constructor per backend
  (`TlsConfig::rustls(...)` / `TlsConfig::openssl(...)`, naming open) when
  both are enabled. Escape hatches (`From<rustls::ClientConfig>` and the
  existing `From<SslConnectorBuilder>`) remain available per backend for
  advanced configuration this crate does not anticipate.
- **Or, application-supplied transport/connector**: accept a
  caller-constructed `rustls::ClientConfig` (or a boxed
  `tokio_rustls::TlsConnector`) directly, so applications that already build
  their own rustls configuration elsewhere in the process do not have to
  reconstruct it through a `ms-mqtt-client`-specific builder. This also
  begins to address the currently unsupported "application-supplied
  transport streams" gap noted in `doc/feature-support.md`, though full
  support for an arbitrary caller-provided `AsyncRead + AsyncWrite` is a
  larger change and can remain out of scope (see "Non-goals").

Either approach is acceptable; the recommendation is to do both: a
convenience constructor for the common PEM/CA-bundle case per backend, plus
a `From<rustls::ClientConfig>` (mirroring the existing
`From<SslConnectorBuilder>`) for applications that need full control. Exact
type/method names are left to the maintainers.

## Behavior to preserve across TCP, TLS, WS, and WSS

The new backend(s) must preserve existing observable behavior for every
transport variant in `src/transport.rs`:

- `ConnectionTransportType::Tcp`: unaffected; must remain buildable with
  zero crypto dependencies when no `tls-*` feature is enabled.
- `ConnectionTransportType::Tls`: hostname is used for both SNI and
  certificate SAN matching on the rustls path, matching the documented
  OpenSSL behavior ("The hostname will be matched against the server cert
  SAN.", `src/io/tokio_tls.rs`).
- `ConnectionTransportType::Ws` / `wss`: an optional `tls_config: Option<TlsConfig>`
  continues to select plaintext `ws` vs. encrypted `wss`, and must work
  identically with either backend.
- `ProxyEndpoint::Https`: the TLS session to the proxy (established before
  the `CONNECT` tunnel) must work with either backend, independent of
  whether the *target* connection also uses TLS.
- Minimum TLS version: the OpenSSL path currently pins
  `SslVersion::TLS1_2` as the floor (`connector.set_min_proto_version(Some(SslVersion::TLS1_2))`
  in `src/transport.rs`). The rustls path should offer the same effective
  floor (rustls 0.23+ defaults to TLS 1.2/1.3 only, which already satisfies
  this; document the equivalence explicitly rather than leaving it implicit).
- mTLS: client certificate + private key + certificate chain, as currently
  supported by `TlsConfig::new`/`TlsConfig::from_pem`, must have an
  equivalent on the rustls path (`rustls::sign::CertifiedKey` or an
  equivalent PEM-based constructor).
- CA trust: a custom CA trust bundle (currently `Vec<X509>` /
  PEM-parsed via `from_pem`) must have a rustls equivalent
  (`rustls::RootCertStore` populated from the same PEM bytes). Whether
  "use the OS/webpki default trust store when no bundle is given" is in
  scope is a design decision for the maintainers; the current OpenSSL path
  falls back to OpenSSL's default verify store when `ca_trust_bundle` is
  empty, so parity should be considered.
- ALPN: not currently exposed on `TlsConfig`; if a backend implicitly
  negotiates or requires ALPN identifiers to interoperate with common MQTT
  brokers/proxies, both backends must behave the same way. If ALPN is not
  currently used anywhere in the client, this is a non-goal for this issue
  beyond ensuring no ALPN identifier is presented by one backend and not the
  other.

## Secrets

Private keys are currently accepted as `openssl::pkey::PKey<Private>` or raw
PEM bytes (`TlsConfig::from_pem`'s `pkey: &[u8]` argument). The rustls-backed
API should:

- Accept private key material in a form that does not require the caller to
  link OpenSSL just to construct it (raw PEM/DER bytes, or
  `rustls_pki_types::PrivateKeyDer`/`rustls::sign::CertifiedKey` types are
  reasonable choices).
- Not log, `Debug`-print, or otherwise expose key material; this should
  already be true given `PKey<Private>` and PEM byte slices do not implement
  `Debug` output of key contents, and the new types must preserve that.
- Continue to leave key storage/zeroization policy (for example on drop) as
  an explicit non-goal unless the maintainers want to adopt it uniformly
  across both backends -- call this out in the PR description either way so
  reviewers know it was considered.

## Error taxonomy

TLS failures currently surface as `std::io::Error` (via `?` inside
`TlsConfig::new`/`from_pem`, and via `ConnectError::Io` wrapping whatever
`tls_handshake` returns in `src/error.rs`). This request asks that the new
backend keep the same *shape* of error surface applications already handle
(`io::Error` reaching `ConnectError::Io`) while adding enough detail to
distinguish the new failure modes introduced by pluggable backends,
specifically:

- Missing ambient `CryptoProvider` (see above) -- must be distinguishable
  from a generic handshake/certificate failure, either via a distinct
  `io::ErrorKind`/wrapped source error, or (preferred, if the maintainers are
  open to a small `ConnectError`/`TlsError` addition) a dedicated error
  variant. Silently mapping this to the same error as "server certificate
  invalid" would make the two failure modes impossible for an application
  to tell apart and handle differently (one is a startup/configuration bug,
  the other is a runtime trust decision).
- Certificate/trust errors, TLS version negotiation failures, and SNI/SAN
  mismatches should remain distinguishable from each other with the same
  granularity the OpenSSL path currently offers (i.e. do not regress by
  collapsing everything into one opaque "TLS failed" error when adding the
  second backend).
- Both backends' errors should be presented to applications in a
  backend-agnostic way where practical: an application that switches from
  `tls-openssl` to `tls-rustls` should not have to change its error-handling
  `match` arms to keep working, beyond genuinely backend-specific detail
  (for example a specific OpenSSL error code) that was never portable in
  the first place.

## Feature unification and `--all-features`

Cargo unifies features across a dependency graph: if any crate in a build
enables `tls-openssl` and another enables `tls-rustls` (including via `dev-dependencies`,
or via this crate's own `--all-features` CI runs), both must end up compiled
in without conflict, per the "mutually additive" requirement above. In
particular:

- `cargo build --all-features` (and equivalent CI jobs; `pr.yaml`'s `check`
  job runs `make check`, which should be checked/updated to exercise this
  combination) must succeed with both `tls-rustls` and `tls-openssl` enabled
  simultaneously.
- `cargo tree --no-default-features` (all TLS features off) must show no
  `openssl`, `tokio-openssl`, `rustls`, or `tokio-rustls` entries at all.
- Enabling only `tls-rustls` must show no `openssl`/`tokio-openssl`, and
  must show no `ring`/`aws-lc-rs` (see above).
- Enabling only `tls-openssl` must show no `rustls`/`tokio-rustls`.

## MSRV

The crate currently pins `rust-version = "1.88"` (`Cargo.toml`) with
`edition = "2024"`, verified in CI across `rust: ["1.88", "stable"]` in
`.github/workflows/pr.yaml`'s `test` job. Whichever `rustls`/`tokio-rustls`
version is selected must build on 1.88, or the MSRV bump must be called out
explicitly in the PR (with the corresponding update to `pr.yaml`'s matrix and
this file's `rust-version`) rather than discovered by CI failure.

## Migration and compatibility

This is a breaking change to the public API (`TlsConfig` stops being
constructible from bare OpenSSL types once `tls-openssl` is not the only
option, and becomes unavailable at all with no `tls-*` feature enabled).
Recommended migration path:

- Existing consumers who do nothing see their build fail to compile once
  `openssl`/`tokio-openssl` move behind `tls-openssl` and it defaults off
  -- this must be called out prominently in the changelog/release notes as
  a required action: enable `tls-openssl` (or migrate to `tls-rustls`) to
  keep using `ConnectionTransportType::Tls`/`ProxyEndpoint::Https`.
- Provide a migration note mapping the old constructors to the new ones
  (`TlsConfig::from_pem` under `tls-openssl` keeps working unchanged if that
  feature is enabled; the rustls equivalent gets its own constructor name).
- Consider (maintainers' call) whether a transitional release keeps
  `tls-openssl` as a default feature for one minor version with a deprecation
  warning before flipping the default off, versus flipping it off
  immediately with a major/minor version bump and a clear changelog entry.
  Either is acceptable; the important part is that the "no default backend"
  end state is reached and documented.

## CI matrix and `cargo-tree` assertions

Extend the existing gate (`.github/workflows/pr.yaml`) rather than only
relying on manual review:

- Add explicit `cargo tree` (or `cargo tree -e features` /
  `cargo metadata`-based) assertions as a CI step, asserting:
  - No `openssl`/`tokio-openssl` in the tree for `--no-default-features` and
    for `--no-default-features --features tls-rustls,websockets`.
  - No `ring`/`aws-lc-rs` in the tree for
    `--no-default-features --features tls-rustls,websockets`.
  - No `rustls`/`tokio-rustls` in the tree for
    `--no-default-features --features tls-openssl,websockets`.
- Extend the `check` job (or add a sibling job) to build
  `--all-features` and each `tls-*` feature in isolation, alongside the
  existing no-feature and `websockets`-only builds.
- Extend `network-mosquitto` (and the scheduled `network.yaml` broker
  matrix) to run the network suite over TLS for each backend: this requires
  a Mosquitto fixture that terminates TLS (Mosquitto supports this via its
  `cert_file`/`key_file`/`cafile` listener options) in addition to the
  existing plaintext listener used today, with the test client configured
  for `tls-openssl` in one leg and `tls-rustls` in another. Add a Windows
  runner leg (or a targeted job) that additionally installs
  `rustls-symcrypt`'s provider for the `tls-rustls` + Mosquitto-TLS
  combination, per the acceptance scenario above.

## Unit and network tests

- Unit tests for `TlsConfig` construction per backend (valid/invalid PEM,
  missing provider on the rustls path, mismatched cert/key pairs) alongside
  the existing test module conventions (`src/io/test.rs`,
  `#[cfg(feature = "__integration")]`).
- Network tests: extend `tests/network/` (currently exercised via the
  `__network` feature and `cargo test --features __network,websockets --test
  network` in both `pr.yaml`'s `network-mosquitto` job and `network.yaml`)
  with a TLS-enabled Mosquitto fixture and a test case per backend that
  connects, completes the MQTT CONNECT/CONNACK exchange, and disconnects
  over TLS, plus a negative test asserting the documented error when no
  `CryptoProvider` is installed and `tls-rustls` is exercised directly (not
  through the crate's own test harness, which may install one).
- Every new test must follow this repository's test documentation
  convention: a doc comment immediately above the test stating the Scenario
  under test and the Guarantee it protects.

## Documentation

- `doc/feature-support.md`'s "MQTT over TLS" row must be updated to describe
  both backends and the no-TLS build, replacing the current
  "OpenSSL TLS 1.2 or later" wording.
- `src/transport.rs`'s module and type docs (including the doctests already
  present, e.g. the `TlsConfig::from_pem` example) must be updated per
  backend, and any doctest gated behind a `tls-*` feature must have its
  `cfg` attribute added so `cargo test --doc` does not fail when a feature is
  disabled.
- `README.md` and `src/lib.rs`'s crate-level docs should mention the
  available TLS features alongside the existing `websockets` feature
  callout.
- Add a short design note (for example under `doc/design/`) explaining the
  ambient-`CryptoProvider` model for applications unfamiliar with it, since
  it is easy to misuse (installing a provider after first use, or installing
  two competing providers).

## Security

- No behavior change is requested for certificate validation semantics
  beyond what each backend already provides; this issue is about which
  backend is compiled in and how its cryptography is supplied, not about
  loosening verification.
- The "missing ambient provider" failure must be a hard error, never a
  silent fallback to an unintended default provider, and never a silent
  skip of TLS.
- `cargo deny`/`cargo machete` (already run in the `check` job per
  `.github/workflows/pr.yaml`) must pass for both new feature combinations,
  and `deny.toml` should be reviewed for whether it needs backend-specific
  advisory allowances.

## Acceptance criteria

- [ ] `openssl` and `tokio-openssl` are `optional = true`, reachable only via
      `tls-openssl`.
- [ ] A new `tls-rustls` feature adds `rustls`/`tokio-rustls` with
      `default-features = false` and no bundled crypto provider enabled.
- [ ] `cargo tree --no-default-features` contains no TLS-crypto crates at all.
- [ ] `cargo tree --no-default-features --features tls-rustls` contains no
      `openssl`, `tokio-openssl`, `ring`, or `aws-lc-rs`.
- [ ] `cargo tree --no-default-features --features tls-openssl` contains no
      `rustls` or `tokio-rustls`.
- [ ] A rustls `TlsConfig` builds and connects successfully against a TLS
      listener when the host process has installed `rustls-symcrypt` as the
      default `CryptoProvider`, on both Windows and Linux, verified in CI.
- [ ] Constructing a rustls `TlsConfig`/connection with no installed
      `CryptoProvider` fails with a clear, distinguishable error rather than
      panicking or hanging.
- [ ] `TCP`, `TLS`, `WS`, and `WSS` transports, plus HTTP/HTTPS `CONNECT`
      proxying, all continue to work with both TLS backends.
- [ ] Both `tls-rustls` and `tls-openssl` enabled together builds and passes
      tests (feature-unification safety).
- [ ] `doc/feature-support.md`, `src/transport.rs` docs, and `README.md` are
      updated.
- [ ] A migration note is published for existing consumers of the current
      OpenSSL-only `TlsConfig` API.
- [ ] CI includes explicit `cargo tree` assertions per the matrix above, not
      just successful compilation.
- [ ] Network tests exercise TLS against Mosquitto for both backends.

## Non-goals

- Removing OpenSSL support entirely. `tls-openssl` remains supported.
- Supporting an arbitrary caller-provided `AsyncRead + AsyncWrite` transport
  for non-TLS use cases (Unix sockets, QUIC); that is the separate,
  already-documented "Application-supplied transport streams" gap in
  `doc/feature-support.md` and can be addressed independently.
- Changing certificate verification policy, trust defaults, or supported
  TLS versions beyond what is needed for rough parity between backends.
- SOCKS5 proxy support, which is already a separate, tracked gap.
- Selecting or shipping a specific `CryptoProvider` on the crate's behalf;
  provider selection stays an application decision.

## Recommended design (summary)

1. Add `tls-rustls` and `tls-openssl` Cargo features; make `openssl` and
   `tokio-openssl` optional under `tls-openssl`; add `rustls`/`tokio-rustls`
   (default features off, no bundled crypto provider) as optional
   dependencies under `tls-rustls`. Neither feature is a default feature.
2. Introduce a small internal TLS-backend abstraction (a trait or an enum
   over per-backend connector types) inside `src/io/`, so `src/io/stream.rs`
   no longer names `tokio_openssl::SslStream` directly; add a
   `src/io/tokio_rustls.rs` module mirroring the structure of the existing
   `src/io/tokio_tls.rs`.
3. Keep `TlsConfig` as the single public type, but give it backend-specific
   constructors (`TlsConfig::from_pem` staying under `tls-openssl` as today,
   plus new rustls-based constructors under `tls-rustls`) and
   backend-specific `From` impls (keep `From<SslConnectorBuilder>`; add
   `From<rustls::ClientConfig>` or `From<Arc<rustls::ClientConfig>>`).
   Exact naming is left to the maintainers.
4. On the rustls path, build `rustls::ClientConfig` using
   `rustls::crypto::CryptoProvider::get_default()` by default, with an
   optional explicit-provider constructor for applications that prefer not
   to rely on process-ambient state; return a clear, distinguishable error
   when no provider is available.
5. Update CI (`pr.yaml`, `network.yaml`) with the feature matrix, `cargo
   tree` assertions, and a TLS-enabled Mosquitto fixture, per "CI matrix and
   cargo-tree assertions" above.
