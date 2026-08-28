# Support an ambient rustls `CryptoProvider` (and a true no-provider build) in the TLS listener

## Status

Draft, upstream-ready. Written against `rumqttd` 0.20.0, pinned commit
[`c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74`](https://github.com/bytebeamio/rumqtt/tree/c03ba8bbb785dc6cd7809ce14fc2845d14b6bb74).
Every claim about current behavior below was verified by reading that exact
commit's source (`rumqttd/Cargo.toml`, `rumqttd/src/server/tls.rs`,
`rumqttd/src/lib.rs`) and the corresponding `rustls` 0.22.4 source, with file
references, not inferred from documentation or changelogs.

## Summary

`rumqttd`'s rustls listener path (`use-rustls` feature) does not let an
embedding application supply its own `rustls::crypto::CryptoProvider`. It
constructs every `rustls::ServerConfig` with `ServerConfig::builder()`, which
in `rustls` 0.22 is only defined `#[cfg(feature = "ring")]` and always
instantiates `crate::crypto::ring::default_provider()` internally -- it does
not consult, and cannot be redirected to, a process-wide default provider
installed by the embedding application via
`rustls::crypto::CryptoProvider::install_default()`. Combined with
`tokio-rustls` being depended on with default features (pulling in `rustls`'s
default `ring` feature), every build of `rumqttd` with `use-rustls` enabled
compiles in and links `ring`, whether or not the embedding application wants
`ring`, and there is no supported way to make it use a different provider,
such as `aws-lc-rs` or a platform-native backend like `rustls-symcrypt`.

This is a dependency-policy and integration request, not a report of a
`ring`/`rustls` defect. It asks for:

1. An upgrade from `rustls` 0.22 (via `tokio-rustls` 0.25) to a current
   `rustls` 0.23.x line (via `tokio-rustls` 0.26+), depended on with
   `default-features = false` and no built-in crypto-provider feature
   (`ring` or `aws-lc-rs`) enabled by `rumqttd` itself.
2. Building `rustls::ServerConfig` with
   `ServerConfig::builder_with_provider(...)`, sourcing the provider from
   `rustls::crypto::CryptoProvider::get_default()` (the ambient, process-wide
   provider the embedding application installed) by default, with a clear,
   typed error -- not a panic or an opaque handshake failure -- when no
   provider has been installed.
3. Preserving an explicit, verified plaintext-only build
   (`--no-default-features`, no `use-rustls`/`use-native-tls`) that pulls in
   zero TLS/crypto crates, alongside the existing `use-native-tls` backend
   for consumers that want to keep it.
4. Verifying, in CI, that a build using the ambient-provider path with
   `rustls-symcrypt` installed as the process default produces zero `ring`
   and zero `aws-lc-rs` in `cargo tree`, on both Windows and Linux.

## Motivation

Applications that embed `rumqttd` as one component among several commonly
already standardize on a single `rustls::crypto::CryptoProvider` for the
whole process -- for example a platform-native or FIPS-validated backend --
so that every TLS consumer in the process (HTTP clients, gRPC channels, an
embedded MQTT broker) shares one implementation, one certificate/key
handling path, and one set of algorithms to audit and keep patched. `rustls`
is explicitly designed to support this: libraries depend on `rustls`'s
traits and consult `CryptoProvider::get_default()`, while the *application*
selects and installs the concrete provider once. A broker library that
instead always compiles in and instantiates its own `ring::default_provider()`
cannot participate in that model. Concretely, this makes it impossible for
an embedding application to:

- Run an embedded `rumqttd` TLS listener with `rustls-symcrypt` (or
  `aws-lc-rs`, or any other `CryptoProvider`) as the sole crypto backend for
  the whole process; `rumqttd`'s own `ring` dependency defeats that goal
  regardless of what the application installs.
- Build with `default-features = false` and get a build that has zero crypto
  dependencies, because the `ring` dependency arrives transitively through
  `tokio-rustls`'s default features whenever `use-rustls` (itself a default
  feature) is enabled, not through an explicit, disableable Cargo feature of
  `rumqttd`'s own.
- Rely on Cargo's workspace-wide feature unification behaving predictably: a
  workspace that also depends on `rustls` 0.23 with, say, `aws-lc-rs`
  elsewhere ends up with *both* `ring` and `aws-lc-rs` compiled in for the
  shared `rustls-webpki` dependency (see "Feature unification" below for a
  verified example from this repository's own lockfile), simply because two
  different crates picked different bundled providers and neither could opt
  out.

None of this requires a defect in `ring` or `rustls` to be a real blocker: it
is a direct consequence of `rumqttd` making a provider choice that belongs to
the embedding application.

## Current behavior (as of `c03ba8bbb7`)

### `Cargo.toml`: `tokio-rustls` is depended on with default features, and `rustls` is never depended on directly

`rumqttd/Cargo.toml` declares:

```toml
tokio-rustls = { version = "0.25.0", optional = true }
rustls-webpki = { version = "0.102.2", optional = true }
rustls-pemfile = { version = "2.1.0", optional = true }
...
[features]
default = ["use-rustls", "websocket"]
use-rustls = ["dep:tokio-rustls", "dep:rustls-webpki", "dep:rustls-pemfile", "dep:x509-parser"]
```

`rumqttd` never depends on `rustls` directly; it only reaches `rustls` types
through `tokio_rustls::rustls::...` re-exports. Because `tokio-rustls` is
declared with no `default-features = false`, and `use-rustls` is a *default*
Cargo feature, a plain `cargo build` (no flags at all) pulls in whatever
crypto provider `rustls` 0.22 defaults to. The repository's own
`Cargo.lock` at this commit confirms the result: it resolves `rustls 0.22.4`
with `ring` listed as one of its direct dependencies, and `ring 0.17.8`
itself present in the dependency graph -- i.e. the default build already
links `ring`, with no feature flag an application can pass to `rumqttd` to
avoid it while still getting TLS.

### `ServerConfig::builder()` hardcodes `ring`'s provider; it does not consult an ambient default

`rumqttd/src/server/tls.rs`, in `TLSAcceptor::rustls`, builds the server
config like this:

```rust
let builder = ServerConfig::builder();

#[cfg(feature = "verify-client-cert")]
let builder = {
    ...
    let verifier = WebPkiClientVerifier::builder(Arc::new(store))
        .build()
        .unwrap();
    builder.with_client_cert_verifier(verifier)
};

#[cfg(not(feature = "verify-client-cert"))]
let builder = builder.with_no_client_auth();

let server_config = builder.with_single_cert(certs, key)?;

let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
```

`rustls` 0.22.4's own source (`rustls/src/server/server_conn.rs`) shows
exactly what `ServerConfig::builder()` does, and why it cannot honor an
ambient provider:

```rust
impl ServerConfig {
    /// Create a builder for a server configuration with the default
    /// [`CryptoProvider`]: [`crypto::ring::default_provider`] and safe ciphersuite and protocol
    /// defaults.
    #[cfg(feature = "ring")]
    pub fn builder() -> ConfigBuilder<Self, WantsVerifier> {
        // Safety: we know the *ring* provider's ciphersuites are compatible with the safe default protocol versions.
        Self::builder_with_provider(crate::crypto::ring::default_provider().into())
            .with_safe_default_protocol_versions()
            .unwrap()
    }

    /// Create a builder for a server configuration with a specific [`CryptoProvider`].
    pub fn builder_with_provider(
        provider: Arc<CryptoProvider>,
    ) -> ConfigBuilder<Self, WantsVersions> {
        ConfigBuilder {
            state: WantsVersions { provider },
            side: PhantomData,
        }
    }
}
```

Two facts follow directly from this, both independent of anything `rumqttd`
itself does wrong -- they are consequences of which entry point `rumqttd`
calls:

- `ServerConfig::builder()` only exists at all when `rustls`'s `ring`
  Cargo feature is enabled, and it unconditionally instantiates
  `crate::crypto::ring::default_provider()` inline. It never calls
  `rustls::crypto::CryptoProvider::get_default()`, so an application that has
  already called `CryptoProvider::install_default()` with a different
  provider (`aws-lc-rs`, `rustls-symcrypt`, or anything else) has that choice
  silently ignored by `rumqttd`'s listener; the listener uses `ring`
  regardless.
- `rustls` already ships the feature-independent, provider-agnostic entry
  point this request asks `rumqttd` to use instead --
  `ServerConfig::builder_with_provider(provider)` -- which takes an
  `Arc<CryptoProvider>` explicitly and does not require the `ring` feature at
  all. `rumqttd` does not call it anywhere in `src/server/tls.rs`.

### The public `TlsConfig` is file-path-only and has no provider or version/suite knobs

`rumqttd/src/lib.rs` defines the broker's TLS configuration purely in terms
of file paths:

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TlsConfig {
    Rustls {
        capath: Option<String>,
        certpath: String,
        keypath: String,
    },
    NativeTls {
        pkcs12path: String,
        pkcs12pass: String,
    },
}
```

and `ServerSettings { tls: Option<TlsConfig>, ... }` is the only way a
listener gets TLS material, matching the config file shape documented in
`rumqttd/README.md` (`[v4.2.tls]` with `certpath`/`keypath`/`capath`). There
is no field, constructor, or builder method anywhere on `TlsConfig`,
`ServerSettings`, `Config`, or `Broker` through which an embedding
application can pass an `Arc<rustls::crypto::CryptoProvider>`, a
pre-built `rustls::ServerConfig`, or a non-default protocol-version/
ciphersuite selection. The only "provider" in play is whatever `ring`'s
compiled-in default happens to allow.

### mTLS (`verify-client-cert`) is built the same way, so it inherits the same limitation

`TLSAcceptor::rustls` builds the client-cert-verifying path with
`WebPkiClientVerifier::builder(Arc::new(store)).build().unwrap()` on top of
the same `ServerConfig::builder()` call described above. `WebPkiClientVerifier`
itself is provider-agnostic in `rustls` 0.22 (it takes its provider from the
`ConfigBuilder` state), so mTLS is not an additional source of hardcoding
beyond the `ServerConfig::builder()` call already covered -- but any fix must
preserve this call path working with an arbitrary provider, not just `ring`.

## Feature unification: this repository's own lockfile already shows the failure mode

`Cargo.lock` at the root of this repository, at the pinned commit, is a
concrete, already-realized example of why "just enable a feature" is not
sufficient and why an application-supplied ambient provider matters. The
workspace resolves two `rustls` major versions side by side --
`rustls 0.22.4` (used by `rumqttd` via `tokio-rustls 0.25`, with its `ring`
feature active) and `rustls 0.23.17` (used by `rumqttc` via
`tokio-rustls 0.26`, with `aws-lc-rs` active) -- and because both versions
depend on the *same* `rustls-webpki 0.102.x`, Cargo unifies that single
crate's features across the whole workspace. The result, visible directly in
the lockfile, is one resolved `rustls-webpki 0.102.8` with **both**
`aws-lc-rs` and `ring` as dependencies simultaneously:

```text
[[package]]
name = "rustls-webpki"
version = "0.102.8"
dependencies = [
 "aws-lc-rs",
 "ring",
 "rustls-pki-types",
 "untrusted",
]
```

Neither `rumqttd` nor `rumqttc` asked for two crypto backends; Cargo's
feature unification produced that outcome because each crate's `rustls`
dependency independently pulled in its own hardcoded default. This is
exactly the failure mode this issue asks to close for `rumqttd`'s side of
that equation: once `rumqttd` depends on `rustls` with
`default-features = false` and no bundled provider feature of its own, its
share of a resolved `rustls`/`rustls-webpki` no longer forces a provider
choice onto the rest of the workspace, and an application standardizing on
one provider (for example `rustls-symcrypt`) can actually achieve a
dependency graph with none of `ring`, `aws-lc-rs`, or OpenSSL in it.

## Prior art: the same pattern already ships in a sibling rumqtt project

The requested pattern is not hypothetical; it is already implemented and
published on crates.io in `rumqttc-core-next` 0.34.0 (tag
`rumqttc-core-next-0.34.0`, commit `8861f73b6a047bc6cdc1c6efb4b9b86b87d720b6`
in [`thehouseisonfire/rumqtt`](https://github.com/thehouseisonfire/rumqtt), a
fork of this repository that publishes shared transport code for its
`rumqttc-*-next` client crates). It is cited here as evidence the shape is
practical and already working elsewhere in the same codebase family, on the
*client* side (`rustls::ClientConfig`) -- not as a required migration target,
since it is an independent, unaffiliated fork, not part of `bytebeamio/rumqtt`.

Its `rumqttc-core/Cargo.toml` depends on `rustls`'s transport crate with
`default-features = false`, exactly as this issue requests:

```toml
tokio-rustls = { version = "0.26.0", optional = true, default-features = false }
```

and its Cargo features separate "TLS support, no bundled provider" from
"TLS support, with an explicit opt-in bundled provider":

```toml
use-rustls = ["use-rustls-aws-lc"]
use-rustls-no-provider = ["dep:tokio-rustls", "dep:rustls-webpki", "dep:rustls-pki-types", "dep:rustls-native-certs", "async-tungstenite?/tokio-rustls-native-certs"]
use-rustls-ring = ["use-rustls-no-provider", "tokio-rustls/ring", "rustls-webpki/ring"]
use-rustls-aws-lc = ["use-rustls-no-provider", "tokio-rustls/aws_lc_rs", "rustls-webpki/aws-lc-rs"]
```

`use-rustls-no-provider` alone enables the rustls-backed transport and pulls
in none of `tokio-rustls/ring`, `tokio-rustls/aws_lc_rs`,
`rustls-webpki/ring`, or `rustls-webpki/aws-lc-rs`; only the separate,
mutually-additive `use-rustls-ring`/`use-rustls-aws-lc` features turn a
bundled provider on, so a consumer building with just
`use-rustls-no-provider` gets a clean dependency graph with respect to those
two backends. `rumqttc-core/src/tls.rs` then resolves the provider ambiently
first, falling back to a bundled provider only when the caller opted into
exactly one of those features, and failing with a typed error otherwise:

```rust
fn rustls_crypto_provider() -> Result<Arc<CryptoProvider>, Error> {
    if let Some(provider) = CryptoProvider::get_default() {
        return Ok(Arc::clone(provider));
    }

    let provider: Option<CryptoProvider> = {
        #[cfg(all(feature = "use-rustls-ring", not(feature = "use-rustls-aws-lc")))]
        {
            Some(rustls::crypto::ring::default_provider())
        }

        #[cfg(all(feature = "use-rustls-aws-lc", not(feature = "use-rustls-ring")))]
        {
            Some(rustls::crypto::aws_lc_rs::default_provider())
        }

        #[cfg(not(any(
            all(feature = "use-rustls-ring", not(feature = "use-rustls-aws-lc")),
            all(feature = "use-rustls-aws-lc", not(feature = "use-rustls-ring"))
        )))]
        {
            None
        }
    };

    provider
        .map(Arc::new)
        .ok_or(Error::CryptoProviderUnavailable)
}

pub fn rustls_client_config_builder() -> Result<RustlsClientConfigBuilder, Error> {
    Ok(
        ClientConfig::builder_with_provider(rustls_crypto_provider()?)
            .with_safe_default_protocol_versions()?,
    )
}
```

This is the client-side sibling of exactly the entry point this issue asks
`rumqttd` to adopt server-side --
`ServerConfig::builder_with_provider(...)` in place of `ServerConfig::builder()`
-- and its `CryptoProviderUnavailable` variant is the same kind of explicit,
typed "no provider installed" error requested in "Error taxonomy" above,
rather than a panic or a generic failure.

## Requested changes

### 1. Upgrade to a current `rustls`/`tokio-rustls` line, depended on with `default-features = false`

Move from `tokio-rustls = "0.25.0"` (`rustls` 0.22) to `tokio-rustls "0.26"`
or later (`rustls` 0.23.x or later; `rustls` 0.23.43 is the current stable
release at the time of writing). Depend on both with
`default-features = false`, enabling only the always-needed, non-provider
capability features (for example `std`, `tls12`, `logging` -- whichever the
adopted `rustls` version calls its baseline features), and explicitly *not*
`ring` or `aws-lc-rs`. Add `rustls` itself as a direct dependency of
`rumqttd` (today it is only reached transitively through `tokio-rustls`),
so the `default-features = false` requirement is unambiguous and does not
depend on `tokio-rustls`'s own default-feature choices in a future release.

### 2. Build `ServerConfig` via `builder_with_provider`, sourcing the provider ambiently

Replace the `ServerConfig::builder()` call in `TLSAcceptor::rustls` with:

```rust
let provider = rustls::crypto::CryptoProvider::get_default()
    .ok_or(Error::NoCryptoProvider)?
    .clone();
let builder = ServerConfig::builder_with_provider(provider)
    .with_safe_default_protocol_versions()?;
```

(or the equivalent for the adopted `rustls` version's API). This works with
`default-features = false` and any `CryptoProvider` the embedding application
has installed via `CryptoProvider::install_default()` -- `ring`,
`aws-lc-rs`, `rustls-symcrypt`, or any other conformant implementation --
without `rumqttd` needing to know which one at compile time.

### 3. Preserve an explicit plaintext build with `default-features = false`

`default = ["use-rustls", "websocket"]` already means `cargo build
--no-default-features` disables `use-rustls`; this request does not change
that shape, only what happens *when* `use-rustls` is enabled. Acceptance for
this half is a `cargo tree` assertion (see below) proving that
`--no-default-features` (with or without re-adding `websocket` alone)
produces zero `rustls`/`tokio-rustls`/`ring`/`aws-lc-rs`/`native-tls` crates,
so this path keeps working exactly as it does today and is explicitly
covered by CI rather than only exercised incidentally.

### 4. Keep `use-native-tls` available, unchanged

This request is scoped to the `use-rustls` path. `use-native-tls` should
remain available as-is for consumers who want it; nothing here proposes
removing or restructuring it.

## Feature topology

| Feature | Default | Requested effect |
| --- | --- | --- |
| *(none)* | -- | No TLS, zero crypto dependency. Unchanged from today's `--no-default-features` behavior; add explicit CI coverage (see below). |
| `use-rustls` | on | Enables the `rustls`-backed `TLSAcceptor::Rustls` path. Requested change: depends on `rustls`/`tokio-rustls` with `default-features = false` and **no** bundled provider feature (`ring`, `aws-lc-rs`) enabled by `rumqttd`. Requires the embedding application to have installed a `CryptoProvider` before accepting a TLS connection; see "Provider ownership" below. |
| `use-native-tls` | off | Unchanged. |
| `verify-client-cert` | off | Unchanged; must continue to work with `builder_with_provider` exactly as it does with `builder()` today, since `WebPkiClientVerifier` takes its provider from the same `ConfigBuilder` state. |
| `validate-tenant-prefix` | off | Unchanged. |

Key requirement: no Cargo feature of `rumqttd`'s own should re-introduce a
bundled crypto provider as a default. If maintainers want to offer a
convenience "batteries-included" mode for consumers who do not want to call
`CryptoProvider::install_default()` themselves, it should be an explicit,
non-default opt-in feature (for example `use-rustls-ring`) layered on top of
`use-rustls`, not folded into `use-rustls` itself.

## Provider ownership: ambient by default, explicit override available

Requested semantics, matching how `rustls` itself is designed to be used:

- By default, `TLSAcceptor::rustls` (and any future async
  certificate-resolution path) should source its `CryptoProvider` from
  `rustls::crypto::CryptoProvider::get_default()` -- whatever the embedding
  application installed once, process-wide, at startup.
- If no default provider has been installed when a TLS listener is
  constructed, fail clearly and early with a typed error (see "Error
  taxonomy" below), not a panic inside `rustls` and not a generic I/O or
  handshake failure surfaced only when the first client connects.
- For applications that prefer not to rely on a process-wide global (for
  example, hosting more than one provider in the same process for different
  listeners), also accept an explicit `Arc<rustls::crypto::CryptoProvider>`
  as an optional parameter on `TLSAcceptor::new`/`TLSAcceptor::rustls` (or
  equivalent), as an alternative to the ambient path. Ambient-by-default with
  an explicit override matches the shape `rustls` itself recommends via
  `builder()` (implicit) versus `builder_with_provider()` (explicit).

## TLS/mTLS and certificate verification semantics to preserve

The rewrite must not change observable listener behavior other than
provider selection:

- Server certificate and key loading (`rustls_pemfile::certs`,
  `first_private_key_in_pemfile` supporting `Sec1Key`/`Pkcs1Key`/`Pkcs8Key`)
  stays as-is; these are provider-independent parsing steps.
- `verify-client-cert`'s `WebPkiClientVerifier::builder(...).build()` path,
  and the existing tenant-id extraction from the client certificate's
  subject organization field (`extract_tenant_id`), must continue to work
  unchanged, since neither depends on which `CryptoProvider` is installed.
- The chosen protocol versions and cipher suites must come from whatever the
  installed `CryptoProvider` reports as supported (via
  `with_safe_default_protocol_versions()` or an explicit
  `with_protocol_versions(...)` call against the provider's own suite list),
  never a list hardcoded against `ring`'s specific suites. A provider such as
  `rustls-symcrypt` that supports a different (even if overlapping) suite set
  than `ring` must work without `rumqttd` special-casing it.

## Error taxonomy

Add an explicit, documented variant to `rumqttd::server::tls::Error` (the
enum already used for every other TLS setup failure in this file), for
example:

```rust
#[error("no rustls CryptoProvider is installed for this process; call \
         CryptoProvider::install_default() before starting a TLS listener")]
NoCryptoProvider,
```

This should be returned from `TLSAcceptor::new`/`TLSAcceptor::rustls` at
listener-construction time (i.e. at `Broker::new`/`Broker::start`, before any
socket is even opened), not discovered lazily on first connection attempt.
This mirrors the existing convention in the same enum, where
`RustlsNotEnabled` and `NativeTlsNotEnabled` already exist as explicit,
named variants for "the requested TLS backend isn't compiled in" rather than
a generic string error.

## Feature unification and `cargo tree`/CI assertions

Because feature unification across a workspace is exactly how the current
problem manifests (see the verified `rustls-webpki` example above), CI
acceptance for this request should assert dependency-graph shape, not only
"the crate compiles":

- A job that builds `rumqttd` with `--no-default-features` (optionally
  `--features websocket`) and asserts, via `cargo tree`, that none of
  `rustls`, `tokio-rustls`, `ring`, `aws-lc-rs`, or `native-tls` appear.
- A job that builds `rumqttd` with `--features use-rustls` (no crypto
  feature of `rumqttd`'s own) and asserts that `ring` and `aws-lc-rs` are
  **absent** from `cargo tree` for the `rumqttd` crate's own dependency
  edges (they may still appear elsewhere in a larger workspace through
  unrelated crates, but `rumqttd` itself must not be the thing pulling them
  in).
- A job that builds and runs the existing TLS integration/network tests with
  `--features use-rustls`, with the test harness calling
  `rustls_symcrypt::default_symcrypt_provider().install_default()` (or
  `aws-lc-rs`'s equivalent) before starting the broker, on both a Windows and
  a Linux runner, asserting the resulting connection succeeds and that
  `cargo tree` for that build contains neither `ring` nor `aws-lc-rs`.
- If `--all-features` is part of the existing CI matrix, add or update an
  assertion that `--all-features` still produces a *working* build (`ring`
  and `aws-lc-rs` both present is acceptable there, since `--all-features`
  is explicitly "enable everything"), so this request does not silently
  break that matrix entry -- only the default and explicit single-backend
  builds are required to be provider-minimal.

## Tests

- Unit test: constructing a `TLSAcceptor` via `TLSAcceptor::rustls(...)`
  before any `CryptoProvider::install_default()` call returns
  `Error::NoCryptoProvider` (not a panic), for a process where no provider
  has been installed.
- Unit test: with a `ring` provider installed via
  `CryptoProvider::install_default()`, the existing TLS accept/handshake
  test suite passes unchanged (regression coverage for the default path).
- Integration test: with a non-`ring` provider (`aws-lc-rs`, or
  `rustls-symcrypt` where available) installed as the process default,
  a client using a matching provider can complete a TLS handshake against
  `rumqttd`'s listener, and `verify-client-cert` mTLS continues to extract
  the tenant id correctly.
- `cargo tree` assertions as described above, run in CI, not only locally.

## Migration and compatibility

- Existing consumers who build `rumqttd` today with no special crypto setup
  get `ring` "for free" because `rustls`'s own default feature enables it.
  After this change, those consumers must add one line at startup --
  `rustls::crypto::ring::default_provider().install_default()`, or the
  `aws-lc-rs`/`rustls-symcrypt` equivalent -- before constructing a `Broker`
  with a `use-rustls` listener configured. This is the single required
  migration step; document it prominently in the changelog and in
  `rumqttd/README.md`'s TLS section, alongside the existing
  `certpath`/`keypath`/`capath` config example.
  This is a **breaking change** for existing `use-rustls` consumers and
  should be released as a major/minor version bump per the crate's own
  versioning policy, with the new required call documented as the migration
  step.
- No change is required for `use-native-tls` consumers, and no change is
  required for consumers who build with `--no-default-features` (plaintext)
  today.
- `verify-client-cert`, `validate-tenant-prefix`, and the on-disk
  `TlsConfig`/config-file shape (`certpath`/`keypath`/`capath`) are unchanged
  by this request.

## Security

- This request does not change what is verified (server certificate chain,
  client certificate chain when `verify-client-cert` is enabled, tenant-id
  extraction); it changes which cryptographic implementation performs that
  verification, and who chooses it.
- Because protocol versions and cipher suites must come from the installed
  provider rather than a hardcoded list, maintainers should document the
  minimum expectations `rumqttd` places on any provider it is given (for
  example, that `with_safe_default_protocol_versions()` is used unless an
  application deliberately overrides it), so an application supplying an
  unusual or intentionally restricted provider understands what `rumqttd`
  will and will not attempt to negotiate.
- Removing `rumqttd`'s own hardcoded dependency on `ring` does not remove
  `ring` from the ecosystem or from any application that chooses to install
  it; it only stops `rumqttd` from making that choice unconditionally for
  every consumer.

## Acceptance criteria

1. `rumqttd/Cargo.toml` depends on `rustls` directly, and on `rustls`/
   `tokio-rustls` with `default-features = false`, enabling no bundled
   crypto-provider feature.
2. `TLSAcceptor::rustls` builds its `ServerConfig` via
   `ServerConfig::builder_with_provider(...)`, sourced from
   `rustls::crypto::CryptoProvider::get_default()` by default, with an
   optional explicit-provider parameter available.
3. Constructing a rustls-backed `TLSAcceptor` with no provider installed
   returns a new, documented `Error::NoCryptoProvider` (or equivalently named
   variant) rather than panicking or failing silently.
4. `cargo build --no-default-features` (optionally with `--features
   websocket`) continues to produce a build with zero TLS/crypto crates in
   `cargo tree`, verified in CI.
5. `cargo build --features use-rustls` (no crypto-provider feature of
   `rumqttd`'s own) produces a build with neither `ring` nor `aws-lc-rs` in
   `cargo tree` for `rumqttd`'s own dependency edges, verified in CI.
6. A CI job installs `rustls-symcrypt` (or another non-`ring`,
   non-`aws-lc-rs` provider) as the process default and successfully
   completes a TLS (and, separately, mTLS) handshake against `rumqttd`'s
   listener on both Windows and Linux, with `cargo tree` for that build
   showing neither `ring` nor `aws-lc-rs`.
7. `verify-client-cert` (mTLS) and tenant-id extraction from the client
   certificate continue to pass their existing test coverage unchanged.
8. The migration step (installing a provider before constructing a
   `use-rustls` `Broker`) is documented in the changelog and in
   `rumqttd/README.md`.

## Non-goals

- This issue does not ask for `rumqttd` to bundle, recommend, or default to
  any specific `CryptoProvider`; provider selection remains entirely the
  embedding application's responsibility.
- This issue does not ask for changes to the on-disk `TlsConfig`/config-file
  format (`certpath`/`keypath`/`capath`), or to certificate/key parsing.
- This issue does not ask for removal or restructuring of the
  `use-native-tls` backend.
- This issue does not ask for `--all-features` builds to be provider-minimal;
  that flag is explicitly "enable everything" and is expected to pull in
  more than one provider.
- This issue does not propose changing MQTT protocol-level behavior, QoS
  handling, or anything outside the TLS listener construction path.
