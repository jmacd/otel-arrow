// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension system for the pipeline engine.
//!
//! Extensions provide capabilities (trait objects) that nodes can consume
//! during construction.  The system uses **name-based binding**: each node
//! declares which extension *instance* provides each *capability*.
//!
//! # Data model
//!
//! ```text
//! PipelineConfig
//!  ├─ extensions:                          (instance_name → ExtensionConfig)
//!  │    oidc_auth_main:
//!  │      type: extension:oidc_auth
//!  │    local_auth:
//!  │      type: extension:basic_auth
//!  │
//!  └─ nodes:
//!       otlp_recv1:
//!         capabilities:                    (capability_name → instance_name)
//!           auth_check: oidc_auth_main
//!       otlp_recv2:
//!         capabilities:
//!           auth_check: local_auth         ← different instance, same capability
//! ```
//!
//! # Type hierarchy
//!
//! | Type | Scope | Purpose |
//! |------|-------|---------|
//! | [`InstanceCapabilities`] | per extension instance | capabilities registered by one extension |
//! | [`ExtensionRegistry`] | per pipeline | all instances, keyed by instance name |
//! | [`Capabilities`] | **per node** | resolved from the node's bindings + registry |
//!
//! # Shared vs Local capabilities
//!
//! Because the engine runs **one `build()` call per core**, capabilities
//! come in two flavours:
//!
//! | Kind | Stored as | Per-core semantics |
//! |------|-----------|---------------------|
//! | **Shared** | `Arc<dyn Trait>` | Same instance on every core |
//! | **Local** | Factory `Fn() → Box<dyn Trait>` | Factory called once per core |
//!
//! ## Registration (extension author)
//!
//! ```ignore
//! impl Extension for OidcAuth {
//!     fn register(&self, caps: &mut InstanceCapabilities) {
//!         // Shared — one Arc for all cores:
//!         caps.set_shared::<dyn AuthCheck>("auth_check", Arc::new(self.clone()));
//!     }
//! }
//! ```
//!
//! ## Retrieval (node factory)
//!
//! ```ignore
//! let auth: &Arc<dyn AuthCheck> =
//!     pipeline_ctx.capabilities().require_shared::<dyn AuthCheck>()?;
//! ```
//!
//! # Lifecycle
//!
//! 1. Pipeline build reads extension configs from [`PipelineConfig`].
//! 2. Each [`ExtensionFactory`] creates a `Box<dyn Extension>`.
//! 3. `extension.register(&mut instance_caps)` — each extension populates
//!    its own [`InstanceCapabilities`].
//! 4. All instances are collected into an [`ExtensionRegistry`].
//! 5. For each node, its `capabilities` bindings are resolved against the
//!    registry to produce a per-node [`Capabilities`].
//! 6. The resolved [`Capabilities`] is set on [`PipelineContext`] before
//!    calling the node factory.
//!
//! [`PipelineConfig`]: otap_df_config::pipeline::PipelineConfig
//! [`PipelineContext`]: crate::context::PipelineContext

use crate::error::Error;
use linkme::distributed_slice;
use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub use otap_df_config::extension::ExtensionConfig;

// ---------------------------------------------------------------------------
// CapabilitySlot — shared instance vs per-core factory
// ---------------------------------------------------------------------------

/// How a single capability is provided: either a shared pre-built instance
/// or a factory that creates one instance per core.
#[derive(Clone)]
enum CapabilitySlot {
    /// A single shared instance wrapped in `Arc<dyn Trait>`, erased to
    /// `Arc<dyn Any + Send + Sync>`.  All cores share the same `Arc`.
    Shared(Arc<dyn Any + Send + Sync>),

    /// A factory closure that produces one `Box<dyn Trait>` per core.
    ///
    /// The factory is behind `Arc` so the slot is cheaply cloneable across
    /// the per-node `Capabilities` maps produced during resolution.
    ///
    /// The factory itself is `Send + Sync`; produced instances may be
    /// `!Send` — each is used only on the core thread that called it.
    Local {
        factory: Arc<dyn Fn() -> Box<dyn Any> + Send + Sync>,
    },
}

impl fmt::Debug for CapabilitySlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shared(_) => write!(f, "Shared(..)"),
            Self::Local { .. } => write!(f, "Local {{ factory }}"),
        }
    }
}

// ---------------------------------------------------------------------------
// RegistryEntry — one capability provided by one extension instance
// ---------------------------------------------------------------------------

/// A single capability entry in an extension instance's registry.
///
/// Pairs the capability's Rust `TypeId` (for type-safe retrieval) with the
/// [`CapabilitySlot`] that provides it.
#[derive(Clone)]
struct RegistryEntry {
    type_id: TypeId,
    slot: CapabilitySlot,
}

// ---------------------------------------------------------------------------
// InstanceCapabilities — capabilities provided by one extension instance
// ---------------------------------------------------------------------------

/// Capabilities registered by a single extension instance.
///
/// Keyed by **capability name** (the string that appears in node config
/// bindings, e.g. `"auth_check"`).  Each entry also records the `TypeId`
/// of the trait so the per-node [`Capabilities`] map can be keyed by type.
pub struct InstanceCapabilities {
    entries: HashMap<String, RegistryEntry>,
}

impl InstanceCapabilities {
    /// Creates an empty instance capability set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers a shared capability — one `Arc` instance for all cores.
    ///
    /// ```ignore
    /// caps.set_shared::<dyn AuthCheck>("auth_check", Arc::new(self.clone()));
    /// ```
    pub fn set_shared<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        capability_name: &str,
        value: Arc<T>,
    ) {
        let _prev = self.entries.insert(
            capability_name.to_owned(),
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                slot: CapabilitySlot::Shared(Arc::new(value) as Arc<dyn Any + Send + Sync>),
            },
        );
    }

    /// Registers a per-core capability factory.
    ///
    /// The factory closure is `Send + Sync` (stored in `Arc`).  Each call
    /// to [`Capabilities::create_local`] invokes it to produce a fresh
    /// `Box<dyn Trait>` for that core.
    ///
    /// ```ignore
    /// let cfg = self.config.clone();
    /// caps.set_local::<dyn ConnectionPool>("connection_pool", move || {
    ///     Box::new(Pool::new(&cfg))
    /// });
    /// ```
    pub fn set_local<T, F>(&mut self, capability_name: &str, factory: F)
    where
        T: ?Sized + 'static,
        F: Fn() -> Box<T> + Send + Sync + 'static,
    {
        // Double-box: Box<dyn Trait> → Box<dyn Any> for type erasure.
        // Recovered in `Capabilities::create_local` via downcast.
        let wrapper: Arc<dyn Fn() -> Box<dyn Any> + Send + Sync> = Arc::new(move || {
            let instance: Box<T> = factory();
            Box::new(instance) as Box<dyn Any>
        });
        let _prev = self.entries.insert(
            capability_name.to_owned(),
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                slot: CapabilitySlot::Local { factory: wrapper },
            },
        );
    }

    /// Returns the capability names registered by this instance.
    #[must_use]
    pub fn capability_names(&self) -> Vec<&str> {
        self.entries.keys().map(String::as_str).collect()
    }
}

impl Default for InstanceCapabilities {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InstanceCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstanceCapabilities")
            .field("capabilities", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// ExtensionRegistry — all extension instances in a pipeline
// ---------------------------------------------------------------------------

/// All extension instances in a pipeline, keyed by instance name.
///
/// Built once during pipeline construction (or once globally and shared
/// via `Arc`).  Used to resolve per-node [`Capabilities`] from the node's
/// config bindings.
pub struct ExtensionRegistry {
    instances: HashMap<String, InstanceCapabilities>,
}

impl ExtensionRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            instances: HashMap::new(),
        }
    }

    /// Inserts an extension instance's capabilities.
    pub fn insert(&mut self, instance_name: String, caps: InstanceCapabilities) {
        let _prev = self.instances.insert(instance_name, caps);
    }

    /// Resolves a node's capability bindings into a per-node [`Capabilities`]
    /// map.
    ///
    /// `bindings` maps capability name → extension instance name (from the
    /// node's `capabilities` config section).
    pub fn resolve(&self, bindings: &HashMap<String, String>) -> Result<Capabilities, Error> {
        let mut caps = Capabilities::new();

        for (cap_name, instance_name) in bindings {
            let instance =
                self.instances
                    .get(instance_name)
                    .ok_or_else(|| Error::ExtensionNotFound {
                        capability: format!(
                            "extension instance `{instance_name}` \
                             (bound by capability `{cap_name}`)"
                        ),
                    })?;

            let entry =
                instance
                    .entries
                    .get(cap_name)
                    .ok_or_else(|| Error::ExtensionNotFound {
                        capability: format!(
                            "capability `{cap_name}` not provided by \
                             extension instance `{instance_name}`"
                        ),
                    })?;

            let _prev = caps.map.insert(entry.type_id, entry.slot.clone());
        }

        Ok(caps)
    }
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ExtensionRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtensionRegistry")
            .field("instances", &self.instances.keys().collect::<Vec<_>>())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Capabilities — per-node resolved capability map
// ---------------------------------------------------------------------------

/// Per-node resolved capabilities, produced by
/// [`ExtensionRegistry::resolve`].
///
/// Keyed by `TypeId` of the trait type — node factories perform typed
/// lookups without knowing instance names:
///
/// ```ignore
/// let auth: &Arc<dyn AuthCheck> =
///     pipeline_ctx.capabilities().require_shared::<dyn AuthCheck>()?;
/// ```
pub struct Capabilities {
    map: HashMap<TypeId, CapabilitySlot>,
}

impl Capabilities {
    /// Creates an empty capability set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    // -- Shared (cross-core) capabilities ----------------------------------

    /// Retrieves a shared capability, returning `None` if absent or if the
    /// capability was registered as local.
    #[must_use]
    pub fn get_shared<T: ?Sized + Send + Sync + 'static>(&self) -> Option<&Arc<T>> {
        match self.map.get(&TypeId::of::<T>())? {
            CapabilitySlot::Shared(arc) => {
                // arc is Arc<dyn Any + Send + Sync>, holding Arc<dyn Trait>.
                // Downcast the inner Any to Arc<T>.
                arc.downcast_ref::<Arc<T>>()
            }
            CapabilitySlot::Local { .. } => None,
        }
    }

    /// Like [`get_shared`](Self::get_shared) but returns an error when the
    /// capability is missing.
    pub fn require_shared<T: ?Sized + Send + Sync + 'static>(&self) -> Result<&Arc<T>, Error> {
        self.get_shared::<T>()
            .ok_or_else(|| Error::ExtensionNotFound {
                capability: type_name::<T>().to_owned(),
            })
    }

    // -- Local (per-core) capabilities -------------------------------------

    /// Creates a new per-core instance of a local capability.
    ///
    /// Each call invokes the factory registered via
    /// [`InstanceCapabilities::set_local`].  Returns `None` if the
    /// capability was not registered or was registered as shared.
    ///
    /// The caller typically wraps the result for sharing among nodes on the
    /// same core:
    ///
    /// ```ignore
    /// let pool: Box<dyn ConnectionPool> =
    ///     capabilities.create_local::<dyn ConnectionPool>()?;
    /// let pool = Rc::new(RefCell::new(pool));
    /// ```
    #[must_use]
    pub fn create_local<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        match self.map.get(&TypeId::of::<T>())? {
            CapabilitySlot::Local { factory } => {
                let any: Box<dyn Any> = factory();
                // Reverse the double-box from set_local:
                // Box<dyn Any> → Box<Box<dyn Trait>> → Box<dyn Trait>
                any.downcast::<Box<T>>().ok().map(|bb| *bb)
            }
            CapabilitySlot::Shared(_) => None,
        }
    }

    /// Like [`create_local`](Self::create_local) but returns an error when
    /// the capability is missing.
    pub fn require_local<T: ?Sized + 'static>(&self) -> Result<Box<T>, Error> {
        self.create_local::<T>()
            .ok_or_else(|| Error::ExtensionNotFound {
                capability: type_name::<T>().to_owned(),
            })
    }

    // -- Introspection -----------------------------------------------------

    /// Returns `true` if no capabilities have been bound.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for Capabilities {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let shared = self
            .map
            .values()
            .filter(|s| matches!(s, CapabilitySlot::Shared(_)))
            .count();
        let local = self
            .map
            .values()
            .filter(|s| matches!(s, CapabilitySlot::Local { .. }))
            .count();
        f.debug_struct("Capabilities")
            .field("shared", &shared)
            .field("local", &local)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Extension trait — implemented by extension authors
// ---------------------------------------------------------------------------

/// An extension that registers one or more capabilities into an
/// [`InstanceCapabilities`] map.
///
/// Extensions are sync-only: the `register` method runs during pipeline
/// build (before any async runtime is involved).  If an extension needs
/// to perform async initialization (e.g. fetching a token), it should
/// spawn its own background task internally and expose a sync read
/// interface (e.g. via `Arc<watch::Receiver<T>>`).
pub trait Extension: Send + Sync {
    /// Populate `caps` with the capabilities this extension provides.
    ///
    /// Use [`InstanceCapabilities::set_shared`] for capabilities shared
    /// across all cores, or [`InstanceCapabilities::set_local`] for
    /// capabilities that need one independent instance per core.
    fn register(&self, caps: &mut InstanceCapabilities);
}

// ---------------------------------------------------------------------------
// ExtensionFactory — for distributed-slice registration
// ---------------------------------------------------------------------------

/// Factory for creating an [`Extension`] from its JSON configuration.
///
/// Extension crates register a static `ExtensionFactory` via
/// [`distributed_slice`]:
///
/// ```ignore
/// #[distributed_slice(OTAP_EXTENSION_FACTORIES)]
/// pub static MY_EXT: ExtensionFactory = ExtensionFactory {
///     name: "my-extension",
///     create: |config| {
///         let ext = MyExtension::from_config(config)?;
///         Ok(Box::new(ext))
///     },
/// };
/// ```
pub struct ExtensionFactory {
    /// Unique name identifying this extension type (e.g. `"oidc_auth"`).
    pub name: &'static str,

    /// Creates an extension instance from its JSON config block.
    pub create:
        fn(config: &serde_json::Value) -> Result<Box<dyn Extension>, otap_df_config::error::Error>,
}

impl fmt::Debug for ExtensionFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExtensionFactory")
            .field("name", &self.name)
            .finish()
    }
}

/// Global registry of extension factories, populated at link time.
#[distributed_slice]
pub static OTAP_EXTENSION_FACTORIES: [ExtensionFactory];

/// Returns a map from extension type name to factory, built lazily from
/// the distributed-slice registry.
#[must_use]
pub fn get_extension_factory_map() -> &'static HashMap<&'static str, &'static ExtensionFactory> {
    use std::sync::OnceLock;
    static MAP: OnceLock<HashMap<&'static str, &'static ExtensionFactory>> = OnceLock::new();
    MAP.get_or_init(|| {
        OTAP_EXTENSION_FACTORIES
            .iter()
            .map(|f| (f.name, f))
            .collect()
    })
}

/// Creates all extension instances declared in config and returns an
/// [`ExtensionRegistry`] ready for per-node resolution.
///
/// For each entry in `extension_configs`:
/// 1. Look up the [`ExtensionFactory`] by type name.
/// 2. Call `factory.create(config)` to build the extension.
/// 3. Call `extension.register(&mut instance_caps)`.
/// 4. Insert the instance into the registry under its config key.
pub fn build_extension_registry(
    extension_configs: &HashMap<String, ExtensionConfig>,
) -> Result<ExtensionRegistry, Error> {
    let factory_map = get_extension_factory_map();
    let mut registry = ExtensionRegistry::new();

    for (instance_name, ext_cfg) in extension_configs {
        let factory =
            factory_map
                .get(ext_cfg.r#type.as_str())
                .ok_or_else(|| Error::UnknownExtension {
                    name: instance_name.clone(),
                    type_name: ext_cfg.r#type.clone(),
                })?;

        let ext = (factory.create)(&ext_cfg.config).map_err(|e| Error::ExtensionCreateError {
            name: instance_name.clone(),
            error: e.to_string(),
        })?;

        let mut instance_caps = InstanceCapabilities::new();
        ext.register(&mut instance_caps);
        registry.insert(instance_name.clone(), instance_caps);
    }

    Ok(registry)
}
