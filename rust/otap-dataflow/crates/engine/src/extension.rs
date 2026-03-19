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
//! # Extension scoping
//!
//! Extension authors declare a **scope** for each capability that determines
//! its lifecycle.  This is orthogonal to node locality (Local/Shared nodes):
//!
//! | Scope | Stored as | Lifecycle |
//! |-------|-----------|-----------|
//! | **Engine** | `Arc<dyn Trait>` | Single instance for the entire engine, shared across all cores |
//! | **Pipeline** | Factory `Fn() → Box<dyn Trait>` | One instance per pipeline build (per core) |
//!
//! The cross product of extension scope × node locality determines the
//! ownership pattern at the node:
//!
//! | Extension scope \ Node kind | **Shared node** | **Local node** |
//! |-----------------------------|-----------------|----------------|
//! | **Engine** (`Arc`) | `Arc<dyn T>` | `Arc<dyn T>` |
//! | **Pipeline** (factory) | `Box<dyn T>` (must be `Send`) | `Rc<RefCell<Box<dyn T>>>` (may be `!Send`) |
//!
//! ## Registration (extension author)
//!
//! ```ignore
//! impl Extension for OidcAuth {
//!     fn register(&self, caps: &mut InstanceCapabilities) {
//!         // Engine-scoped — one Arc for all cores:
//!         caps.set_engine_scoped::<dyn AuthCheck>("auth_check", Arc::new(self.clone()));
//!     }
//! }
//! ```
//!
//! ## Retrieval (node factory)
//!
//! ```ignore
//! // Engine-scoped capability:
//! let auth: &Arc<dyn AuthCheck> =
//!     pipeline_ctx.capabilities().require_engine::<dyn AuthCheck>()?;
//!
//! // Pipeline-scoped capability (created fresh per pipeline build):
//! let pool: Box<dyn ConnectionPool> =
//!     pipeline_ctx.capabilities().require_pipeline::<dyn ConnectionPool>()?;
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

pub use otap_df_config::{CapabilityId, NodeId, extension::ExtensionConfig};

// ---------------------------------------------------------------------------
// CapabilitySlot — engine-scoped instance vs pipeline-scoped factory
// ---------------------------------------------------------------------------

/// How a single capability is provided, determined by extension scope.
///
/// See the [module docs](self) for the full scoping × locality matrix.
#[derive(Clone)]
enum CapabilitySlot {
    /// Engine-scoped: a single instance wrapped in `Arc<dyn Trait>`, erased
    /// to `Arc<dyn Any + Send + Sync>`.  Shared across all cores/pipelines.
    Engine(Arc<dyn Any + Send + Sync>),

    /// Pipeline-scoped: a factory closure that produces one `Box<dyn Trait>`
    /// per pipeline build (per core).
    ///
    /// The factory is behind `Arc` so the slot is cheaply cloneable across
    /// the per-node `Capabilities` maps produced during resolution.
    ///
    /// The factory itself is `Send + Sync`; produced instances may be
    /// `!Send` — each is used only on the core thread that called it.
    Pipeline(Arc<dyn Fn() -> Box<dyn Any> + Send + Sync>),
}

impl fmt::Debug for CapabilitySlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(_) => write!(f, "Engine(..)"),
            Self::Pipeline { .. } => write!(f, "Pipeline {{ factory }}"),
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
    entries: HashMap<CapabilityId, RegistryEntry>,
}

impl InstanceCapabilities {
    /// Creates an empty instance capability set.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers an engine-scoped capability — one `Arc` instance shared
    /// across all cores and pipeline builds.
    ///
    /// ```ignore
    /// caps.set_engine_scoped::<dyn AuthCheck>("auth_check", Arc::new(self.clone()));
    /// ```
    pub fn set_engine_scoped<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        capability_name: CapabilityId,
        value: Arc<T>,
    ) {
        let _prev = self.entries.insert(
            capability_name,
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                slot: CapabilitySlot::Engine(Arc::new(value) as Arc<dyn Any + Send + Sync>),
            },
        );
    }

    /// Registers a pipeline-scoped capability factory.
    ///
    /// The factory closure is `Send + Sync` (stored in `Arc`).  Each call
    /// to [`Capabilities::create_pipeline`] invokes it to produce a fresh
    /// `Box<dyn Trait>` for that pipeline build.
    ///
    /// When a Local node consumes this, the result is typically wrapped in
    /// `Rc<RefCell<…>>` — enabling `!Send` capability instances on the
    /// single-threaded per-core runtime.
    ///
    /// ```ignore
    /// let cfg = self.config.clone();
    /// caps.set_pipeline_scoped::<dyn ConnectionPool>("connection_pool", move || {
    ///     Box::new(Pool::new(&cfg))
    /// });
    /// ```
    pub fn set_pipeline_scoped<T, F>(&mut self, capability_name: CapabilityId, factory: F)
    where
        T: ?Sized + 'static,
        F: Fn() -> Box<T> + Send + Sync + 'static,
    {
        // Double-box: Box<dyn Trait> → Box<dyn Any> for type erasure.
        // Recovered in `Capabilities::create_pipeline` via downcast.
        let wrapper: Arc<dyn Fn() -> Box<dyn Any> + Send + Sync> = Arc::new(move || {
            let instance: Box<T> = factory();
            Box::new(instance) as Box<dyn Any>
        });
        let _prev = self.entries.insert(
            capability_name,
            RegistryEntry {
                type_id: TypeId::of::<T>(),
                slot: CapabilitySlot::Pipeline(wrapper),
            },
        );
    }

    /// Returns the capability names registered by this instance.
    #[must_use]
    pub fn capability_names(&self) -> Vec<CapabilityId> {
        self.entries.keys().cloned().collect()
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
    instances: HashMap<NodeId, InstanceCapabilities>,
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
    pub fn insert(&mut self, instance_name: NodeId, caps: InstanceCapabilities) {
        let _prev = self.instances.insert(instance_name, caps);
    }

    /// Resolves a node's capability bindings into a per-node [`Capabilities`]
    /// map.
    ///
    /// `bindings` maps capability name → extension instance name (from the
    /// node's `capabilities` config section).
    pub fn resolve(&self, bindings: &HashMap<CapabilityId, NodeId>) -> Result<Capabilities, Error> {
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

            let entry = instance
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
///     pipeline_ctx.capabilities().require_engine::<dyn AuthCheck>()?;
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

    // -- Engine-scoped capabilities ----------------------------------------

    /// Retrieves an engine-scoped capability, returning `None` if absent or
    /// if the capability was registered as pipeline-scoped.
    #[must_use]
    pub fn get_engine<T: ?Sized + Send + Sync + 'static>(&self) -> Option<&Arc<T>> {
        match self.map.get(&TypeId::of::<T>())? {
            CapabilitySlot::Engine(arc) => {
                // arc is Arc<dyn Any + Send + Sync>, holding Arc<dyn Trait>.
                // Downcast the inner Any to Arc<T>.
                arc.downcast_ref::<Arc<T>>()
            }
            CapabilitySlot::Pipeline { .. } => None,
        }
    }

    /// Like [`get_engine`](Self::get_engine) but returns an error when the
    /// capability is missing.
    pub fn require_engine<T: ?Sized + Send + Sync + 'static>(&self) -> Result<&Arc<T>, Error> {
        self.get_engine::<T>()
            .ok_or_else(|| Error::ExtensionNotFound {
                capability: type_name::<T>().to_owned(),
            })
    }

    // -- Pipeline-scoped capabilities --------------------------------------

    /// Creates a new instance of a pipeline-scoped capability.
    ///
    /// Each call invokes the factory registered via
    /// [`InstanceCapabilities::set_pipeline_scoped`].  Returns `None` if
    /// the capability was not registered or was registered as engine-scoped.
    ///
    /// For Local nodes, the caller typically wraps the result for sharing
    /// among nodes on the same core:
    ///
    /// ```ignore
    /// let pool: Box<dyn ConnectionPool> =
    ///     capabilities.create_pipeline::<dyn ConnectionPool>()?;
    /// let pool = Rc::new(RefCell::new(pool));
    /// ```
    #[must_use]
    pub fn create_pipeline<T: ?Sized + 'static>(&self) -> Option<Box<T>> {
        match self.map.get(&TypeId::of::<T>())? {
            CapabilitySlot::Pipeline(factory) => {
                let any: Box<dyn Any> = factory();
                // Reverse the double-box from set_pipeline_scoped:
                // Box<dyn Any> → Box<Box<dyn Trait>> → Box<dyn Trait>
                any.downcast::<Box<T>>().ok().map(|bb| *bb)
            }
            CapabilitySlot::Engine(_) => None,
        }
    }

    /// Like [`create_pipeline`](Self::create_pipeline) but returns an error
    /// when the capability is missing.
    pub fn require_pipeline<T: ?Sized + 'static>(&self) -> Result<Box<T>, Error> {
        self.create_pipeline::<T>()
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
        let engine = self
            .map
            .values()
            .filter(|s| matches!(s, CapabilitySlot::Engine(_)))
            .count();
        let pipeline = self
            .map
            .values()
            .filter(|s| matches!(s, CapabilitySlot::Pipeline { .. }))
            .count();
        f.debug_struct("Capabilities")
            .field("engine", &engine)
            .field("pipeline", &pipeline)
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
    /// Use [`InstanceCapabilities::set_engine_scoped`] for capabilities
    /// that live for the entire engine lifetime (shared across all cores),
    /// or [`InstanceCapabilities::set_pipeline_scoped`] for capabilities
    /// that get one independent instance per pipeline build.
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
    extension_configs: &HashMap<NodeId, ExtensionConfig>,
) -> Result<ExtensionRegistry, Error> {
    let factory_map = get_extension_factory_map();
    let mut registry = ExtensionRegistry::new();

    for (instance_name, ext_cfg) in extension_configs {
        let factory =
            factory_map
                .get(ext_cfg.r#type.as_str())
                .ok_or_else(|| Error::UnknownExtension {
                    name: instance_name.to_string(),
                    type_name: ext_cfg.r#type.clone(),
                })?;

        let ext = (factory.create)(&ext_cfg.config).map_err(|e| Error::ExtensionCreateError {
            name: instance_name.to_string(),
            error: e.to_string(),
        })?;

        let mut instance_caps = InstanceCapabilities::new();
        ext.register(&mut instance_caps);
        registry.insert(instance_name.clone(), instance_caps);
    }

    Ok(registry)
}
