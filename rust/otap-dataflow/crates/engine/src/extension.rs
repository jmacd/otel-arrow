// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Extension system for the pipeline engine.
//!
//! Extensions provide capabilities (trait objects) that nodes can consume
//! during construction.  The core data structure is [`Extensions`], a
//! type-safe heterogeneous map keyed by [`TypeId`].
//!
//! # Shared vs Local capabilities
//!
//! Because the engine runs **one `build()` call per core**, capabilities
//! come in two flavours:
//!
//! | Kind | Stored as | Per-core semantics |
//! |------|-----------|---------------------|
//! | **Shared** | `Arc<dyn Trait>` | Same instance on every core (thread-safe) |
//! | **Local** | Factory `Fn() → Box<dyn Trait>` | Factory called once per core; instance is `!Send`-safe |
//!
//! ## Registration (extension author)
//!
//! ```ignore
//! impl Extension for MyAuth {
//!     fn register(&self, ext: &mut Extensions) {
//!         // Shared — one Arc for all cores:
//!         ext.set_shared::<dyn BearerTokenProvider>(Arc::new(self.clone()));
//!
//!         // — or, Local — factory creates one instance per core:
//!         let cfg = self.config.clone();
//!         ext.set_local::<dyn ConnectionPool>(move || {
//!             Box::new(Pool::new(&cfg))
//!         });
//!     }
//! }
//! ```
//!
//! ## Retrieval (node factory)
//!
//! ```ignore
//! // Shared capability — same Arc on every core:
//! let auth: &Arc<dyn BearerTokenProvider> =
//!     pipeline_ctx.extensions().require_shared::<dyn BearerTokenProvider>()?;
//!
//! // Local capability — new instance for this core:
//! let pool: Box<dyn ConnectionPool> =
//!     pipeline_ctx.extensions().create_local::<dyn ConnectionPool>()?;
//! let pool = Rc::new(RefCell::new(pool)); // wrap for sharing within the core
//! ```
//!
//! # Lifecycle
//!
//! 1. Pipeline build reads extension configs from [`PipelineConfig`].
//! 2. Each [`ExtensionFactory`] creates a `Box<dyn Extension>`.
//! 3. `extension.register(&mut extensions)` populates shared values and
//!    local factories.
//! 4. The populated `Arc<Extensions>` is set on [`PipelineContext`].
//! 5. `build()` runs once per core — shared values are cloned via `Arc`,
//!    local factories are called to create per-core instances.
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
enum CapabilitySlot {
    /// A single shared instance wrapped in `Arc<dyn Trait>`, erased to
    /// `Box<dyn Any + Send + Sync>`.  All cores share the same `Arc`.
    Shared(Box<dyn Any + Send + Sync>),

    /// A factory closure that produces one `Box<dyn Trait>` per core.
    ///
    /// The factory itself is `Send + Sync` (it lives in `Arc<Extensions>`
    /// which crosses thread boundaries via `PipelineContext`).  The
    /// instances it returns may be `!Send` — each instance is used only
    /// on the core thread that called the factory.
    ///
    /// Internally the factory returns `Box<dyn Any>` (type-erased).
    /// Retrieval via [`Extensions::create_local`] downcasts back to
    /// `Box<dyn Trait>`.
    Local {
        factory: Box<dyn Fn() -> Box<dyn Any> + Send + Sync>,
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
// Extensions — the typed capability map
// ---------------------------------------------------------------------------

/// A type-safe, heterogeneous map of capabilities.
///
/// Stores [`CapabilitySlot`] entries keyed by `TypeId` of the *unsized*
/// trait type (e.g. `TypeId::of::<dyn BearerTokenProvider>()`).
///
/// The struct itself is `Send + Sync` — shared values are `Arc` and local
/// factories are `Send + Sync` closures.
pub struct Extensions {
    map: HashMap<TypeId, CapabilitySlot>,
}

impl Extensions {
    /// Creates an empty capability map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    // -- Shared (cross-core) capabilities ----------------------------------

    /// Registers a shared capability — one `Arc` instance for all cores.
    ///
    /// `T` is typically an unsized trait type:
    ///
    /// ```ignore
    /// extensions.set_shared::<dyn BearerTokenProvider>(arc_impl);
    /// ```
    pub fn set_shared<T: ?Sized + Send + Sync + 'static>(&mut self, value: Arc<T>) {
        let _prev = self
            .map
            .insert(TypeId::of::<T>(), CapabilitySlot::Shared(Box::new(value)));
    }

    /// Retrieves a shared capability, returning `None` if absent or if the
    /// capability was registered as local.
    #[must_use]
    pub fn get_shared<T: ?Sized + Send + Sync + 'static>(&self) -> Option<&Arc<T>> {
        match self.map.get(&TypeId::of::<T>())? {
            CapabilitySlot::Shared(boxed) => boxed.downcast_ref::<Arc<T>>(),
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

    /// Registers a per-core capability factory.
    ///
    /// The factory closure is `Send + Sync` (it lives inside `Arc<Extensions>`
    /// which is shared across cores).  Each call to [`create_local`] invokes
    /// the factory to produce a fresh `Box<dyn Trait>`.
    ///
    /// The produced instance may be `!Send` — it will be used only on the
    /// core thread that called `create_local`.
    ///
    /// ```ignore
    /// let cfg = self.config.clone();
    /// extensions.set_local::<dyn ConnectionPool>(move || {
    ///     Box::new(Pool::new(&cfg))
    /// });
    /// ```
    pub fn set_local<T, F>(&mut self, factory: F)
    where
        T: ?Sized + 'static,
        F: Fn() -> Box<T> + Send + Sync + 'static,
    {
        // Double-box: Box<dyn Trait> → Box<dyn Any> for type erasure.
        // Recovered in `create_local` via downcast.
        let wrapper: Box<dyn Fn() -> Box<dyn Any> + Send + Sync> = Box::new(move || {
            let instance: Box<T> = factory();
            Box::new(instance) as Box<dyn Any>
        });
        let _prev = self.map.insert(
            TypeId::of::<T>(),
            CapabilitySlot::Local { factory: wrapper },
        );
    }

    /// Creates a new per-core instance of a local capability.
    ///
    /// Each call invokes the factory registered via [`set_local`].  Returns
    /// `None` if the capability was not registered or was registered as
    /// shared.
    ///
    /// The caller typically wraps the result for sharing among nodes on the
    /// same core:
    ///
    /// ```ignore
    /// let pool: Box<dyn ConnectionPool> =
    ///     extensions.create_local::<dyn ConnectionPool>()?;
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

    /// Returns the number of registered capabilities (shared + local).
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if no capabilities have been registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for Extensions {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Extensions {
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
        f.debug_struct("Extensions")
            .field("shared", &shared)
            .field("local", &local)
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Extension trait — implemented by extension authors
// ---------------------------------------------------------------------------

/// An extension that registers one or more capabilities into the
/// [`Extensions`] map.
///
/// Extensions are sync-only: the `register` method runs during pipeline
/// build (before any async runtime is involved).  If an extension needs
/// to perform async initialization (e.g. fetching a token), it should
/// spawn its own background task internally and expose a sync read
/// interface (e.g. via `Arc<watch::Receiver<T>>`).
pub trait Extension: Send + Sync {
    /// Populate `extensions` with the capabilities this extension provides.
    ///
    /// Use [`Extensions::set_shared`] for capabilities shared across all
    /// cores, or [`Extensions::set_local`] for capabilities that need one
    /// independent instance per core.
    fn register(&self, extensions: &mut Extensions);
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
    /// Unique name identifying this extension type (e.g. `"bearer-token"`).
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

/// Returns a map from extension name to factory, built lazily from the
/// distributed-slice registry.
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

/// Creates all extensions declared in config and returns the populated
/// [`Extensions`] map.
///
/// For each entry in `extension_configs`:
/// 1. Look up the [`ExtensionFactory`] by name.
/// 2. Call `factory.create(config)` to build the extension.
/// 3. Call `extension.register(&mut extensions)` — the extension decides
///    whether each capability is shared or local.
pub fn build_extensions(
    extension_configs: &HashMap<String, ExtensionConfig>,
) -> Result<Extensions, Error> {
    let factory_map = get_extension_factory_map();
    let mut extensions = Extensions::new();

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

        ext.register(&mut extensions);
    }

    Ok(extensions)
}
