//! Actor System Extensions
//!
//! Extensions provide a way to register shared resources (HTTP clients, database pools, etc.)
//! in the actor system and access them from any actor without holding them in actor state.
//!
//! This is critical for:
//! - Event sourcing (actors must be serializable)
//! - Actor migration (move actors between nodes)
//! - Stateless actor design (resources shared, not owned)
//!
//! # Example
//!
//! ```ignore
//! use aktor::*;
//! use std::sync::Arc;
//!
//! // 1. Define your extension
//! struct HttpClientExtension {
//!     client: reqwest::blocking::Client,
//! }
//!
//! impl Extension for HttpClientExtension {
//!     fn new_extension() -> Self {
//!         let client = reqwest::blocking::Client::builder()
//!             .timeout(std::time::Duration::from_secs(10))
//!             .build()
//!             .unwrap();
//!         Self { client }
//!     }
//! }
//!
//! impl HttpClientExtension {
//!     fn get(&self, url: &str) -> reqwest::blocking::Response {
//!         self.client.get(url).send().unwrap()
//!     }
//! }
//!
//! // 2. Register extension with system
//! let system = ActorSystem::new(config).await?;
//! system.register_extension(HttpClientExtension::new_extension());
//!
//! // 3. Use in any actor
//! impl Actor<MyMsg> for MyActor {
//!     fn handle(&mut self, msg: MyMsg, ctx: &ActorContext<MyMsg>) {
//!         let http = ctx.system().extension::<HttpClientExtension>();
//!         let response = http.get("https://example.com");
//!     }
//! }
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Trait for actor system extensions
///
/// Extensions are shared resources that can be accessed by any actor.
/// They must be Send + Sync since they're shared across threads.
pub trait Extension: Send + Sync + 'static {
    /// Create a new instance of this extension
    ///
    /// Called once when the extension is first registered.
    /// Has access to the actor system for initialization if needed.
    fn new_extension() -> Self where Self: Sized;
}

/// Registry of actor system extensions
///
/// Stores type-erased extensions and provides type-safe access.
#[derive(Default)]
pub struct ExtensionRegistry {
    /// Map of TypeId -> Arc<dyn Any + Send + Sync>
    extensions: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl ExtensionRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            extensions: RwLock::new(HashMap::new()),
        }
    }

    /// Register an extension
    ///
    /// # Panics
    /// Panics if an extension of this type is already registered.
    pub fn register<T: Extension>(&self, extension: T) {
        let type_id = TypeId::of::<T>();
        let arc_extension = Arc::new(extension);

        let mut extensions = self.extensions.write().unwrap();

        if extensions.contains_key(&type_id) {
            panic!(
                "Extension {} already registered",
                std::any::type_name::<T>()
            );
        }

        extensions.insert(type_id, arc_extension);
    }

    /// Get an extension by type
    ///
    /// # Panics
    /// Panics if the extension is not registered.
    pub fn get<T: Extension>(&self) -> Arc<T> {
        let type_id = TypeId::of::<T>();
        let extensions = self.extensions.read().unwrap();

        extensions
            .get(&type_id)
            .unwrap_or_else(|| {
                panic!(
                    "Extension {} not registered. Call system.register_extension() first.",
                    std::any::type_name::<T>()
                )
            })
            .clone()
            .downcast::<T>()
            .expect("Extension type mismatch - this is a bug")
    }

    /// Get an extension by type, returning None if not registered
    pub fn get_optional<T: Extension>(&self) -> Option<Arc<T>> {
        let type_id = TypeId::of::<T>();
        let extensions = self.extensions.read().unwrap();

        extensions
            .get(&type_id)
            .and_then(|ext| ext.clone().downcast::<T>().ok())
    }

    /// Check if an extension is registered
    pub fn has<T: Extension>(&self) -> bool {
        let type_id = TypeId::of::<T>();
        let extensions = self.extensions.read().unwrap();
        extensions.contains_key(&type_id)
    }

    /// Get or create an extension
    ///
    /// If the extension is already registered, returns it.
    /// Otherwise, creates a new instance and registers it.
    pub fn get_or_create<T: Extension>(&self) -> Arc<T> {
        let type_id = TypeId::of::<T>();

        // Fast path: extension already exists
        {
            let extensions = self.extensions.read().unwrap();
            if let Some(ext) = extensions.get(&type_id) {
                return ext
                    .clone()
                    .downcast::<T>()
                    .expect("Extension type mismatch - this is a bug");
            }
        }

        // Slow path: create and register
        let new_extension = T::new_extension();
        self.register(new_extension);
        self.get::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestExtension {
        value: i32,
    }

    impl Extension for TestExtension {
        fn new_extension() -> Self {
            TestExtension { value: 42 }
        }
    }

    #[derive(Debug)]
    struct AnotherExtension {
        name: String,
    }

    impl Extension for AnotherExtension {
        fn new_extension() -> Self {
            AnotherExtension {
                name: "test".to_string(),
            }
        }
    }

    #[test]
    fn test_register_and_get_extension() {
        let registry = ExtensionRegistry::new();
        let ext = TestExtension { value: 100 };

        registry.register(ext);

        let retrieved = registry.get::<TestExtension>();
        assert_eq!(retrieved.value, 100);
    }

    #[test]
    fn test_multiple_extensions() {
        let registry = ExtensionRegistry::new();

        registry.register(TestExtension { value: 42 });
        registry.register(AnotherExtension {
            name: "hello".to_string(),
        });

        let ext1 = registry.get::<TestExtension>();
        let ext2 = registry.get::<AnotherExtension>();

        assert_eq!(ext1.value, 42);
        assert_eq!(ext2.name, "hello");
    }

    #[test]
    #[should_panic(expected = "not registered")]
    fn test_get_unregistered_extension_panics() {
        let registry = ExtensionRegistry::new();
        let _ = registry.get::<TestExtension>();
    }

    #[test]
    fn test_get_optional_returns_none() {
        let registry = ExtensionRegistry::new();
        assert!(registry.get_optional::<TestExtension>().is_none());
    }

    #[test]
    fn test_has_extension() {
        let registry = ExtensionRegistry::new();

        assert!(!registry.has::<TestExtension>());

        registry.register(TestExtension { value: 42 });

        assert!(registry.has::<TestExtension>());
    }

    #[test]
    fn test_get_or_create() {
        let registry = ExtensionRegistry::new();

        // First call creates
        let ext1 = registry.get_or_create::<TestExtension>();
        assert_eq!(ext1.value, 42);

        // Second call returns same instance
        let ext2 = registry.get_or_create::<TestExtension>();
        assert_eq!(ext2.value, 42);

        // Verify it's the same Arc
        assert!(Arc::ptr_eq(&ext1, &ext2));
    }

    #[test]
    #[should_panic(expected = "already registered")]
    fn test_double_register_panics() {
        let registry = ExtensionRegistry::new();

        registry.register(TestExtension { value: 42 });
        registry.register(TestExtension { value: 100 }); // Should panic
    }

    #[test]
    fn test_extension_is_shared() {
        let registry = ExtensionRegistry::new();
        registry.register(TestExtension { value: 42 });

        let ext1 = registry.get::<TestExtension>();
        let ext2 = registry.get::<TestExtension>();

        // Both should point to the same instance
        assert!(Arc::ptr_eq(&ext1, &ext2));
    }
}