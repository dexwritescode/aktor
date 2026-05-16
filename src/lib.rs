// Core modules
pub mod core;
pub mod system;
pub mod reference;
pub mod extensions;

// Optional testing module
#[cfg(feature = "test-util")]
pub mod testing;

// Convenience prelude module
pub mod prelude;

// Re-export commonly used items
pub use core::*;
pub use system::*;
pub use reference::{ask, ask_with_actor_ref, AskError, AskExt, AskFuture, ActorRef, LocalActorRef, RemoteActorRef};

#[cfg(feature = "test-util")]
pub use testing::*;
