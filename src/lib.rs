// Core modules
pub mod core;
pub mod extensions;
pub mod reference;
pub mod system;

// Optional testing module
#[cfg(feature = "test-util")]
pub mod testing;

// Convenience prelude module
pub mod prelude;

// Re-export commonly used items
pub use core::*;
pub use reference::{
    ActorRef, AskError, AskExt, AskFuture, LocalActorRef, RemoteActorRef, ask, ask_with_actor_ref,
};
pub use system::*;

#[cfg(feature = "test-util")]
pub use testing::*;
