//! Prelude module for common imports
//!
//! This module re-exports the most commonly used types and traits,
//! allowing users to import everything they need with a single use statement:
//!
//! ```rust
//! use aktor::prelude::*;
//! ```

// Core traits and types
pub use crate::core::{Actor, ActorError, ActorProps, Message};

// System types
pub use crate::system::{ActorAddress, ActorContext, ActorPath, ActorSystem, ActorSystemConfig};

// Reference types
pub use crate::reference::{
    ActorRef, AskError, AskExt, AskFuture, LocalActorRef, RemoteActorRef, ask, ask_with_actor_ref,
};

// Testing utilities (when feature is enabled)
#[cfg(feature = "test-util")]
pub use crate::testing::{ActorTestKit, ExpectationResult, TestContext, TestMessage, TestProbe};
