//! Prelude module for common imports
//!
//! This module re-exports the most commonly used types and traits,
//! allowing users to import everything they need with a single use statement:
//!
//! ```rust
//! use aktor::prelude::*;
//! ```

// Core traits and types
pub use crate::core::{Actor, Message, ActorError, ActorProps};

// System types
pub use crate::system::{ActorSystem, ActorSystemConfig, ActorContext,
                        ActorAddress, ActorPath};

// Reference types
pub use crate::reference::{ActorRef, LocalActorRef, RemoteActorRef,
                          ask, ask_with_actor_ref, AskError, AskExt, AskFuture};

// Testing utilities (when feature is enabled)
#[cfg(feature = "test-util")]
pub use crate::testing::{ActorTestKit, TestProbe, TestContext, TestMessage, ExpectationResult};