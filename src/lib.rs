use std::fmt::Debug;
use thiserror::Error;

pub mod actor;
pub mod context;
pub mod reference;
pub mod address;
pub mod system;
pub mod ask;

#[cfg(feature = "test-util")]
pub mod test_kit;

#[cfg(test)]
mod ask_integration_test;

pub use actor::*;
pub use context::*;
pub use reference::*;
pub use address::*;
pub use system::*;
pub use ask::{ask, ask_with_actor_ref, AskError, AskExt, AskFuture};

#[cfg(feature = "test-util")]
pub use test_kit::*;

/// Core trait that all messages must implement
/// Messages must be Send + Sync for distributed actors
/// and 'static for actor lifetime management
pub trait Message: Send + Sync + Debug + Clone + 'static {
    /// Message type identifier for routing and serialization
    fn type_id(&self) -> &'static str;
}

/// Error types for the actor system
#[derive(Error, Debug)]
pub enum ActorError {
    #[error("Actor not found: {0}")]
    ActorNotFound(String),

    #[error("Message delivery failed: {0}")]
    MessageDeliveryFailed(String),

    #[error("Actor creation failed: {0}")]
    ActorCreationFailed(String),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Network error: {0}")]
    NetworkError(String),
}
