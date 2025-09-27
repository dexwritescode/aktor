use crate::{ActorContext, ActorError, Message};
use async_trait::async_trait;
use std::any::Any;

/// Core trait that all actors must implement
/// Generic over the message type for type safety
#[async_trait]
pub trait Actor<M: Message>: Send + Sync + 'static {
    /// Handle incoming messages
    /// This is the main message processing method
    async fn handle(&mut self, msg: M, ctx: &ActorContext<M>) -> Result<(), ActorError>;

    /// Called when the actor is starting up
    /// Use this for initialization logic
    async fn pre_start(&mut self, _ctx: &ActorContext<M>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor is shutting down
    /// Use this for cleanup logic
    async fn post_stop(&mut self, _ctx: &ActorContext<M>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor encounters an error
    /// Return true to restart, false to stop
    async fn on_error(&mut self, error: &ActorError, _ctx: &ActorContext<M>) -> bool {
        tracing::error!("Actor error: {}", error);
        false // Default: stop on error
    }

    /// Get the actor's current state as Any for inspection
    /// Used for debugging and state persistence
    fn as_any(&self) -> &dyn Any;

    /// Get mutable reference to actor state as Any
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Typed actor trait for stronger type safety
/// This ensures actors can only receive their specific message type
#[async_trait]
pub trait TypedActor<M: Message>: Actor<M> {
    /// The specific message type this actor handles
    type Msg: Message;

    /// Type-safe message handling
    async fn receive(
        &mut self,
        msg: Self::Msg,
        ctx: &ActorContext<Self::Msg>
    ) -> Result<(), ActorError>;
}

/// Supervision strategy for handling actor failures
#[derive(Debug, Clone, PartialEq)]
pub enum SupervisionStrategy {
    /// Restart the failed actor
    Restart,
    /// Stop the failed actor
    Stop,
    /// Escalate the failure to the parent supervisor
    Escalate,
    /// Resume the actor (ignore the failure)
    Resume,
}

/// Actor factory trait for creating actors with parameters
pub trait ActorFactory<M: Message>: Send + Sync + 'static {
    /// The actor type this factory creates
    type Actor: Actor<M>;

    /// Create a new actor instance
    fn create(&self) -> Self::Actor;
}

/// Simple actor factory for actors with Default implementation
pub struct DefaultActorFactory<A> {
    _phantom: std::marker::PhantomData<A>,
}

impl<A> Default for DefaultActorFactory<A> {
    fn default() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<M, A> ActorFactory<M> for DefaultActorFactory<A>
where
    M: Message,
    A: Actor<M> + Default,
{
    type Actor = A;

    fn create(&self) -> Self::Actor {
        A::default()
    }
}

/// Actor props for configuring actor creation
#[derive(Debug, Clone)]
pub struct ActorProps {
    /// Supervision strategy for this actor
    pub supervision_strategy: SupervisionStrategy,
    /// Whether this actor should be restarted on failure
    pub restart_on_failure: bool,
    /// Maximum number of restart attempts
    pub max_restarts: u32,
    /// Time window for restart counting (in seconds)
    pub restart_window_secs: u64,
}

impl Default for ActorProps {
    fn default() -> Self {
        Self {
            supervision_strategy: SupervisionStrategy::Stop,
            restart_on_failure: false,
            max_restarts: 3,
            restart_window_secs: 60,
        }
    }
}

impl ActorProps {
    /// Create new actor props with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set supervision strategy
    pub fn with_supervision(mut self, strategy: SupervisionStrategy) -> Self {
        self.supervision_strategy = strategy;
        self
    }

    /// Enable restart on failure
    pub fn with_restart(mut self, max_restarts: u32, window_secs: u64) -> Self {
        self.restart_on_failure = true;
        self.max_restarts = max_restarts;
        self.restart_window_secs = window_secs;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ActorContext;

    #[derive(Debug, Clone)]
    struct TestMessage {
        content: String,
    }

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    struct TestActor {
        received_messages: Vec<String>,
    }

    impl Default for TestActor {
        fn default() -> Self {
            Self {
                received_messages: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl Actor<TestMessage> for TestActor {
        async fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            self.received_messages.push(msg.content);
            Ok(())
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[tokio::test]
    async fn test_actor_props() {
        let props = ActorProps::new()
            .with_supervision(SupervisionStrategy::Restart)
            .with_restart(5, 120);

        assert_eq!(props.supervision_strategy, SupervisionStrategy::Restart);
        assert_eq!(props.max_restarts, 5);
        assert_eq!(props.restart_window_secs, 120);
        assert!(props.restart_on_failure);
    }

    #[tokio::test]
    async fn test_test_actor_functionality() {
        let actor = TestActor::default();
        let message = TestMessage {
            content: "test message".to_string(),
        };

        // Mock context - we don't actually use it in this test
        // but we need it for the Actor trait
        assert_eq!(actor.received_messages.len(), 0);
        assert_eq!(message.content, "test message");
        assert_eq!(Message::type_id(&message), "TestMessage");
    }
}