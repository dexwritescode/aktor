use crate::{ActorContext, ActorError, Message};
use async_trait::async_trait;

/// Core trait that all actors must implement.
///
/// Each actor declares its message type via `type Msg`. The runtime dispatches
/// only that type to the actor, giving compile-time message-type safety without
/// forcing the actor system to be generic over `M`.
///
/// `handle` is synchronous — use `ctx.pipe_to_self(future)` for async I/O.
/// `pre_start` is async and runs before the first message is dispatched.
#[async_trait]
pub trait Actor: Send + Sync + std::fmt::Debug + 'static {
    type Msg: Message;

    /// Handle incoming messages (both Tell and Ask).
    ///
    /// For Ask messages: use `ctx.is_ask_request()` and `ctx.respond(response)`.
    /// For async I/O: use `ctx.pipe_to_self(future)` — handle returns immediately
    /// and the future result arrives as a subsequent message.
    fn handle(&mut self, msg: Self::Msg, ctx: &ActorContext<Self::Msg>);

    /// Called once before the first message is dispatched.
    /// Spawn children, open connections, or load initial state here.
    async fn pre_start(&mut self, _ctx: &ActorContext<Self::Msg>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor is shutting down. Use for cleanup.
    fn post_stop(&mut self, _ctx: &ActorContext<Self::Msg>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor encounters an error. Return true to restart, false to stop.
    fn on_error(&mut self, error: &ActorError, _ctx: &ActorContext<Self::Msg>) -> bool {
        tracing::error!("Actor error: {}", error);
        false
    }
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

/// Factory trait for actors that can be created with arguments
pub trait ActorFactoryArgs<Args: Send + 'static>: Actor + Sized {
    /// Create actor with arguments
    fn create_args(args: Args) -> Self;
}

/// Simple actor factory for actors with Default implementation
#[derive(Debug)]
pub struct DefaultActorFactory<A> {
    _phantom: std::marker::PhantomData<A>,
}

impl<A> DefaultActorFactory<A> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn create_actor(&self) -> A
    where
        A: Actor + Default,
    {
        A::default()
    }
}

impl<A> Default for DefaultActorFactory<A> {
    fn default() -> Self {
        Self::new()
    }
}

/// Args-based actor factory
#[derive(Debug, Default)]
pub struct ArgsActorFactory<A, Args> {
    _phantom_actor: std::marker::PhantomData<A>,
    _phantom_args: std::marker::PhantomData<Args>,
}

impl<A, Args> ArgsActorFactory<A, Args> {
    pub fn new() -> Self {
        Self {
            _phantom_actor: std::marker::PhantomData,
            _phantom_args: std::marker::PhantomData,
        }
    }

    pub fn create_actor(&self, args: Args) -> A
    where
        A: ActorFactoryArgs<Args>,
        Args: Send + 'static,
    {
        A::create_args(args)
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
    /// Mailbox size for this actor
    pub mailbox_size: usize,
    /// Dispatcher name for thread pool assignment
    pub dispatcher: Option<String>,
}

impl ActorProps {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.mailbox_size = size;
        self
    }

    pub fn with_dispatcher(mut self, dispatcher: impl Into<String>) -> Self {
        self.dispatcher = Some(dispatcher.into());
        self
    }

    pub fn with_supervision(mut self, strategy: SupervisionStrategy) -> Self {
        self.supervision_strategy = strategy;
        self
    }

    pub fn with_restart(mut self, max_restarts: u32, window_secs: u64) -> Self {
        self.restart_on_failure = true;
        self.max_restarts = max_restarts;
        self.restart_window_secs = window_secs;
        self
    }
}

impl Default for ActorProps {
    fn default() -> Self {
        Self {
            supervision_strategy: SupervisionStrategy::Stop,
            restart_on_failure: false,
            max_restarts: 3,
            restart_window_secs: 60,
            mailbox_size: 1000,
            dispatcher: None,
        }
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

    #[derive(Debug)]
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
    impl Actor for TestActor {
        type Msg = TestMessage;

        fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            self.received_messages.push(msg.content);
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

        assert_eq!(actor.received_messages.len(), 0);
        assert_eq!(message.content, "test message");
        assert_eq!(Message::type_id(&message), "TestMessage");
    }
}
