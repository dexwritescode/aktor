use crate::{ActorContext, ActorError, Message};

/// Core trait that all actors must implement
/// Generic over the message type for type safety
///
/// Actors can handle both Tell and Ask messages through the same handle() method.
/// Use ctx.is_ask_request() to check if a response is expected.
/// Use ctx.respond() to send responses for Ask messages.
///
/// This is a synchronous trait for maximum performance - no async overhead
pub trait Actor<M: Message>: Send + Sync + std::fmt::Debug + 'static {
    /// Handle incoming messages synchronously (both Tell and Ask)
    ///
    /// For Ask messages:
    /// - Use ctx.is_ask_request() to detect ask requests
    /// - Use ctx.respond(response) to send responses back
    /// - Ask messages MUST send a response, or the request will timeout
    ///
    /// For Tell messages:
    /// - Process normally, no response needed
    /// - ctx.respond() will return an error if called during Tell
    ///
    /// Note: This method is synchronous for maximum performance
    fn handle(&mut self, msg: M, ctx: &ActorContext<M>);

    /// Called when the actor is starting up
    /// Use this for initialization logic
    fn pre_start(&mut self, _ctx: &ActorContext<M>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor is shutting down
    /// Use this for cleanup logic
    fn post_stop(&mut self, _ctx: &ActorContext<M>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor encounters an error
    /// Return true to restart, false to stop
    fn on_error(&mut self, error: &ActorError, _ctx: &ActorContext<M>) -> bool {
        tracing::error!("Actor error: {}", error);
        false // Default: stop on error
    }
}

/// Typed actor trait for stronger type safety
/// This ensures actors can only receive their specific message type
pub trait TypedActor<M: Message>: Actor<M> {
    /// The specific message type this actor handles
    type Msg: Message;

    /// Type-safe message handling
    fn receive(
        &mut self,
        msg: Self::Msg,
        ctx: &ActorContext<Self::Msg>
    );
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
pub trait ActorFactoryArgs<M: Message, Args: Send + 'static>: Actor<M> + Sized {
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

    pub fn create_actor<M>(&self) -> A
    where
        A: Actor<M> + Default,
        M: Message,
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
#[derive(Debug)]
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

    pub fn create_actor<M>(&self, args: Args) -> A
    where
        A: ActorFactoryArgs<M, Args>,
        M: Message,
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

/// Enhanced ActorProps with builder pattern
impl ActorProps {
    /// Create new ActorProps
    pub fn new() -> Self {
        Self::default()
    }

    /// Set mailbox size
    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.mailbox_size = size;
        self
    }

    /// Set dispatcher
    pub fn with_dispatcher(mut self, dispatcher: impl Into<String>) -> Self {
        self.dispatcher = Some(dispatcher.into());
        self
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

    impl Actor<TestMessage> for TestActor {
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

        // Mock context - we don't actually use it in this test
        // but we need it for the Actor trait
        assert_eq!(actor.received_messages.len(), 0);
        assert_eq!(message.content, "test message");
        assert_eq!(Message::type_id(&message), "TestMessage");
    }
}