use crate::{ActorContext, ActorError, Message};

/// Core trait that all actors must implement.
///
/// Each actor declares its message type via `type Msg`. The runtime dispatches
/// only that type to the actor, giving compile-time message-type safety without
/// forcing the actor system to be generic over `M`.
///
/// `handle` is synchronous — use `ctx.pipe_to_self(future)` for async I/O.
/// `pre_start` is synchronous; for async init, call `ctx.pipe_to_self(future)`
/// and handle the result as the first incoming message.
pub trait Actor: Send + Sync + std::fmt::Debug + 'static {
    type Msg: Message;

    /// Handle incoming messages (both Tell and Ask).
    ///
    /// For Ask messages: use `ctx.is_ask_request()` and `ctx.respond(response)`.
    /// For async I/O: use `ctx.pipe_to_self(future)` — handle returns immediately
    /// and the future result arrives as a subsequent message.
    fn handle(&mut self, msg: Self::Msg, ctx: &ActorContext<Self::Msg>);

    /// Called once before the first message is dispatched.
    /// Spawn children or set up initial state here.
    fn pre_start(&mut self, _ctx: &ActorContext<Self::Msg>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called when the actor is shutting down. Use for cleanup.
    fn post_stop(&mut self, _ctx: &ActorContext<Self::Msg>) -> Result<(), ActorError> {
        Ok(())
    }

    /// Called by the parent supervisor when a child actor fails.
    ///
    /// The parent decides how to handle the failure by returning a
    /// [`SupervisionStrategy`]. The default is `Stop` — failed children are
    /// stopped and not restarted unless the parent explicitly overrides this.
    ///
    /// ## Strategies
    /// - `Stop` — terminate the child permanently.
    /// - `Restart` — recreate the child using its factory closure and resume processing.
    /// - `Escalate` — propagate the failure up to *this* actor's own parent.
    /// - `Resume` — keep the existing (potentially dirty) child instance and let
    ///   it continue processing. Use only when you are sure the failure left no
    ///   inconsistent state. See aktor-bsk for the planned future overhaul.
    ///
    /// ## Ownership
    ///
    /// Supervision is the parent's responsibility, not the child's. Children
    /// do not implement `on_error` — they simply fail and report upward via the
    /// `ChildFailed` system message. This mirrors Akka Typed's model.
    fn on_child_failed(
        &mut self,
        _child: &crate::ActorAddress,
        _error: &ActorError,
        _ctx: &ActorContext<Self::Msg>,
    ) -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }
}

/// Supervision strategy returned by [`Actor::on_child_failed`].
#[derive(Debug, Clone, PartialEq)]
pub enum SupervisionStrategy {
    /// Restart the failed actor using its factory closure.
    Restart,
    /// Stop the failed actor permanently.
    Stop,
    /// Escalate the failure to this actor's own parent.
    Escalate,
    /// Resume the actor with the same (potentially dirty) instance.
    /// See aktor-bsk for the planned future overhaul of this variant.
    Resume,
}

/// Actor props for configuring actor creation and supervision.
#[derive(Debug, Clone)]
pub struct ActorProps {
    /// Supervision strategy applied to *this* actor by its parent.
    pub supervision_strategy: SupervisionStrategy,
    /// Maximum number of restart attempts within `restart_window_secs`.
    pub max_restarts: u32,
    /// Window (seconds) over which `max_restarts` is counted.
    /// If the actor runs longer than this without failing, the restart
    /// counter resets to zero.
    pub restart_window_secs: u64,
    /// Mailbox capacity. `None` means use `ActorSystemConfig::default_mailbox_size`.
    pub mailbox_size: Option<usize>,
    /// Dispatcher name for thread pool assignment.
    pub dispatcher: Option<String>,
    // ------------------------------------------------------------------
    // Exponential backoff (used by the Restart strategy)
    // ------------------------------------------------------------------
    /// Initial backoff before the first restart attempt (milliseconds).
    pub backoff_base_ms: u64,
    /// Maximum backoff cap (milliseconds).
    pub backoff_max_ms: u64,
    /// Jitter factor in [0.0, 1.0]. The actual delay is multiplied by
    /// `1.0 + jitter * rand(-1, 1)` so successive retries spread out.
    pub backoff_jitter: f64,
}

impl ActorProps {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.mailbox_size = Some(size);
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

    /// Configure restart limits (implies `SupervisionStrategy::Restart`).
    pub fn with_restart(mut self, max_restarts: u32, window_secs: u64) -> Self {
        self.supervision_strategy = SupervisionStrategy::Restart;
        self.max_restarts = max_restarts;
        self.restart_window_secs = window_secs;
        self
    }

    /// Override the exponential backoff parameters.
    pub fn with_backoff(mut self, base_ms: u64, max_ms: u64, jitter: f64) -> Self {
        self.backoff_base_ms = base_ms;
        self.backoff_max_ms = max_ms;
        self.backoff_jitter = jitter.clamp(0.0, 1.0);
        self
    }
}

impl Default for ActorProps {
    fn default() -> Self {
        Self {
            supervision_strategy: SupervisionStrategy::Stop,
            max_restarts: 3,
            restart_window_secs: 60,
            mailbox_size: None,
            dispatcher: None,
            backoff_base_ms: 100,
            backoff_max_ms: 60_000,
            backoff_jitter: 0.2,
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

    impl crate::Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[derive(Debug)]
    struct TestActor;

    impl Actor for TestActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}
    }

    #[tokio::test]
    async fn test_actor_props_defaults() {
        let props = ActorProps::default();
        assert_eq!(props.supervision_strategy, SupervisionStrategy::Stop);
        assert_eq!(props.max_restarts, 3);
        assert_eq!(props.restart_window_secs, 60);
        assert_eq!(props.backoff_base_ms, 100);
        assert_eq!(props.backoff_max_ms, 60_000);
        assert!((props.backoff_jitter - 0.2).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_with_restart_sets_strategy() {
        let props = ActorProps::new()
            .with_supervision(SupervisionStrategy::Restart)
            .with_restart(5, 120);

        assert_eq!(props.supervision_strategy, SupervisionStrategy::Restart);
        assert_eq!(props.max_restarts, 5);
        assert_eq!(props.restart_window_secs, 120);
    }

    #[tokio::test]
    async fn test_with_backoff() {
        let props = ActorProps::new().with_backoff(200, 30_000, 0.5);
        assert_eq!(props.backoff_base_ms, 200);
        assert_eq!(props.backoff_max_ms, 30_000);
        assert!((props.backoff_jitter - 0.5).abs() < f64::EPSILON);
    }
}
