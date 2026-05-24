use crate::core::{Actor, ActorError, ActorFactoryArgs, ActorProps, Message};
use crate::reference::ActorRef;
use crate::system::{
    ActorAddress, SystemMessage,
    extension::{Extension, ExtensionRegistry},
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tracing::{debug, error, info};
use uuid::Uuid;

/// Type-erased handle used for the children map.
/// Allows a parent actor to stop children regardless of their message type.
pub(crate) trait AnyActorRef: Send + Sync {
    fn stop_now(&self) -> Result<(), ActorError>;
}

impl<M: Message> AnyActorRef for ActorRef<M> {
    fn stop_now(&self) -> Result<(), ActorError> {
        self.stop_sync()
    }
}

/// Actor context provides the runtime environment for an actor.
/// Parameterised on the message type `M`, not on the actor type, so it can be
/// freely cloned and passed without the caller knowing the concrete actor type.
pub struct ActorContext<M: Message> {
    pub actor_ref: ActorRef<M>,
    /// Reference to the non-generic actor system.
    pub system: Arc<ActorSystem>,
    /// Children spawned by this actor (type-erased for heterogeneous message types).
    children: Arc<Mutex<HashMap<String, Box<dyn AnyActorRef>>>>,
    /// Parent actor address (if this is a child actor).
    parent: Option<ActorAddress>,
    props: ActorProps,
    /// Set by stop_self() to signal the worker loop to tear down this actor.
    stop_requested: Arc<AtomicBool>,
}

// Manual Clone — derive would add a spurious `M: Clone` bound even though
// we only clone Arcs and an ActorRef, never an `M` value itself.
impl<M: Message> Clone for ActorContext<M> {
    fn clone(&self) -> Self {
        Self {
            actor_ref: self.actor_ref.clone(),
            system: self.system.clone(),
            children: self.children.clone(),
            parent: self.parent.clone(),
            props: self.props.clone(),
            stop_requested: self.stop_requested.clone(),
        }
    }
}

// ------------------------------------------------------------------
// Per-actor runner
// ------------------------------------------------------------------

/// Concrete typed runner — owns the actor, its context, and its mailboxes.
/// Each actor gets its own tokio task that blocks on `recv().await`.
struct ActorRunnerImpl<A: Actor> {
    actor: A,
    context: Arc<ActorContext<A::Msg>>,
    receiver: mpsc::Receiver<crate::reference::ActorMessage<A::Msg>>,
    system_receiver: mpsc::UnboundedReceiver<SystemMessage>,
    stop_requested: Arc<AtomicBool>,
    address: ActorAddress,
}

impl<A: Actor> ActorRunnerImpl<A> {
    fn dispatch_one(&mut self, msg: crate::reference::ActorMessage<A::Msg>) {
        let ctx = Arc::clone(&self.context);
        match msg {
            crate::reference::ActorMessage::Tell { message, sender: _ } => {
                self.actor.handle(message, &ctx);
            }
        }
    }

    async fn run(
        mut self,
        actor_storage: Arc<DashMap<ActorAddress, mpsc::UnboundedSender<SystemMessage>>>,
    ) {
        'run: loop {
            tokio::select! {
                biased; // system channel checked first — PoisonPill takes priority
                sys = self.system_receiver.recv() => {
                    match sys {
                        Some(SystemMessage::PoisonPill) | None => break 'run,
                        Some(SystemMessage::ActorStopped { address }) => {
                            self.context.remove_child_by_address(&address);
                        }
                    }
                }
                msg = self.receiver.recv() => {
                    let Some(m) = msg else { break 'run };
                    self.dispatch_one(m);
                    if self.stop_requested.load(Ordering::Acquire) {
                        break 'run;
                    }
                    // Drain remaining messages without waiting (throughput batch)
                    loop {
                        match self.system_receiver.try_recv() {
                            Ok(SystemMessage::PoisonPill) => break 'run,
                            Ok(SystemMessage::ActorStopped { address }) => {
                                self.context.remove_child_by_address(&address);
                            }
                            Err(_) => {}
                        }
                        match self.receiver.try_recv() {
                            Ok(m) => {
                                self.dispatch_one(m);
                                if self.stop_requested.load(Ordering::Acquire) {
                                    break 'run;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        }

        if let Err(e) = self.actor.post_stop(&self.context) {
            error!("Actor post_stop failed for {}: {}", self.address, e);
        }
        actor_storage.remove(&self.address);

        // Notify parent so it can remove us from its children map.
        // Parent may already be gone — that's fine, we just skip.
        if let Some(parent_addr) = self.context.parent_address()
            && let Some(parent_sender) = actor_storage.get(parent_addr)
        {
            let _ = parent_sender.send(SystemMessage::ActorStopped {
                address: self.address.clone(),
            });
        }

        info!("Actor stopped: {}", self.address);
    }
}

// ------------------------------------------------------------------
// Actor system
// ------------------------------------------------------------------

/// The actor system. No longer generic over a message type — actors with
/// different `type Msg` can all coexist in the same system.
pub struct ActorSystem {
    config: ActorSystemConfig,
    node_id: String,
    /// System-channel senders keyed by address — used for shutdown and `contains_actor`.
    actor_storage: Arc<DashMap<ActorAddress, mpsc::UnboundedSender<SystemMessage>>>,
    extensions: Arc<ExtensionRegistry>,
}

/// Configuration for the actor system
#[derive(Debug, Clone)]
pub struct ActorSystemConfig {
    pub max_actors: usize,
    pub default_mailbox_size: usize,
    pub distributed: bool,
    pub bind_address: Option<String>,
    pub seed_nodes: Vec<String>,
    pub thread_pool_size: usize,
}

impl Default for ActorSystemConfig {
    fn default() -> Self {
        Self {
            max_actors: 1_000_000,
            default_mailbox_size: 1000,
            distributed: false,
            bind_address: None,
            seed_nodes: Vec::new(),
            thread_pool_size: 4,
        }
    }
}

// ------------------------------------------------------------------
// ActorContext<M> impl
// ------------------------------------------------------------------

impl<M: Message> ActorContext<M> {
    pub fn new(
        actor_ref: ActorRef<M>,
        system: Arc<ActorSystem>,
        parent: Option<ActorAddress>,
        props: ActorProps,
        stop_requested: Arc<AtomicBool>,
    ) -> Self {
        Self {
            actor_ref,
            system,
            children: Arc::new(Mutex::new(HashMap::new())),
            parent,
            props,
            stop_requested,
        }
    }

    /// Signal the worker loop to stop this actor after the current message returns.
    pub fn stop_self(&self) {
        self.stop_requested.store(true, Ordering::Release);
    }

    /// Spawn a future and deliver its result back to this actor's mailbox.
    ///
    /// `handle` stays sync and returns immediately. The future runs on the
    /// Tokio thread pool and the resolved `Ok(msg)` is sent as a normal tell.
    /// `Err(e)` is logged and dropped — encode errors as message variants if
    /// the actor needs to react to them.
    pub fn pipe_to_self<F, E>(&self, future: F)
    where
        F: std::future::Future<Output = Result<M, E>> + Send + 'static,
        E: std::fmt::Debug + Send + 'static,
    {
        let actor_ref = self.actor_ref.clone();
        tokio::spawn(async move {
            match future.await {
                Ok(msg) => {
                    if let Err(e) = actor_ref.tell(msg, None) {
                        tracing::error!("pipe_to_self delivery failed: {}", e);
                    }
                }
                Err(e) => {
                    tracing::error!("pipe_to_self future failed: {:?}", e);
                }
            }
        });
    }

    pub fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Spawn a child actor. The child can have any `Actor` type and message type.
    /// Callable from both `pre_start` and `handle` — no `.await` needed.
    pub fn spawn_child<A: Actor>(
        &self,
        name: &str,
        actor: A,
        props: Option<ActorProps>,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        let props = props.unwrap_or_default();

        let child_address = self
            .actor_ref
            .address()
            .child(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        let parent_addr = Some(self.actor_ref.address().clone());

        let child_ref =
            self.system
                .spawn_actor_with_address(child_address, actor, props, parent_addr)?;

        self.children
            .lock()
            .unwrap()
            .insert(name.to_string(), Box::new(child_ref.clone()));

        info!("Spawned child actor: {}", child_ref.address());
        Ok(child_ref)
    }

    /// Stop a named child actor.
    pub fn stop_child(&self, name: &str) -> Result<(), ActorError> {
        let child = self.children.lock().unwrap().remove(name);
        if let Some(child_ref) = child {
            child_ref.stop_now()?;
            debug!("Stopped child actor: {}", name);
        }
        Ok(())
    }

    /// Stop all child actors.
    pub fn stop_all_children(&self) -> Result<(), ActorError> {
        let children = std::mem::take(&mut *self.children.lock().unwrap());
        for (name, child) in children {
            if let Err(e) = child.stop_now() {
                error!("Failed to stop child actor {}: {}", name, e);
            }
        }
        Ok(())
    }

    /// Returns the parent actor's address, if this is a child actor.
    ///
    /// This is an address only — it cannot be used to send messages directly.
    /// Parent and child may have different message types, so there is no
    /// typed `ActorRef` to call `.tell()` on.
    ///
    /// **To send messages to the parent**, store the parent's `ActorRef<ParentMsg>`
    /// in the child actor's own fields and pass it at construction:
    ///
    /// ```ignore
    /// struct WorkerActor {
    ///     parent: ActorRef<SupervisorMsg>,
    /// }
    /// impl Actor for WorkerActor {
    ///     type Msg = WorkerMsg;
    ///     fn handle(&mut self, msg: WorkerMsg, _ctx: &ActorContext<WorkerMsg>) {
    ///         self.parent.tell(SupervisorMsg::Done, None).unwrap();
    ///     }
    /// }
    /// ```
    ///
    /// `parent_address()` is intended for supervision and death-watch only.
    pub fn parent_address(&self) -> Option<&ActorAddress> {
        self.parent.as_ref()
    }

    /// Send a message to another actor with the same message type.
    pub fn send_to(&self, target: &ActorRef<M>, message: M) -> Result<(), ActorError> {
        target.tell(message, Some(self.actor_ref.clone()))
    }

    pub fn props(&self) -> &ActorProps {
        &self.props
    }

    /// How many children this actor currently has. Useful for supervision introspection.
    pub fn children_count(&self) -> usize {
        self.children.lock().unwrap().len()
    }

    /// Remove a child from the children map by its full address.
    /// Called by the runner when it receives ActorStopped for a child.
    /// Private — only the runner in this module may call it.
    fn remove_child_by_address(&self, address: &ActorAddress) {
        if let Some(name) = address.name() {
            self.children.lock().unwrap().remove(name);
        }
    }

    /// Schedule a one-shot message delivery to `target` after `delay`.
    ///
    /// Callable directly from `handle` — no `.await` needed. Internally
    /// spawns a Tokio task and returns immediately, same shape as `pipe_to_self`.
    pub fn schedule_once(
        &self,
        delay: std::time::Duration,
        target: &ActorRef<M>,
        message: M,
    ) -> Uuid {
        let target = target.clone();
        let sender = Some(self.actor_ref.clone());
        let task_id = Uuid::new_v4();

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Err(e) = target.tell(message, sender) {
                error!("Scheduled message delivery failed: {}", e);
            }
        });

        task_id
    }

    /// Schedule a one-shot message back to this actor's own mailbox after `delay`.
    ///
    /// The Akka `timers.startSingleTimer` equivalent — synchronous at the call site,
    /// message arrives through the normal mailbox after the delay expires.
    ///
    /// ```ignore
    /// fn handle(&mut self, msg: DomainMsg, ctx: &ActorContext<DomainMsg>) {
    ///     match msg {
    ///         DomainMsg::Queue(url) => {
    ///             self.pending.push_back(url);
    ///             ctx.schedule_to_self(self.crawl_delay, DomainMsg::Dispatch);
    ///         }
    ///         DomainMsg::Dispatch => { /* send next URL to crawler */ }
    ///     }
    /// }
    /// ```
    pub fn schedule_to_self(&self, delay: std::time::Duration, message: M) -> Uuid {
        self.schedule_once(delay, &self.actor_ref.clone(), message)
    }
}

// ------------------------------------------------------------------
// ActorSystem impl
// ------------------------------------------------------------------

impl ActorSystem {
    pub async fn new(config: ActorSystemConfig) -> Result<Arc<Self>, ActorError> {
        let node_id =
            std::env::var("NODE_ID").unwrap_or_else(|_| format!("node-{}", Uuid::new_v4()));

        let system = Arc::new(Self {
            config,
            node_id: node_id.clone(),
            actor_storage: Arc::new(DashMap::new()),
            extensions: Arc::new(ExtensionRegistry::new()),
        });

        info!("Created actor system with node ID: {}", node_id);
        Ok(system)
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn config(&self) -> &ActorSystemConfig {
        &self.config
    }

    /// Spawn an actor with an auto-generated address.
    pub fn spawn_actor<A: Actor>(
        self: &Arc<Self>,
        name: &str,
        actor: A,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        let path = crate::system::ActorPath::user(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;
        let address = ActorAddress::new(&self.node_id, path)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        self.spawn_actor_with_address(address, actor, props, None)
    }

    /// Spawn an actor at a specific address with an optional parent address.
    pub fn spawn_actor_with_address<A: Actor>(
        self: &Arc<Self>,
        address: ActorAddress,
        mut actor: A,
        props: ActorProps,
        parent: Option<ActorAddress>,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        if self.actor_storage.contains_key(&address) {
            return Err(ActorError::ActorCreationFailed(format!(
                "Actor already exists at address: {}",
                address
            )));
        }

        let capacity = props
            .mailbox_size
            .unwrap_or(self.config.default_mailbox_size);
        let (sender, receiver) = mpsc::channel(capacity);
        let (system_sender, system_receiver) = mpsc::unbounded_channel::<SystemMessage>();

        let mut actor_ref = ActorRef::new_local(address.clone(), sender);
        actor_ref.set_system_sender(system_sender.clone());

        let stop_requested = Arc::new(AtomicBool::new(false));

        let context = Arc::new(ActorContext::new(
            actor_ref.clone(),
            self.clone(),
            parent,
            props.clone(),
            stop_requested.clone(),
        ));

        if let Err(e) = actor.pre_start(&context) {
            return Err(ActorError::ActorCreationFailed(format!(
                "Actor pre_start failed: {}",
                e
            )));
        }

        // Register before spawning so contains_actor() is immediately true.
        self.actor_storage.insert(address.clone(), system_sender);

        let runner = ActorRunnerImpl {
            actor,
            context,
            receiver,
            system_receiver,
            stop_requested,
            address: address.clone(),
        };

        let actor_storage = self.actor_storage.clone();
        tokio::spawn(async move {
            runner.run(actor_storage).await;
        });

        info!("Spawned actor: {}", address);
        Ok(actor_ref)
    }

    /// Spawn a `Default` actor by type.
    pub fn actor_of<A: Actor + Default>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        self.spawn_actor(name, A::default(), ActorProps::default())
    }

    /// Spawn an actor using the `ActorFactoryArgs` trait.
    pub fn actor_of_args<A, Args>(
        self: &Arc<Self>,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: ActorFactoryArgs<Args> + 'static,
        Args: Send + 'static,
    {
        let actor = A::create_args(args);
        self.spawn_actor(name, actor, ActorProps::default())
    }

    /// Spawn a `Default` actor with custom props.
    pub fn actor_of_props<A: Actor + Default>(
        self: &Arc<Self>,
        name: &str,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        self.spawn_actor(name, A::default(), props)
    }

    /// Spawn an actor with args and custom props.
    pub fn actor_of_args_props<A, Args>(
        self: &Arc<Self>,
        name: &str,
        args: Args,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: ActorFactoryArgs<Args> + 'static,
        Args: Send + 'static,
    {
        let actor = A::create_args(args);
        self.spawn_actor(name, actor, props)
    }

    /// Returns true if an actor at `address` is currently alive in the system.
    pub fn contains_actor(&self, address: &ActorAddress) -> bool {
        self.actor_storage.contains_key(address)
    }

    /// Shut down the actor system gracefully.
    pub async fn shutdown(self: Arc<Self>) -> Result<(), ActorError> {
        info!("Shutting down actor system");

        // Signal every live actor to stop.
        for entry in self.actor_storage.iter() {
            let _ = entry.value().send(SystemMessage::PoisonPill);
        }

        // Wait for all actor tasks to self-remove (5 s timeout).
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while !self.actor_storage.is_empty() {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    "Shutdown timeout — {} actors did not stop cleanly",
                    self.actor_storage.len()
                );
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        info!("Actor system shutdown complete");
        Ok(())
    }

    /// Register a shared extension (HTTP client, DB pool, etc.).
    ///
    /// # Panics
    /// Panics if an extension of this type is already registered.
    pub fn register_extension<T: Extension>(&self, extension: T) {
        self.extensions.register(extension);
    }

    /// Get a registered extension by type.
    ///
    /// # Panics
    /// Panics if the extension is not registered.
    pub fn extension<T: Extension>(&self) -> Arc<T> {
        self.extensions.get::<T>()
    }

    pub fn extension_optional<T: Extension>(&self) -> Option<Arc<T>> {
        self.extensions.get_optional::<T>()
    }

    pub fn get_or_create_extension<T: Extension>(&self) -> Arc<T> {
        self.extensions.get_or_create::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, ActorError, Message};

    #[derive(Debug, Clone)]
    struct TestMessage {
        data: String,
    }

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[derive(Debug)]
    struct TestActor {
        received_count: usize,
        received_messages: Vec<String>,
    }

    impl Default for TestActor {
        fn default() -> Self {
            Self {
                received_count: 0,
                received_messages: Vec::new(),
            }
        }
    }

    impl Actor for TestActor {
        type Msg = TestMessage;

        fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            self.received_count += 1;
            self.received_messages.push(msg.data);
        }
    }

    #[tokio::test]
    async fn test_actor_system_creation() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();
        assert!(!system.node_id().is_empty());
    }

    #[tokio::test]
    async fn test_actor_spawning() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
    }

    #[tokio::test]
    async fn test_message_sending() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).unwrap();

        let message = TestMessage {
            data: "Hello".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[derive(Debug)]
    struct ParameterizedActor {
        name: String,
        initial_value: i32,
        messages: Vec<String>,
    }

    impl ActorFactoryArgs<(String, i32)> for ParameterizedActor {
        fn create_args(args: (String, i32)) -> Self {
            Self {
                name: args.0,
                initial_value: args.1,
                messages: Vec::new(),
            }
        }
    }

    impl Actor for ParameterizedActor {
        type Msg = TestMessage;

        fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            self.messages.push(format!("{}: {}", self.name, msg.data));
        }
    }

    #[tokio::test]
    async fn test_actor_of() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let actor_ref = system.actor_of::<TestActor>("test-actor").unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));

        let message = TestMessage {
            data: "factory test".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_actor_of_args() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let args = ("worker".to_string(), 42);
        let actor_ref = system
            .actor_of_args::<ParameterizedActor, _>("param-actor", args)
            .unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("param-actor"));

        let message = TestMessage {
            data: "parameterized test".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_actor_factory_traits() {
        let factory = crate::DefaultActorFactory::<TestActor>::default();
        let actor = factory.create_actor();
        assert_eq!(actor.received_count, 0);

        let actor = ParameterizedActor::create_args(("test".to_string(), 100));
        assert_eq!(actor.name, "test");
        assert_eq!(actor.initial_value, 100);
        assert!(actor.messages.is_empty());
    }

    #[tokio::test]
    async fn test_props_builder() {
        use crate::SupervisionStrategy;

        let props = ActorProps::new()
            .with_mailbox_size(2000)
            .with_dispatcher("test-dispatcher")
            .with_supervision(SupervisionStrategy::Restart)
            .with_restart(5, 120);

        assert_eq!(props.mailbox_size, Some(2000));
        assert_eq!(props.dispatcher, Some("test-dispatcher".to_string()));
        assert_eq!(props.supervision_strategy, SupervisionStrategy::Restart);
        assert_eq!(props.max_restarts, 5);
        assert_eq!(props.restart_window_secs, 120);
        assert!(props.restart_on_failure);
    }

    #[tokio::test]
    async fn test_actor_of_props() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let props = ActorProps::new()
            .with_mailbox_size(5000)
            .with_dispatcher("custom-dispatcher");

        let actor_ref = system
            .actor_of_props::<TestActor>("props-actor", props)
            .unwrap();

        assert!(actor_ref.is_local());

        let message = TestMessage {
            data: "props test".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_actor_of_args_props() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let args = ("custom-worker".to_string(), 999);
        let props = ActorProps::new()
            .with_mailbox_size(3000)
            .with_supervision(crate::SupervisionStrategy::Restart);

        let actor_ref = system
            .actor_of_args_props::<ParameterizedActor, _>("args-props-actor", args, props)
            .unwrap();

        assert!(actor_ref.is_local());

        let message = TestMessage {
            data: "args props test".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_actors_different_factories() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let default_actor = system.actor_of::<TestActor>("default").unwrap();
        let param_actor = system
            .actor_of_args::<ParameterizedActor, _>("parameterized", ("worker-1".to_string(), 10))
            .unwrap();

        assert!(default_actor.is_local());
        assert!(param_actor.is_local());

        let msg1 = TestMessage {
            data: "msg1".to_string(),
        };
        let msg2 = TestMessage {
            data: "msg2".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(default_actor.tell(msg1, None).is_ok());
        assert!(param_actor.tell(msg2, None).is_ok());
    }

    #[tokio::test]
    async fn test_actor_name_uniqueness() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let actor1 = system.actor_of::<TestActor>("unique-name").unwrap();
        assert!(actor1.is_local());

        let result = system.actor_of::<TestActor>("unique-name");
        assert!(result.is_err());

        if let Err(ActorError::ActorCreationFailed(msg)) = result {
            assert!(msg.contains("Actor already exists"));
        } else {
            panic!("Expected ActorCreationFailed error");
        }
    }

    // Captures ctx.parent_address() during pre_start for test assertions.
    #[derive(Debug)]
    struct ParentProbeActor {
        captured: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl Actor for ParentProbeActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}

        fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            *self.captured.lock().unwrap() = ctx.parent_address().map(|a| a.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_root_actor_has_no_parent() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let captured = Arc::new(std::sync::Mutex::new(None::<String>));
        system
            .spawn_actor(
                "root",
                ParentProbeActor {
                    captured: captured.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        assert!(captured.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn test_child_actor_receives_parent_ref() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let parent_ref = system
            .spawn_actor("parent", TestActor::default(), ActorProps::default())
            .unwrap();

        let captured = Arc::new(std::sync::Mutex::new(None::<String>));
        let child_address = parent_ref.address().child("child").unwrap();
        system
            .spawn_actor_with_address(
                child_address,
                ParentProbeActor {
                    captured: captured.clone(),
                },
                ActorProps::default(),
                Some(parent_ref.address().clone()),
            )
            .unwrap();

        // pre_start runs synchronously inside spawn_actor_with_address
        assert_eq!(
            *captured.lock().unwrap(),
            Some(parent_ref.address().to_string())
        );
    }

    #[derive(Debug, Default)]
    struct SelfStoppingActor;

    impl Actor for SelfStoppingActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, ctx: &ActorContext<TestMessage>) {
            ctx.stop_self();
        }
    }

    #[tokio::test]
    async fn test_stop_self_removes_actor_from_system() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor("self-stopper", SelfStoppingActor, ActorProps::default())
            .unwrap();

        actor_ref
            .tell(TestMessage { data: "go".into() }, None)
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor should have been removed from the system after stop_self()"
        );
    }

    #[tokio::test]
    async fn test_poison_pill_removes_actor_from_system() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor("pill-target", TestActor::default(), ActorProps::default())
            .unwrap();

        actor_ref.stop().await.unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor should have been removed from the system after PoisonPill"
        );
    }

    #[tokio::test]
    async fn test_all_messages_delivered_across_batch_boundary() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct CountingActor {
            counter: Arc<AtomicUsize>,
        }

        impl Actor for CountingActor {
            type Msg = TestMessage;

            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                self.counter.fetch_add(1, AOrdering::Relaxed);
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor(
                "counter",
                CountingActor {
                    counter: counter.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        const MSG_COUNT: usize = 25;
        for i in 0..MSG_COUNT {
            actor_ref
                .tell(
                    TestMessage {
                        data: i.to_string(),
                    },
                    None,
                )
                .unwrap();
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert_eq!(
            counter.load(AOrdering::Relaxed),
            MSG_COUNT,
            "all messages must be delivered with no stuck messages"
        );
    }

    #[derive(Debug)]
    struct PreStartChildActor {
        child_address: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl Actor for PreStartChildActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}

        fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            let child = ctx.spawn_child("child", TestActor::default(), None)?;
            *self.child_address.lock().unwrap() = Some(child.address().to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_pre_start_runs_before_first_message() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let child_addr = Arc::new(std::sync::Mutex::new(None::<String>));

        system
            .spawn_actor(
                "pre-start-actor",
                PreStartChildActor {
                    child_address: child_addr.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        assert!(
            child_addr.lock().unwrap().is_some(),
            "child spawned in pre_start must be registered before spawn_actor returns"
        );
    }

    #[derive(Debug)]
    struct PipeActor {
        phase: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl Actor for PipeActor {
        type Msg = TestMessage;

        fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
            if msg.data == "start" {
                self.phase.lock().unwrap().push("handle:start".into());
                ctx.pipe_to_self(async {
                    tokio::task::yield_now().await;
                    Ok::<_, std::convert::Infallible>(TestMessage {
                        data: "piped".into(),
                    })
                });
            } else if msg.data == "piped" {
                self.phase.lock().unwrap().push("handle:piped".into());
            }
        }
    }

    #[tokio::test]
    async fn test_pipe_to_self_delivers_future_result_as_message() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let phase = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

        let actor_ref = system
            .spawn_actor(
                "pipe-actor",
                PipeActor {
                    phase: phase.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "start".into(),
                },
                None,
            )
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let observed = phase.lock().unwrap().clone();
        assert_eq!(
            observed,
            vec!["handle:start", "handle:piped"],
            "handle must return after pipe_to_self, then piped result must arrive as a message"
        );
    }

    #[tokio::test]
    async fn test_pipe_to_self_err_is_dropped() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let received = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let received_clone = received.clone();

        #[derive(Debug)]
        struct CountActor {
            count: Arc<std::sync::atomic::AtomicUsize>,
        }

        impl Actor for CountActor {
            type Msg = TestMessage;

            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                if msg.data == "start" {
                    ctx.pipe_to_self(async { Err::<TestMessage, &str>("simulated failure") });
                } else {
                    self.count
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        let actor_ref = system
            .spawn_actor(
                "err-pipe-actor",
                CountActor {
                    count: received_clone,
                },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "start".into(),
                },
                None,
            )
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(
            received.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "Err from pipe_to_self must not deliver a message"
        );
    }

    #[tokio::test]
    async fn test_pipe_to_self_does_not_block_subsequent_messages() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let gate = Arc::new(tokio::sync::Notify::new());

        #[derive(Debug)]
        struct InterleavingActor {
            log: Arc<std::sync::Mutex<Vec<String>>>,
            gate: Arc<tokio::sync::Notify>,
        }

        impl Actor for InterleavingActor {
            type Msg = TestMessage;

            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                if msg.data == "pipe" {
                    self.log.lock().unwrap().push("pipe".into());
                    let gate = self.gate.clone();
                    ctx.pipe_to_self(async move {
                        gate.notified().await;
                        Ok::<_, std::convert::Infallible>(TestMessage {
                            data: "piped".into(),
                        })
                    });
                } else {
                    self.log.lock().unwrap().push(msg.data.clone());
                }
            }
        }

        let actor_ref = system
            .spawn_actor(
                "interleave-actor",
                InterleavingActor {
                    log: log.clone(),
                    gate: gate.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "msg-a".into(),
                },
                None,
            )
            .unwrap();
        actor_ref
            .tell(
                TestMessage {
                    data: "pipe".into(),
                },
                None,
            )
            .unwrap();
        actor_ref
            .tell(
                TestMessage {
                    data: "msg-b".into(),
                },
                None,
            )
            .unwrap();
        actor_ref
            .tell(
                TestMessage {
                    data: "msg-c".into(),
                },
                None,
            )
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        {
            let observed = log.lock().unwrap().clone();
            assert!(
                observed.contains(&"msg-b".to_string()),
                "msg-b must be processed while future is in-flight"
            );
            assert!(
                observed.contains(&"msg-c".to_string()),
                "msg-c must be processed while future is in-flight"
            );
            assert!(
                !observed.contains(&"piped".to_string()),
                "piped must not arrive before gate is opened, got: {:?}",
                observed
            );
        }

        gate.notify_one();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let observed = log.lock().unwrap().clone();
        assert!(
            observed.contains(&"piped".to_string()),
            "piped must arrive after gate opens"
        );
        let msg_b_pos = observed.iter().position(|s| s == "msg-b").unwrap();
        let msg_c_pos = observed.iter().position(|s| s == "msg-c").unwrap();
        let piped_pos = observed.iter().position(|s| s == "piped").unwrap();
        assert!(
            piped_pos > msg_b_pos && piped_pos > msg_c_pos,
            "piped must appear after msg-b and msg-c, got: {:?}",
            observed
        );
    }

    // Verifies that schedule_to_self delivers a message to the actor after the
    // given delay without requiring .await in handle().
    #[tokio::test]
    async fn test_schedule_to_self_delivers_after_delay() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct TimerActor {
            counter: Arc<AtomicUsize>,
        }

        impl Actor for TimerActor {
            type Msg = TestMessage;

            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                if msg.data == "start" {
                    // schedule_to_self is sync — no .await
                    ctx.schedule_to_self(
                        std::time::Duration::from_millis(20),
                        TestMessage {
                            data: "tick".into(),
                        },
                    );
                } else if msg.data == "tick" {
                    self.counter.fetch_add(1, AOrdering::Relaxed);
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor(
                "timer-actor",
                TimerActor {
                    counter: counter.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "start".into(),
                },
                None,
            )
            .unwrap();

        // Before delay expires counter should still be zero.
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            0,
            "tick must not arrive before delay"
        );

        // After delay expires the tick message should have been delivered.
        tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            1,
            "tick must arrive after delay"
        );
    }

    // schedule_once can target a different actor (cross-actor scheduling).
    #[tokio::test]
    async fn test_schedule_once_cross_actor() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct SenderActor {
            target: ActorRef<TestMessage>,
        }

        impl Actor for SenderActor {
            type Msg = TestMessage;

            fn handle(&mut self, _msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                ctx.schedule_once(
                    std::time::Duration::from_millis(20),
                    &self.target,
                    TestMessage {
                        data: "ping".into(),
                    },
                );
            }
        }

        #[derive(Debug)]
        struct ReceiverActor {
            counter: Arc<AtomicUsize>,
        }

        impl Actor for ReceiverActor {
            type Msg = TestMessage;

            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                self.counter.fetch_add(1, AOrdering::Relaxed);
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let receiver_ref = system
            .spawn_actor(
                "receiver-actor",
                ReceiverActor {
                    counter: counter.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        let sender_ref = system
            .spawn_actor(
                "sender-actor",
                SenderActor {
                    target: receiver_ref,
                },
                ActorProps::default(),
            )
            .unwrap();

        sender_ref
            .tell(TestMessage { data: "go".into() }, None)
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            0,
            "must not arrive before delay"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            1,
            "must arrive after delay"
        );
    }

    // Multiple schedule_to_self calls from one handle invocation fire independently.
    #[tokio::test]
    async fn test_schedule_to_self_multiple_timers() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct MultiTimerActor {
            counter: Arc<AtomicUsize>,
        }

        impl Actor for MultiTimerActor {
            type Msg = TestMessage;

            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                if msg.data == "start" {
                    ctx.schedule_to_self(
                        std::time::Duration::from_millis(20),
                        TestMessage {
                            data: "tick".into(),
                        },
                    );
                    ctx.schedule_to_self(
                        std::time::Duration::from_millis(40),
                        TestMessage {
                            data: "tick".into(),
                        },
                    );
                } else if msg.data == "tick" {
                    self.counter.fetch_add(1, AOrdering::Relaxed);
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor(
                "multi-timer-actor",
                MultiTimerActor {
                    counter: counter.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "start".into(),
                },
                None,
            )
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            0,
            "neither tick should have fired yet"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            1,
            "first tick should have fired"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            2,
            "both ticks should have fired"
        );
    }

    // A self-stopping child must be removed from the parent's children map,
    // not just from actor_storage. Regression test for the memory leak where
    // dead Box<dyn AnyActorRef> entries accumulated in the parent's children map.
    #[tokio::test]
    async fn test_self_stopping_child_removed_from_parent_children_map() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};

        let reported_count = Arc::new(AtomicUsize::new(99)); // 99 = unreported sentinel

        #[derive(Debug)]
        struct ParentActor {
            child: Option<ActorRef<TestMessage>>,
            reported_count: Arc<AtomicUsize>,
        }

        impl Actor for ParentActor {
            type Msg = TestMessage;

            fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
                let child_ref = ctx.spawn_child("child", SelfStoppingActor, None)?;
                self.child = Some(child_ref);
                Ok(())
            }

            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                match msg.data.as_str() {
                    "trigger" => {
                        if let Some(child) = &self.child {
                            let _ = child.tell(TestMessage { data: "go".into() }, None);
                        }
                    }
                    "report" => {
                        self.reported_count
                            .store(ctx.children_count(), AOrdering::Relaxed);
                    }
                    _ => {}
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let parent_ref = system
            .spawn_actor(
                "parent",
                ParentActor {
                    child: None,
                    reported_count: reported_count.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        let child_addr = parent_ref.address().child("child").unwrap();

        // Trigger stop_self() on the child via the parent.
        parent_ref
            .tell(
                TestMessage {
                    data: "trigger".into(),
                },
                None,
            )
            .unwrap();

        // Wait for child to stop and ActorStopped to propagate to parent.
        tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;
        assert!(
            !system.contains_actor(&child_addr),
            "child must be removed from actor_storage after stop_self"
        );

        // Ask parent to report its current children count.
        parent_ref
            .tell(
                TestMessage {
                    data: "report".into(),
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(30)).await;

        assert_eq!(
            reported_count.load(AOrdering::Relaxed),
            0,
            "parent's children map must be empty after child self-stops (memory leak fix)"
        );
    }
}
