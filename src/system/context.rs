use crate::core::{Actor, ActorError, ActorFactoryArgs, ActorProps, Message};
use crate::reference::{ActorRef, ResponseEnvelope};
use crate::system::{
    ActorAddress, SystemMessage,
    extension::{Extension, ExtensionRegistry},
};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info};
use uuid::Uuid;

/// Response capability for ask pattern - embedded in context during ask handling
pub(crate) struct ResponseCapability {
    pub correlation_id: Uuid,
    sender: mpsc::UnboundedSender<ResponseEnvelope>,
}

impl ResponseCapability {
    pub(crate) fn new(
        correlation_id: Uuid,
        sender: mpsc::UnboundedSender<ResponseEnvelope>,
    ) -> Self {
        Self {
            correlation_id,
            sender,
        }
    }

    pub(crate) async fn send_response<R: Message + 'static>(
        &self,
        response: R,
    ) -> Result<(), ActorError> {
        let envelope = ResponseEnvelope {
            data: Box::new(response),
            type_name: std::any::type_name::<R>(),
            correlation_id: self.correlation_id,
        };
        self.sender.send(envelope).map_err(|_| {
            ActorError::MessageDeliveryFailed("Response channel closed".to_string())
        })?;
        Ok(())
    }
}

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
#[derive(Clone)]
pub struct ActorContext<M: Message> {
    pub actor_ref: ActorRef<M>,
    /// Reference to the non-generic actor system.
    pub system: Arc<ActorSystem>,
    /// Children spawned by this actor (type-erased for heterogeneous message types).
    children: Arc<RwLock<HashMap<String, Box<dyn AnyActorRef>>>>,
    /// Parent actor address (if this is a child actor).
    parent: Option<ActorAddress>,
    props: ActorProps,
    response_capability: Option<Arc<ResponseCapability>>,
    /// Set by stop_self() to signal the worker loop to tear down this actor.
    stop_requested: Arc<AtomicBool>,
}

// ------------------------------------------------------------------
// Type-erased dispatch layer
// ------------------------------------------------------------------

enum RunResult {
    Idle,
    Stop,
}

/// Type-erased interface used by the worker loop to drive any actor.
trait ActorRunner: Send + Sync {
    fn run_batch(&mut self, batch_size: usize) -> RunResult;
    fn do_post_stop(&mut self);
}

/// Concrete typed runner — owns the actor, its context, and its mailbox.
struct ActorRunnerImpl<A: Actor> {
    actor: A,
    context: Arc<ActorContext<A::Msg>>,
    receiver: mpsc::UnboundedReceiver<crate::reference::ActorMessage<A::Msg>>,
    system_receiver: mpsc::UnboundedReceiver<SystemMessage>,
    scheduled: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    address: ActorAddress,
    work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
}

impl<A: Actor> ActorRunnerImpl<A> {
    fn dispatch_one(&mut self, msg: crate::reference::ActorMessage<A::Msg>) {
        let ctx = Arc::clone(&self.context);
        match msg {
            crate::reference::ActorMessage::Tell { message, sender: _ } => {
                self.actor.handle(message, &ctx);
            }
            crate::reference::ActorMessage::Ask {
                request,
                message_id: _,
                timestamp: _,
            } => {
                let ask_ctx = ActorContext {
                    actor_ref: ctx.actor_ref.clone(),
                    system: ctx.system.clone(),
                    children: ctx.children.clone(),
                    parent: ctx.parent.clone(),
                    props: ctx.props.clone(),
                    response_capability: Some(Arc::new(ResponseCapability::new(
                        request.correlation_id,
                        request.response_to.sender,
                    ))),
                    stop_requested: ctx.stop_requested.clone(),
                };
                self.actor.handle(request.message, &ask_ctx);
            }
        }
    }
}

impl<A: Actor> ActorRunner for ActorRunnerImpl<A> {
    fn run_batch(&mut self, batch_size: usize) -> RunResult {
        // Check system channel first — PoisonPill takes priority over user messages.
        let mut should_stop = matches!(
            self.system_receiver.try_recv(),
            Ok(SystemMessage::PoisonPill)
        );

        if !should_stop {
            let mut processed = 0;
            while processed < batch_size {
                match self.receiver.try_recv() {
                    Ok(msg) => {
                        self.dispatch_one(msg);
                        processed += 1;
                        if self.stop_requested.load(Ordering::Acquire) {
                            should_stop = true;
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            // Rescheduling race-fix: clear scheduled BEFORE the final try_recv
            // so concurrent tell()s that arrive in the window push to the queue
            // themselves — closing the stuck-message race.
            if !should_stop {
                self.scheduled.store(false, Ordering::Release);
                if let Ok(msg) = self.receiver.try_recv() {
                    self.scheduled.store(true, Ordering::Release);
                    self.dispatch_one(msg);
                    if self.stop_requested.load(Ordering::Acquire) {
                        should_stop = true;
                    } else {
                        self.work_queue.push(self.address.clone());
                    }
                }
                // Err: truly empty — scheduled stays false
            }
        }

        if should_stop {
            RunResult::Stop
        } else {
            RunResult::Idle
        }
    }

    fn do_post_stop(&mut self) {
        if let Err(e) = self.actor.post_stop(&self.context) {
            error!("Actor post_stop failed for {}: {}", self.address, e);
        }
    }
}

// ------------------------------------------------------------------
// Worker pool
// ------------------------------------------------------------------

struct WorkerPool {
    shutdown: Arc<AtomicBool>,
}

impl WorkerPool {
    fn new(
        worker_count: usize,
        actor_storage: Arc<DashMap<ActorAddress, Box<dyn ActorRunner>>>,
        work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));

        for worker_id in 0..worker_count {
            let storage = actor_storage.clone();
            let queue = work_queue.clone();
            let shutdown_signal = shutdown.clone();

            tokio::spawn(async move {
                Self::worker_loop(worker_id, storage, queue, shutdown_signal).await;
            });
        }

        Self { shutdown }
    }

    async fn worker_loop(
        worker_id: usize,
        actor_storage: Arc<DashMap<ActorAddress, Box<dyn ActorRunner>>>,
        work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
        shutdown: Arc<AtomicBool>,
    ) {
        info!("Worker {} started", worker_id);
        // TODO aktor-0sm: move to ActorSystemConfig::throughput
        const BATCH_SIZE: usize = 10;

        while !shutdown.load(Ordering::Relaxed) {
            match work_queue.steal() {
                crossbeam::deque::Steal::Success(address) => {
                    let should_stop = {
                        let guard = actor_storage.get_mut(&address);
                        if let Some(mut runner) = guard {
                            let stop = matches!(runner.run_batch(BATCH_SIZE), RunResult::Stop);
                            if stop {
                                runner.do_post_stop();
                            }
                            stop
                        } else {
                            false
                        }
                    }; // RefMut released before remove
                    if should_stop {
                        actor_storage.remove(&address);
                        info!("Actor stopped: {}", address);
                    }
                }
                crossbeam::deque::Steal::Empty => {
                    tokio::task::yield_now().await;
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                }
                crossbeam::deque::Steal::Retry => {
                    continue;
                }
            }
        }

        info!("Worker {} stopped", worker_id);
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
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
    worker_pool: Arc<RwLock<Option<WorkerPool>>>,
    /// Type-erased per-actor state (actor + mailbox + context).
    actor_storage: Arc<DashMap<ActorAddress, Box<dyn ActorRunner>>>,
    work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
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
            children: Arc::new(RwLock::new(HashMap::new())),
            parent,
            props,
            response_capability: None,
            stop_requested,
        }
    }

    /// Send a response back (only works during ask handling).
    pub async fn respond<R: Message + 'static>(&self, response: R) -> Result<(), ActorError> {
        if let Some(capability) = &self.response_capability {
            capability.as_ref().send_response(response).await
        } else {
            Err(ActorError::MessageDeliveryFailed(
                "Cannot respond: not an ask request".to_string(),
            ))
        }
    }

    /// Returns true if this context is wrapping an ask request.
    pub fn is_ask_request(&self) -> bool {
        self.response_capability.is_some()
    }

    /// Get the correlation ID for the current ask request (if any).
    pub fn correlation_id(&self) -> Option<Uuid> {
        self.response_capability
            .as_ref()
            .map(|cap| cap.correlation_id)
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
    pub async fn spawn_child<A: Actor>(
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

        let child_ref = self
            .system
            .spawn_actor_with_address(child_address, actor, props, parent_addr)
            .await?;

        {
            let mut children = self.children.write().await;
            children.insert(name.to_string(), Box::new(child_ref.clone()));
        }

        info!("Spawned child actor: {}", child_ref.address());
        Ok(child_ref)
    }

    /// Stop a named child actor.
    pub async fn stop_child(&self, name: &str) -> Result<(), ActorError> {
        let child = {
            let mut children = self.children.write().await;
            children.remove(name)
        };
        if let Some(child_ref) = child {
            child_ref.stop_now()?;
            debug!("Stopped child actor: {}", name);
        }
        Ok(())
    }

    /// Stop all child actors.
    pub async fn stop_all_children(&self) -> Result<(), ActorError> {
        let children = {
            let mut guard = self.children.write().await;
            std::mem::take(&mut *guard)
        };
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

        let actor_storage: Arc<DashMap<ActorAddress, Box<dyn ActorRunner>>> =
            Arc::new(DashMap::new());
        let work_queue = Arc::new(crossbeam::deque::Injector::new());

        let system = Arc::new(Self {
            config: config.clone(),
            node_id,
            worker_pool: Arc::new(RwLock::new(None)),
            actor_storage: actor_storage.clone(),
            work_queue: work_queue.clone(),
            extensions: Arc::new(ExtensionRegistry::new()),
        });

        let worker_count = config.thread_pool_size;
        let worker_pool = WorkerPool::new(worker_count, actor_storage, work_queue);
        *system.worker_pool.write().await = Some(worker_pool);

        info!(
            "Created actor system with node ID: {} and {} workers",
            system.node_id, worker_count
        );
        Ok(system)
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn config(&self) -> &ActorSystemConfig {
        &self.config
    }

    /// Spawn an actor with an auto-generated address.
    pub async fn spawn_actor<A: Actor>(
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
            .await
    }

    /// Spawn an actor at a specific address with an optional parent address.
    pub async fn spawn_actor_with_address<A: Actor>(
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

        let (sender, receiver) = mpsc::unbounded_channel();
        let (system_sender, system_receiver) = mpsc::unbounded_channel::<SystemMessage>();

        let mut actor_ref = ActorRef::new_local(address.clone(), sender);

        let scheduled = Arc::new(AtomicBool::new(false));
        actor_ref.set_scheduling(self.work_queue.clone(), scheduled.clone());
        actor_ref.set_system_sender(system_sender);

        let stop_requested = Arc::new(AtomicBool::new(false));

        let context = Arc::new(ActorContext::new(
            actor_ref.clone(),
            self.clone(),
            parent,
            props.clone(),
            stop_requested.clone(),
        ));

        if let Err(e) = actor.pre_start(&context).await {
            return Err(ActorError::ActorCreationFailed(format!(
                "Actor pre_start failed: {}",
                e
            )));
        }

        let runner = Box::new(ActorRunnerImpl {
            actor,
            context,
            receiver,
            system_receiver,
            scheduled,
            stop_requested,
            address: address.clone(),
            work_queue: self.work_queue.clone(),
        }) as Box<dyn ActorRunner>;

        self.actor_storage.insert(address.clone(), runner);

        info!("Spawned actor: {}", address);
        Ok(actor_ref)
    }

    /// Spawn a `Default` actor by type.
    pub async fn actor_of<A: Actor + Default>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        self.spawn_actor(name, A::default(), ActorProps::default())
            .await
    }

    /// Spawn an actor using the `ActorFactoryArgs` trait.
    pub async fn actor_of_args<A, Args>(
        self: &Arc<Self>,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: ActorFactoryArgs<Args> + 'static,
        Args: Send + 'static,
    {
        let actor = A::create_args(args);
        self.spawn_actor(name, actor, ActorProps::default()).await
    }

    /// Spawn a `Default` actor with custom props.
    pub async fn actor_of_props<A: Actor + Default>(
        self: &Arc<Self>,
        name: &str,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        self.spawn_actor(name, A::default(), props).await
    }

    /// Spawn an actor with args and custom props.
    pub async fn actor_of_args_props<A, Args>(
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
        self.spawn_actor(name, actor, props).await
    }

    /// Returns true if an actor at `address` is currently alive in the system.
    pub fn contains_actor(&self, address: &ActorAddress) -> bool {
        self.actor_storage.contains_key(address)
    }

    /// Shut down the actor system gracefully.
    pub async fn shutdown(self: Arc<Self>) -> Result<(), ActorError> {
        info!("Shutting down actor system");

        if let Some(pool) = self.worker_pool.read().await.as_ref() {
            pool.shutdown();
        }

        for mut entry in self.actor_storage.iter_mut() {
            entry.value_mut().do_post_stop();
        }
        self.actor_storage.clear();

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
    use async_trait::async_trait;

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

    #[async_trait]
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

        let actor_ref = system
            .spawn_actor("test-actor", actor, props)
            .await
            .unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
    }

    #[tokio::test]
    async fn test_message_sending() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem> = ActorSystem::new(config).await.unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system
            .spawn_actor("test-actor", actor, props)
            .await
            .unwrap();

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

    #[async_trait]
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

        let actor_ref = system.actor_of::<TestActor>("test-actor").await.unwrap();

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
            .await
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

        assert_eq!(props.mailbox_size, 2000);
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
            .await
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
            .await
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

        let default_actor = system.actor_of::<TestActor>("default").await.unwrap();
        let param_actor = system
            .actor_of_args::<ParameterizedActor, _>("parameterized", ("worker-1".to_string(), 10))
            .await
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

        let actor1 = system.actor_of::<TestActor>("unique-name").await.unwrap();
        assert!(actor1.is_local());

        let result = system.actor_of::<TestActor>("unique-name").await;
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

    #[async_trait]
    impl Actor for ParentProbeActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}

        async fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
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
            .await
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
            .await
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
            .await
            .unwrap();

        // pre_start is awaited inside spawn_actor_with_address
        assert_eq!(
            *captured.lock().unwrap(),
            Some(parent_ref.address().to_string())
        );
    }

    #[derive(Debug, Default)]
    struct SelfStoppingActor;

    #[async_trait]
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
            .await
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
            .await
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

        #[async_trait]
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
            .await
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
    struct AsyncPreStartActor {
        initialized: Arc<std::sync::Mutex<bool>>,
        pre_start_child_address: Arc<std::sync::Mutex<Option<String>>>,
    }

    #[async_trait]
    impl Actor for AsyncPreStartActor {
        type Msg = TestMessage;

        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}

        async fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            tokio::task::yield_now().await;
            *self.initialized.lock().unwrap() = true;

            let child = ctx.spawn_child("child", TestActor::default(), None).await?;
            *self.pre_start_child_address.lock().unwrap() = Some(child.address().to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_async_pre_start_runs_before_first_message() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let initialized = Arc::new(std::sync::Mutex::new(false));
        let child_addr = Arc::new(std::sync::Mutex::new(None::<String>));

        system
            .spawn_actor(
                "async-init",
                AsyncPreStartActor {
                    initialized: initialized.clone(),
                    pre_start_child_address: child_addr.clone(),
                },
                ActorProps::default(),
            )
            .await
            .unwrap();

        assert!(
            *initialized.lock().unwrap(),
            "pre_start async work must complete before spawn returns"
        );
        assert!(
            child_addr.lock().unwrap().is_some(),
            "child spawned in async pre_start must be registered before spawn returns"
        );
    }

    #[derive(Debug)]
    struct PipeActor {
        phase: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
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
            .await
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

        #[async_trait]
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
            .await
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

        #[async_trait]
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
            .await
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

        #[async_trait]
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
            .await
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

        #[async_trait]
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

        #[async_trait]
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
            .await
            .unwrap();

        let sender_ref = system
            .spawn_actor(
                "sender-actor",
                SenderActor {
                    target: receiver_ref,
                },
                ActorProps::default(),
            )
            .await
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

        #[async_trait]
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
            .await
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
}
