use crate::core::{Actor, ActorError, Message, ActorFactoryArgs, ActorProps};
use crate::system::{ActorAddress, extension::{Extension, ExtensionRegistry}};
use crate::reference::{ActorRef, ResponseEnvelope};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use dashmap::DashMap;
use tracing::{debug, error, info};
use uuid::Uuid;
use std::sync::atomic::{AtomicBool, Ordering};

/// Response capability for ask pattern - embedded in context during ask handling
pub(crate) struct ResponseCapability {
    /// Correlation ID for this ask request
    pub correlation_id: Uuid,
    /// Channel to send response back
    sender: mpsc::UnboundedSender<ResponseEnvelope>,
}

impl ResponseCapability {
    pub(crate) fn new(correlation_id: Uuid, sender: mpsc::UnboundedSender<ResponseEnvelope>) -> Self {
        Self {
            correlation_id,
            sender,
        }
    }

    /// Send a response back through this capability
    pub(crate) async fn send_response<R: Message + 'static>(&self, response: R) -> Result<(), ActorError> {
        let envelope = ResponseEnvelope {
            data: Box::new(response),
            type_name: std::any::type_name::<R>(),
            correlation_id: self.correlation_id,
        };

        self.sender
            .send(envelope)
            .map_err(|_| ActorError::MessageDeliveryFailed("Response channel closed".to_string()))?;

        Ok(())
    }
}

/// Actor context provides the runtime environment for an actor
/// This is passed to actors during message handling and lifecycle events
#[derive(Clone)]
pub struct ActorContext<M: Message> {
    /// Reference to this actor
    pub actor_ref: ActorRef<M>,
    /// Reference to the actor system
    pub system: Arc<ActorSystem<M>>,
    /// Child actors spawned by this actor
    children: Arc<RwLock<HashMap<String, ActorRef<M>>>>,
    /// Parent actor reference (if this is a child actor)
    parent: Option<ActorRef<M>>,
    /// Actor properties and configuration
    props: ActorProps,
    /// Response capability (only present during ask requests)
    response_capability: Option<Arc<ResponseCapability>>,
}

/// Worker pool for high-performance actor message processing
struct WorkerPool {
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
}

/// Actor storage in the worker pool
struct ActorData<M: Message> {
    /// The actor instance
    actor: Box<dyn Actor<M>>,
    /// Actor context
    context: Arc<ActorContext<M>>,
    /// Message receiver
    receiver: mpsc::UnboundedReceiver<crate::reference::ActorMessage<M>>,
    /// Flag to prevent duplicate scheduling in work queue
    scheduled: Arc<AtomicBool>,
}

/// Actor system manages the lifecycle of all actors
pub struct ActorSystem<M: Message> {
    /// System configuration
    config: ActorSystemConfig,
    /// All actors in the system by address
    actors: Arc<DashMap<ActorAddress, ActorRef<M>>>,
    /// Node ID for this actor system instance
    node_id: String,
    /// Worker pool for processing actor messages
    worker_pool: Arc<RwLock<Option<WorkerPool>>>,
    /// Actor storage (actors + contexts + receivers) - DashMap for fine-grained locking
    actor_storage: Arc<DashMap<ActorAddress, ActorData<M>>>,
    /// Work-stealing queue for reactive scheduling
    work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
    /// Extension registry for shared resources (HTTP clients, DB pools, etc.)
    extensions: Arc<crate::system::extension::ExtensionRegistry>,
}

/// Configuration for the actor system
#[derive(Debug, Clone)]
pub struct ActorSystemConfig {
    /// Maximum number of actors in the system
    pub max_actors: usize,
    /// Default message mailbox size
    pub default_mailbox_size: usize,
    /// Enable distributed mode
    pub distributed: bool,
    /// Network bind address for distributed mode
    pub bind_address: Option<String>,
    /// Cluster seed nodes for discovery
    pub seed_nodes: Vec<String>,
    /// Number of threads in the actor execution thread pool
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

impl<M: Message> ActorContext<M> {
    /// Create a new actor context for regular (tell) messages
    pub fn new(
        actor_ref: ActorRef<M>,
        system: Arc<ActorSystem<M>>,
        parent: Option<ActorRef<M>>,
        props: ActorProps,
    ) -> Self {
        Self {
            actor_ref,
            system,
            children: Arc::new(RwLock::new(HashMap::new())),
            parent,
            props,
            response_capability: None,
        }
    }

    /// Create a new actor context with response capability for ask messages
    pub(crate) fn with_response_capability(
        actor_ref: ActorRef<M>,
        system: Arc<ActorSystem<M>>,
        parent: Option<ActorRef<M>>,
        props: ActorProps,
        response_capability: ResponseCapability,
    ) -> Self {
        Self {
            actor_ref,
            system,
            children: Arc::new(RwLock::new(HashMap::new())),
            parent,
            props,
            response_capability: Some(Arc::new(response_capability)),
        }
    }

    /// Send a response back (only works during ask handling)
    pub async fn respond<R: Message + 'static>(&self, response: R) -> Result<(), ActorError> {
        if let Some(capability) = &self.response_capability {
            capability.as_ref().send_response(response).await
        } else {
            Err(ActorError::MessageDeliveryFailed(
                "Cannot respond: not an ask request".to_string(),
            ))
        }
    }

    /// Check if this is an ask request that expects a response
    pub fn is_ask_request(&self) -> bool {
        self.response_capability.is_some()
    }

    /// Get the correlation ID for the current ask request (if any)
    pub fn correlation_id(&self) -> Option<Uuid> {
        self.response_capability.as_ref().map(|cap| cap.correlation_id)
    }

    /// Get reference to this actor
    pub fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    /// Get reference to the actor system
    pub fn system(&self) -> &ActorSystem<M> {
        &self.system
    }

    /// Spawn a child actor
    pub async fn spawn_child<A>(
        &self,
        name: &str,
        actor: A,
        props: Option<ActorProps>,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: Actor<M> + 'static,
    {
        let props = props.unwrap_or_default();

        // Create child address
        let child_address = self.actor_ref.address()
            .child(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        // Create the child actor reference
        let child_ref = self.system.spawn_actor_with_address(child_address, actor, props).await?;

        // Register child
        {
            let mut children = self.children.write().await;
            children.insert(name.to_string(), child_ref.clone());
        }

        info!("Spawned child actor: {}", child_ref.address());
        Ok(child_ref)
    }

    /// Get a child actor by name
    pub async fn get_child(&self, name: &str) -> Option<ActorRef<M>> {
        let children = self.children.read().await;
        children.get(name).cloned()
    }

    /// Get all child actors
    pub async fn get_children(&self) -> Vec<ActorRef<M>> {
        let children = self.children.read().await;
        children.values().cloned().collect()
    }

    /// Stop a child actor
    pub async fn stop_child(&self, name: &str) -> Result<(), ActorError> {
        let child = {
            let mut children = self.children.write().await;
            children.remove(name)
        };

        if let Some(child_ref) = child {
            child_ref.stop().await?;
            debug!("Stopped child actor: {}", name);
        }

        Ok(())
    }

    /// Stop all child actors
    pub async fn stop_all_children(&self) -> Result<(), ActorError> {
        let children = {
            let mut children_guard = self.children.write().await;
            let children = children_guard.clone();
            children_guard.clear();
            children
        };

        for (name, child_ref) in children {
            if let Err(e) = child_ref.stop().await {
                error!("Failed to stop child actor {}: {}", name, e);
            }
        }

        Ok(())
    }

    /// Get the parent actor reference
    pub fn parent(&self) -> Option<&ActorRef<M>> {
        self.parent.as_ref()
    }

    /// Send a message to another actor
    pub fn send_to(
        &self,
        target: &ActorRef<M>,
        message: M,
    ) -> Result<(), ActorError> {
        target.tell(message, Some(self.actor_ref.clone()))
    }

    /// Look up an actor by address
    pub async fn select(&self, address: &ActorAddress) -> Option<ActorRef<M>> {
        self.system.get_actor(address).await
    }

    /// Get actor properties
    pub fn props(&self) -> &ActorProps {
        &self.props
    }

    /// Schedule a message to be sent after a delay
    pub async fn schedule_once(
        &self,
        delay: std::time::Duration,
        target: &ActorRef<M>,
        message: M,
    ) -> Result<Uuid, ActorError> {
        let target = target.clone();
        let sender = Some(self.actor_ref.clone());
        let task_id = Uuid::new_v4();

        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if let Err(e) = target.tell(message, sender) {
                error!("Scheduled message delivery failed: {}", e);
            }
        });

        Ok(task_id)
    }

    /// Watch another actor for termination
    pub async fn watch(&self, target: &ActorRef<M>) -> Result<(), ActorError> {
        // TODO: Implement actor death watch
        // This will be implemented with proper supervision in Phase 3
        debug!("Watching actor: {}", target.address());
        Ok(())
    }

    /// Stop watching an actor
    pub async fn unwatch(&self, target: &ActorRef<M>) -> Result<(), ActorError> {
        // TODO: Implement actor death watch removal
        debug!("Unwatching actor: {}", target.address());
        Ok(())
    }
}

impl WorkerPool {
    /// Create a new worker pool with work-stealing architecture
    fn new<M: Message>(
        worker_count: usize,
        actor_storage: Arc<DashMap<ActorAddress, ActorData<M>>>,
        work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
    ) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::new();

        // Start worker tasks
        for worker_id in 0..worker_count {
            let storage = actor_storage.clone();
            let queue = work_queue.clone();
            let shutdown_signal = shutdown.clone();

            let worker = tokio::spawn(async move {
                Self::worker_loop(worker_id, storage, queue, shutdown_signal).await;
            });

            workers.push(worker);
        }

        Self {
            shutdown,
        }
    }

    /// Worker loop with work-stealing - only processes actors with pending messages
    async fn worker_loop<M: Message>(
        worker_id: usize,
        actor_storage: Arc<DashMap<ActorAddress, ActorData<M>>>,
        work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
        shutdown: Arc<AtomicBool>
    ) {
        info!("Worker {} started", worker_id);
        const BATCH_SIZE: usize = 10; // Process up to 10 messages per actor before re-scheduling

        while !shutdown.load(Ordering::Relaxed) {
            // Try to steal an actor address from the work queue
            match work_queue.steal() {
                crossbeam::deque::Steal::Success(address) => {
                    // Get mutable access to the actor
                    if let Some(mut actor_entry) = actor_storage.get_mut(&address) {
                        let mut processed = 0;

                        // Clone context Arc to avoid borrow checker issues
                        let context = actor_entry.context.clone();

                        // Process messages in batches for better cache locality
                        while processed < BATCH_SIZE {
                            match actor_entry.receiver.try_recv() {
                                Ok(message) => {
                                    match message {
                                        crate::reference::ActorMessage::Tell { message, sender: _ } => {
                                            // Process tell message synchronously
                                            actor_entry.actor.handle(message, &context);
                                        }
                                        crate::reference::ActorMessage::Ask { request, message_id: _, timestamp: _ } => {
                                            // Create ask context with response capability
                                            let response_capability = ResponseCapability::new(
                                                request.correlation_id,
                                                request.response_to.sender,
                                            );

                                            let ask_context = ActorContext::with_response_capability(
                                                context.actor_ref.clone(),
                                                context.system.clone(),
                                                context.parent.clone(),
                                                context.props.clone(),
                                                response_capability,
                                            );

                                            // Process ask message synchronously
                                            actor_entry.actor.handle(request.message, &ask_context);
                                        }
                                    }
                                    processed += 1;
                                }
                                Err(_) => break, // No more messages for this actor
                            }
                        }

                        // Re-schedule actor if more messages remain
                        match actor_entry.receiver.try_recv() {
                            Ok(message) => {
                                // Put the message back (we can't, so just process it)
                                match message {
                                    crate::reference::ActorMessage::Tell { message, sender: _ } => {
                                        actor_entry.actor.handle(message, &context);
                                    }
                                    crate::reference::ActorMessage::Ask { request, message_id: _, timestamp: _ } => {
                                        let response_capability = ResponseCapability::new(
                                            request.correlation_id,
                                            request.response_to.sender,
                                        );
                                        let ask_context = ActorContext::with_response_capability(
                                            context.actor_ref.clone(),
                                            context.system.clone(),
                                            context.parent.clone(),
                                            context.props.clone(),
                                            response_capability,
                                        );
                                        actor_entry.actor.handle(request.message, &ask_context);
                                    }
                                }
                                // Re-push to queue if there might be more messages
                                work_queue.push(address);
                            }
                            Err(_) => {
                                // No more messages - clear scheduled flag
                                actor_entry.scheduled.store(false, Ordering::Release);
                            }
                        }
                    }
                }
                crossbeam::deque::Steal::Empty => {
                    // No work available - yield briefly
                    tokio::task::yield_now().await;
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                }
                crossbeam::deque::Steal::Retry => {
                    // Retry immediately
                    continue;
                }
            }
        }

        info!("Worker {} stopped", worker_id);
    }

    /// Shutdown the worker pool
    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

impl<M: Message> ActorSystem<M> {
    /// Create a new actor system
    pub async fn new(config: ActorSystemConfig) -> Result<Arc<Self>, ActorError> {
        let node_id = std::env::var("NODE_ID")
            .unwrap_or_else(|_| format!("node-{}", Uuid::new_v4()));

        let actor_storage = Arc::new(DashMap::new());
        let work_queue = Arc::new(crossbeam::deque::Injector::new());

        let system = Arc::new(Self {
            config: config.clone(),
            actors: Arc::new(DashMap::new()),
            node_id,
            worker_pool: Arc::new(RwLock::new(None)),
            actor_storage: actor_storage.clone(),
            work_queue: work_queue.clone(),
            extensions: Arc::new(ExtensionRegistry::new()),
        });

        // Automatically start worker pool
        let worker_count = config.thread_pool_size;
        let worker_pool = WorkerPool::new(worker_count, actor_storage, work_queue);
        *system.worker_pool.write().await = Some(worker_pool);

        info!("Created actor system with node ID: {} and {} workers", system.node_id, worker_count);
        Ok(system)
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Get system configuration
    pub fn config(&self) -> &ActorSystemConfig {
        &self.config
    }

    /// Spawn an actor with automatic address generation
    pub async fn spawn_actor<A>(
        self: &Arc<Self>,
        name: &str,
        actor: A,
        props: ActorProps,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: Actor<M> + 'static,
    {
        let path = crate::system::ActorPath::user(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;
        let address = ActorAddress::new(&self.node_id, path)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        self.spawn_actor_with_address(address, actor, props).await
    }

    /// Spawn an actor with a specific address (unified worker pool architecture)
    pub async fn spawn_actor_with_address<A>(
        self: &Arc<Self>,
        address: ActorAddress,
        mut actor: A,
        props: ActorProps,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: Actor<M> + 'static,
    {
        // Check if actor already exists
        if self.actors.contains_key(&address) {
            return Err(ActorError::ActorCreationFailed(
                format!("Actor already exists at address: {}", address),
            ));
        }

        // Create message channel for this actor
        let (sender, receiver) = mpsc::unbounded_channel();

        // Create actor reference
        let mut actor_ref = ActorRef::new_local(address.clone(), sender);

        // Create scheduled flag for reactive scheduling
        let scheduled = Arc::new(AtomicBool::new(false));

        // Set up reactive scheduling on the actor reference
        actor_ref.set_scheduling(self.work_queue.clone(), scheduled.clone());

        // Create actor context
        let context = Arc::new(ActorContext::new(
            actor_ref.clone(),
            self.clone(),
            None, // TODO: Set parent for child actors
            props.clone(),
        ));

        // Call pre_start
        if let Err(e) = actor.pre_start(&context) {
            return Err(ActorError::ActorCreationFailed(
                format!("Actor pre_start failed: {}", e),
            ));
        }

        // Add actor to the worker pool storage
        let actor_data = ActorData {
            actor: Box::new(actor),
            context,
            receiver,
            scheduled,
        };

        // Insert into DashMap (no need for write lock)
        self.actor_storage.insert(address.clone(), actor_data);

        // Register actor
        self.actors.insert(address.clone(), actor_ref.clone());

        info!("Spawned actor: {}", address);
        Ok(actor_ref)
    }

    /// Create an actor using the default factory (for actors with Default trait)
    pub async fn actor_of<A>(
        self: &Arc<Self>,
        name: &str,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: Actor<M> + Default + 'static,
    {
        let actor = A::default();
        let props = ActorProps::default();
        self.spawn_actor(name, actor, props).await
    }

    /// Create an actor with arguments using ActorFactoryArgs trait
    pub async fn actor_of_args<A, Args>(
        self: &Arc<Self>,
        name: &str,
        args: Args,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: ActorFactoryArgs<M, Args> + 'static,
        Args: Send + 'static,
    {
        let actor = A::create_args(args);
        let props = ActorProps::default();
        self.spawn_actor(name, actor, props).await
    }

    /// Create an actor with custom props
    pub async fn actor_of_props<A>(
        self: &Arc<Self>,
        name: &str,
        props: ActorProps,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: Actor<M> + Default + 'static,
    {
        let actor = A::default();
        self.spawn_actor(name, actor, props).await
    }

    /// Create an actor with arguments and custom props
    pub async fn actor_of_args_props<A, Args>(
        self: &Arc<Self>,
        name: &str,
        args: Args,
        props: ActorProps,
    ) -> Result<ActorRef<M>, ActorError>
    where
        A: ActorFactoryArgs<M, Args> + 'static,
        Args: Send + 'static,
    {
        let actor = A::create_args(args);
        self.spawn_actor(name, actor, props).await
    }

    /// Get an actor by address
    pub async fn get_actor(&self, address: &ActorAddress) -> Option<ActorRef<M>> {
        self.actors.get(address).map(|entry| entry.value().clone())
    }

    /// Get all actors in the system
    pub async fn get_all_actors(&self) -> Vec<ActorRef<M>> {
        self.actors.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Shutdown the actor system gracefully
    pub async fn shutdown(self: Arc<Self>) -> Result<(), ActorError> {
        info!("Shutting down actor system");

        // Shutdown worker pool
        if let Some(pool) = self.worker_pool.read().await.as_ref() {
            pool.shutdown();
        }

        // Call post_stop on all actors
        for mut entry in self.actor_storage.iter_mut() {
            let (address, actor_data) = entry.pair_mut();
            if let Err(e) = actor_data.actor.post_stop(&actor_data.context) {
                error!("Actor post_stop failed for {}: {}", address, e);
            }
        }
        self.actor_storage.clear();

        // Clear actor registry
        self.actors.clear();

        info!("Actor system shutdown complete");
        Ok(())
    }

    /// Register an extension with the actor system
    ///
    /// Extensions are shared resources (HTTP clients, DB pools, etc.) that actors can access.
    ///
    /// # Panics
    /// Panics if an extension of this type is already registered.
    ///
    /// # Example
    /// ```ignore
    /// system.register_extension(HttpClientExtension::new());
    /// ```
    pub fn register_extension<T: Extension>(&self, extension: T) {
        self.extensions.register(extension);
    }

    /// Get an extension by type
    ///
    /// # Panics
    /// Panics if the extension is not registered.
    ///
    /// # Example
    /// ```ignore
    /// let http = system.extension::<HttpClientExtension>();
    /// ```
    pub fn extension<T: Extension>(&self) -> Arc<T> {
        self.extensions.get::<T>()
    }

    /// Get an extension by type, returning None if not registered
    ///
    /// # Example
    /// ```ignore
    /// if let Some(http) = system.extension_optional::<HttpClientExtension>() {
    ///     // Use http client
    /// }
    /// ```
    pub fn extension_optional<T: Extension>(&self) -> Option<Arc<T>> {
        self.extensions.get_optional::<T>()
    }

    /// Get or create an extension
    ///
    /// If the extension is already registered, returns it.
    /// Otherwise, creates a new instance and registers it.
    ///
    /// # Example
    /// ```ignore
    /// let http = system.get_or_create_extension::<HttpClientExtension>();
    /// ```
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

    impl Actor<TestMessage> for TestActor {
        fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            self.received_count += 1;
            self.received_messages.push(msg.data);
        }
    }

    #[tokio::test]
    async fn test_actor_system_creation() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();
        assert!(!system.node_id().is_empty());
    }

    #[tokio::test]
    async fn test_actor_spawning() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).await.unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
    }

    #[tokio::test]
    async fn test_message_sending() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).await.unwrap();

        let message = TestMessage {
            data: "Hello".to_string(),
        };

        // Give the actor a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    // Test actor with arguments for factory testing
    #[derive(Debug)]
    struct ParameterizedActor {
        name: String,
        initial_value: i32,
        messages: Vec<String>,
    }

    impl ActorFactoryArgs<TestMessage, (String, i32)> for ParameterizedActor {
        fn create_args(args: (String, i32)) -> Self {
            Self {
                name: args.0,
                initial_value: args.1,
                messages: Vec::new(),
            }
        }
    }

    impl Actor<TestMessage> for ParameterizedActor {
        fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            self.messages.push(format!("{}: {}", self.name, msg.data));
        }
    }

    #[tokio::test]
    async fn test_actor_of() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Test actor_of with Default actors
        let actor_ref = system.actor_of::<TestActor>("test-actor").await.unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));

        // Test that we can send messages
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
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Test actor_of_args with parameterized actors
        let args = ("worker".to_string(), 42);
        let actor_ref = system.actor_of_args::<ParameterizedActor, _>("param-actor", args).await.unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("param-actor"));

        // Test message sending
        let message = TestMessage {
            data: "parameterized test".to_string(),
        };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_actor_factory_traits() {
        // Test DefaultActorFactory
        let factory = crate::DefaultActorFactory::<TestActor>::default();
        let actor = factory.create_actor::<TestMessage>();
        assert_eq!(actor.received_count, 0);

        // Test ActorFactoryArgs
        let actor = ParameterizedActor::create_args(("test".to_string(), 100));
        assert_eq!(actor.name, "test");
        assert_eq!(actor.initial_value, 100);
        assert!(actor.messages.is_empty());
    }

    #[tokio::test]
    async fn test_props_builder() {
        use crate::SupervisionStrategy;

        // Test Props builder pattern
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
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Create actor with custom props
        let props = ActorProps::new()
            .with_mailbox_size(5000)
            .with_dispatcher("custom-dispatcher");

        let actor_ref = system.actor_of_props::<TestActor>("props-actor", props).await.unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("props-actor"));

        // Test message sending
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
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Create actor with arguments and custom props
        let args = ("custom-worker".to_string(), 999);
        let props = ActorProps::new()
            .with_mailbox_size(3000)
            .with_supervision(crate::SupervisionStrategy::Restart);

        let actor_ref = system.actor_of_args_props::<ParameterizedActor, _>(
            "args-props-actor",
            args,
            props
        ).await.unwrap();

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("args-props-actor"));

        // Test message sending
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
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Create multiple actors using different factory methods
        let default_actor = system.actor_of::<TestActor>("default").await.unwrap();
        let param_actor = system.actor_of_args::<ParameterizedActor, _>(
            "parameterized",
            ("worker-1".to_string(), 10)
        ).await.unwrap();

        // Verify they're both working
        assert!(default_actor.is_local());
        assert!(param_actor.is_local());
        assert_eq!(default_actor.address().name(), Some("default"));
        assert_eq!(param_actor.address().name(), Some("parameterized"));

        // Send messages to both
        let msg1 = TestMessage { data: "msg1".to_string() };
        let msg2 = TestMessage { data: "msg2".to_string() };

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert!(default_actor.tell(msg1, None).is_ok());
        assert!(param_actor.tell(msg2, None).is_ok());
    }

    #[tokio::test]
    async fn test_actor_name_uniqueness() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).await.unwrap();

        // Create first actor
        let actor1 = system.actor_of::<TestActor>("unique-name").await.unwrap();
        assert!(actor1.is_local());

        // Try to create second actor with same name - should fail
        let result = system.actor_of::<TestActor>("unique-name").await;
        assert!(result.is_err());

        if let Err(ActorError::ActorCreationFailed(msg)) = result {
            assert!(msg.contains("Actor already exists"));
        } else {
            panic!("Expected ActorCreationFailed error");
        }
    }
}