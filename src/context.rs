use crate::{Actor, ActorAddress, ActorError, ActorProps, ActorRef, Message};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info};
use uuid::Uuid;

/// Actor context provides the runtime environment for an actor
/// This is passed to actors during message handling and lifecycle events
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
}

/// Actor system manages the lifecycle of all actors
pub struct ActorSystem<M: Message> {
    /// System configuration
    config: ActorSystemConfig,
    /// All actors in the system by address
    actors: Arc<RwLock<HashMap<ActorAddress, ActorRef<M>>>>,
    /// Node ID for this actor system instance
    node_id: String,
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
}

impl Default for ActorSystemConfig {
    fn default() -> Self {
        Self {
            max_actors: 1_000_000,
            default_mailbox_size: 1000,
            distributed: false,
            bind_address: None,
            seed_nodes: Vec::new(),
        }
    }
}

impl<M: Message> ActorContext<M> {
    /// Create a new actor context
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
        }
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
    pub async fn send_to(
        &self,
        target: &ActorRef<M>,
        message: M,
    ) -> Result<(), ActorError> {
        target.tell(message, Some(self.actor_ref.clone())).await
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
            if let Err(e) = target.tell(message, sender).await {
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

impl<M: Message> ActorSystem<M> {
    /// Create a new actor system
    pub fn new(config: ActorSystemConfig) -> Result<Arc<Self>, ActorError> {
        let node_id = std::env::var("NODE_ID")
            .unwrap_or_else(|_| format!("node-{}", Uuid::new_v4()));

        let system = Arc::new(Self {
            config,
            actors: Arc::new(RwLock::new(HashMap::new())),
            node_id,
        });

        info!("Created actor system with node ID: {}", system.node_id);
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
        let path = crate::ActorPath::user(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;
        let address = ActorAddress::new(&self.node_id, path)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        self.spawn_actor_with_address(address, actor, props).await
    }

    /// Spawn an actor with a specific address
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
        {
            let actors = self.actors.read().await;
            if actors.contains_key(&address) {
                return Err(ActorError::ActorCreationFailed(
                    format!("Actor already exists at address: {}", address),
                ));
            }
        }

        // Create message channel
        let (sender, mut receiver) = mpsc::unbounded_channel();

        // Create actor reference
        let actor_ref = ActorRef::new_local(address.clone(), sender);

        // Create actor context
        let context = ActorContext::new(
            actor_ref.clone(),
            self.clone(),
            None, // TODO: Set parent for child actors
            props.clone(),
        );

        // Register actor
        {
            let mut actors = self.actors.write().await;
            actors.insert(address.clone(), actor_ref.clone());
        }

        // Spawn actor task
        let actor_ref_clone = actor_ref.clone();
        let context_arc = Arc::new(context);

        tokio::spawn(async move {
            // Call pre_start
            if let Err(e) = actor.pre_start(&context_arc).await {
                error!("Actor pre_start failed: {}", e);
                return;
            }

            // Update state to running
            if let Some(crate::ActorState::Starting) = actor_ref_clone.state().await {
                // TODO: Update state to Running
            }

            // Message processing loop
            while let Some(actor_message) = receiver.recv().await {
                match actor.handle(actor_message.message, &context_arc).await {
                    Ok(()) => {
                        debug!("Message processed successfully");
                    }
                    Err(e) => {
                        error!("Message handling failed: {}", e);

                        // Apply supervision strategy
                        let should_restart = actor.on_error(&e, &context_arc).await;
                        if !should_restart {
                            break;
                        }
                    }
                }
            }

            // Call post_stop
            if let Err(e) = actor.post_stop(&context_arc).await {
                error!("Actor post_stop failed: {}", e);
            }

            // TODO: Clean up actor from system registry
        });

        info!("Spawned actor: {}", address);
        Ok(actor_ref)
    }

    /// Get an actor by address
    pub async fn get_actor(&self, address: &ActorAddress) -> Option<ActorRef<M>> {
        let actors = self.actors.read().await;
        actors.get(address).cloned()
    }

    /// Get all actors in the system
    pub async fn get_all_actors(&self) -> Vec<ActorRef<M>> {
        let actors = self.actors.read().await;
        actors.values().cloned().collect()
    }

    /// Shutdown the actor system gracefully
    pub async fn shutdown(self: Arc<Self>) -> Result<(), ActorError> {
        info!("Shutting down actor system");

        // Stop all actors
        let actors = {
            let actors_guard = self.actors.read().await;
            actors_guard.values().cloned().collect::<Vec<_>>()
        };

        for actor_ref in actors {
            if let Err(e) = actor_ref.stop().await {
                error!("Failed to stop actor {}: {}", actor_ref.address(), e);
            }
        }

        info!("Actor system shutdown complete");
        Ok(())
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
    impl Actor<TestMessage> for TestActor {
        async fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            self.received_count += 1;
            self.received_messages.push(msg.data);
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[tokio::test]
    async fn test_actor_system_creation() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).unwrap();
        assert!(!system.node_id().is_empty());
    }

    #[tokio::test]
    async fn test_actor_spawning() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).await.unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
    }

    #[tokio::test]
    async fn test_message_sending() {
        let config = ActorSystemConfig::default();
        let system: Arc<ActorSystem<TestMessage>> = ActorSystem::new(config).unwrap();

        let actor = TestActor::default();
        let props = ActorProps::default();

        let actor_ref = system.spawn_actor("test-actor", actor, props).await.unwrap();

        let message = TestMessage {
            data: "Hello".to_string(),
        };

        // Give the actor a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let result = actor_ref.tell(message, None).await;
        assert!(result.is_ok());
    }
}