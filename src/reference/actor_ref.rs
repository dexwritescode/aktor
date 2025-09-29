use crate::{ActorAddress, ActorError, Message, AskError};
use crate::ask::AskRequest;
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

/// Actor reference - a handle to communicate with an actor
/// This provides location transparency - the same interface for local and remote actors
#[derive(Debug, Clone)]
pub struct ActorRef<M: Message> {
    /// Unique identifier for this actor reference
    pub id: Uuid,
    /// Actor address (location)
    pub address: ActorAddress,
    /// Internal implementation (local or remote)
    inner: ActorRefInner<M>,
}

/// Internal actor reference implementation
#[derive(Debug, Clone)]
enum ActorRefInner<M: Message> {
    /// Local actor reference with direct channel
    Local(LocalActorRef<M>),
    /// Remote actor reference with network transport
    Remote(RemoteActorRef<M>),
}

/// Local actor reference implementation
#[derive(Debug, Clone)]
pub struct LocalActorRef<M: Message> {
    /// Channel sender for message delivery
    sender: mpsc::UnboundedSender<ActorMessage<M>>,
    /// Actor lifecycle state
    state: Arc<RwLock<ActorState>>,
    /// Actor address for reactive scheduling
    address: ActorAddress,
    /// Work queue for reactive scheduling
    work_queue: Option<Arc<crossbeam::deque::Injector<ActorAddress>>>,
    /// Scheduled flag to prevent duplicate scheduling
    scheduled: Option<Arc<std::sync::atomic::AtomicBool>>,
}

/// Remote actor reference implementation
#[derive(Debug, Clone)]
pub struct RemoteActorRef<M: Message> {
    /// Target node for the remote actor
    target_node: String,
    /// Network transport for message delivery
    transport: Arc<dyn NetworkTransport<M>>,
}

/// Actor lifecycle state
#[derive(Debug, Clone, PartialEq)]
pub enum ActorState {
    /// Actor is starting up
    Starting,
    /// Actor is running and can receive messages
    Running,
    /// Actor is stopping and won't accept new messages
    Stopping,
    /// Actor has stopped
    Stopped,
    /// Actor failed and may be restarting
    Failed(String),
}

/// Message envelope for actor communication
#[derive(Debug)]
pub enum ActorMessage<M: Message> {
    /// Regular tell message
    Tell {
        /// The actual message
        message: M,
        /// Sender reference for replies
        sender: Option<ActorRef<M>>,
        // /// Message ID for tracking
        // message_id: Uuid,
        // /// Timestamp when message was sent
        // timestamp: std::time::SystemTime,
    },
    /// Ask pattern request message
    Ask {
        /// Ask request containing message and response channel
        request: AskRequest<M>,
        /// Message ID for tracking
        message_id: Uuid,
        /// Timestamp when message was sent
        timestamp: std::time::SystemTime,
    },
}

/// Trait for network transport implementations
#[async_trait]
pub trait NetworkTransport<M: Message>: Send + Sync + fmt::Debug {
    /// Send a message to a remote actor
    fn send(
        &self,
        target_address: &ActorAddress,
        message: ActorMessage<M>,
    ) -> Result<(), ActorError>;

    /// Check if the remote node is reachable
    fn is_reachable(&self, node_id: &str) -> bool;
}

impl<M: Message> ActorRef<M> {
    /// Create a new local actor reference
    pub fn new_local(
        address: ActorAddress,
        sender: mpsc::UnboundedSender<ActorMessage<M>>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            address: address.clone(),
            inner: ActorRefInner::Local(LocalActorRef {
                sender,
                state: Arc::new(RwLock::new(ActorState::Starting)),
                address,
                work_queue: None,
                scheduled: None,
            }),
        }
    }

    /// Set reactive scheduling components (called by ActorSystem after creation)
    pub(crate) fn set_scheduling(
        &mut self,
        work_queue: Arc<crossbeam::deque::Injector<ActorAddress>>,
        scheduled: Arc<std::sync::atomic::AtomicBool>,
    ) {
        if let ActorRefInner::Local(ref mut local_ref) = self.inner {
            local_ref.work_queue = Some(work_queue);
            local_ref.scheduled = Some(scheduled);
        }
    }

    /// Create a new remote actor reference
    pub fn new_remote(
        address: ActorAddress,
        transport: Arc<dyn NetworkTransport<M>>,
    ) -> Self {
        let target_node = address.node_id.clone();
        Self {
            id: Uuid::new_v4(),
            address,
            inner: ActorRefInner::Remote(RemoteActorRef {
                target_node,
                transport,
            }),
        }
    }

    /// Send a message to the actor (fire-and-forget)
    pub fn tell(&self, message: M, sender: Option<ActorRef<M>>) -> Result<(), ActorError> {
        let actor_message = ActorMessage::Tell {
            message,
            sender,
        };

        match &self.inner {
            ActorRefInner::Local(local_ref) => local_ref.send(actor_message),
            ActorRefInner::Remote(remote_ref) => remote_ref.send(&self.address, actor_message),
        }
    }

    /// Send an ask request (internal method)
    pub(crate) async fn tell_ask_request(&self, request: AskRequest<M>) -> Result<(), ActorError> {
        let actor_message = ActorMessage::Ask {
            request,
            message_id: Uuid::new_v4(),
            timestamp: std::time::SystemTime::now(),
        };

        match &self.inner {
            ActorRefInner::Local(local_ref) => local_ref.send(actor_message),
            ActorRefInner::Remote(remote_ref) => remote_ref.send(&self.address, actor_message),
        }
    }

    /// Send a message and wait for a response (ask pattern)
    pub async fn ask<R>(&self, message: M, timeout: Duration) -> Result<R, AskError>
    where
        R: Message + 'static,
        M: Message,
    {
        // We need a system reference to use the ask function
        // For now, we'll create a placeholder implementation
        // This will be properly integrated when we have access to the system
        crate::ask::ask_with_actor_ref(self, message, timeout).await
    }

    /// Check if this reference points to a local actor
    pub fn is_local(&self) -> bool {
        matches!(self.inner, ActorRefInner::Local(_))
    }

    /// Get the actor's current state (only for local actors)
    pub async fn state(&self) -> Option<ActorState> {
        match &self.inner {
            ActorRefInner::Local(local_ref) => {
                Some(local_ref.state.read().await.clone())
            }
            ActorRefInner::Remote(_) => None,
        }
    }

    /// Stop the actor gracefully
    pub async fn stop(&self) -> Result<(), ActorError> {
        match &self.inner {
            ActorRefInner::Local(local_ref) => {
                let mut state = local_ref.state.write().await;
                *state = ActorState::Stopping;
                // TODO: Send stop message to actor
                Ok(())
            }
            ActorRefInner::Remote(_) => {
                // TODO: Send remote stop message
                Err(ActorError::MessageDeliveryFailed(
                    "Remote actor stop not yet implemented".to_string(),
                ))
            }
        }
    }

    /// Get the actor's address
    pub fn address(&self) -> &ActorAddress {
        &self.address
    }

    /// Get the actor's unique ID
    pub fn id(&self) -> Uuid {
        self.id
    }
}

impl<M: Message> LocalActorRef<M> {
    /// Send a message to the local actor with reactive scheduling
    fn send(&self, message: ActorMessage<M>) -> Result<(), ActorError> {
        self.sender
            .send(message)
            .map_err(|e| ActorError::MessageDeliveryFailed(e.to_string()))?;

        // Reactively schedule actor if not already scheduled
        if let (Some(work_queue), Some(scheduled)) = (&self.work_queue, &self.scheduled) {
            // Use compare-and-swap to atomically check and set scheduled flag
            if !scheduled.swap(true, std::sync::atomic::Ordering::AcqRel) {
                // Actor was not scheduled, push it to the work queue
                work_queue.push(self.address.clone());
            }
        }

        Ok(())
    }

    /// Update the actor's state
    pub async fn update_state(&self, new_state: ActorState) {
        let mut state = self.state.write().await;
        *state = new_state;
    }
}

impl<M: Message> RemoteActorRef<M> {
    /// Send a message to the remote actor
    fn send(
        &self,
        target_address: &ActorAddress,
        message: ActorMessage<M>,
    ) -> Result<(), ActorError> {
        if !self.transport.is_reachable(&self.target_node) {
            return Err(ActorError::NetworkError(
                format!("Node {} is not reachable", self.target_node),
            ));
        }

        self.transport.send(target_address, message)
    }
}

impl<M: Message> fmt::Display for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorRef({})", self.address)
    }
}

impl<M: Message> PartialEq for ActorRef<M> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<M: Message> Eq for ActorRef<M> {}

impl<M: Message> std::hash::Hash for ActorRef<M> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

/// Dummy network transport for testing
#[derive(Debug)]
pub struct DummyTransport;

#[async_trait]
impl<M: Message> NetworkTransport<M> for DummyTransport {
    fn send(
        &self,
        _target_address: &ActorAddress,
        _message: ActorMessage<M>,
    ) -> Result<(), ActorError> {
        Err(ActorError::NetworkError(
            "DummyTransport: not implemented".to_string(),
        ))
    }

    fn is_reachable(&self, _node_id: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActorPath, ActorAddress};

    #[derive(Debug, Clone)]
    struct TestMessage {
    }

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[tokio::test]
    async fn test_local_actor_ref_creation() {
        let (sender, _receiver) = mpsc::unbounded_channel();
        let path = ActorPath::user("test-actor").unwrap();
        let address = ActorAddress::local(path);

        let actor_ref: ActorRef<TestMessage> = ActorRef::new_local(address.clone(), sender);

        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address(), &address);
    }

    #[tokio::test]
    async fn test_remote_actor_ref_creation() {
        let path = ActorPath::user("test-actor").unwrap();
        let address = ActorAddress::new("remote-node", path).unwrap();
        let transport = Arc::new(DummyTransport);

        let actor_ref: ActorRef<TestMessage> = ActorRef::new_remote(address.clone(), transport);

        assert!(!actor_ref.is_local());
        assert_eq!(actor_ref.address(), &address);
    }

    #[tokio::test]
    async fn test_actor_state_management() {
        let (sender, _receiver) = mpsc::unbounded_channel();
        let path = ActorPath::user("test-actor").unwrap();
        let address = ActorAddress::local(path);

        let actor_ref: ActorRef<TestMessage> = ActorRef::new_local(address, sender);

        // Check initial state
        let state = actor_ref.state().await.unwrap();
        assert_eq!(state, ActorState::Starting);

        // Update state through inner reference
        if let ActorRefInner::Local(local_ref) = &actor_ref.inner {
            local_ref.update_state(ActorState::Running).await;
        }

        let state = actor_ref.state().await.unwrap();
        assert_eq!(state, ActorState::Running);
    }
}