use crate::system::SystemMessage;
use crate::{ActorAddress, ActorError, AskError, Message};
use crate::reference::ask::{ReplyTo, ask as ask_fn};
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;
use uuid::Uuid;

/// Actor reference - a handle to communicate with an actor
/// This provides location transparency - the same interface for local and remote actors
#[derive(Debug)]
pub struct ActorRef<M: Message> {
    /// Unique identifier for this actor reference
    pub id: Uuid,
    /// Actor address (location)
    pub address: ActorAddress,
    /// Internal implementation (local or remote)
    inner: ActorRefInner<M>,
}

/// Internal actor reference implementation
#[derive(Debug)]
enum ActorRefInner<M: Message> {
    /// Local actor reference with direct channel
    Local(LocalActorRef<M>),
    /// Remote actor reference with network transport
    Remote(RemoteActorRef<M>),
}

/// Local actor reference implementation
#[derive(Debug)]
pub struct LocalActorRef<M: Message> {
    /// Channel sender for message delivery
    sender: mpsc::Sender<ActorMessage<M>>,
    /// Channel sender for system-level signals (PoisonPill, Watch, etc.)
    system_sender: Option<mpsc::UnboundedSender<SystemMessage>>,
    /// Actor lifecycle state
    state: Arc<RwLock<ActorState>>,
}

/// Remote actor reference implementation
#[derive(Debug)]
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

/// Message envelope for actor communication.
///
/// Only `Tell` exists — ask-pattern reply channels are embedded in the message
/// itself via [`ReplyTo<R>`](crate::ReplyTo), so the mailbox is always uniform.
#[derive(Debug)]
pub enum ActorMessage<M: Message> {
    /// Regular tell message (the only variant; also carries ask-style messages
    /// when the message contains a [`ReplyTo`] field).
    Tell {
        /// The actual message
        message: M,
        /// Sender reference for replies
        sender: Option<ActorRef<M>>,
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
    pub fn new_local(address: ActorAddress, sender: mpsc::Sender<ActorMessage<M>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            address: address.clone(),
            inner: ActorRefInner::Local(LocalActorRef {
                sender,
                system_sender: None,
                state: Arc::new(RwLock::new(ActorState::Starting)),
            }),
        }
    }

    /// Set the system message sender (called by ActorSystem after creation)
    pub(crate) fn set_system_sender(
        &mut self,
        system_sender: mpsc::UnboundedSender<SystemMessage>,
    ) {
        if let ActorRefInner::Local(ref mut local_ref) = self.inner {
            local_ref.system_sender = Some(system_sender);
        }
    }

    /// Create a new remote actor reference
    pub fn new_remote(address: ActorAddress, transport: Arc<dyn NetworkTransport<M>>) -> Self {
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
        let actor_message = ActorMessage::Tell { message, sender };

        match &self.inner {
            ActorRefInner::Local(local_ref) => local_ref.send(actor_message),
            ActorRefInner::Remote(remote_ref) => remote_ref.send(&self.address, actor_message),
        }
    }

    /// Ask an actor a question and wait for a typed reply.
    ///
    /// `make_msg` receives a [`ReplyTo<R>`] and must embed it in the returned
    /// message. The actor calls `reply_to.reply(value)` in its handler.
    ///
    /// # Example
    /// ```ignore
    /// let count: u64 = actor_ref
    ///     .ask(|rt| CounterMsg::GetCount { reply_to: rt }, Duration::from_secs(5))
    ///     .await?;
    /// ```
    pub async fn ask<R, F>(&self, make_msg: F, timeout: Duration) -> Result<R, AskError>
    where
        R: Send + 'static,
        F: FnOnce(ReplyTo<R>) -> M,
    {
        ask_fn(self, make_msg, timeout).await
    }

    /// Check if this reference points to a local actor
    pub fn is_local(&self) -> bool {
        matches!(self.inner, ActorRefInner::Local(_))
    }

    /// Get the actor's current state (only for local actors)
    pub async fn state(&self) -> Option<ActorState> {
        match &self.inner {
            ActorRefInner::Local(local_ref) => Some(local_ref.state.read().await.clone()),
            ActorRefInner::Remote(_) => None,
        }
    }

    /// Stop the actor gracefully
    pub async fn stop(&self) -> Result<(), ActorError> {
        match &self.inner {
            ActorRefInner::Local(local_ref) => {
                let sender = local_ref.system_sender.as_ref().ok_or_else(|| {
                    ActorError::MessageDeliveryFailed(
                        "Actor system sender not initialised".to_string(),
                    )
                })?;
                sender.send(SystemMessage::PoisonPill).map_err(|_| {
                    ActorError::MessageDeliveryFailed("System channel closed".to_string())
                })
            }
            ActorRefInner::Remote(_) => Err(ActorError::MessageDeliveryFailed(
                "Remote actor stop not yet implemented".to_string(),
            )),
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

    /// Synchronous stop — sends a PoisonPill without blocking.
    /// Used by the type-erased children map in ActorContext.
    pub fn stop_sync(&self) -> Result<(), ActorError> {
        match &self.inner {
            ActorRefInner::Local(local_ref) => {
                let sender = local_ref.system_sender.as_ref().ok_or_else(|| {
                    ActorError::MessageDeliveryFailed(
                        "Actor system sender not initialised".to_string(),
                    )
                })?;
                sender
                    .send(crate::system::SystemMessage::PoisonPill)
                    .map_err(|_| {
                        ActorError::MessageDeliveryFailed("System channel closed".to_string())
                    })
            }
            ActorRefInner::Remote(_) => Err(ActorError::MessageDeliveryFailed(
                "Remote actor stop not yet implemented".to_string(),
            )),
        }
    }
}

impl<M: Message> LocalActorRef<M> {
    fn send(&self, message: ActorMessage<M>) -> Result<(), ActorError> {
        match self.sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("mailbox full — message dropped to DLQ");
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!("actor stopped — message dropped to DLQ");
                Ok(())
            }
        }
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
            warn!(
                "node {} unreachable — message dropped to DLQ",
                self.target_node
            );
            return Ok(());
        }

        if let Err(e) = self.transport.send(target_address, message) {
            warn!("remote send failed — message dropped to DLQ: {e}");
        }
        Ok(())
    }
}

// Manual Clone impls — derive would add a spurious `M: Clone` bound even
// though we only clone Arcs and channel senders, never an `M` value itself.
impl<M: Message> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            address: self.address.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<M: Message> Clone for ActorRefInner<M> {
    fn clone(&self) -> Self {
        match self {
            ActorRefInner::Local(l) => ActorRefInner::Local(l.clone()),
            ActorRefInner::Remote(r) => ActorRefInner::Remote(r.clone()),
        }
    }
}

impl<M: Message> Clone for LocalActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            system_sender: self.system_sender.clone(),
            state: self.state.clone(),
        }
    }
}

impl<M: Message> Clone for RemoteActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            target_node: self.target_node.clone(),
            transport: self.transport.clone(),
        }
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
    use crate::{ActorAddress, ActorPath};

    #[derive(Debug, Clone)]
    struct TestMessage {}

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[tokio::test]
    async fn test_local_actor_ref_creation() {
        let (sender, _receiver) = mpsc::channel(1024);
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
        let (sender, _receiver) = mpsc::channel(1024);
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
