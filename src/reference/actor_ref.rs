use crate::reference::ask::{ReplyTo, ask as ask_fn};
use crate::system::SystemMessage;
use crate::{ActorAddress, ActorError, AskError, Message};
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};
use tracing::warn;
use uuid::Uuid;

// ------------------------------------------------------------------
// Mailbox<M> — canonical routing object per actor incarnation
// ------------------------------------------------------------------

/// The typed routing object for a single actor incarnation.
///
/// Created once in [`ActorSystem::spawn_actor_with_address`] and shared via
/// `Arc` between the [`ActorRef`] and the system registry. The runner holds
/// only the receivers; the senders live here.
pub(crate) struct Mailbox<M: Message> {
    /// Stable per-incarnation id. Survives ref clones; changes on restart.
    pub(crate) incarnation_id: Uuid,
    /// Message channel sender (typed hot path).
    pub(crate) msg_tx: mpsc::Sender<ActorMessage<M>>,
    /// System channel sender (PoisonPill, Watch, …).
    pub(crate) sys_tx: mpsc::UnboundedSender<SystemMessage>,
    /// Actor lifecycle state — transitioned by the runner.
    pub(crate) state: Arc<RwLock<ActorState>>,
    /// Set to `false` by the runner when it exits.
    pub(crate) alive: Arc<AtomicBool>,
}

impl<M: Message> Mailbox<M> {
    pub(crate) async fn update_state(&self, new_state: ActorState) {
        *self.state.write().await = new_state;
    }
}

impl<M: Message> fmt::Debug for Mailbox<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mailbox")
            .field("incarnation_id", &self.incarnation_id)
            .finish_non_exhaustive()
    }
}

// ------------------------------------------------------------------
// AnyMailbox — type-erased handle stored in the system registry
// ------------------------------------------------------------------

/// Type-erased mailbox stored in the system registry.
///
/// Lets the registry send system messages and query liveness without knowing
/// the actor's message type `M`.
pub(crate) trait AnyMailbox: Send + Sync {
    fn send_system(&self, msg: SystemMessage) -> Result<(), ActorError>;
    fn is_alive(&self) -> bool;
    fn incarnation_id(&self) -> Uuid;
}

impl<M: Message> AnyMailbox for Mailbox<M> {
    fn send_system(&self, msg: SystemMessage) -> Result<(), ActorError> {
        self.sys_tx
            .send(msg)
            .map_err(|_| ActorError::MessageDeliveryFailed("System channel closed".to_string()))
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    fn incarnation_id(&self) -> Uuid {
        self.incarnation_id
    }
}

// ------------------------------------------------------------------
// ActorRouting — private routing enum
// ------------------------------------------------------------------

#[derive(Debug)]
enum ActorRouting<M: Message> {
    Local(Arc<Mailbox<M>>),
    Remote(RemoteEndpoint<M>),
}

#[derive(Debug)]
struct RemoteEndpoint<M: Message> {
    target_node: String,
    transport: Arc<dyn NetworkTransport<M>>,
}

// ------------------------------------------------------------------
// ActorState
// ------------------------------------------------------------------

/// Actor lifecycle state.
#[derive(Debug, Clone, PartialEq)]
pub enum ActorState {
    /// `pre_start` is running.
    Starting,
    /// Accepting and processing messages.
    Running,
    /// Received a stop signal; draining remaining messages.
    Stopping,
    /// Fully stopped; no more messages will be processed.
    Stopped,
    /// Panicked or returned an error from `handle` / `pre_start`.
    Failed(String),
}

// ------------------------------------------------------------------
// ActorMessage
// ------------------------------------------------------------------

/// Message envelope for actor communication.
///
/// Only `Tell` exists — ask-pattern reply channels are embedded in the message
/// itself via [`ReplyTo<R>`](crate::ReplyTo), so the mailbox is always uniform.
#[derive(Debug)]
pub enum ActorMessage<M: Message> {
    /// Regular tell (also carries ask-style messages when the message contains
    /// a [`ReplyTo`] field).
    Tell {
        message: M,
        sender: Option<ActorRef<M>>,
    },
}

// ------------------------------------------------------------------
// NetworkTransport
// ------------------------------------------------------------------

/// Trait for network transport implementations.
#[async_trait]
pub trait NetworkTransport<M: Message>: Send + Sync + fmt::Debug {
    fn send(
        &self,
        target_address: &ActorAddress,
        message: ActorMessage<M>,
    ) -> Result<(), ActorError>;

    fn is_reachable(&self, node_id: &str) -> bool;
}

// ------------------------------------------------------------------
// ActorRef — public handle
// ------------------------------------------------------------------

/// A typed, cloneable handle to an actor.
///
/// ## Equality
///
/// Equality follows Akka's semantics: two refs are equal iff they share the
/// same `address` **and** the same `incarnation_id`. Refs to different
/// incarnations at the same address (e.g. after a restart) are NOT equal.
///
/// ## Clone cost
///
/// `Clone` is `O(1)` — increments an `Arc` refcount and copies two value
/// types. No `M: Clone` bound required.
#[derive(Debug)]
pub struct ActorRef<M: Message> {
    /// Stable path identity.
    address: ActorAddress,
    /// Per-incarnation unique id. Never changes after construction.
    incarnation_id: Uuid,
    /// Routing — not serialised.
    routing: ActorRouting<M>,
}

impl<M: Message> ActorRef<M> {
    /// Create a local actor reference backed by `mailbox`.
    ///
    /// Called exclusively by [`ActorSystem::spawn_actor_with_address`].
    pub(crate) fn new_local(address: ActorAddress, mailbox: Arc<Mailbox<M>>) -> Self {
        let incarnation_id = mailbox.incarnation_id;
        Self {
            address,
            incarnation_id,
            routing: ActorRouting::Local(mailbox),
        }
    }

    /// Create a remote actor reference.
    pub fn new_remote(address: ActorAddress, transport: Arc<dyn NetworkTransport<M>>) -> Self {
        let target_node = address.node_id.clone();
        Self {
            address,
            // Placeholder — overwritten when deserialising a ref received over the wire.
            incarnation_id: Uuid::new_v4(),
            routing: ActorRouting::Remote(RemoteEndpoint {
                target_node,
                transport,
            }),
        }
    }

    /// Send a message (fire-and-forget).
    pub fn tell(&self, message: M, sender: Option<ActorRef<M>>) -> Result<(), ActorError> {
        let envelope = ActorMessage::Tell { message, sender };
        match &self.routing {
            ActorRouting::Local(mb) => mailbox_send(&mb.msg_tx, envelope),
            ActorRouting::Remote(ep) => remote_send(ep, &self.address, envelope),
        }
    }

    /// Ask an actor a question and wait for a typed reply.
    ///
    /// `make_msg` receives a [`ReplyTo<R>`] and must embed it in the returned message.
    ///
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

    /// Returns `true` if this ref points to a local actor.
    pub fn is_local(&self) -> bool {
        matches!(self.routing, ActorRouting::Local(_))
    }

    /// Returns the actor's current lifecycle state.
    ///
    /// Returns `None` for remote actors (state is not observable remotely).
    pub async fn state(&self) -> Option<ActorState> {
        match &self.routing {
            ActorRouting::Local(mb) => Some(mb.state.read().await.clone()),
            ActorRouting::Remote(_) => None,
        }
    }

    /// Returns `true` if the actor's runner is still active.
    pub fn is_alive(&self) -> bool {
        match &self.routing {
            ActorRouting::Local(mb) => mb.is_alive(),
            ActorRouting::Remote(_) => true, // optimistic
        }
    }

    /// Stop the actor gracefully.
    pub async fn stop(&self) -> Result<(), ActorError> {
        match &self.routing {
            ActorRouting::Local(mb) => mb.sys_tx.send(SystemMessage::PoisonPill).map_err(|_| {
                ActorError::MessageDeliveryFailed("System channel closed".to_string())
            }),
            ActorRouting::Remote(_) => Err(ActorError::MessageDeliveryFailed(
                "Remote actor stop not yet implemented".to_string(),
            )),
        }
    }

    /// Stop the actor (sync, non-blocking).
    ///
    /// Used by the type-erased children map; prefer [`stop`](Self::stop) where
    /// an async context is available.
    pub(crate) fn stop_sync(&self) -> Result<(), ActorError> {
        match &self.routing {
            ActorRouting::Local(mb) => mb.sys_tx.send(SystemMessage::PoisonPill).map_err(|_| {
                ActorError::MessageDeliveryFailed("System channel closed".to_string())
            }),
            ActorRouting::Remote(_) => Err(ActorError::MessageDeliveryFailed(
                "Remote actor stop not yet implemented".to_string(),
            )),
        }
    }

    /// The actor's address (path identity).
    pub fn address(&self) -> &ActorAddress {
        &self.address
    }

    /// The actor's incarnation id.
    ///
    /// Stable across clones of the same ref; changes when a new actor is
    /// spawned at the same address after the previous one stopped. Mirrors
    /// Akka's `path.uid`.
    pub fn incarnation_id(&self) -> Uuid {
        self.incarnation_id
    }
}

// ------------------------------------------------------------------
// Send helpers (free functions — keep hot path inline-able)
// ------------------------------------------------------------------

#[inline]
fn mailbox_send<M: Message>(
    tx: &mpsc::Sender<ActorMessage<M>>,
    msg: ActorMessage<M>,
) -> Result<(), ActorError> {
    match tx.try_send(msg) {
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

#[inline]
fn remote_send<M: Message>(
    ep: &RemoteEndpoint<M>,
    address: &ActorAddress,
    msg: ActorMessage<M>,
) -> Result<(), ActorError> {
    if !ep.transport.is_reachable(&ep.target_node) {
        warn!(
            "node {} unreachable — message dropped to DLQ",
            ep.target_node
        );
        return Ok(());
    }
    if let Err(e) = ep.transport.send(address, msg) {
        warn!("remote send failed — message dropped to DLQ: {e}");
    }
    Ok(())
}

// ------------------------------------------------------------------
// Clone — manual to avoid spurious `M: Clone` bound
// ------------------------------------------------------------------

impl<M: Message> Clone for ActorRef<M> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            incarnation_id: self.incarnation_id,
            routing: match &self.routing {
                ActorRouting::Local(mb) => ActorRouting::Local(Arc::clone(mb)),
                ActorRouting::Remote(ep) => ActorRouting::Remote(RemoteEndpoint {
                    target_node: ep.target_node.clone(),
                    transport: Arc::clone(&ep.transport),
                }),
            },
        }
    }
}

// ------------------------------------------------------------------
// PartialEq / Eq / Hash — Akka semantics: address + incarnation_id
// ------------------------------------------------------------------

impl<M: Message> PartialEq for ActorRef<M> {
    fn eq(&self, other: &Self) -> bool {
        self.incarnation_id == other.incarnation_id && self.address == other.address
    }
}

impl<M: Message> Eq for ActorRef<M> {}

impl<M: Message> std::hash::Hash for ActorRef<M> {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        self.incarnation_id.hash(h);
    }
}

// ------------------------------------------------------------------
// Display
// ------------------------------------------------------------------

impl<M: Message> fmt::Display for ActorRef<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorRef({}#{})", self.address, self.incarnation_id)
    }
}

// ------------------------------------------------------------------
// Serde — serialise as "{address}#{incarnation_id}"
//
// `serde` is already a hard dependency (used by ActorAddress / ActorPath).
// Only the address and incarnation_id cross the wire — the channel handles
// (`routing`) are process-local and never serialised.
//
// Deserialisation produces a ref with no routing — use `ActorRefResolver` to
// turn the address back into a live ref after deserialising.
// ------------------------------------------------------------------

impl<M: Message> serde::Serialize for ActorRef<M> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        // Wire format: "<address>#<incarnation_id>"
        s.serialize_str(&format!("{}#{}", self.address, self.incarnation_id))
    }
}

impl<'de, M: Message> serde::Deserialize<'de> for ActorRef<M> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        use std::str::FromStr as _;

        let s = String::deserialize(d)?;
        let (addr_str, uid_str) = s
            .split_once('#')
            .ok_or_else(|| D::Error::custom("expected format '<address>#<incarnation_id>'"))?;

        let address = ActorAddress::from_str(addr_str).map_err(D::Error::custom)?;
        let incarnation_id = Uuid::parse_str(uid_str).map_err(D::Error::custom)?;

        // Produce a ref with Remote routing as a stand-in — the caller must use
        // ActorRefResolver to obtain a live ref with proper Local routing.
        // Using DummyTransport signals "unresolved" clearly: all sends fail until
        // the ref is resolved via ActorRefResolver.
        Ok(Self {
            address,
            incarnation_id,
            routing: ActorRouting::Remote(RemoteEndpoint {
                target_node: String::new(),
                transport: Arc::new(DummyTransport),
            }),
        })
    }
}

// ------------------------------------------------------------------
// DummyTransport — test stub
// ------------------------------------------------------------------

/// Stub transport for tests — always returns an error / unreachable.
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

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ActorAddress, ActorPath};

    #[derive(Debug, Clone)]
    struct TestMsg;

    impl Message for TestMsg {
        fn type_id(&self) -> &'static str {
            "TestMsg"
        }
    }

    /// Helper: build a local ActorRef with fresh channels.
    fn make_ref(name: &str) -> ActorRef<TestMsg> {
        let (msg_tx, _) = mpsc::channel(1024);
        let (sys_tx, _) = mpsc::unbounded_channel();
        let path = ActorPath::user(name).unwrap();
        let address = ActorAddress::local(path);
        let mb = Arc::new(Mailbox {
            incarnation_id: Uuid::new_v4(),
            msg_tx,
            sys_tx,
            state: Arc::new(RwLock::new(ActorState::Starting)),
            alive: Arc::new(AtomicBool::new(true)),
        });
        ActorRef::new_local(address, mb)
    }

    #[tokio::test]
    async fn test_local_ref_creation() {
        let r = make_ref("test-actor");
        let path = ActorPath::user("test-actor").unwrap();
        assert!(r.is_local());
        assert_eq!(r.address(), &ActorAddress::local(path));
        assert_eq!(r.state().await, Some(ActorState::Starting));
    }

    #[tokio::test]
    async fn test_remote_ref_creation() {
        let path = ActorPath::user("test-actor").unwrap();
        let address = ActorAddress::new("remote-node", path).unwrap();
        let r: ActorRef<TestMsg> = ActorRef::new_remote(address.clone(), Arc::new(DummyTransport));
        assert!(!r.is_local());
        assert_eq!(r.address(), &address);
        assert_eq!(r.state().await, None);
    }

    #[tokio::test]
    async fn test_state_transitions() {
        let r = make_ref("test-actor");
        assert_eq!(r.state().await, Some(ActorState::Starting));

        if let ActorRouting::Local(mb) = &r.routing {
            mb.update_state(ActorState::Running).await;
        }
        assert_eq!(r.state().await, Some(ActorState::Running));

        if let ActorRouting::Local(mb) = &r.routing {
            mb.update_state(ActorState::Stopping).await;
        }
        assert_eq!(r.state().await, Some(ActorState::Stopping));
    }

    #[tokio::test]
    async fn test_equality_clone_is_equal() {
        let a = make_ref("actor");
        let b = a.clone();
        assert_eq!(a, b);
        assert_eq!(a.incarnation_id(), b.incarnation_id());
    }

    #[tokio::test]
    async fn test_equality_different_incarnation_same_address() {
        // Two independently spawned refs at the same address must NOT be equal.
        let a = make_ref("actor");
        let b = make_ref("actor");
        assert_ne!(a, b, "different incarnation_ids → not equal");
    }

    #[tokio::test]
    async fn test_is_alive_and_stop() {
        let (msg_tx, _msg_rx) = mpsc::channel(1024);
        let (sys_tx, mut sys_rx) = mpsc::unbounded_channel();
        let path = ActorPath::user("stoppable").unwrap();
        let address = ActorAddress::local(path);
        let alive = Arc::new(AtomicBool::new(true));
        let mb = Arc::new(Mailbox::<TestMsg> {
            incarnation_id: Uuid::new_v4(),
            msg_tx,
            sys_tx,
            state: Arc::new(RwLock::new(ActorState::Running)),
            alive: alive.clone(),
        });
        let r = ActorRef::new_local(address, mb);

        assert!(r.is_alive());
        r.stop().await.unwrap();
        assert_eq!(sys_rx.recv().await, Some(SystemMessage::PoisonPill));

        // Simulate runner exit: set alive = false.
        alive.store(false, Ordering::Release);
        assert!(!r.is_alive());
    }
}
