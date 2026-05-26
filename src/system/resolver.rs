//! Actor reference resolver — converts address strings back into live [`ActorRef`]s.
//!
//! This is the local-only half of what Akka calls `ActorRefResolver`. Remote
//! resolution (deserialising a ref received over the network and routing through
//! the transport layer) is deferred to the distributed-messaging feature (`aktor-b9b`).
//!
//! # Usage
//!
//! ```ignore
//! let resolver = system.resolver();
//!
//! // Obtain a live ref from a serialised address string.
//! let actor_ref: ActorRef<MyMsg> = resolver
//!     .resolve::<MyMsg>("local://user/my-actor#<incarnation_id>")
//!     .expect("actor not found or stale");
//! ```
//!
//! # Address format
//!
//! Serialised `ActorRef` strings follow `"{address}#{incarnation_id}"` where
//! `address` is the `ActorAddress` display form and `incarnation_id` is a UUID.
//! `resolve` parses this format; it also accepts a plain address string (no `#`
//! suffix) and returns the live actor at that address regardless of incarnation.

use crate::Message;
use crate::reference::ActorRef;
use crate::reference::actor_ref::Mailbox;
use crate::system::ActorAddress;
use crate::system::context::RegistryEntry;
use dashmap::DashMap;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Resolves address strings to live, typed [`ActorRef`]s.
///
/// Obtained via [`ActorSystem::resolver`].  Holds a shared reference to the
/// system registry — always reflects the current live set of actors.
pub struct ActorRefResolver {
    registry: Arc<DashMap<ActorAddress, RegistryEntry>>,
}

impl ActorRefResolver {
    pub(crate) fn new(registry: Arc<DashMap<ActorAddress, RegistryEntry>>) -> Self {
        Self { registry }
    }

    /// Resolve a serialised actor ref string to a live [`ActorRef<M>`].
    ///
    /// Accepts two formats:
    /// - `"{address}#{incarnation_id}"` — exact incarnation match; returns `None`
    ///   if the actor has been restarted since the ref was serialised.
    /// - `"{address}"` — returns the current live actor at that address regardless
    ///   of incarnation; useful for human-readable addresses in config.
    ///
    /// Returns `None` if:
    /// - The address string cannot be parsed.
    /// - No live actor exists at the parsed address.
    /// - A `#{uid}` suffix was given and the live actor's incarnation id doesn't match.
    /// - The caller supplied the wrong `M` (safe: returns `None`, no UB).
    pub fn resolve<M: Message>(&self, addr_str: &str) -> Option<ActorRef<M>> {
        // Split on `#` to separate address from optional incarnation_id.
        let (addr_part, uid_part) = match addr_str.split_once('#') {
            Some((a, u)) => (a, Some(u)),
            None => (addr_str, None),
        };

        let address = ActorAddress::from_str(addr_part).ok()?;

        // Clone the typed Arc out of the map so we release the DashMap shard lock
        // before the downcast — no lock held during allocation.
        let typed = self.registry.get(&address)?.value().typed.clone();

        // If an incarnation_id was given, verify it matches the live actor.
        // We check via the mailbox (erased) side which exposes incarnation_id cheaply.
        if let Some(uid_str) = uid_part {
            let uid = Uuid::parse_str(uid_str).ok()?;
            let live_iid = self
                .registry
                .get(&address)?
                .value()
                .mailbox
                .incarnation_id();
            if live_iid != uid {
                return None; // stale ref — actor was restarted at this address
            }
        }

        // Safe downcast: Arc<dyn Any + Send + Sync> → Arc<Mailbox<M>>.
        //
        // Arc::downcast::<T>() is a stable safe method that checks TypeId internally.
        // If M is wrong it returns Err — we propagate that as None. No unsafe needed.
        let mailbox: Arc<Mailbox<M>> = typed.downcast::<Mailbox<M>>().ok()?;

        Some(ActorRef::new_local(address, mailbox))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reference::actor_ref::{ActorMessage, ActorState, AnyMailbox};
    use crate::system::ActorAddress;
    use crate::{ActorPath, Message};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::{RwLock, mpsc};
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    struct Ping;
    impl Message for Ping {
        fn type_id(&self) -> &'static str {
            "Ping"
        }
    }

    #[derive(Debug, Clone)]
    struct Pong;
    impl Message for Pong {
        fn type_id(&self) -> &'static str {
            "Pong"
        }
    }

    fn make_entry<M: Message>(name: &str) -> (ActorAddress, Uuid, RegistryEntry) {
        let (msg_tx, _) = mpsc::channel::<ActorMessage<M>>(8);
        let (sys_tx, _) = mpsc::unbounded_channel();
        let path = ActorPath::user(name).unwrap();
        let address = ActorAddress::local(path);
        let iid = Uuid::new_v4();
        let mb = Arc::new(Mailbox::<M> {
            incarnation_id: iid,
            msg_tx,
            sys_tx,
            state: Arc::new(RwLock::new(ActorState::Running)),
            alive: Arc::new(AtomicBool::new(true)),
        });
        let entry = RegistryEntry {
            mailbox: Arc::clone(&mb) as Arc<dyn AnyMailbox>,
            typed: Arc::clone(&mb) as Arc<dyn std::any::Any + Send + Sync>,
        };
        (address, iid, entry)
    }

    #[test]
    fn resolve_by_address_only() {
        let registry = Arc::new(DashMap::new());
        let (address, _iid, entry) = make_entry::<Ping>("my-actor");
        registry.insert(address.clone(), entry);

        let resolver = ActorRefResolver::new(registry);
        let r: Option<ActorRef<Ping>> = resolver.resolve(&address.to_string());
        assert!(r.is_some());
        assert_eq!(r.unwrap().address(), &address);
    }

    #[test]
    fn resolve_with_matching_uid() {
        let registry = Arc::new(DashMap::new());
        let (address, iid, entry) = make_entry::<Ping>("my-actor");
        registry.insert(address.clone(), entry);

        let addr_str = format!("{}#{}", address, iid);
        let resolver = ActorRefResolver::new(registry);
        let r: Option<ActorRef<Ping>> = resolver.resolve(&addr_str);
        assert!(r.is_some());
    }

    #[test]
    fn resolve_with_stale_uid_returns_none() {
        let registry = Arc::new(DashMap::new());
        let (address, _live_iid, entry) = make_entry::<Ping>("my-actor");
        registry.insert(address.clone(), entry);

        let stale_uid = Uuid::new_v4();
        let addr_str = format!("{}#{}", address, stale_uid);
        let resolver = ActorRefResolver::new(registry);
        let r: Option<ActorRef<Ping>> = resolver.resolve(&addr_str);
        assert!(r.is_none(), "stale uid must return None");
    }

    #[test]
    fn resolve_unknown_address_returns_none() {
        let registry: Arc<DashMap<ActorAddress, RegistryEntry>> = Arc::new(DashMap::new());
        let resolver = ActorRefResolver::new(registry);
        let r: Option<ActorRef<Ping>> = resolver.resolve("local://user/ghost");
        assert!(r.is_none());
    }

    #[test]
    fn resolve_wrong_type_returns_none() {
        let registry = Arc::new(DashMap::new());
        let (address, _iid, entry) = make_entry::<Ping>("my-actor");
        registry.insert(address.clone(), entry);

        let resolver = ActorRefResolver::new(registry);
        // Pong != Ping → Arc::downcast returns Err → None
        let r: Option<ActorRef<Pong>> = resolver.resolve(&address.to_string());
        assert!(r.is_none(), "wrong message type must return None");
    }

    /// Regression: resolve gives a ref whose `tell` actually reaches the actor.
    #[tokio::test]
    async fn resolved_ref_can_send_messages() {
        let (msg_tx, mut msg_rx) = mpsc::channel::<ActorMessage<Ping>>(8);
        let (sys_tx, _) = mpsc::unbounded_channel();
        let path = ActorPath::user("sender-test").unwrap();
        let address = ActorAddress::local(path);
        let iid = Uuid::new_v4();
        let mb = Arc::new(Mailbox::<Ping> {
            incarnation_id: iid,
            msg_tx,
            sys_tx,
            state: Arc::new(RwLock::new(ActorState::Running)),
            alive: Arc::new(AtomicBool::new(true)),
        });
        let registry = Arc::new(DashMap::new());
        registry.insert(
            address.clone(),
            RegistryEntry {
                mailbox: Arc::clone(&mb) as Arc<dyn AnyMailbox>,
                typed: Arc::clone(&mb) as Arc<dyn std::any::Any + Send + Sync>,
            },
        );

        let resolver = ActorRefResolver::new(registry);
        let actor_ref: ActorRef<Ping> = resolver.resolve(&address.to_string()).unwrap();
        actor_ref.tell(Ping, None).unwrap();

        let received = msg_rx.recv().await.unwrap();
        assert!(matches!(received, ActorMessage::Tell { .. }));
    }
}
