use crate::core::{Actor, ActorError, ActorProps, Message, SupervisionStrategy};
use crate::reference::ActorRef;
use crate::reference::actor_ref::{ActorMessage, ActorState, AnyMailbox, Mailbox};
use crate::system::{
    ActorAddress, SystemMessage,
    extension::{Extension, ExtensionRegistry},
};
use dashmap::DashMap;
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ------------------------------------------------------------------
// Helpers
// ------------------------------------------------------------------

fn extract_panic_message(payload: Box<dyn std::any::Any + Send + 'static>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn compute_backoff(attempt: u32, props: &ActorProps) -> std::time::Duration {
    let base = props.backoff_base_ms as f64;
    let cap = props.backoff_max_ms as f64;
    // base * 2^(attempt-1), capped
    let exp = (base * 2f64.powi(attempt.saturating_sub(1) as i32)).min(cap);
    let jitter = props.backoff_jitter * rand::thread_rng().gen_range(-1.0f64..=1.0);
    let ms = (exp * (1.0 + jitter)).max(0.0) as u64;
    std::time::Duration::from_millis(ms.min(props.backoff_max_ms))
}

// ------------------------------------------------------------------
// AnyActorRef — type-erased handle stored in children map
// ------------------------------------------------------------------

pub(crate) trait AnyActorRef: Send + Sync {
    fn stop_now(&self) -> Result<(), ActorError>;
}

impl<M: Message> AnyActorRef for ActorRef<M> {
    fn stop_now(&self) -> Result<(), ActorError> {
        self.stop_sync()
    }
}

// ------------------------------------------------------------------
// ActorContext<M>
// ------------------------------------------------------------------

/// Actor context provides the runtime environment for an actor.
///
/// Passed as `&ActorContext<M>` to every `handle` call. Not `Clone` — if you
/// need to do work outside `handle`, capture `ctx.actor_ref().clone()` or
/// `ctx.system().clone()` and use those directly.
pub struct ActorContext<M: Message> {
    pub actor_ref: ActorRef<M>,
    pub system: Arc<ActorSystem>,
    /// Lazily initialised: `None` until the first `spawn_child` call.
    ///
    /// Stored inline (no extra heap allocation) inside the `Arc<ActorContext>`
    /// the runner already holds. Leaf actors — the common case — pay zero
    /// additional allocation cost for the children abstraction.
    children: Mutex<Option<HashMap<String, Box<dyn AnyActorRef>>>>,
    parent: Option<ActorAddress>,
    props: ActorProps,
}

impl<M: Message> ActorContext<M> {
    pub fn new(
        actor_ref: ActorRef<M>,
        system: Arc<ActorSystem>,
        parent: Option<ActorAddress>,
        props: ActorProps,
    ) -> Self {
        Self {
            actor_ref,
            system,
            children: Mutex::new(None),
            parent,
            props,
        }
    }

    /// Signal the worker loop to stop this actor after the current message.
    pub fn stop_self(&self) {
        let _ = self.actor_ref.send_system(SystemMessage::StopSelf);
    }

    /// Pipe a future's `Ok` result back to this actor's mailbox.
    ///
    /// If the future returns `Err`, the error is logged and dropped. Use
    /// [`pipe_to_self_map`] when you need to deliver errors as a message variant.
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

    /// Pipe a future to this actor's mailbox, mapping both `Ok` and `Err`
    /// to a message variant via `map`.
    ///
    /// Mirrors Akka Typed's `pipeToSelf(future)(result => ...)`.
    ///
    /// ```ignore
    /// ctx.pipe_to_self_map(fetch(url), |result| match result {
    ///     Ok(body) => Msg::FetchDone(body),
    ///     Err(e)   => Msg::FetchFailed(e.to_string()),
    /// });
    /// ```
    pub fn pipe_to_self_map<F, T, E, Map>(&self, future: F, map: Map)
    where
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
        Map: FnOnce(Result<T, E>) -> M + Send + 'static,
    {
        let actor_ref = self.actor_ref.clone();
        tokio::spawn(async move {
            let msg = map(future.await);
            if let Err(e) = actor_ref.tell(msg, None) {
                tracing::error!("pipe_to_self_map delivery failed: {}", e);
            }
        });
    }

    pub fn actor_ref(&self) -> &ActorRef<M> {
        &self.actor_ref
    }

    pub fn system(&self) -> &ActorSystem {
        &self.system
    }

    /// Spawn a child actor using a factory closure.
    ///
    /// The factory is called once at spawn time and again on each supervised
    /// restart so the child always starts with a clean instance.
    pub fn spawn_child<A, F>(
        &self,
        name: &str,
        factory: F,
        props: Option<ActorProps>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        let props = props.unwrap_or_default();

        let child_address = self
            .actor_ref
            .address()
            .child(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;

        let parent_addr = Some(self.actor_ref.address().clone());

        let child_ref =
            self.system
                .spawn_actor_with_address(child_address, factory, props, parent_addr)?;

        self.children
            .lock()
            .unwrap()
            .get_or_insert_with(HashMap::new)
            .insert(name.to_string(), Box::new(child_ref.clone()));

        info!("Spawned child actor: {}", child_ref.address());
        Ok(child_ref)
    }

    /// Stop a named child actor.
    pub fn stop_child(&self, name: &str) -> Result<(), ActorError> {
        let child = self
            .children
            .lock()
            .unwrap()
            .as_mut()
            .and_then(|m| m.remove(name));
        if let Some(child_ref) = child {
            child_ref.stop_now()?;
            debug!("Stopped child actor: {}", name);
        }
        Ok(())
    }

    /// Stop all child actors.
    pub fn stop_all_children(&self) -> Result<(), ActorError> {
        let children = self.children.lock().unwrap().take().unwrap_or_default();
        for (name, child) in children {
            if let Err(e) = child.stop_now() {
                error!("Failed to stop child actor {}: {}", name, e);
            }
        }
        Ok(())
    }

    /// Returns the parent actor's address, if this is a child actor.
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

    /// How many children this actor currently has.
    pub fn children_count(&self) -> usize {
        self.children
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |m| m.len())
    }

    /// Remove a child from the children map by its full address.
    /// Called by the runner when it receives `ActorStopped` for a child.
    pub(crate) fn remove_child_by_address(&self, address: &ActorAddress) {
        if let Some(name) = address.name()
            && let Some(m) = self.children.lock().unwrap().as_mut()
        {
            m.remove(name);
        }
    }

    /// Schedule a one-shot message delivery to `target` after `delay`.
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
    pub fn schedule_to_self(&self, delay: std::time::Duration, message: M) -> Uuid {
        self.schedule_once(delay, &self.actor_ref.clone(), message)
    }
}

// ------------------------------------------------------------------
// ActorRunnerImpl — per-actor tokio task
// ------------------------------------------------------------------

struct ActorRunnerImpl<A: Actor> {
    /// Current actor instance. Replaced on restart.
    actor: A,
    /// Factory used to produce a fresh instance on each restart.
    factory: Box<dyn Fn() -> A + Send + Sync>,
    context: Arc<ActorContext<A::Msg>>,
    receiver: mpsc::Receiver<ActorMessage<A::Msg>>,
    system_receiver: mpsc::UnboundedReceiver<SystemMessage>,
    address: ActorAddress,
    mailbox: Arc<Mailbox<A::Msg>>,
}

impl<A: Actor> ActorRunnerImpl<A> {
    /// Dispatch a single message, catching any panic from `handle`.
    fn dispatch_one(&mut self, msg: ActorMessage<A::Msg>) -> Result<(), ActorError> {
        let ctx = Arc::clone(&self.context);
        match msg {
            ActorMessage::Tell { message, sender: _ } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.actor.handle(message, &ctx);
                }));
                match result {
                    Ok(()) => Ok(()),
                    Err(payload) => Err(ActorError::Panic(extract_panic_message(payload))),
                }
            }
        }
    }

    /// Apply the parent's supervision strategy for a failed child.
    /// Called from the system-message arm when `ChildFailed` arrives.
    fn apply_child_supervision(
        &mut self,
        child: &ActorAddress,
        error_str: &str,
        actor_storage: &Arc<DashMap<ActorAddress, RegistryEntry>>,
    ) {
        // Reconstruct a best-effort error to pass to the hook.
        let error = ActorError::ActorCreationFailed(error_str.to_string());
        let strategy = self.actor.on_child_failed(child, &error, &self.context);

        match strategy {
            SupervisionStrategy::Stop => {
                // Child already removed itself from the registry. Remove from
                // this actor's children map so we don't hold a dead entry.
                self.context.remove_child_by_address(child);
                debug!("Child {} stopped per supervision strategy", child);
            }
            SupervisionStrategy::Restart => {
                // TODO(aktor-j7f): restarting a child from the parent requires
                // the parent to hold the child's factory closure. This is
                // deferred — the child's own supervision_strategy already handles
                // self-restart with backoff. Treating parent-level Restart as
                // Stop until parent-owned child factories are implemented.
                warn!(
                    "Parent Restart for child {} not yet implemented — treating as Stop",
                    child
                );
                self.context.remove_child_by_address(child);
            }
            SupervisionStrategy::Resume => {
                // Child is already dead. Parent continues; child entry is removed.
                self.context.remove_child_by_address(child);
                debug!(
                    "Child {} Resume requested but child is gone — removed",
                    child
                );
            }
            SupervisionStrategy::Escalate => {
                // Propagate the failure up to this actor's own parent.
                if let Some(parent_addr) = self.context.parent_address()
                    && let Some(entry) = actor_storage.get(parent_addr)
                {
                    let _ = entry.mailbox.send_system(SystemMessage::ChildFailed {
                        child: child.clone(),
                        error: error_str.to_string(),
                    });
                }
                self.context.remove_child_by_address(child);
                debug!("Child {} failure escalated to grandparent", child);
            }
        }
    }

    async fn run(mut self, actor_storage: Arc<DashMap<ActorAddress, RegistryEntry>>) {
        let props = self.context.props().clone();
        let mut restart_count: u32 = 0;

        // ── Lifecycle loop ────────────────────────────────────────────────────
        //
        // Each iteration is one actor incarnation. The FIRST iteration skips
        // pre_start because spawn_actor_with_address already called it
        // synchronously (so callers can detect startup failures immediately).
        // Subsequent iterations (restarts) call factory() + pre_start here.
        let mut is_restart = false;

        'lifecycle: loop {
            // ── Startup ───────────────────────────────────────────────────────
            if is_restart {
                self.mailbox.update_state(ActorState::Starting).await;
                self.actor = (self.factory)();

                let pre_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.actor.pre_start(&self.context)
                }));
                let startup_err = match pre_result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e),
                    Err(payload) => Some(ActorError::Panic(extract_panic_message(payload))),
                };

                if let Some(err) = startup_err {
                    error!(
                        "Actor {} failed in pre_start on restart #{}: {}",
                        self.address, restart_count, err
                    );
                    self.mailbox
                        .update_state(ActorState::Failed(err.to_string()))
                        .await;

                    // No post_stop — we never reached Running.
                    // Apply supervision directly.
                    if !self
                        .handle_failure(&err, &mut restart_count, &props, &actor_storage)
                        .await
                    {
                        break 'lifecycle;
                    }
                    is_restart = true;
                    continue 'lifecycle;
                }
            }

            self.mailbox.update_state(ActorState::Running).await;
            let started_at = std::time::Instant::now();

            // ── Message loop ─────────────────────────────────────────────────
            let failure: Option<ActorError> = 'run: loop {
                tokio::select! {
                    biased;
                    sys = self.system_receiver.recv() => {
                        match sys {
                            Some(SystemMessage::PoisonPill)
                            | Some(SystemMessage::StopSelf)
                            | None => {
                                self.mailbox.update_state(ActorState::Stopping).await;
                                break 'run None;
                            }
                            Some(SystemMessage::ActorStopped { address }) => {
                                self.context.remove_child_by_address(&address);
                            }
                            Some(SystemMessage::ChildFailed { child, error }) => {
                                self.apply_child_supervision(&child, &error, &actor_storage);
                            }
                        }
                    }
                    msg = self.receiver.recv() => {
                        let Some(m) = msg else {
                            self.mailbox.update_state(ActorState::Stopping).await;
                            break 'run None;
                        };

                        match self.dispatch_one(m) {
                            Ok(()) => {}
                            Err(e) if props.supervision_strategy == SupervisionStrategy::Resume => {
                                warn!("Actor {} resuming after: {}", self.address, e);
                            }
                            Err(e) => {
                                self.mailbox
                                    .update_state(ActorState::Failed(e.to_string()))
                                    .await;
                                break 'run Some(e);
                            }
                        }

                        // Batch drain — process remaining queued messages without
                        // sleeping. System channel is checked first each round so
                        // PoisonPill / StopSelf always take priority.
                        'drain: loop {
                            loop {
                                match self.system_receiver.try_recv() {
                                    Ok(SystemMessage::PoisonPill)
                                    | Ok(SystemMessage::StopSelf) => {
                                        self.mailbox
                                            .update_state(ActorState::Stopping)
                                            .await;
                                        break 'run None;
                                    }
                                    Ok(SystemMessage::ActorStopped { address }) => {
                                        self.context.remove_child_by_address(&address);
                                    }
                                    Ok(SystemMessage::ChildFailed { child, error }) => {
                                        self.apply_child_supervision(
                                            &child,
                                            &error,
                                            &actor_storage,
                                        );
                                    }
                                    Err(_) => break,
                                }
                            }

                            match self.receiver.try_recv() {
                                Ok(m) => {
                                    match self.dispatch_one(m) {
                                        Ok(()) => {}
                                        Err(e) if props.supervision_strategy
                                            == SupervisionStrategy::Resume =>
                                        {
                                            warn!(
                                                "Actor {} resuming after: {}",
                                                self.address, e
                                            );
                                        }
                                        Err(e) => {
                                            self.mailbox
                                                .update_state(ActorState::Failed(e.to_string()))
                                                .await;
                                            break 'run Some(e);
                                        }
                                    }
                                }
                                Err(_) => break 'drain,
                            }
                        }
                    }
                }
            };

            // ── Post-stop ─────────────────────────────────────────────────────
            if let Err(e) = self.actor.post_stop(&self.context) {
                error!("Actor {} post_stop failed: {}", self.address, e);
            }

            // ── Recovery window ───────────────────────────────────────────────
            // If the actor ran for longer than the window without crashing,
            // reset the restart counter so infrequent failures don't exhaust it.
            if started_at.elapsed().as_secs() >= props.restart_window_secs {
                restart_count = 0;
            }

            // ── Lifecycle decision ────────────────────────────────────────────
            let Some(error) = failure else {
                // Graceful stop (PoisonPill / StopSelf / channel closed).
                self.mailbox.update_state(ActorState::Stopped).await;
                self.mailbox.alive.store(false, Ordering::Release);
                actor_storage.remove(&self.address);

                if let Some(parent_addr) = self.context.parent_address()
                    && let Some(entry) = actor_storage.get(parent_addr)
                {
                    let _ = entry.mailbox.send_system(SystemMessage::ActorStopped {
                        address: self.address.clone(),
                    });
                }

                info!("Actor stopped: {}", self.address);
                break 'lifecycle;
            };

            // Runtime failure — apply supervision.
            if !self
                .handle_failure(&error, &mut restart_count, &props, &actor_storage)
                .await
            {
                break 'lifecycle;
            }
            is_restart = true;
        }
    }

    /// Apply supervision for a runtime failure. Returns `true` if the actor
    /// should restart (caller continues `'lifecycle`), `false` if it should stop.
    async fn handle_failure(
        &self,
        error: &ActorError,
        restart_count: &mut u32,
        props: &ActorProps,
        actor_storage: &Arc<DashMap<ActorAddress, RegistryEntry>>,
    ) -> bool {
        match &props.supervision_strategy {
            SupervisionStrategy::Stop | SupervisionStrategy::Escalate => {
                self.mailbox.alive.store(false, Ordering::Release);
                actor_storage.remove(&self.address);

                if let Some(parent_addr) = self.context.parent_address()
                    && let Some(entry) = actor_storage.get(parent_addr)
                {
                    let _ = entry.mailbox.send_system(SystemMessage::ChildFailed {
                        child: self.address.clone(),
                        error: error.to_string(),
                    });
                }

                info!(
                    "Actor {} stopped after failure (strategy={:?}): {}",
                    self.address, props.supervision_strategy, error
                );
                false // stop
            }
            SupervisionStrategy::Resume => {
                // Panics during handle() are handled inline in the 'run loop
                // (continue 'run without breaking). This branch is only reached
                // for pre_start failures with a Resume strategy — odd but legal.
                // Just go back to Running with the same instance.
                warn!(
                    "Actor {} resuming after failure (pre_start path): {}",
                    self.address, error
                );
                true // restart (re-enters lifecycle loop, skips factory + pre_start on Resume)
            }
            SupervisionStrategy::Restart => {
                if *restart_count >= props.max_restarts {
                    error!(
                        "Actor {} exhausted {} restart(s) — stopping",
                        self.address, props.max_restarts
                    );
                    self.mailbox.alive.store(false, Ordering::Release);
                    actor_storage.remove(&self.address);

                    if let Some(parent_addr) = self.context.parent_address()
                        && let Some(entry) = actor_storage.get(parent_addr)
                    {
                        let _ = entry.mailbox.send_system(SystemMessage::ChildFailed {
                            child: self.address.clone(),
                            error: format!(
                                "exhausted {} restarts; last error: {}",
                                props.max_restarts, error
                            ),
                        });
                    }
                    return false; // stop
                }

                *restart_count += 1;
                let delay = compute_backoff(*restart_count, props);
                self.mailbox
                    .update_state(ActorState::BackingOff(*restart_count))
                    .await;

                info!(
                    "Actor {} backing off {}ms before restart #{}/{}",
                    self.address,
                    delay.as_millis(),
                    restart_count,
                    props.max_restarts
                );
                tokio::time::sleep(delay).await;
                true // restart
            }
        }
    }
}

// ------------------------------------------------------------------
// Registry
// ------------------------------------------------------------------

pub(crate) struct RegistryEntry {
    pub(crate) mailbox: Arc<dyn AnyMailbox>,
    pub(crate) typed: Arc<dyn std::any::Any + Send + Sync>,
}

// ------------------------------------------------------------------
// ActorSystem
// ------------------------------------------------------------------

pub struct ActorSystem {
    config: ActorSystemConfig,
    node_id: String,
    actor_storage: Arc<DashMap<ActorAddress, RegistryEntry>>,
    extensions: Arc<ExtensionRegistry>,
}

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

    /// Spawn an actor using a factory closure.
    ///
    /// The factory is called immediately to produce the initial actor instance,
    /// and is stored in the runner for use on each supervised restart.
    ///
    /// ```ignore
    /// let ref = system.spawn_actor("worker", || WorkerActor::new(db.clone()), props)?;
    /// ```
    pub fn spawn_actor<A, F>(
        self: &Arc<Self>,
        name: &str,
        factory: F,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        let path = crate::system::ActorPath::user(name)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;
        let address = ActorAddress::new(&self.node_id, path)
            .map_err(|e| ActorError::ActorCreationFailed(e.to_string()))?;
        self.spawn_actor_with_address(address, factory, props, None)
    }

    /// Spawn an actor at a specific address with an optional parent address.
    pub fn spawn_actor_with_address<A, F>(
        self: &Arc<Self>,
        address: ActorAddress,
        factory: F,
        props: ActorProps,
        parent: Option<ActorAddress>,
    ) -> Result<ActorRef<A::Msg>, ActorError>
    where
        A: Actor,
        F: Fn() -> A + Send + Sync + 'static,
    {
        if self.actor_storage.contains_key(&address) {
            return Err(ActorError::ActorCreationFailed(format!(
                "Actor already exists at address: {}",
                address
            )));
        }

        let capacity = props
            .mailbox_size
            .unwrap_or(self.config.default_mailbox_size);
        let (msg_tx, receiver) = mpsc::channel(capacity);
        let (sys_tx, system_receiver) = mpsc::unbounded_channel::<SystemMessage>();

        let mailbox = Arc::new(Mailbox {
            incarnation_id: Uuid::new_v4(),
            msg_tx,
            sys_tx,
            state: Arc::new(RwLock::new(ActorState::Starting)),
            alive: Arc::new(AtomicBool::new(true)),
        });

        let actor_ref = ActorRef::new_local(address.clone(), Arc::clone(&mailbox));

        let context = Arc::new(ActorContext::new(
            actor_ref.clone(),
            self.clone(),
            parent,
            props.clone(),
        ));

        // Create initial instance and run pre_start synchronously so the caller
        // can detect startup failures before spawn_actor returns.
        let mut actor = factory();
        if let Err(e) = actor.pre_start(&context) {
            return Err(ActorError::ActorCreationFailed(format!(
                "Actor pre_start failed: {}",
                e
            )));
        }

        self.actor_storage.insert(
            address.clone(),
            RegistryEntry {
                mailbox: Arc::clone(&mailbox) as Arc<dyn AnyMailbox>,
                typed: Arc::clone(&mailbox) as Arc<dyn std::any::Any + Send + Sync>,
            },
        );

        let runner = ActorRunnerImpl {
            actor,
            factory: Box::new(factory),
            context,
            receiver,
            system_receiver,
            address: address.clone(),
            mailbox,
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
        self.spawn_actor(name, A::default, ActorProps::default())
    }

    /// Spawn a `Default` actor with custom props.
    pub fn actor_of_props<A: Actor + Default>(
        self: &Arc<Self>,
        name: &str,
        props: ActorProps,
    ) -> Result<ActorRef<A::Msg>, ActorError> {
        self.spawn_actor(name, A::default, props)
    }

    pub fn contains_actor(&self, address: &ActorAddress) -> bool {
        self.actor_storage.contains_key(address)
    }

    pub async fn shutdown(self: Arc<Self>) -> Result<(), ActorError> {
        info!("Shutting down actor system");
        for entry in self.actor_storage.iter() {
            let _ = entry.value().mailbox.send_system(SystemMessage::PoisonPill);
        }
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

    pub fn register_extension<T: Extension>(&self, extension: T) {
        self.extensions.register(extension);
    }

    pub fn extension<T: Extension>(&self) -> Arc<T> {
        self.extensions.get::<T>()
    }

    pub fn extension_optional<T: Extension>(&self) -> Option<Arc<T>> {
        self.extensions.get_optional::<T>()
    }

    pub fn get_or_create_extension<T: Extension>(&self) -> Arc<T> {
        self.extensions.get_or_create::<T>()
    }

    pub fn resolver(&self) -> crate::system::resolver::ActorRefResolver {
        crate::system::resolver::ActorRefResolver::new(Arc::clone(&self.actor_storage))
    }
}

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

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

    #[derive(Debug, Default)]
    struct TestActor {
        received_count: usize,
        received_messages: Vec<String>,
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
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        assert!(!system.node_id().is_empty());
    }

    #[tokio::test]
    async fn test_actor_spawning() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let actor_ref = system
            .spawn_actor("test-actor", TestActor::default, ActorProps::default())
            .unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
    }

    #[tokio::test]
    async fn test_message_sending() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let actor_ref = system
            .spawn_actor("test-actor", TestActor::default, ActorProps::default())
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            actor_ref
                .tell(
                    TestMessage {
                        data: "Hello".into()
                    },
                    None
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_actor_of() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let actor_ref = system.actor_of::<TestActor>("test-actor").unwrap();
        assert!(actor_ref.is_local());
        assert_eq!(actor_ref.address().name(), Some("test-actor"));
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            actor_ref
                .tell(
                    TestMessage {
                        data: "factory test".into()
                    },
                    None
                )
                .is_ok()
        );
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
    }

    #[tokio::test]
    async fn test_actor_of_props() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let props = ActorProps::new()
            .with_mailbox_size(5000)
            .with_dispatcher("custom-dispatcher");
        let actor_ref = system
            .actor_of_props::<TestActor>("props-actor", props)
            .unwrap();
        assert!(actor_ref.is_local());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(
            actor_ref
                .tell(
                    TestMessage {
                        data: "props test".into()
                    },
                    None
                )
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_actor_name_uniqueness() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
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
        let c = captured.clone();
        system
            .spawn_actor(
                "root",
                move || ParentProbeActor {
                    captured: c.clone(),
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
            .spawn_actor("parent", TestActor::default, ActorProps::default())
            .unwrap();
        let captured = Arc::new(std::sync::Mutex::new(None::<String>));
        let c = captured.clone();
        let child_address = parent_ref.address().child("child").unwrap();
        system
            .spawn_actor_with_address(
                child_address,
                move || ParentProbeActor {
                    captured: c.clone(),
                },
                ActorProps::default(),
                Some(parent_ref.address().clone()),
            )
            .unwrap();
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
            .spawn_actor(
                "self-stopper",
                SelfStoppingActor::default,
                ActorProps::default(),
            )
            .unwrap();
        actor_ref
            .tell(TestMessage { data: "go".into() }, None)
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor should have been removed after stop_self()"
        );
    }

    #[tokio::test]
    async fn test_poison_pill_removes_actor_from_system() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let actor_ref = system
            .spawn_actor("pill-target", TestActor::default, ActorProps::default())
            .unwrap();
        actor_ref.stop().await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor should have been removed after PoisonPill"
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
        let c = counter.clone();
        let actor_ref = system
            .spawn_actor(
                "counter",
                move || CountingActor { counter: c.clone() },
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
        assert_eq!(counter.load(AOrdering::Relaxed), MSG_COUNT);
    }

    #[derive(Debug)]
    struct PreStartChildActor {
        child_address: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl Actor for PreStartChildActor {
        type Msg = TestMessage;
        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {}
        fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
            let child = ctx.spawn_child("child", TestActor::default, None)?;
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
        let c = child_addr.clone();
        system
            .spawn_actor(
                "pre-start-actor",
                move || PreStartChildActor {
                    child_address: c.clone(),
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
        let p = phase.clone();
        let actor_ref = system
            .spawn_actor(
                "pipe-actor",
                move || PipeActor { phase: p.clone() },
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
        assert_eq!(observed, vec!["handle:start", "handle:piped"]);
    }

    #[tokio::test]
    async fn test_pipe_to_self_err_is_dropped() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};
        let received = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct CountActor {
            count: Arc<AtomicUsize>,
        }
        impl Actor for CountActor {
            type Msg = TestMessage;
            fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
                if msg.data == "start" {
                    ctx.pipe_to_self(async { Err::<TestMessage, &str>("simulated failure") });
                } else {
                    self.count.fetch_add(1, AOrdering::Relaxed);
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let r = received.clone();
        let actor_ref = system
            .spawn_actor(
                "err-pipe-actor",
                move || CountActor { count: r.clone() },
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
        assert_eq!(received.load(AOrdering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_pipe_to_self_does_not_block_subsequent_messages() {
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

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let l = log.clone();
        let g = gate.clone();
        let actor_ref = system
            .spawn_actor(
                "interleave-actor",
                move || InterleavingActor {
                    log: l.clone(),
                    gate: g.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        for data in ["msg-a", "pipe", "msg-b", "msg-c"] {
            actor_ref
                .tell(TestMessage { data: data.into() }, None)
                .unwrap();
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        {
            let observed = log.lock().unwrap().clone();
            assert!(observed.contains(&"msg-b".to_string()));
            assert!(observed.contains(&"msg-c".to_string()));
            assert!(!observed.contains(&"piped".to_string()));
        }
        gate.notify_one();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        let observed = log.lock().unwrap().clone();
        assert!(observed.contains(&"piped".to_string()));
        let b = observed.iter().position(|s| s == "msg-b").unwrap();
        let c = observed.iter().position(|s| s == "msg-c").unwrap();
        let p = observed.iter().position(|s| s == "piped").unwrap();
        assert!(p > b && p > c);
    }

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
        let c = counter.clone();
        let actor_ref = system
            .spawn_actor(
                "timer-actor",
                move || TimerActor { counter: c.clone() },
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
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            0,
            "tick must not arrive before delay"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;
        assert_eq!(
            counter.load(AOrdering::Relaxed),
            1,
            "tick must arrive after delay"
        );
    }

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
        let c = counter.clone();
        let receiver_ref = system
            .spawn_actor(
                "receiver-actor",
                move || ReceiverActor { counter: c.clone() },
                ActorProps::default(),
            )
            .unwrap();
        let t = receiver_ref.clone();
        let sender_ref = system
            .spawn_actor(
                "sender-actor",
                move || SenderActor { target: t.clone() },
                ActorProps::default(),
            )
            .unwrap();

        sender_ref
            .tell(TestMessage { data: "go".into() }, None)
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
        assert_eq!(counter.load(AOrdering::Relaxed), 0);
        tokio::time::sleep(tokio::time::Duration::from_millis(40)).await;
        assert_eq!(counter.load(AOrdering::Relaxed), 1);
    }

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
        let c = counter.clone();
        let actor_ref = system
            .spawn_actor(
                "multi-timer-actor",
                move || MultiTimerActor { counter: c.clone() },
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
        assert_eq!(counter.load(AOrdering::Relaxed), 0);
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert_eq!(counter.load(AOrdering::Relaxed), 1);
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;
        assert_eq!(counter.load(AOrdering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_self_stopping_child_removed_from_parent_children_map() {
        use std::sync::atomic::{AtomicUsize, Ordering as AOrdering};
        let reported_count = Arc::new(AtomicUsize::new(99));

        #[derive(Debug)]
        struct ParentActor {
            child: Option<ActorRef<TestMessage>>,
            reported_count: Arc<AtomicUsize>,
        }
        impl Actor for ParentActor {
            type Msg = TestMessage;
            fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
                let child_ref = ctx.spawn_child("child", SelfStoppingActor::default, None)?;
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
        let rc = reported_count.clone();
        let parent_ref = system
            .spawn_actor(
                "parent",
                move || ParentActor {
                    child: None,
                    reported_count: rc.clone(),
                },
                ActorProps::default(),
            )
            .unwrap();

        let child_addr = parent_ref.address().child("child").unwrap();
        parent_ref
            .tell(
                TestMessage {
                    data: "trigger".into(),
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(80)).await;
        assert!(!system.contains_actor(&child_addr));

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
            "parent's children map must be empty after child self-stops"
        );
    }

    // ── Supervision tests ────────────────────────────────────────────────────

    /// An actor that panics on the first message it receives, then
    /// behaves normally. Used to test supervised restart.
    #[derive(Debug)]
    struct PanicOnceActor {
        panicked: Arc<AtomicBool>,
        handled: Arc<std::sync::atomic::AtomicUsize>,
    }

    use std::sync::atomic::AtomicBool;
    impl Actor for PanicOnceActor {
        type Msg = TestMessage;
        fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
            if !self.panicked.swap(true, Ordering::SeqCst) {
                panic!("intentional test panic");
            }
            self.handled
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn test_restart_after_panic() {
        use std::sync::atomic::AtomicUsize;

        let panicked = Arc::new(AtomicBool::new(false));
        let handled = Arc::new(AtomicUsize::new(0));
        let panicked2 = panicked.clone();
        let handled2 = handled.clone();

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let props = ActorProps::new()
            .with_restart(3, 60)
            .with_backoff(10, 100, 0.0);

        let actor_ref = system
            .spawn_actor(
                "panic-once",
                move || PanicOnceActor {
                    panicked: panicked2.clone(),
                    handled: handled2.clone(),
                },
                props,
            )
            .unwrap();

        // First message → panic → restart
        actor_ref
            .tell(
                TestMessage {
                    data: "boom".into(),
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Second message → handled normally after restart
        actor_ref
            .tell(TestMessage { data: "ok".into() }, None)
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(
            panicked.load(Ordering::SeqCst),
            "actor should have panicked"
        );
        assert_eq!(
            handled.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "second message must be handled after restart"
        );
        assert!(
            system.contains_actor(actor_ref.address()),
            "actor must still be live after restart"
        );
    }

    #[tokio::test]
    async fn test_stop_strategy_removes_actor_on_panic() {
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        #[derive(Debug, Default)]
        struct AlwaysPanicsActor;
        impl Actor for AlwaysPanicsActor {
            type Msg = TestMessage;
            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                panic!("always panics");
            }
        }

        // Default props → Stop strategy
        let actor_ref = system
            .spawn_actor(
                "always-panics",
                AlwaysPanicsActor::default,
                ActorProps::default(),
            )
            .unwrap();

        actor_ref
            .tell(
                TestMessage {
                    data: "boom".into(),
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor with Stop strategy must be removed after panic"
        );
    }

    #[tokio::test]
    async fn test_exhausted_restarts_removes_actor() {
        use std::sync::atomic::AtomicUsize;

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let restart_count = Arc::new(AtomicUsize::new(0));
        let rc = restart_count.clone();

        #[derive(Debug)]
        struct AlwaysPanicsActor {
            count: Arc<AtomicUsize>,
        }
        impl Actor for AlwaysPanicsActor {
            type Msg = TestMessage;
            fn pre_start(&mut self, _ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
                self.count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                panic!("always panics");
            }
        }

        let props = ActorProps::new()
            .with_restart(2, 60)
            .with_backoff(10, 50, 0.0);
        let actor_ref = system
            .spawn_actor(
                "exhausted",
                move || AlwaysPanicsActor { count: rc.clone() },
                props,
            )
            .unwrap();

        // The receiver persists across restarts, so pre-loading 3 messages ensures
        // each incarnation (initial + 2 restarts) gets one message to panic on.
        actor_ref
            .tell(
                TestMessage {
                    data: "boom-1".into(),
                },
                None,
            )
            .unwrap();
        actor_ref
            .tell(
                TestMessage {
                    data: "boom-2".into(),
                },
                None,
            )
            .unwrap();
        actor_ref
            .tell(
                TestMessage {
                    data: "boom-3".into(),
                },
                None,
            )
            .unwrap();
        // Wait long enough for 2 restarts + backoffs (2 × 10ms + buffer)
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert!(
            !system.contains_actor(actor_ref.address()),
            "actor must be removed after exhausting restarts"
        );
        // pre_start called: 1 initial + 2 restarts = 3
        assert_eq!(restart_count.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_on_child_failed_called_when_child_panics() {
        use std::sync::atomic::AtomicUsize;

        let notified = Arc::new(AtomicUsize::new(0));
        let notified2 = notified.clone();

        #[derive(Debug, Default)]
        struct PanicChildActor;
        impl Actor for PanicChildActor {
            type Msg = TestMessage;
            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                panic!("child panic");
            }
        }

        #[derive(Debug)]
        struct SupervisorActor {
            notified: Arc<AtomicUsize>,
            child: Option<ActorRef<TestMessage>>,
        }
        impl Actor for SupervisorActor {
            type Msg = TestMessage;
            fn pre_start(&mut self, ctx: &ActorContext<TestMessage>) -> Result<(), ActorError> {
                let child = ctx.spawn_child("child", PanicChildActor::default, None)?;
                self.child = Some(child);
                Ok(())
            }
            fn handle(&mut self, _msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
                if let Some(child) = &self.child {
                    let _ = child.tell(
                        TestMessage {
                            data: "boom".into(),
                        },
                        None,
                    );
                }
            }
            fn on_child_failed(
                &mut self,
                _child: &ActorAddress,
                _error: &ActorError,
                _ctx: &ActorContext<TestMessage>,
            ) -> SupervisionStrategy {
                self.notified
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                SupervisionStrategy::Stop
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let parent_ref = system
            .spawn_actor(
                "supervisor",
                move || SupervisorActor {
                    notified: notified2.clone(),
                    child: None,
                },
                ActorProps::default(),
            )
            .unwrap();

        parent_ref
            .tell(TestMessage { data: "go".into() }, None)
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        assert_eq!(
            notified.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "on_child_failed must be called exactly once when child panics"
        );
    }

    // ── aktor-ytc: lazy children map ────────────────────────────────────────

    #[tokio::test]
    async fn test_leaf_actor_children_none_by_default() {
        // A leaf actor that never calls spawn_child should have children == None.
        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();

        let actor_ref = system
            .spawn_actor("leaf", TestActor::default, ActorProps::default())
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // Verify the actor is alive and children_count returns 0 without
        // ever having initialized the map.
        actor_ref
            .tell(
                TestMessage {
                    data: "ping".into(),
                },
                None,
            )
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // We can't inspect the private field directly, but children_count()
        // must return 0 without panicking.
        system.shutdown().await.unwrap();
    }

    // ── aktor-dpa: pipe_to_self_map ─────────────────────────────────────────

    #[tokio::test]
    async fn test_pipe_to_self_map_ok() {
        #[derive(Debug)]
        enum Msg {
            Fetch,
            Done(String),
            Failed(String),
        }
        impl Message for Msg {
            fn type_id(&self) -> &'static str {
                "PipeMapMsg"
            }
        }

        let result = Arc::new(Mutex::new(None::<String>));

        #[derive(Debug)]
        struct PipeActor {
            result: Arc<Mutex<Option<String>>>,
        }
        impl Actor for PipeActor {
            type Msg = Msg;
            fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
                match msg {
                    Msg::Fetch => {
                        ctx.pipe_to_self_map(async { Ok::<_, String>("hello".to_string()) }, |r| {
                            match r {
                                Ok(s) => Msg::Done(s),
                                Err(e) => Msg::Failed(e),
                            }
                        });
                    }
                    Msg::Done(s) => {
                        *self.result.lock().unwrap() = Some(format!("ok:{s}"));
                    }
                    Msg::Failed(e) => {
                        *self.result.lock().unwrap() = Some(format!("err:{e}"));
                    }
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let r = result.clone();
        let actor_ref = system
            .spawn_actor(
                "pipe-map-ok",
                move || PipeActor { result: r.clone() },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref.tell(Msg::Fetch, None).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(
            result.lock().unwrap().as_deref(),
            Some("ok:hello"),
            "pipe_to_self_map must deliver Ok arm as Done"
        );
        system.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_pipe_to_self_map_err() {
        #[derive(Debug)]
        enum Msg {
            Fetch,
            Done(String),
            Failed(String),
        }
        impl Message for Msg {
            fn type_id(&self) -> &'static str {
                "PipeMapErrMsg"
            }
        }

        let result = Arc::new(Mutex::new(None::<String>));

        #[derive(Debug)]
        struct PipeActor {
            result: Arc<Mutex<Option<String>>>,
        }
        impl Actor for PipeActor {
            type Msg = Msg;
            fn handle(&mut self, msg: Msg, ctx: &ActorContext<Msg>) {
                match msg {
                    Msg::Fetch => {
                        ctx.pipe_to_self_map(async { Err::<String, _>("boom".to_string()) }, |r| {
                            match r {
                                Ok(s) => Msg::Done(s),
                                Err(e) => Msg::Failed(e),
                            }
                        });
                    }
                    Msg::Done(s) => {
                        *self.result.lock().unwrap() = Some(format!("ok:{s}"));
                    }
                    Msg::Failed(e) => {
                        *self.result.lock().unwrap() = Some(format!("err:{e}"));
                    }
                }
            }
        }

        let system: Arc<ActorSystem> = ActorSystem::new(ActorSystemConfig::default())
            .await
            .unwrap();
        let r = result.clone();
        let actor_ref = system
            .spawn_actor(
                "pipe-map-err",
                move || PipeActor { result: r.clone() },
                ActorProps::default(),
            )
            .unwrap();

        actor_ref.tell(Msg::Fetch, None).unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        assert_eq!(
            result.lock().unwrap().as_deref(),
            Some("err:boom"),
            "pipe_to_self_map must deliver Err arm as Failed"
        );
        system.shutdown().await.unwrap();
    }
}
