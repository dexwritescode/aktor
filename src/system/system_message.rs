/// System-level signals between actors, separate from user message channel.
///
/// These travel through a dedicated system channel on every ActorRef so they
/// are never intermixed with domain messages and carry no M type parameter —
/// making them compatible with type-erased actors (aktor-30a) and remote
/// transport (aktor-rmb).
#[derive(Debug)]
pub enum SystemMessage {
    /// Instructs the actor to call post_stop and remove itself from the system.
    PoisonPill,
}
