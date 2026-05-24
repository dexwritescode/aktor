use crate::{ActorError, ActorRef, Message};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// Ask pattern error types.
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    #[error("Ask timeout after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Response channel closed")]
    ChannelClosed,

    #[error("Actor error: {0}")]
    ActorError(#[from] ActorError),
}

/// A typed, one-shot reply channel.
///
/// Include a `ReplyTo<R>` field in a message variant to implement the ask
/// pattern with compile-time type safety and no context injection.
///
/// # Example
///
/// ```ignore
/// enum CounterMsg {
///     GetCount { reply_to: ReplyTo<u64> },
///     Increment,
/// }
///
/// // Actor side — in handle():
/// CounterMsg::GetCount { reply_to } => reply_to.reply(self.count),
///
/// // Caller side:
/// let count: u64 = actor_ref
///     .ask(|reply_to| CounterMsg::GetCount { reply_to }, Duration::from_secs(5))
///     .await?;
/// ```
pub struct ReplyTo<R> {
    sender: oneshot::Sender<R>,
}

impl<R: Send + 'static> ReplyTo<R> {
    /// Send the reply. Consumes `self` — compile-time guarantee of one reply per ask.
    /// If the caller has already timed out the value is silently dropped.
    pub fn reply(self, value: R) {
        let _ = self.sender.send(value);
    }
}

impl<R> std::fmt::Debug for ReplyTo<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplyTo").finish_non_exhaustive()
    }
}

/// Ask an actor a question and wait for a typed reply.
///
/// Creates a [`ReplyTo<R>`] channel, passes it to `make_msg` to build the
/// message, delivers it via [`tell`](ActorRef::tell), then awaits the reply.
///
/// No `Box<dyn Any>`, no context injection, no runtime downcast.
///
/// # Example
///
/// ```ignore
/// let status = ask(&actor_ref, |r| Msg::GetStatus { reply_to: r }, Duration::from_secs(5)).await?;
/// ```
pub async fn ask<M, R>(
    actor_ref: &ActorRef<M>,
    make_msg: impl FnOnce(ReplyTo<R>) -> M,
    timeout_duration: Duration,
) -> Result<R, AskError>
where
    M: Message,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let message = make_msg(ReplyTo { sender: tx });
    actor_ref.tell(message, None)?;

    timeout(timeout_duration, rx)
        .await
        .map_err(|_| AskError::Timeout {
            timeout: timeout_duration,
        })?
        .map_err(|_| AskError::ChannelClosed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reply_to_sends_value() {
        let (tx, rx) = oneshot::channel::<u64>();
        let reply_to = ReplyTo { sender: tx };
        reply_to.reply(42);
        assert_eq!(rx.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_reply_to_dropped_receiver_does_not_panic() {
        let (tx, rx) = oneshot::channel::<u64>();
        let reply_to = ReplyTo { sender: tx };
        drop(rx);
        reply_to.reply(42); // must not panic
    }
}