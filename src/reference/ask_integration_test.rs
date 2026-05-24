#[cfg(test)]
mod tests {
    use crate::{Actor, ActorContext, ActorProps, ActorSystem, ActorSystemConfig, Message};
    use crate::reference::ask::ReplyTo;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    // ── Message types ─────────────────────────────────────────────────────────

    #[derive(Debug)]
    enum EchoMsg {
        /// Ask variant: actor replies with an EchoResponse.
        Echo {
            content: String,
            reply_to: ReplyTo<EchoResponse>,
        },
        /// Tell variant: fire-and-forget.
        Fire { content: String },
    }

    impl Message for EchoMsg {
        fn type_id(&self) -> &'static str {
            "EchoMsg"
        }
    }

    #[derive(Debug, Clone, PartialEq)]
    struct EchoResponse {
        echoed: String,
    }

    // ── Actor ─────────────────────────────────────────────────────────────────

    #[derive(Debug, Default)]
    struct EchoActor {
        message_count: usize,
    }

    impl Actor for EchoActor {
        type Msg = EchoMsg;

        fn handle(&mut self, msg: EchoMsg, _ctx: &ActorContext<EchoMsg>) {
            self.message_count += 1;
            match msg {
                EchoMsg::Echo { content, reply_to } => {
                    reply_to.reply(EchoResponse {
                        echoed: format!("Echo: {content}"),
                    });
                }
                EchoMsg::Fire { content } => {
                    // fire-and-forget — no reply
                    let _ = content;
                }
            }
        }
    }

    // ── Slow actor (never replies — used for timeout tests) ───────────────────

    #[derive(Debug)]
    enum SlowMsg {
        Query { reply_to: ReplyTo<u64> },
    }

    impl Message for SlowMsg {
        fn type_id(&self) -> &'static str {
            "SlowMsg"
        }
    }

    #[derive(Debug, Default)]
    struct SlowActor {
        /// Hold reply channels without replying so the oneshot sender stays
        /// alive long enough for the caller's timeout to fire.
        pending: Vec<ReplyTo<u64>>,
    }

    impl Actor for SlowActor {
        type Msg = SlowMsg;

        fn handle(&mut self, msg: SlowMsg, _ctx: &ActorContext<SlowMsg>) {
            match msg {
                SlowMsg::Query { reply_to } => self.pending.push(reply_to),
            }
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    async fn echo_system() -> (
        Arc<crate::ActorSystem>,
        crate::ActorRef<EchoMsg>,
    ) {
        let system = ActorSystem::new(ActorSystemConfig::default()).await.unwrap();
        let actor_ref = system
            .spawn_actor("echo", EchoActor::default(), ActorProps::default())
            .unwrap();
        sleep(Duration::from_millis(10)).await;
        (system, actor_ref)
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_ask_basic() {
        let (_system, actor_ref) = echo_system().await;

        let response = actor_ref
            .ask(
                |rt| EchoMsg::Echo {
                    content: "hello".into(),
                    reply_to: rt,
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(response.echoed, "Echo: hello");
    }

    #[tokio::test]
    async fn test_ask_free_function() {
        use crate::reference::ask::ask;

        let (_system, actor_ref) = echo_system().await;

        let response: EchoResponse = ask(
            &actor_ref,
            |rt| EchoMsg::Echo {
                content: "free fn".into(),
                reply_to: rt,
            },
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert_eq!(response.echoed, "Echo: free fn");
    }

    #[tokio::test]
    async fn test_ask_multiple_sequential() {
        let (_system, actor_ref) = echo_system().await;

        for i in 0..5 {
            let response = actor_ref
                .ask(
                    |rt| EchoMsg::Echo {
                        content: format!("msg-{i}"),
                        reply_to: rt,
                    },
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
            assert_eq!(response.echoed, format!("Echo: msg-{i}"));
        }
    }

    #[tokio::test]
    async fn test_ask_concurrent() {
        let (_system, actor_ref) = echo_system().await;

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let r = actor_ref.clone();
                tokio::spawn(async move {
                    r.ask(
                        |rt| EchoMsg::Echo {
                            content: format!("concurrent-{i}"),
                            reply_to: rt,
                        },
                        Duration::from_secs(2),
                    )
                    .await
                })
            })
            .collect();

        for (i, h) in handles.into_iter().enumerate() {
            let resp = h.await.unwrap().unwrap();
            assert_eq!(resp.echoed, format!("Echo: concurrent-{i}"));
        }
    }

    #[tokio::test]
    async fn test_tell_still_works_alongside_ask() {
        let (_system, actor_ref) = echo_system().await;

        // Tell should succeed without affecting ask
        actor_ref
            .tell(
                EchoMsg::Fire {
                    content: "fire".into(),
                },
                None,
            )
            .unwrap();

        // Ask should still work after tells
        let response = actor_ref
            .ask(
                |rt| EchoMsg::Echo {
                    content: "after tell".into(),
                    reply_to: rt,
                },
                Duration::from_secs(1),
            )
            .await
            .unwrap();

        assert_eq!(response.echoed, "Echo: after tell");
    }

    #[tokio::test]
    async fn test_ask_timeout() {
        let system = ActorSystem::new(ActorSystemConfig::default()).await.unwrap();
        let actor_ref = system
            .spawn_actor("slow", SlowActor::default(), ActorProps::default())
            .unwrap();
        sleep(Duration::from_millis(10)).await;

        let start = std::time::Instant::now();
        let result = actor_ref
            .ask(
                |rt| SlowMsg::Query { reply_to: rt },
                Duration::from_millis(50),
            )
            .await;

        assert!(
            matches!(result, Err(crate::AskError::Timeout { .. })),
            "expected Timeout, got {result:?}"
        );
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "timeout should fire promptly"
        );
    }

    #[tokio::test]
    async fn test_channel_closed_when_actor_stops() {
        let system = ActorSystem::new(ActorSystemConfig::default()).await.unwrap();
        let actor_ref = system
            .spawn_actor("doomed", SlowActor::default(), ActorProps::default())
            .unwrap();
        sleep(Duration::from_millis(10)).await;

        // Stop the actor while a query is in flight — reply_to is dropped,
        // the oneshot receiver gets a ChannelClosed error.
        let ask_fut = actor_ref.ask(
            |rt| SlowMsg::Query { reply_to: rt },
            Duration::from_secs(5),
        );
        actor_ref.stop().await.unwrap();

        let result = ask_fut.await;
        assert!(
            matches!(
                result,
                Err(crate::AskError::ChannelClosed) | Err(crate::AskError::Timeout { .. })
            ),
            "expected ChannelClosed or Timeout, got {result:?}"
        );
    }
}