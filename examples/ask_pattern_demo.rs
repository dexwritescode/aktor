/// Demonstrates the ReplyTo<R> ask pattern.
///
/// The reply channel is embedded directly in the message variant — no context
/// injection, no Box<dyn Any>, no runtime downcast.
use aktor::*;
use std::time::Duration;
use tokio::time::sleep;

// ── Messages ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum StatusMsg {
    /// Ask variant: caller receives a Status reply.
    GetStatus { reply_to: ReplyTo<Status> },
    /// Tell variant: fire-and-forget increment.
    Increment,
}

impl Message for StatusMsg {
    fn type_id(&self) -> &'static str {
        "StatusMsg"
    }
}

#[derive(Debug)]
struct Status {
    message_count: u64,
    uptime: Duration,
}

// ── Actor ─────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct StatusActor {
    message_count: u64,
    start_time: std::time::Instant,
}

impl Default for StatusActor {
    fn default() -> Self {
        Self {
            message_count: 0,
            start_time: std::time::Instant::now(),
        }
    }
}

impl Actor for StatusActor {
    type Msg = StatusMsg;

    fn handle(&mut self, msg: StatusMsg, _ctx: &ActorContext<StatusMsg>) {
        self.message_count += 1;
        match msg {
            StatusMsg::GetStatus { reply_to } => {
                reply_to.reply(Status {
                    message_count: self.message_count,
                    uptime: self.start_time.elapsed(),
                });
            }
            StatusMsg::Increment => {
                // fire-and-forget — nothing to reply
            }
        }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Ask Pattern Demo (ReplyTo<R>) ===\n");

    let system = ActorSystem::new(ActorSystemConfig::default()).await?;
    let actor_ref = system.spawn_actor("status", StatusActor::default(), ActorProps::default())?;
    sleep(Duration::from_millis(50)).await;

    // 1. Basic ask via ActorRef::ask
    println!("1. Basic ask via actor_ref.ask(...):");
    let status = actor_ref
        .ask(
            |rt| StatusMsg::GetStatus { reply_to: rt },
            Duration::from_secs(1),
        )
        .await?;
    println!(
        "   ✅ count={}, uptime={:?}",
        status.message_count, status.uptime
    );

    // 2. Ask via free function
    println!("\n2. Ask via ask() free function:");
    let status: Status = ask(
        &actor_ref,
        |rt| StatusMsg::GetStatus { reply_to: rt },
        Duration::from_secs(1),
    )
    .await?;
    println!("   ✅ count={}", status.message_count);

    // 3. Tell still works alongside ask
    println!("\n3. Tell (fire-and-forget):");
    actor_ref.tell(StatusMsg::Increment, None)?;
    actor_ref.tell(StatusMsg::Increment, None)?;
    sleep(Duration::from_millis(20)).await;
    let status = actor_ref
        .ask(
            |rt| StatusMsg::GetStatus { reply_to: rt },
            Duration::from_secs(1),
        )
        .await?;
    println!(
        "   ✅ count after 2 increments + ask = {}",
        status.message_count
    );

    // 4. Concurrent asks
    println!("\n4. 5 concurrent asks:");
    let handles: Vec<_> = (0..5)
        .map(|_| {
            let r = actor_ref.clone();
            tokio::spawn(async move {
                r.ask(
                    |rt| StatusMsg::GetStatus { reply_to: rt },
                    Duration::from_secs(2),
                )
                .await
            })
        })
        .collect();
    for h in handles {
        let s = h.await??;
        println!("   ✅ count={}", s.message_count);
    }

    // 5. Timeout
    println!("\n5. Timeout demo (50 ms against a stopped actor):");
    actor_ref.stop().await?;
    sleep(Duration::from_millis(20)).await;
    let system2 = ActorSystem::new(ActorSystemConfig::default()).await?;
    // Spawn a dummy that never replies
    #[derive(Debug, Default)]
    struct SilentActor;
    #[derive(Debug)]
    enum SilentMsg {
        Ping { reply_to: ReplyTo<u64> },
    }
    impl Message for SilentMsg {
        fn type_id(&self) -> &'static str {
            "SilentMsg"
        }
    }
    impl Actor for SilentActor {
        type Msg = SilentMsg;
        fn handle(&mut self, _msg: SilentMsg, _ctx: &ActorContext<SilentMsg>) {
            // intentionally no reply
        }
    }
    let silent_ref = system2.spawn_actor("silent", SilentActor, ActorProps::default())?;
    sleep(Duration::from_millis(10)).await;
    match silent_ref
        .ask(
            |rt| SilentMsg::Ping { reply_to: rt },
            Duration::from_millis(50),
        )
        .await
    {
        Err(AskError::Timeout { timeout }) => {
            println!("   ✅ Timed out after {:?} as expected", timeout);
        }
        other => println!("   ❌ Unexpected result: {:?}", other),
    }

    println!("\nDone.");
    Ok(())
}
