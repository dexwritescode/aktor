use aktor::*;
use async_trait::async_trait;
use std::time::Duration;
use tokio::time::sleep;

// Example messages as specified in the implementation plan
#[derive(Debug, Clone)]
struct GetStatus;

impl Message for GetStatus {
    fn type_id(&self) -> &'static str {
        "GetStatus"
    }
}

#[derive(Debug, Clone)]
struct Status {
    #[allow(dead_code)]
    running: bool,
    #[allow(dead_code)]
    uptime: Duration,
    message_count: u64,
}

impl Message for Status {
    fn type_id(&self) -> &'static str {
        "Status"
    }
}



// Example actor that can respond to ask pattern requests
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

#[async_trait]
impl Actor<GetStatus> for StatusActor {
    async fn handle(&mut self, _msg: GetStatus, ctx: &ActorContext<GetStatus>) -> Result<(), ActorError> {
        self.message_count += 1;

        if ctx.is_ask_request() {
            // This is an ask request - send response
            println!("StatusActor received ask request #{} (correlation: {:?})",
                     self.message_count, ctx.correlation_id());

            let status = Status {
                running: true,
                uptime: self.start_time.elapsed(),
                message_count: self.message_count,
            };

            ctx.respond(status).await?;
        } else {
            // This is a tell message
            println!("StatusActor received tell message #{}", self.message_count);
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for better debugging
    tracing_subscriber::fmt::init();

    println!("=== Ask Pattern Demo ===");

    // Create actor system
    let config = ActorSystemConfig::default();
    let system = ActorSystem::new(config)?;

    // Spawn status actor
    let status_actor = StatusActor::default();
    let actor_ref = system.spawn_actor("status-actor", status_actor, ActorProps::default()).await?;

    // Give the actor time to start
    sleep(Duration::from_millis(100)).await;

    println!("\n1. Basic Ask Pattern Example (from implementation plan):");

    // Example 1: Basic ask pattern as shown in the plan
    let message = GetStatus;
    let future = ask_with_actor_ref::<GetStatus, Status>(&actor_ref, message, Duration::from_secs(5));

    match future.await {
        Ok(response) => {
            println!("✅ Received response: {:?}", response);
        }
        Err(e) => {
            println!("❌ Ask failed: {:?}", e);
        }
    }

    println!("\n2. Using ActorRef.ask method:");

    // Example 2: Using the ask method directly on ActorRef
    let message = GetStatus;
    let result: Result<Status, AskError> = actor_ref.ask(message, Duration::from_secs(5)).await;

    match result {
        Ok(response) => {
            println!("✅ Direct ask response: {:?}", response);
        }
        Err(e) => {
            println!("❌ Direct ask failed: {:?}", e);
        }
    }

    println!("\n3. Using Extension Trait:");

    // Example 3: Using the extension trait
    use aktor::AskExt;
    let message = GetStatus;
    let future = actor_ref.ask_ext::<Status>(message, Duration::from_secs(5));

    match future.await {
        Ok(response) => {
            println!("✅ Extension trait response: {:?}", response);
        }
        Err(e) => {
            println!("❌ Extension trait ask failed: {:?}", e);
        }
    }

    println!("\n4. Ask with Default Timeout:");

    // Example 4: Using default timeout (5 seconds)
    let message = GetStatus;
    let future = actor_ref.ask_default::<Status>(message);

    match future.await {
        Ok(response) => {
            println!("✅ Default timeout response: {:?}", response);
        }
        Err(e) => {
            println!("❌ Default timeout ask failed: {:?}", e);
        }
    }

    println!("\n5. Timeout Example:");

    // Example 5: Demonstrating timeout behavior
    let message = GetStatus;
    let result: Result<Status, AskError> = actor_ref.ask(message, Duration::from_millis(1)).await;

    match result {
        Ok(response) => {
            println!("✅ Fast response: {:?}", response);
        }
        Err(AskError::Timeout { timeout }) => {
            println!("⏰ Ask timed out after {:?} (this is expected)", timeout);
        }
        Err(e) => {
            println!("❌ Other error: {:?}", e);
        }
    }

    println!("\n6. Multiple Concurrent Asks:");

    // Example 6: Multiple concurrent ask requests
    let futures = (0..3).map(|i| {
        let actor_ref = actor_ref.clone();
        tokio::spawn(async move {
            let message = GetStatus;
            let result: Result<Status, AskError> = actor_ref.ask(message, Duration::from_secs(5)).await;
            (i, result)
        })
    });

    for future in futures {
        if let Ok((i, result)) = future.await {
            match result {
                Ok(response) => {
                    println!("✅ Concurrent ask #{}: message_count={}", i, response.message_count);
                }
                Err(e) => {
                    println!("❌ Concurrent ask #{} failed: {:?}", i, e);
                }
            }
        }
    }

    println!("\n7. Tell vs Ask Comparison:");

    // Example 7: Compare tell vs ask
    println!("Sending tell message (fire-and-forget)...");
    actor_ref.tell(GetStatus, None).await?;

    println!("Sending ask message (request-response)...");
    let message = GetStatus;
    match actor_ref.ask::<Status>(message, Duration::from_secs(5)).await {
        Ok(response) => {
            println!("✅ Ask response shows message count: {}", response.message_count);
        }
        Err(e) => {
            println!("❌ Ask failed: {:?}", e);
        }
    }

    // Shutdown the system gracefully
    system.shutdown().await?;

    println!("\n=== Demo Complete ===");
    println!("\nNote: Some asks may timeout because the current implementation");
    println!("doesn't fully integrate ask-aware actors with the message processing loop.");
    println!("This demonstrates the basic ask pattern infrastructure is in place!");

    Ok(())
}