//! Demonstration of the Aktor TestKit
//!
//! This example shows how to use the test-util feature for testing actors
//! in both synchronous (unit testing) and asynchronous (integration testing) modes.
//!
//! Run with: cargo run --example test_kit_demo --features test-util

use aktor::*;
use std::time::Duration;

// ── Messages ──────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum CounterMsg {
    Increment,
    Decrement,
    /// Ask variant — actor replies with the current count.
    GetCount {
        reply_to: ReplyTo<i32>,
    },
}

impl Message for CounterMsg {
    fn type_id(&self) -> &'static str {
        "CounterMsg"
    }
}

// ── Actor ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct CounterActor {
    count: i32,
}

impl Actor for CounterActor {
    type Msg = CounterMsg;

    fn handle(&mut self, msg: CounterMsg, _ctx: &ActorContext<CounterMsg>) {
        match msg {
            CounterMsg::Increment => {
                self.count += 1;
                println!("Counter incremented to: {}", self.count);
            }
            CounterMsg::Decrement => {
                self.count -= 1;
                println!("Counter decremented to: {}", self.count);
            }
            CounterMsg::GetCount { reply_to } => {
                reply_to.reply(self.count);
            }
        }
    }
}

// ── Also show a response type for the probe demo ──────────────────────────────

#[derive(Debug, Clone, PartialEq)]
struct CounterResponse {
    count: i32,
}

impl Message for CounterResponse {
    fn type_id(&self) -> &'static str {
        "CounterResponse"
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aktor TestKit Demo ===\n");

    println!("1. Asynchronous Integration Testing:");
    asynchronous_testing_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    println!("2. Ask Pattern Testing:");
    ask_pattern_testing_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    println!("3. Test Probe Message Capture:");
    test_probe_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    println!("4. Synchronous Unit Testing:");
    synchronous_testing_demo().await?;

    println!("\n=== Demo Complete ===");
    Ok(())
}

async fn asynchronous_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating ActorTestKit for integration testing...");

    let test_kit = ActorTestKit::new().await;
    let counter = test_kit.spawn(CounterActor::default(), "counter")?;

    println!("✅ Counter actor spawned successfully");

    counter.tell(CounterMsg::Increment, None)?;
    counter.tell(CounterMsg::Increment, None)?;
    counter.tell(CounterMsg::Decrement, None)?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("✅ Basic operations completed");

    test_kit.shutdown().await?;
    println!("✅ Test kit shutdown complete");

    Ok(())
}

async fn ask_pattern_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing ask pattern with ActorTestKit...");

    let test_kit = ActorTestKit::new().await;
    let counter = test_kit.spawn(CounterActor::default(), "ask-counter")?;

    counter.tell(CounterMsg::Increment, None)?;
    counter.tell(CounterMsg::Increment, None)?;
    counter.tell(CounterMsg::Increment, None)?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let count: i32 = counter
        .ask(
            |rt| CounterMsg::GetCount { reply_to: rt },
            Duration::from_secs(1),
        )
        .await?;

    println!("✅ Ask pattern successful! Count: {}", count);
    assert_eq!(count, 3);

    test_kit.shutdown().await?;
    Ok(())
}

async fn test_probe_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demonstrating test probes for message capture...");

    let test_kit = ActorTestKit::new().await;
    let probe = test_kit.create_test_probe::<CounterResponse>().await;

    println!("✅ Test probe created");

    probe
        .actor_ref()
        .tell(TestMessage::new(CounterResponse { count: 42 }), None)?;

    tokio::time::sleep(Duration::from_millis(50)).await;

    let result = probe.expect_any_message().await;
    match result {
        ExpectationResult::Success(response) => {
            println!("✅ Probe captured message: count = {}", response.count);
            assert_eq!(response.count, 42);
        }
        ExpectationResult::Timeout => println!("❌ Probe timed out"),
        other => println!("❌ Unexpected: {:?}", other),
    }

    let no_msg_result = probe.expect_no_message(Duration::from_millis(100)).await;
    match no_msg_result {
        ExpectationResult::Success(_) => println!("✅ Correctly detected no additional messages"),
        other => println!("❌ Expected no message but got: {:?}", other),
    }

    test_kit.shutdown().await?;
    Ok(())
}

async fn synchronous_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demonstrating synchronous unit testing...");

    // Tell context
    let test_context = TestContext::<CounterResponse>::new();
    let mock_context = test_context.mock_actor_context().await;
    assert!(!mock_context.is_ask_request());
    assert!(mock_context.correlation_id().is_none());
    println!("✅ Tell context correctly configured");

    // Ask context
    let ask_context = TestContext::<CounterResponse>::new_ask();
    let mock_ask_context = ask_context.mock_actor_context().await;
    assert!(mock_ask_context.is_ask_request());
    assert!(mock_ask_context.correlation_id().is_some());
    println!("✅ Ask context correctly configured");

    let response = CounterResponse { count: 123 };
    let respond_result = mock_ask_context.mock_respond(response).await;
    assert!(respond_result.is_ok());
    println!("✅ Mock response successful");

    let captured = ask_context.response().await;
    assert_eq!(captured.unwrap().count, 123);
    println!("✅ Response correctly captured");

    // Error case: respond on a tell context
    let tell_context = TestContext::<CounterResponse>::new();
    let mock_tell = tell_context.mock_actor_context().await;
    assert!(
        mock_tell
            .mock_respond(CounterResponse { count: 999 })
            .await
            .is_err()
    );
    println!("✅ Correctly rejected response to tell message");

    Ok(())
}
