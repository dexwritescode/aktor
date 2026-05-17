//! Demonstration of the Aktor TestKit
//!
//! This example shows how to use the test-util feature for testing actors
//! in both synchronous (unit testing) and asynchronous (integration testing) modes.
//!
//! Run with: cargo run --example test_kit_demo --features test-util

use aktor::*;
use std::time::Duration;

// Example messages for our demo
#[derive(Debug, Clone, PartialEq)]
struct CounterMessage {
    operation: CounterOp,
}

#[derive(Debug, Clone, PartialEq)]
enum CounterOp {
    Increment,
    Decrement,
    GetCount,
}

impl Message for CounterMessage {
    fn type_id(&self) -> &'static str {
        "CounterMessage"
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CounterResponse {
    count: i32,
}

impl Message for CounterResponse {
    fn type_id(&self) -> &'static str {
        "CounterResponse"
    }
}

// Example actor for testing
#[derive(Debug)]
struct CounterActor {
    count: i32,
}

impl Default for CounterActor {
    fn default() -> Self {
        Self { count: 0 }
    }
}

impl Actor for CounterActor {
    type Msg = TestMessage;

    fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
        if let Some(counter_msg) = msg.extract::<CounterMessage>() {
            match counter_msg.operation {
                CounterOp::Increment => {
                    self.count += 1;
                    println!("Counter incremented to: {}", self.count);
                }
                CounterOp::Decrement => {
                    self.count -= 1;
                    println!("Counter decremented to: {}", self.count);
                }
                CounterOp::GetCount => {
                    let response = CounterResponse { count: self.count };

                    if ctx.is_ask_request() {
                        println!("Responding to ask with count: {}", self.count);
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            let _ = ctx.respond(TestMessage::new(response)).await;
                        });
                    } else {
                        println!("Tell message - current count: {}", self.count);
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Aktor TestKit Demo ===\n");

    // Demo 1: Asynchronous Integration Testing
    println!("1. Asynchronous Integration Testing:");
    asynchronous_testing_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    // Demo 2: Ask Pattern Testing
    println!("2. Ask Pattern Testing:");
    ask_pattern_testing_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    // Demo 3: Test Probe Message Capture
    println!("3. Test Probe Message Capture:");
    test_probe_demo().await?;

    println!("\n{}\n", "=".repeat(50));

    // Demo 4: Synchronous Unit Testing
    println!("4. Synchronous Unit Testing:");
    synchronous_testing_demo().await?;

    println!("\n=== Demo Complete ===");
    Ok(())
}

async fn asynchronous_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating ActorTestKit for integration testing...");

    let test_kit = ActorTestKit::new().await;
    let counter = test_kit.spawn(CounterActor::default(), "counter").await?;

    println!("✅ Counter actor spawned successfully");

    // Test basic operations
    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Increment,
        }),
        None,
    )?;

    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Increment,
        }),
        None,
    )?;

    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Decrement,
        }),
        None,
    )?;

    // Wait for message processing
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("✅ Basic operations completed");

    test_kit.shutdown().await?;
    println!("✅ Test kit shutdown complete");

    Ok(())
}

async fn ask_pattern_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing ask pattern with ActorTestKit...");

    let test_kit = ActorTestKit::new().await;
    let counter = test_kit
        .spawn(CounterActor::default(), "ask-counter")
        .await?;

    // Increment a few times
    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Increment,
        }),
        None,
    )?;
    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Increment,
        }),
        None,
    )?;
    counter.tell(
        TestMessage::new(CounterMessage {
            operation: CounterOp::Increment,
        }),
        None,
    )?;

    // Wait for processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Ask for the current count
    let response: TestMessage = counter
        .ask(
            TestMessage::new(CounterMessage {
                operation: CounterOp::GetCount,
            }),
            Duration::from_secs(1),
        )
        .await?;

    if let Some(counter_response) = response.extract::<CounterResponse>() {
        println!(
            "✅ Ask pattern successful! Count: {}",
            counter_response.count
        );
        assert_eq!(counter_response.count, 3);
    } else {
        println!("❌ Failed to extract CounterResponse from ask response");
    }

    test_kit.shutdown().await?;
    Ok(())
}

async fn test_probe_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demonstrating test probes for message capture...");

    let test_kit = ActorTestKit::new().await;
    let probe = test_kit.create_test_probe::<CounterResponse>().await;

    println!("✅ Test probe created");

    // Send a message directly to the probe
    probe
        .actor_ref()
        .tell(TestMessage::new(CounterResponse { count: 42 }), None)?;

    // Wait for message processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify the probe captured the message
    let result = probe.expect_any_message().await;
    match result {
        ExpectationResult::Success(response) => {
            println!("✅ Probe captured message: count = {}", response.count);
            assert_eq!(response.count, 42);
        }
        ExpectationResult::Timeout => {
            println!("❌ Probe timed out waiting for message");
        }
        other => {
            println!("❌ Unexpected probe result: {:?}", other);
        }
    }

    // Test expect_no_message
    let no_msg_result = probe.expect_no_message(Duration::from_millis(100)).await;
    match no_msg_result {
        ExpectationResult::Success(_) => {
            println!("✅ Correctly detected no additional messages");
        }
        other => {
            println!("❌ Expected no message but got: {:?}", other);
        }
    }

    test_kit.shutdown().await?;
    Ok(())
}

async fn synchronous_testing_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("Demonstrating synchronous unit testing...");

    // Test tell message handling
    println!("Testing tell message handling...");
    let test_context = TestContext::<CounterMessage>::new();
    let mock_context = test_context.mock_actor_context().await;

    assert!(!mock_context.is_ask_request());
    assert!(mock_context.correlation_id().is_none());
    println!("✅ Tell context correctly configured");

    // Test ask message handling
    println!("Testing ask message handling...");
    let ask_context = TestContext::<CounterResponse>::new_ask();
    let mock_ask_context = ask_context.mock_actor_context().await;

    assert!(mock_ask_context.is_ask_request());
    assert!(mock_ask_context.correlation_id().is_some());
    println!("✅ Ask context correctly configured");

    // Simulate responding to an ask
    let response = CounterResponse { count: 123 };
    let respond_result = mock_ask_context.mock_respond(response.clone()).await;
    assert!(respond_result.is_ok());
    println!("✅ Mock response successful");

    // Verify the response was captured
    let captured_response = ask_context.response().await;
    assert!(captured_response.is_some());
    assert_eq!(captured_response.unwrap().count, 123);
    println!("✅ Response correctly captured");

    // Test error handling - try to respond to tell
    let tell_context = TestContext::<CounterResponse>::new();
    let mock_tell_context = tell_context.mock_actor_context().await;

    let error_response = CounterResponse { count: 999 };
    let error_result = mock_tell_context.mock_respond(error_response).await;
    assert!(error_result.is_err());
    println!("✅ Correctly rejected response to tell message");

    Ok(())
}
