//! Test utilities for the Aktor actor system
//!
//! This module provides testing utilities similar to Akka's TestKit, enabling both
//! synchronous and asynchronous testing of actors.
//!
//! # Features
//! - **ActorTestKit**: Full actor system for integration testing
//! - **TestProbe**: Message capture and verification
//! - **TestContext**: Lightweight context for unit testing
//! - **Expectation helpers**: Fluent assertion API
//!
//! # Examples
//!
//! ## Asynchronous Testing (Integration)
//! ```rust
//! use aktor::test_kit::*;
//!
//! #[tokio::test]
//! async fn test_echo_actor() {
//!     let test_kit = ActorTestKit::new();
//!     let probe = test_kit.create_test_probe::<String>();
//!
//!     let echo = test_kit.spawn(EchoActor::default(), "echo").await?;
//!
//!     echo.tell("hello".to_string(), Some(probe.actor_ref())).await?;
//!
//!     probe.expect_message("hello").await;
//!     probe.expect_no_message(Duration::from_millis(100)).await;
//! }
//! ```
//!
//! ## Synchronous Testing (Unit)
//! ```rust
//! use aktor::test_kit::*;
//!
//! #[tokio::test]
//! async fn test_echo_actor_logic() {
//!     let mut actor = EchoActor::default();
//!     let test_context = TestContext::new();
//!
//!     actor.handle("test".to_string(), &test_context).await?;
//!
//!     assert_eq!(test_context.sent_messages().len(), 1);
//! }
//! ```

use crate::{Actor, ActorContext, ActorError, ActorProps, ActorRef, ActorSystem, ActorSystemConfig, Message};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{timeout, sleep};
use uuid::Uuid;

/// Test kit for actor system testing
///
/// Provides a full actor system environment for integration testing.
/// Similar to Akka's ActorTestKit but designed for Rust.
pub struct ActorTestKit {
    /// The underlying actor system
    system: Arc<ActorSystem<TestMessage>>,
    /// Test probes created by this test kit
    probes: Arc<RwLock<Vec<Arc<dyn TestProbeRef>>>>,
}

/// Generic test message that can wrap any message type
#[derive(Debug)]
pub struct TestMessage {
    /// The actual message content
    pub content: Box<dyn std::any::Any + Send + Sync>,
    /// Type name for debugging
    pub type_name: &'static str,
    /// Message ID for tracking
    pub id: Uuid,
}

impl Clone for TestMessage {
    fn clone(&self) -> Self {
        // We can't clone the boxed Any, so we create a new message with the same metadata
        // This is acceptable for testing purposes where we mainly care about the type information
        Self {
            content: Box::new(format!("Cloned TestMessage (original type: {})", self.type_name)),
            type_name: self.type_name,
            id: self.id,
        }
    }
}

impl Message for TestMessage {
    fn type_id(&self) -> &'static str {
        "TestMessage"
    }
}

impl TestMessage {
    /// Create a new test message
    pub fn new<M: Message + 'static>(message: M) -> Self {
        Self {
            content: Box::new(message),
            type_name: std::any::type_name::<M>(),
            id: Uuid::new_v4(),
        }
    }

    /// Try to extract the message as a specific type
    pub fn extract<M: Message + 'static>(&self) -> Option<&M> {
        self.content.downcast_ref::<M>()
    }

    /// Try to extract the message as a specific type (owned)
    pub fn into_inner<M: Message + 'static>(self) -> Result<M, TestMessage> {
        match self.content.downcast::<M>() {
            Ok(message) => Ok(*message),
            Err(content) => Err(TestMessage {
                content,
                type_name: self.type_name,
                id: self.id,
            }),
        }
    }
}

/// Test probe for capturing and verifying messages
///
/// A test probe is a special actor that captures all messages sent to it
/// and provides utilities for verifying those messages in tests.
pub struct TestProbe<M: Message> {
    /// Actor reference for this probe
    actor_ref: ActorRef<TestMessage>,
    /// Captured messages
    messages: Arc<Mutex<VecDeque<M>>>,
    /// Probe ID for debugging
    id: Uuid,
}

/// Trait for type-erased test probe references
pub trait TestProbeRef: Send + Sync {
    #[allow(dead_code)]
    fn id(&self) -> Uuid;
    #[allow(dead_code)]
    fn actor_ref(&self) -> &ActorRef<TestMessage>;
}

impl<M: Message> TestProbeRef for TestProbe<M> {
    fn id(&self) -> Uuid {
        self.id
    }

    fn actor_ref(&self) -> &ActorRef<TestMessage> {
        &self.actor_ref
    }
}

/// Test expectation result
#[derive(Debug)]
pub enum ExpectationResult<T> {
    /// Expected message received
    Success(T),
    /// No message received within timeout
    Timeout,
    /// Wrong message type received
    WrongType {
        expected: &'static str,
        actual: &'static str,
    },
    /// Unexpected message content
    WrongContent {
        expected: String,
        actual: String,
    },
}

impl<T> ExpectationResult<T> {
    /// Unwrap the result, panicking with a descriptive message on failure
    pub fn unwrap(self) -> T {
        match self {
            ExpectationResult::Success(value) => value,
            ExpectationResult::Timeout => {
                panic!("Expected message but none received within timeout");
            }
            ExpectationResult::WrongType { expected, actual } => {
                panic!("Expected message of type {} but got {}", expected, actual);
            }
            ExpectationResult::WrongContent { expected, actual } => {
                panic!("Expected message content '{}' but got '{}'", expected, actual);
            }
        }
    }

    /// Check if the expectation was successful
    pub fn is_success(&self) -> bool {
        matches!(self, ExpectationResult::Success(_))
    }

    /// Check if the expectation timed out
    pub fn is_timeout(&self) -> bool {
        matches!(self, ExpectationResult::Timeout)
    }
}

impl ActorTestKit {
    /// Create a new test kit with default configuration
    pub fn new() -> Self {
        let config = ActorSystemConfig::default();
        let system = ActorSystem::new(config).expect("Failed to create test actor system");

        Self {
            system,
            probes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a new test kit with custom configuration
    pub fn with_config(config: ActorSystemConfig) -> Self {
        let system = ActorSystem::new(config).expect("Failed to create test actor system");

        Self {
            system,
            probes: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Get the underlying actor system
    pub fn system(&self) -> &Arc<ActorSystem<TestMessage>> {
        &self.system
    }

    /// Spawn an actor in the test environment
    pub async fn spawn<A>(&self, actor: A, name: &str) -> Result<ActorRef<TestMessage>, ActorError>
    where
        A: Actor<TestMessage> + 'static,
    {
        self.system.spawn_actor(name, actor, ActorProps::default()).await
    }

    /// Spawn an actor with custom props
    pub async fn spawn_with_props<A>(
        &self,
        actor: A,
        name: &str,
        props: ActorProps,
    ) -> Result<ActorRef<TestMessage>, ActorError>
    where
        A: Actor<TestMessage> + 'static,
    {
        self.system.spawn_actor(name, actor, props).await
    }

    /// Create a test probe for capturing messages of type M
    pub async fn create_test_probe<M: Message + 'static>(&self) -> Arc<TestProbe<M>> {
        let probe_id = Uuid::new_v4();
        let messages = Arc::new(Mutex::new(VecDeque::new()));

        // Create probe actor
        let probe_actor = TestProbeActor {
            messages: messages.clone(),
            message_type: std::marker::PhantomData::<M>,
        };

        let actor_ref = self
            .spawn(probe_actor, &format!("test-probe-{}", probe_id))
            .await
            .expect("Failed to spawn test probe actor");

        let probe = Arc::new(TestProbe {
            actor_ref,
            messages,
            id: probe_id,
        });

        // Register probe
        {
            let mut probes = self.probes.write().await;
            probes.push(probe.clone() as Arc<dyn TestProbeRef>);
        }

        probe
    }

    /// Shutdown the test kit and all actors
    pub async fn shutdown(self) -> Result<(), ActorError> {
        self.system.shutdown().await
    }

    /// Get all test probes created by this test kit
    pub async fn probes(&self) -> Vec<Arc<dyn TestProbeRef>> {
        let probes = self.probes.read().await;
        probes.clone()
    }
}

impl Default for ActorTestKit {
    fn default() -> Self {
        Self::new()
    }
}

impl<M: Message + 'static> TestProbe<M> {
    /// Get the actor reference for this probe
    pub fn actor_ref(&self) -> &ActorRef<TestMessage> {
        &self.actor_ref
    }

    /// Get the probe ID
    pub fn probe_id(&self) -> Uuid {
        self.id
    }

    /// Expect a specific message within the default timeout (1 second)
    pub async fn expect_message(&self, expected: M) -> ExpectationResult<M>
    where
        M: PartialEq + Clone,
    {
        self.expect_message_timeout(expected, Duration::from_secs(1)).await
    }

    /// Expect a specific message within the specified timeout
    pub async fn expect_message_timeout(&self, expected: M, timeout_duration: Duration) -> ExpectationResult<M>
    where
        M: PartialEq + Clone,
    {
        let result = timeout(timeout_duration, async {
            loop {
                {
                    let mut messages = self.messages.lock().await;
                    if let Some(message) = messages.pop_front() {
                        if message == expected {
                            return ExpectationResult::Success(message);
                        } else {
                            return ExpectationResult::WrongContent {
                                expected: format!("{:?}", expected),
                                actual: format!("{:?}", message),
                            };
                        }
                    }
                }
                // Wait a bit before checking again
                sleep(Duration::from_millis(10)).await;
            }
        }).await;

        match result {
            Ok(expectation) => expectation,
            Err(_) => ExpectationResult::Timeout,
        }
    }

    /// Expect any message of type M within the default timeout
    pub async fn expect_any_message(&self) -> ExpectationResult<M> {
        self.expect_any_message_timeout(Duration::from_secs(1)).await
    }

    /// Expect any message of type M within the specified timeout
    pub async fn expect_any_message_timeout(&self, timeout_duration: Duration) -> ExpectationResult<M> {
        let result = timeout(timeout_duration, async {
            loop {
                {
                    let mut messages = self.messages.lock().await;
                    if let Some(message) = messages.pop_front() {
                        return ExpectationResult::Success(message);
                    }
                }
                // Wait a bit before checking again
                sleep(Duration::from_millis(10)).await;
            }
        }).await;

        match result {
            Ok(expectation) => expectation,
            Err(_) => ExpectationResult::Timeout,
        }
    }

    /// Expect no message for the specified duration
    pub async fn expect_no_message(&self, duration: Duration) -> ExpectationResult<()> {
        sleep(duration).await;

        let messages = self.messages.lock().await;
        if messages.is_empty() {
            ExpectationResult::Success(())
        } else {
            ExpectationResult::WrongContent {
                expected: "no message".to_string(),
                actual: format!("{} messages in queue", messages.len()),
            }
        }
    }

    /// Get the number of messages currently in the probe's queue
    pub async fn message_count(&self) -> usize {
        let messages = self.messages.lock().await;
        messages.len()
    }

    /// Drain all messages from the probe
    pub async fn drain_messages(&self) -> Vec<M> {
        let mut messages = self.messages.lock().await;
        messages.drain(..).collect()
    }

    /// Peek at the next message without removing it
    pub async fn peek_message(&self) -> Option<M>
    where
        M: Clone,
    {
        let messages = self.messages.lock().await;
        messages.front().cloned()
    }
}

/// Test probe actor implementation
struct TestProbeActor<M: Message> {
    messages: Arc<Mutex<VecDeque<M>>>,
    message_type: std::marker::PhantomData<M>,
}

#[async_trait::async_trait]
impl<M: Message + 'static> Actor<TestMessage> for TestProbeActor<M> {
    async fn handle(&mut self, msg: TestMessage, _ctx: &ActorContext<TestMessage>) {
        // Try to extract the message as type M
        if let Some(typed_message) = msg.extract::<M>() {
            let mut messages = self.messages.lock().await;
            messages.push_back(typed_message.clone());
        }
        // Ignore messages that don't match our type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// TestContext for synchronous testing of actor logic
///
/// Provides a lightweight context for testing actor behavior without spinning up
/// a full actor system. This is useful for unit testing individual actor methods.
pub struct TestContext<M: Message> {
    /// Messages sent by the actor during testing
    sent_messages: Arc<Mutex<Vec<M>>>,
    /// Whether this is an ask request
    is_ask: bool,
    /// Response sent by the actor (for ask testing)
    response: Arc<Mutex<Option<M>>>,
}

impl<M: Message> TestContext<M> {
    /// Create a new test context for tell messages
    pub fn new() -> Self {
        Self {
            sent_messages: Arc::new(Mutex::new(Vec::new())),
            is_ask: false,
            response: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a new test context for ask messages
    pub fn new_ask() -> Self {
        Self {
            sent_messages: Arc::new(Mutex::new(Vec::new())),
            is_ask: true,
            response: Arc::new(Mutex::new(None)),
        }
    }

    /// Create a mock ActorContext for testing
    ///
    /// Note: This creates a minimal context that may not support all operations.
    /// For full integration testing, use ActorTestKit instead.
    pub async fn mock_actor_context(&self) -> MockActorContext<M> {
        MockActorContext {
            sent_messages: self.sent_messages.clone(),
            is_ask: self.is_ask,
            response: self.response.clone(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get all messages sent during testing
    pub async fn sent_messages(&self) -> Vec<M>
    where
        M: Clone,
    {
        let messages = self.sent_messages.lock().await;
        messages.clone()
    }

    /// Get the response (for ask testing)
    pub async fn response(&self) -> Option<M>
    where
        M: Clone,
    {
        let response = self.response.lock().await;
        response.clone()
    }

    /// Clear all recorded messages and responses
    pub async fn clear(&self) {
        let mut messages = self.sent_messages.lock().await;
        messages.clear();
        let mut response = self.response.lock().await;
        *response = None;
    }
}

impl<M: Message> Default for TestContext<M> {
    fn default() -> Self {
        Self::new()
    }
}

/// Mock ActorContext for synchronous testing
pub struct MockActorContext<M: Message> {
    sent_messages: Arc<Mutex<Vec<M>>>,
    is_ask: bool,
    response: Arc<Mutex<Option<M>>>,
    _phantom: std::marker::PhantomData<M>,
}

impl<M: Message> MockActorContext<M> {
    /// Record a message as "sent" for testing purposes
    pub async fn record_sent_message(&self, message: M) {
        let mut messages = self.sent_messages.lock().await;
        messages.push(message);
    }

    /// Simulate responding to an ask request
    pub async fn mock_respond(&self, response: M) -> Result<(), ActorError> {
        if !self.is_ask {
            return Err(ActorError::MessageDeliveryFailed(
                "Cannot respond: not an ask request".to_string(),
            ));
        }

        let mut response_slot = self.response.lock().await;
        *response_slot = Some(response);
        Ok(())
    }

    /// Check if this is an ask request
    pub fn is_ask_request(&self) -> bool {
        self.is_ask
    }

    /// Get correlation ID (returns a dummy ID for testing)
    pub fn correlation_id(&self) -> Option<Uuid> {
        if self.is_ask {
            Some(Uuid::new_v4())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, ActorContext, ActorError, Message};
    use async_trait::async_trait;

    #[derive(Debug, Clone, PartialEq)]
    struct EchoMessage {
        content: String,
    }

    impl Message for EchoMessage {
        fn type_id(&self) -> &'static str {
            "EchoMessage"
        }
    }

    struct EchoActor;

    #[async_trait]
    impl Actor<TestMessage> for EchoActor {
        async fn handle(&mut self, msg: TestMessage, ctx: &ActorContext<TestMessage>) {
            if let Some(echo_msg) = msg.extract::<EchoMessage>() {
                if ctx.is_ask_request() {
                    // For ask requests, respond with the message
                    let _ = ctx.respond(TestMessage::new(echo_msg.clone())).await;
                } else {
                    // For tell messages, we need to manually send to sender if available
                    // This simulates echoing back to the sender
                    // In a real test scenario, you might want to use ask pattern instead
                    println!("EchoActor received tell message: {:?}", echo_msg);

                    // For this test, let's simulate sending a message to the probe
                    // This is a simplified approach - in real usage, you'd typically use ask pattern
                }
            }
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[tokio::test]
    async fn test_actor_test_kit_creation() {
        let test_kit = ActorTestKit::new();
        assert!(test_kit.system().node_id().starts_with("node-"));

        test_kit.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_test_probe_creation() {
        let test_kit = ActorTestKit::new();
        let probe = test_kit.create_test_probe::<EchoMessage>().await;

        assert_eq!(probe.message_count().await, 0);

        test_kit.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_message_expectation() {
        let test_kit = ActorTestKit::new();
        let probe = test_kit.create_test_probe::<EchoMessage>().await;

        let _echo = test_kit.spawn(EchoActor, "echo").await.unwrap();

        let message = EchoMessage {
            content: "test".to_string(),
        };

        // Send message directly to the probe to test basic functionality
        probe.actor_ref().tell(TestMessage::new(message.clone()), None).await.unwrap();

        // Wait a bit for message processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Debug: Check how many messages the probe received
        println!("Probe message count: {}", probe.message_count().await);

        // Expect the message
        let result = probe.expect_any_message().await;
        match &result {
            ExpectationResult::Success(msg) => {
                println!("Success: Got message: {:?}", msg);
            }
            ExpectationResult::Timeout => {
                println!("Timeout: No message received");
            }
            other => {
                println!("Other result: {:?}", other);
            }
        }
        assert!(result.is_success());

        test_kit.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_ask_pattern_with_test_kit() {
        let test_kit = ActorTestKit::new();
        let echo = test_kit.spawn(EchoActor, "echo").await.unwrap();

        let message = EchoMessage {
            content: "ask test".to_string(),
        };

        // Test ask pattern with echo actor
        let response: Result<TestMessage, _> = echo.ask(TestMessage::new(message.clone()), Duration::from_secs(1)).await;

        match response {
            Ok(response_msg) => {
                if let Some(echo_response) = response_msg.extract::<EchoMessage>() {
                    assert_eq!(echo_response.content, "ask test");
                    println!("Ask pattern test successful: {:?}", echo_response);
                } else {
                    panic!("Expected EchoMessage in response");
                }
            }
            Err(e) => {
                panic!("Ask pattern failed: {:?}", e);
            }
        }

        test_kit.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_test_context_synchronous_testing() {
        // Test synchronous actor logic testing with TestContext
        let test_context = TestContext::<EchoMessage>::new();
        let mock_context = test_context.mock_actor_context().await;

        // Simulate handling a message
        let message = EchoMessage {
            content: "sync test".to_string(),
        };

        // Test that we can detect this is not an ask request
        assert!(!mock_context.is_ask_request());
        assert!(mock_context.correlation_id().is_none());

        // Record that a message was "sent"
        mock_context.record_sent_message(message.clone()).await;

        // Verify the message was recorded
        let sent_messages = test_context.sent_messages().await;
        assert_eq!(sent_messages.len(), 1);
        assert_eq!(sent_messages[0].content, "sync test");
    }

    #[tokio::test]
    async fn test_test_context_ask_testing() {
        // Test ask pattern with TestContext
        let test_context = TestContext::<EchoMessage>::new_ask();
        let mock_context = test_context.mock_actor_context().await;

        // Test that we can detect this is an ask request
        assert!(mock_context.is_ask_request());
        assert!(mock_context.correlation_id().is_some());

        let response_message = EchoMessage {
            content: "ask response".to_string(),
        };

        // Simulate responding
        let result = mock_context.mock_respond(response_message.clone()).await;
        assert!(result.is_ok());

        // Verify the response was recorded
        let response = test_context.response().await;
        assert!(response.is_some());
        assert_eq!(response.unwrap().content, "ask response");
    }

    #[tokio::test]
    async fn test_test_context_error_handling() {
        // Test error handling when trying to respond to tell message
        let test_context = TestContext::<EchoMessage>::new(); // Tell context
        let mock_context = test_context.mock_actor_context().await;

        let response_message = EchoMessage {
            content: "should fail".to_string(),
        };

        // Should fail because this is not an ask request
        let result = mock_context.mock_respond(response_message).await;
        assert!(result.is_err());

        if let Err(ActorError::MessageDeliveryFailed(msg)) = result {
            assert!(msg.contains("not an ask request"));
        } else {
            panic!("Expected MessageDeliveryFailed error");
        }
    }
}