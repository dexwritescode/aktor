use crate::{ActorError, ActorRef, ActorSystem, Message};
use futures::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

/// Ask pattern error types
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    #[error("Ask timeout after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Response channel closed")]
    ChannelClosed,

    #[error("Actor error: {0}")]
    ActorError(#[from] ActorError),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

/// Response envelope for ask pattern
#[derive(Debug, Clone)]
pub struct AskResponse<R: Message> {
    /// The response message
    pub response: R,
    /// Correlation ID for matching request/response
    pub correlation_id: Uuid,
    /// Response timestamp
    pub timestamp: std::time::SystemTime,
}

/// Request envelope for ask pattern
#[derive(Debug, Clone)]
pub struct AskRequest<M: Message> {
    /// The request message
    pub message: M,
    /// Correlation ID for matching request/response
    pub correlation_id: Uuid,
    /// Response channel (as an actor ref to a temporary response actor)
    pub response_to: ResponseChannel,
    /// Request timestamp
    pub timestamp: std::time::SystemTime,
}

/// Response channel abstraction
#[derive(Debug, Clone)]
pub struct ResponseChannel {
    /// Channel for sending response back
    pub(crate) sender: tokio::sync::mpsc::UnboundedSender<ResponseEnvelope>,
    /// Correlation ID for this request
    pub correlation_id: Uuid,
}

/// Internal response envelope that can hold any message type
#[derive(Debug)]
pub(crate) struct ResponseEnvelope {
    /// Serialized response data
    pub data: Box<dyn std::any::Any + Send + Sync>,
    /// Type name for response
    pub type_name: &'static str,
    /// Correlation ID
    pub correlation_id: Uuid,
}

impl ResponseChannel {
    /// Send a response back through this channel
    pub async fn respond<R: Message + 'static>(&self, response: R) -> Result<(), AskError> {
        let envelope = ResponseEnvelope {
            data: Box::new(response),
            type_name: std::any::type_name::<R>(),
            correlation_id: self.correlation_id,
        };

        self.sender
            .send(envelope)
            .map_err(|_| AskError::ChannelClosed)?;

        Ok(())
    }
}

/// Core ask function - sends a message and waits for a response
pub async fn ask<M, R>(
    _system: &ActorSystem<M>,
    actor_ref: &ActorRef<M>,
    message: M,
    timeout_duration: Duration,
) -> Result<R, AskError>
where
    M: Message,
    R: Message + 'static,
{
    ask_with_actor_ref(actor_ref, message, timeout_duration).await
}

/// Ask function that works directly with an ActorRef (no system needed)
pub async fn ask_with_actor_ref<M, R>(
    actor_ref: &ActorRef<M>,
    message: M,
    timeout_duration: Duration,
) -> Result<R, AskError>
where
    M: Message,
    R: Message + 'static,
{
    // Create a one-shot channel for the response
    let (response_tx, mut response_rx) = tokio::sync::mpsc::unbounded_channel();
    let correlation_id = Uuid::new_v4();

    // Create response channel
    let response_channel = ResponseChannel {
        sender: response_tx,
        correlation_id,
    };

    // Create ask request envelope
    let ask_request = AskRequest {
        message,
        correlation_id,
        response_to: response_channel,
        timestamp: std::time::SystemTime::now(),
    };

    // Send the message through the ask request method
    actor_ref.tell_ask_request(ask_request).await?;

    // Wait for response with timeout
    let response_future = async {
        while let Some(envelope) = response_rx.recv().await {
            if envelope.correlation_id == correlation_id {
                // Try to downcast the response to the expected type
                if let Ok(response) = envelope.data.downcast::<R>() {
                    return Ok(*response);
                } else {
                    return Err(AskError::SerializationError(format!(
                        "Expected type {}, got {}",
                        std::any::type_name::<R>(),
                        envelope.type_name
                    )));
                }
            }
        }
        Err(AskError::ChannelClosed)
    };

    timeout(timeout_duration, response_future)
        .await
        .map_err(|_| AskError::Timeout {
            timeout: timeout_duration,
        })?
}

/// Future type for ask operations
pub type AskFuture<R> = Pin<Box<dyn Future<Output = Result<R, AskError>> + Send>>;

/// Create an ask future that can be awaited later
pub fn ask_future<M, R>(
    actor_ref: &ActorRef<M>,
    message: M,
    timeout_duration: Duration,
) -> AskFuture<R>
where
    M: Message,
    R: Message + 'static,
{
    let actor_ref = actor_ref.clone();

    Box::pin(async move { ask_with_actor_ref(&actor_ref, message, timeout_duration).await })
}

/// Extension trait for ActorRef to support ask pattern
pub trait AskExt<M: Message> {
    /// Ask pattern - send message and wait for response
    fn ask_ext<R>(&self, message: M, timeout: Duration) -> AskFuture<R>
    where
        R: Message + 'static;

    /// Ask with default timeout (5 seconds)
    fn ask_default<R>(&self, message: M) -> AskFuture<R>
    where
        R: Message + 'static;
}

impl<M: Message> AskExt<M> for ActorRef<M> {
    fn ask_ext<R>(&self, message: M, timeout: Duration) -> AskFuture<R>
    where
        R: Message + 'static,
    {
        let actor_ref = self.clone();
        Box::pin(async move { ask_with_actor_ref(&actor_ref, message, timeout).await })
    }

    fn ask_default<R>(&self, message: M) -> AskFuture<R>
    where
        R: Message + 'static,
    {
        self.ask_ext(message, Duration::from_secs(5))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[derive(Debug, Clone)]
    struct TestMessage {
        content: String,
    }

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[derive(Debug, Clone)]
    struct TestResponse {
        result: String,
    }

    impl Message for TestResponse {
        fn type_id(&self) -> &'static str {
            "TestResponse"
        }
    }

    #[tokio::test]
    async fn test_ask_request_creation() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let correlation_id = Uuid::new_v4();

        let response_channel = ResponseChannel {
            sender: tx,
            correlation_id,
        };

        let message = TestMessage {
            content: "test".to_string(),
        };

        let ask_request = AskRequest {
            message: message.clone(),
            correlation_id,
            response_to: response_channel,
            timestamp: SystemTime::now(),
        };

        assert_eq!(ask_request.correlation_id, correlation_id);
        assert_eq!(ask_request.message.content, "test");
    }

    #[tokio::test]
    async fn test_response_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let correlation_id = Uuid::new_v4();

        let response_channel = ResponseChannel {
            sender: tx,
            correlation_id,
        };

        let response = TestResponse {
            result: "success".to_string(),
        };

        // Send response
        let result = response_channel.respond(response).await;
        assert!(result.is_ok());

        // Receive response
        let envelope = rx.recv().await.unwrap();
        assert_eq!(envelope.correlation_id, correlation_id);
        assert_eq!(envelope.type_name, std::any::type_name::<TestResponse>());

        // Downcast and verify
        let received_response = envelope.data.downcast::<TestResponse>().unwrap();
        assert_eq!(received_response.result, "success");
    }
}
