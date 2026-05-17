#[cfg(test)]
mod tests {
    use crate::reference::ask::{AskRequest, ResponseChannel};
    use crate::{Actor, ActorContext, ActorSystem, ActorSystemConfig, Message};
    use std::time::Duration;
    use tokio::time::sleep;

    #[derive(Debug, Clone)]
    struct EchoMessage {
        content: String,
    }

    impl Message for EchoMessage {
        fn type_id(&self) -> &'static str {
            "EchoMessage"
        }
    }

    #[derive(Debug, Clone)]
    struct EchoResponse {
        echoed: String,
    }

    impl Message for EchoResponse {
        fn type_id(&self) -> &'static str {
            "EchoResponse"
        }
    }

    #[derive(Debug)]
    struct EchoActor {
        message_count: usize,
    }

    impl Default for EchoActor {
        fn default() -> Self {
            Self { message_count: 0 }
        }
    }

    impl Actor<EchoMessage> for EchoActor {
        fn handle(&mut self, msg: EchoMessage, ctx: &ActorContext<EchoMessage>) {
            self.message_count += 1;

            if ctx.is_ask_request() {
                // This is an ask request - send response
                let response = EchoResponse {
                    echoed: format!("Echo: {}", msg.content),
                };
                // Spawn task to send async response from sync handler
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    let _ = ctx.respond(response).await;
                });
            } else {
                // This is a tell message
                println!("EchoActor received: {}", msg.content);
            }
        }
    }

    #[tokio::test]
    async fn test_ask_basic_functionality() {
        let config = ActorSystemConfig::default();
        let system = ActorSystem::new(config).await.unwrap();

        let echo_actor = EchoActor::default();
        let actor_ref = system
            .spawn_actor("echo-actor", echo_actor, crate::ActorProps::default())
            .await
            .unwrap();

        // Give the actor time to start
        sleep(Duration::from_millis(100)).await;

        let message = EchoMessage {
            content: "Hello, Ask Pattern!".to_string(),
        };

        // Test ask pattern - should now work with context-based responses
        let result: Result<EchoResponse, crate::AskError> =
            actor_ref.ask(message, Duration::from_secs(1)).await;

        // Should now succeed with the new implementation
        match result {
            Ok(response) => {
                assert_eq!(response.echoed, "Echo: Hello, Ask Pattern!");
                println!("✅ Ask pattern working: {}", response.echoed);
            }
            Err(e) => {
                panic!("Ask should succeed now, got error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_ask_extension_trait() {
        use crate::AskExt;

        let config = ActorSystemConfig::default();
        let system = ActorSystem::new(config).await.unwrap();

        let echo_actor = EchoActor::default();
        let actor_ref = system
            .spawn_actor("echo-ext-actor", echo_actor, crate::ActorProps::default())
            .await
            .unwrap();

        // Give the actor time to start
        sleep(Duration::from_millis(100)).await;

        let message = EchoMessage {
            content: "Extension Test".to_string(),
        };

        // Test ask extension with custom timeout
        let future = actor_ref.ask_ext::<EchoResponse>(message.clone(), Duration::from_secs(1));
        let result = future.await;

        // Should now succeed
        match result {
            Ok(response) => {
                assert_eq!(response.echoed, "Echo: Extension Test");
                println!("✅ Ask extension trait working: {}", response.echoed);
            }
            Err(e) => {
                panic!("Ask extension should succeed now, got error: {:?}", e);
            }
        }

        // Test ask extension with default timeout
        let future = actor_ref.ask_default::<EchoResponse>(message);
        let result = future.await;

        // Should now succeed
        match result {
            Ok(response) => {
                assert_eq!(response.echoed, "Echo: Extension Test");
                println!("✅ Ask default timeout working: {}", response.echoed);
            }
            Err(e) => {
                panic!("Ask default should succeed now, got error: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_ask_request_envelope() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let correlation_id = uuid::Uuid::new_v4();

        let response_channel = ResponseChannel {
            sender: tx,
            correlation_id,
        };

        let message = EchoMessage {
            content: "test message".to_string(),
        };

        let ask_request = AskRequest {
            message: message.clone(),
            correlation_id,
            response_to: response_channel.clone(),
            timestamp: std::time::SystemTime::now(),
        };

        assert_eq!(ask_request.correlation_id, correlation_id);
        assert_eq!(ask_request.message.content, "test message");

        // Test response channel
        let response = EchoResponse {
            echoed: "test response".to_string(),
        };

        let send_result = response_channel.respond(response).await;
        assert!(send_result.is_ok());

        // Verify we can receive the response
        let envelope = rx.recv().await.unwrap();
        assert_eq!(envelope.correlation_id, correlation_id);

        // Verify we can downcast the response
        let received_response = envelope.data.downcast::<EchoResponse>().unwrap();
        assert_eq!(received_response.echoed, "test response");
    }

    #[tokio::test]
    async fn test_regular_tell_still_works() {
        let config = ActorSystemConfig::default();
        let system = ActorSystem::new(config).await.unwrap();

        let echo_actor = EchoActor::default();
        let actor_ref = system
            .spawn_actor("tell-actor", echo_actor, crate::ActorProps::default())
            .await
            .unwrap();

        // Give the actor time to start
        sleep(Duration::from_millis(100)).await;

        let message = EchoMessage {
            content: "Regular tell message".to_string(),
        };

        // Test that regular tell still works
        let result = actor_ref.tell(message, None);
        assert!(result.is_ok());

        // Give time for message processing
        sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn test_ask_timeout_behavior() {
        let config = ActorSystemConfig::default();
        let system = ActorSystem::new(config).await.unwrap();

        let echo_actor = EchoActor::default();
        let actor_ref = system
            .spawn_actor("timeout-actor", echo_actor, crate::ActorProps::default())
            .await
            .unwrap();

        // Give the actor time to start
        sleep(Duration::from_millis(100)).await;

        let message = EchoMessage {
            content: "Timeout test".to_string(),
        };

        // Test timeout behavior with very short timeout
        let start_time = std::time::Instant::now();
        let result: Result<EchoResponse, crate::AskError> =
            actor_ref.ask(message, Duration::from_millis(1)).await;

        let elapsed = start_time.elapsed();

        // Should either succeed quickly or timeout
        match result {
            Ok(response) => {
                // Response was very fast
                assert_eq!(response.echoed, "Echo: Timeout test");
                println!("✅ Very fast response: {}", response.echoed);
            }
            Err(crate::AskError::Timeout { timeout }) => {
                // Timed out as expected
                assert_eq!(timeout, Duration::from_millis(1));
                assert!(elapsed < Duration::from_millis(50)); // Should timeout quickly
                println!("✅ Timeout behavior working correctly");
            }
            Err(e) => {
                panic!("Unexpected error: {:?}", e);
            }
        }
    }
}
