// Re-export main system types
pub use crate::context::{ActorSystem, ActorSystemConfig};

// This module can be expanded later with additional system-level functionality
// such as:
// - Cluster management
// - Actor supervision hierarchies
// - System metrics and monitoring
// - Configuration management
// - Plugin systems

/// Builder for configuring an actor system
pub struct ActorSystemBuilder {
    config: ActorSystemConfig,
}

impl ActorSystemBuilder {
    /// Create a new actor system builder with default configuration
    pub fn new() -> Self {
        Self {
            config: ActorSystemConfig::default(),
        }
    }

    /// Set the maximum number of actors
    pub fn with_max_actors(mut self, max_actors: usize) -> Self {
        self.config.max_actors = max_actors;
        self
    }

    /// Set the default mailbox size
    pub fn with_mailbox_size(mut self, size: usize) -> Self {
        self.config.default_mailbox_size = size;
        self
    }

    /// Enable distributed mode
    pub fn with_distributed(mut self, bind_address: String) -> Self {
        self.config.distributed = true;
        self.config.bind_address = Some(bind_address);
        self
    }

    /// Add seed nodes for cluster discovery
    pub fn with_seed_nodes(mut self, seed_nodes: Vec<String>) -> Self {
        self.config.seed_nodes = seed_nodes;
        self
    }

    /// Build the actor system
    pub async fn build(self) -> Result<std::sync::Arc<ActorSystem>, crate::ActorError> {
        ActorSystem::new(self.config).await
    }
}

impl Default for ActorSystemBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Message;

    #[derive(Debug, Clone)]
    struct TestMessage;

    impl Message for TestMessage {
        fn type_id(&self) -> &'static str {
            "TestMessage"
        }
    }

    #[tokio::test]
    async fn test_actor_system_builder() {
        let system: std::sync::Arc<ActorSystem> = ActorSystemBuilder::new()
            .with_max_actors(50000)
            .with_mailbox_size(500)
            .build()
            .await
            .unwrap();

        assert_eq!(system.config().max_actors, 50000);
        assert_eq!(system.config().default_mailbox_size, 500);
        assert!(!system.config().distributed);
    }

    #[tokio::test]
    async fn test_distributed_config() {
        let system: std::sync::Arc<ActorSystem> = ActorSystemBuilder::new()
            .with_distributed("0.0.0.0:8080".to_string())
            .with_seed_nodes(vec!["node1:8080".to_string(), "node2:8080".to_string()])
            .build()
            .await
            .unwrap();

        assert!(system.config().distributed);
        assert_eq!(
            system.config().bind_address,
            Some("0.0.0.0:8080".to_string())
        );
        assert_eq!(system.config().seed_nodes.len(), 2);
    }
}
