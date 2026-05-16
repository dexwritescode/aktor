//! HTTP Client Extension for Aktor
//!
//! This extension provides a shared blocking HTTP client that can be used by any actor
//! without needing to hold the client in actor state (which would prevent serialization).
//!
//! # Example
//!
//! ```ignore
//! use aktor::*;
//! use aktor::extensions::HttpClientExtension;
//!
//! // 1. Register extension with system
//! let system = ActorSystem::new(config).await?;
//! system.register_extension(HttpClientExtension::new_extension());
//!
//! // 2. Use in any actor
//! impl Actor<MyMsg> for CrawlerActor {
//!     fn handle(&mut self, msg: MyMsg, ctx: &ActorContext<MyMsg>) {
//!         let http = ctx.system().extension::<HttpClientExtension>();
//!         let response = http.get("https://example.com").send()?;
//!     }
//! }
//! ```

use crate::Extension;
use std::time::Duration;

/// HTTP client extension for making blocking HTTP requests from actors
///
/// This extension wraps a `reqwest::blocking::Client` and provides it to all actors
/// without requiring them to hold the client in their state.
///
/// Uses blocking I/O to avoid async in actor message handlers.
#[derive(Debug, Clone)]
pub struct HttpClientExtension {
    client: reqwest::blocking::Client,
}

impl HttpClientExtension {
    /// Create a new HTTP client extension with default settings
    pub fn new() -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent("Aktor-HTTP-Client/0.1.0")
            .build()
            .expect("Failed to create HTTP client");

        Self { client }
    }

    /// Create a new HTTP client extension with custom timeout
    pub fn with_timeout(timeout: Duration) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .user_agent("Aktor-HTTP-Client/0.1.0")
            .build()
            .expect("Failed to create HTTP client");

        Self { client }
    }

    /// Create a new HTTP client extension with custom user agent
    pub fn with_user_agent(user_agent: &str) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(user_agent)
            .build()
            .expect("Failed to create HTTP client");

        Self { client }
    }

    /// Create a new HTTP client extension with custom settings
    pub fn with_builder(builder: reqwest::blocking::ClientBuilder) -> Self {
        let client = builder.build().expect("Failed to create HTTP client");
        Self { client }
    }

    /// Get a reference to the underlying reqwest blocking client
    pub fn client(&self) -> &reqwest::blocking::Client {
        &self.client
    }

    /// Start building a GET request to the specified URL
    pub fn get(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.get(url)
    }

    /// Start building a POST request to the specified URL
    pub fn post(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.post(url)
    }

    /// Start building a PUT request to the specified URL
    pub fn put(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.put(url)
    }

    /// Start building a DELETE request to the specified URL
    pub fn delete(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.delete(url)
    }

    /// Start building a HEAD request to the specified URL
    pub fn head(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.head(url)
    }

    /// Start building a PATCH request to the specified URL
    pub fn patch(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        self.client.patch(url)
    }
}

impl Default for HttpClientExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Extension for HttpClientExtension {
    fn new_extension() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_client_creation() {
        let ext = HttpClientExtension::new();
        // Verify client exists
        let _ = ext.client();
    }

    #[test]
    fn test_http_client_with_timeout() {
        let ext = HttpClientExtension::with_timeout(Duration::from_secs(10));
        // Verify client exists
        let _ = ext.client();
    }

    #[test]
    fn test_http_client_default() {
        let ext = HttpClientExtension::default();
        // Verify client exists
        let _ = ext.client();
    }

    #[test]
    fn test_http_client_with_user_agent() {
        let ext = HttpClientExtension::with_user_agent("CustomAgent/1.0");
        // Verify client exists
        let _ = ext.client();
    }

    #[test]
    fn test_http_client_request_builders() {
        let ext = HttpClientExtension::new();

        // Verify these don't panic
        let _ = ext.get("https://example.com");
        let _ = ext.post("https://example.com");
        let _ = ext.put("https://example.com");
        let _ = ext.delete("https://example.com");
        let _ = ext.head("https://example.com");
        let _ = ext.patch("https://example.com");
    }

    #[test]
    fn test_http_client_clone() {
        let ext1 = HttpClientExtension::new();
        let ext2 = ext1.clone();

        // Verify both work
        let _ = ext1.get("https://example.com");
        let _ = ext2.get("https://example.com");
    }
}