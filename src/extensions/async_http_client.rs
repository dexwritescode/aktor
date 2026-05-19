//! Async HTTP Client Extension for Aktor
//!
//! Provides a shared async HTTP client that actors can access without holding
//! it in actor state. Designed for use inside `ctx.pipe_to_self` closures,
//! where the client is cloned into the async future.
//!
//! # Example
//!
//! ```ignore
//! use aktor::extensions::AsyncHttpClientExtension;
//!
//! // 1. Register once at startup
//! system.register_extension(AsyncHttpClientExtension::new());
//!
//! // 2. Use inside pipe_to_self from any actor
//! fn handle(&mut self, msg: MyMsg, ctx: &ActorContext<MyMsg>) {
//!     let client = ctx.system().extension::<AsyncHttpClientExtension>().client();
//!     ctx.pipe_to_self::<_, String>(async move {
//!         let text = client.get("https://example.com").send().await?.text().await?;
//!         Ok(MyMsg::Result(text))
//!     });
//! }
//! ```

use crate::Extension;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_USER_AGENT: &str = concat!("aktor/", env!("CARGO_PKG_VERSION"));
const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Async HTTP client extension backed by `reqwest::Client`.
///
/// `reqwest::Client` already pools connections internally; registering it as
/// an extension ensures every actor shares the same pool rather than each
/// creating its own client.
///
/// Clone `client()` into `pipe_to_self` closures — the clone is cheap
/// (it's an `Arc` internally).
#[derive(Debug, Clone)]
pub struct AsyncHttpClientExtension {
    client: Arc<reqwest::Client>,
}

impl AsyncHttpClientExtension {
    /// Create with default timeout (30 s) and a descriptive user-agent.
    pub fn new() -> Self {
        Self::build(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
                .user_agent(DEFAULT_USER_AGENT),
        )
    }

    /// Create with a custom request timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::build(
            reqwest::Client::builder()
                .timeout(timeout)
                .user_agent(DEFAULT_USER_AGENT),
        )
    }

    /// Create with a custom user-agent string.
    pub fn with_user_agent(user_agent: impl Into<String>) -> Self {
        Self::build(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
                .user_agent(user_agent.into()),
        )
    }

    /// Create from a fully configured `reqwest::ClientBuilder`.
    pub fn with_builder(builder: reqwest::ClientBuilder) -> Self {
        Self::build(builder)
    }

    fn build(builder: reqwest::ClientBuilder) -> Self {
        let client = builder.build().expect("failed to build async HTTP client");
        Self {
            client: Arc::new(client),
        }
    }

    /// Return the shared async client. Cheap to clone — backed by an `Arc`.
    pub fn client(&self) -> Arc<reqwest::Client> {
        self.client.clone()
    }
}

impl Default for AsyncHttpClientExtension {
    fn default() -> Self {
        Self::new()
    }
}

impl Extension for AsyncHttpClientExtension {
    fn new_extension() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_construction() {
        let ext = AsyncHttpClientExtension::new();
        let c1 = ext.client();
        let c2 = ext.client();
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[test]
    fn test_with_timeout() {
        let ext = AsyncHttpClientExtension::with_timeout(Duration::from_secs(5));
        let _ = ext.client();
    }

    #[test]
    fn test_with_user_agent() {
        let ext = AsyncHttpClientExtension::with_user_agent("my-crawler/1.0");
        let _ = ext.client();
    }

    #[test]
    fn test_clone_shares_pool() {
        let ext1 = AsyncHttpClientExtension::new();
        let ext2 = ext1.clone();
        assert!(Arc::ptr_eq(&ext1.client(), &ext2.client()));
    }

    #[test]
    fn test_with_builder() {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(10));
        let ext = AsyncHttpClientExtension::with_builder(builder);
        let _ = ext.client();
    }

    #[tokio::test]
    async fn test_happy_path_httpbin() {
        let ext = AsyncHttpClientExtension::new();
        let client = ext.client();
        let resp = client
            .get("https://httpbin.org/get")
            .send()
            .await
            .expect("request to httpbin.org should succeed");
        assert!(
            resp.status().is_success(),
            "expected 2xx from httpbin.org, got {}",
            resp.status()
        );
        let body = resp.text().await.expect("body should be readable");
        assert!(
            body.contains("httpbin.org"),
            "expected httpbin.org in response body"
        );
    }

    /// A DNS lookup for a guaranteed-nonexistent domain must fail.
    #[tokio::test]
    async fn test_error_path_nonexistent_domain() {
        let ext = AsyncHttpClientExtension::with_timeout(Duration::from_secs(5));
        let client = ext.client();
        let result = client
            .get("https://this-domain-does-not-exist.invalid")
            .send()
            .await;
        assert!(
            result.is_err(),
            "expected an error for nonexistent domain, got a response"
        );
        let err = result.unwrap_err();
        assert!(
            err.is_connect() || err.is_timeout() || err.is_request(),
            "expected a connection/dns error, got: {err}"
        );
    }
}
