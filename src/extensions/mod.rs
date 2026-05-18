//! Built-in extensions for Aktor
//!
//! This module provides commonly-used extensions that can be registered
//! with the actor system and accessed from any actor.

pub mod async_http_client;
pub mod http_client;

pub use async_http_client::AsyncHttpClientExtension;
pub use http_client::HttpClientExtension;
