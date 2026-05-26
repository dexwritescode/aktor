//! Built-in extensions for Aktor
//!
//! This module provides commonly-used extensions that can be registered
//! with the actor system and accessed from any actor.
//!
//! ## Feature flags
//!
//! - `http` — enables [`HttpClientExtension`] and [`AsyncHttpClientExtension`],
//!   which bring in `reqwest` as a dependency. Not enabled by default so that
//!   library users who only need the actor runtime do not pay the compile cost.

#[cfg(feature = "http")]
pub mod async_http_client;
#[cfg(feature = "http")]
pub mod http_client;

#[cfg(feature = "http")]
pub use async_http_client::AsyncHttpClientExtension;
#[cfg(feature = "http")]
pub use http_client::HttpClientExtension;
