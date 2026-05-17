//! Built-in extensions for Aktor
//!
//! This module provides commonly-used extensions that can be registered
//! with the actor system and accessed from any actor.

pub mod http_client;

pub use http_client::HttpClientExtension;
