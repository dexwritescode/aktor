use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use thiserror::Error;

/// Actor address in the format: actor://node_id/actor_path
/// Examples:
/// - actor://node1/user/crawler-manager
/// - actor://localhost/system/deadletter
/// - actor://cluster-node-007/user/crawler/url-123456
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorAddress {
    /// Node identifier where the actor resides
    pub node_id: String,
    /// Hierarchical path to the actor within the node
    pub path: ActorPath,
}

/// Hierarchical actor path within a node
/// Examples:
/// - /user/crawler-manager
/// - /system/deadletter
/// - /user/crawler/url-123456
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPath {
    /// Path segments (e.g., ["user", "crawler-manager"])
    pub segments: Vec<String>,
}

#[derive(Error, Debug)]
pub enum AddressError {
    #[error("Invalid address format: {0}")]
    InvalidFormat(String),

    #[error("Empty node ID")]
    EmptyNodeId,

    #[error("Empty actor path")]
    EmptyPath,

    #[error("Invalid path segment: {0}")]
    InvalidPathSegment(String),
}

impl ActorAddress {
    /// Create a new actor address
    pub fn new(node_id: impl Into<String>, path: ActorPath) -> Result<Self, AddressError> {
        let node_id = node_id.into();
        if node_id.is_empty() {
            return Err(AddressError::EmptyNodeId);
        }

        Ok(Self { node_id, path })
    }

    /// Create a local actor address (current node)
    pub fn local(path: ActorPath) -> Self {
        Self {
            node_id: "localhost".to_string(),
            path,
        }
    }

    /// Check if this address points to a local actor
    pub fn is_local(&self) -> bool {
        self.node_id == "localhost" || self.node_id == std::env::var("NODE_ID").unwrap_or_default()
    }

    /// Get the parent address (remove last path segment)
    pub fn parent(&self) -> Option<ActorAddress> {
        self.path.parent().map(|parent_path| ActorAddress {
            node_id: self.node_id.clone(),
            path: parent_path,
        })
    }

    /// Create a child address by appending a segment
    pub fn child(&self, segment: impl Into<String>) -> Result<ActorAddress, AddressError> {
        let child_path = self.path.child(segment)?;
        Ok(ActorAddress {
            node_id: self.node_id.clone(),
            path: child_path,
        })
    }

    /// Get the actor name (last path segment)
    pub fn name(&self) -> Option<&str> {
        self.path.name()
    }
}

impl ActorPath {
    /// Create a new actor path from segments
    pub fn new(segments: Vec<String>) -> Result<Self, AddressError> {
        if segments.is_empty() {
            return Err(AddressError::EmptyPath);
        }

        // Validate segments
        for segment in &segments {
            if segment.is_empty() || segment.contains('/') {
                return Err(AddressError::InvalidPathSegment(segment.clone()));
            }
        }

        Ok(Self { segments })
    }

    /// Create a root path with a single segment
    pub fn root(name: impl Into<String>) -> Result<Self, AddressError> {
        Self::new(vec![name.into()])
    }

    /// Create a user path (under /user/)
    pub fn user(name: impl Into<String>) -> Result<Self, AddressError> {
        Self::new(vec!["user".to_string(), name.into()])
    }

    /// Create a system path (under /system/)
    pub fn system(name: impl Into<String>) -> Result<Self, AddressError> {
        Self::new(vec!["system".to_string(), name.into()])
    }

    /// Get the parent path (remove last segment)
    pub fn parent(&self) -> Option<ActorPath> {
        if self.segments.len() <= 1 {
            return None;
        }

        let mut parent_segments = self.segments.clone();
        parent_segments.pop();
        ActorPath::new(parent_segments).ok()
    }

    /// Create a child path by appending a segment
    pub fn child(&self, segment: impl Into<String>) -> Result<ActorPath, AddressError> {
        let segment = segment.into();
        if segment.is_empty() || segment.contains('/') {
            return Err(AddressError::InvalidPathSegment(segment));
        }

        let mut child_segments = self.segments.clone();
        child_segments.push(segment);
        ActorPath::new(child_segments)
    }

    /// Get the actor name (last segment)
    pub fn name(&self) -> Option<&str> {
        self.segments.last().map(|s| s.as_str())
    }

    /// Check if this is a system path
    pub fn is_system(&self) -> bool {
        self.segments.first().map(|s| s == "system").unwrap_or(false)
    }

    /// Check if this is a user path
    pub fn is_user(&self) -> bool {
        self.segments.first().map(|s| s == "user").unwrap_or(false)
    }
}

impl fmt::Display for ActorAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "actor://{}{}", self.node_id, self.path)
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "/{}", self.segments.join("/"))
    }
}

impl FromStr for ActorAddress {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with("actor://") {
            return Err(AddressError::InvalidFormat(format!(
                "Address must start with 'actor://': {}", s
            )));
        }

        let without_scheme = &s[8..]; // Remove "actor://"
        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();

        if parts.len() != 2 {
            return Err(AddressError::InvalidFormat(format!(
                "Invalid format, expected 'actor://node_id/path': {}", s
            )));
        }

        let node_id = parts[0].to_string();
        let path_str = format!("/{}", parts[1]);
        let path = ActorPath::from_str(&path_str)?;

        ActorAddress::new(node_id, path)
    }
}

impl FromStr for ActorPath {
    type Err = AddressError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !s.starts_with('/') {
            return Err(AddressError::InvalidFormat(format!(
                "Path must start with '/': {}", s
            )));
        }

        let segments: Vec<String> = s[1..] // Remove leading '/'
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();

        ActorPath::new(segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_actor_path_creation() {
        let path = ActorPath::user("crawler-manager").unwrap();
        assert_eq!(path.segments, vec!["user", "crawler-manager"]);
        assert_eq!(path.to_string(), "/user/crawler-manager");
        assert!(path.is_user());
        assert!(!path.is_system());
    }

    #[test]
    fn test_actor_path_child() {
        let path = ActorPath::user("crawler").unwrap();
        let child = path.child("worker-1").unwrap();
        assert_eq!(child.to_string(), "/user/crawler/worker-1");
    }

    #[test]
    fn test_actor_path_parent() {
        let path = ActorPath::user("crawler").unwrap();
        let child = path.child("worker-1").unwrap();
        let parent = child.parent().unwrap();
        assert_eq!(parent, path);
    }

    #[test]
    fn test_actor_address_creation() {
        let path = ActorPath::user("crawler-manager").unwrap();
        let addr = ActorAddress::new("node1", path).unwrap();
        assert_eq!(addr.to_string(), "actor://node1/user/crawler-manager");
    }

    #[test]
    fn test_actor_address_parsing() {
        let addr_str = "actor://node1/user/crawler-manager";
        let addr = ActorAddress::from_str(addr_str).unwrap();
        assert_eq!(addr.node_id, "node1");
        assert_eq!(addr.path.segments, vec!["user", "crawler-manager"]);
        assert_eq!(addr.to_string(), addr_str);
    }

    #[test]
    fn test_local_address() {
        let path = ActorPath::user("test").unwrap();
        let addr = ActorAddress::local(path);
        assert!(addr.is_local());
    }

    #[test]
    fn test_address_child() {
        let path = ActorPath::user("crawler").unwrap();
        let addr = ActorAddress::new("node1", path).unwrap();
        let child = addr.child("worker-1").unwrap();
        assert_eq!(child.to_string(), "actor://node1/user/crawler/worker-1");
    }
}