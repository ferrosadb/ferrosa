pub mod seeds;
pub use seeds::SeedDiscovery;

use std::net::SocketAddr;

/// Trait for peer discovery mechanisms.
pub trait Discovery: Send + Sync {
    /// Return the current set of known peer addresses.
    fn peers(&self) -> Vec<SocketAddr>;
}
