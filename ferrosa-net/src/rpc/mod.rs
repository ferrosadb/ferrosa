// ferrosa-net/src/rpc/mod.rs
pub mod client;
pub mod handler;
pub mod server;

pub use handler::{HandlerRegistry, PeerId, RpcHandler};
