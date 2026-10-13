// ferrosa-net/src/rpc/mod.rs
pub mod client;
pub mod error_reply;
pub mod handler;
pub mod server;

pub use handler::{HandlerOutcome, HandlerRegistry, PeerId, PingHandler, RpcHandler};
pub use server::InboundPeerCallback;
