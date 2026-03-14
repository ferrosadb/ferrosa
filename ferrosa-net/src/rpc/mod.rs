// ferrosa-net/src/rpc/mod.rs
pub mod handler;
// pub mod server; — added in Task 6
// pub mod client; — added in Task 7

pub use handler::{HandlerRegistry, PeerId, RpcHandler};
