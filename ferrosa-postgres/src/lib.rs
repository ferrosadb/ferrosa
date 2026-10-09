//! Postgres wire-protocol front-end for Ferrosa.
//!
//! This crate implements the Postgres frontend/backend protocol (v3) and the
//! connection/session state machine for a Postgres listener that shares
//! Ferrosa's storage, schema, and auth. CQL transaction coordination remains
//! on Accord outside this crate. It does **not** contain the relational query engine —
//! that lives in `ferrosa-sql`.
//!
//! Blueprint: `specs/proposed/postgres-frontend/`.
//!
//! The first implemented slice is the wire **codec** (`codec`) and message
//! **types** (`messages`) — the pure, infra-free foundation (harness layer H1)
//! that the connection state machine and SCRAM exchange build on.

pub(crate) mod authz;
pub mod catalog;
pub mod codec;
pub mod connection;
pub mod copy_decode;
pub mod ddl;
pub mod extended;
pub mod handshake;
pub mod jsonb_wire;
pub mod messages;
mod mvcc;
pub mod pg_key;
pub mod pg_types;
pub mod portal_limits;
pub mod query;
pub(crate) mod result_stream;
pub mod scram;
pub mod server;
pub mod storage_provider;
pub mod store;

mod accord_access;

pub use connection::{ConnError, Connection, TlsPolicy};
pub use ddl::{ClusterDdl, DdlExecutor};
pub use handshake::{Handshake, HandshakeError, VerifierStore};
pub use store::SchemaVerifierStore;

pub use codec::{CodecError, MAX_MESSAGE_LEN};
pub use messages::{
    BackendMessage, FieldDescription, FrontendMessage, StartupFrame, StartupMessage,
    TransactionStatus,
};
pub use mvcc::{MvccManager, PgWrite};
pub use portal_limits::{PortalLimits, SuspendedPortals};
pub use scram::{ScramError, ScramServerFirst, ScramVerifier};
pub use server::{PgTls, QueryContext};

pub use accord_access::AccordAccess;
mod synthetic_key;
