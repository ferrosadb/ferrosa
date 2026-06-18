//! Postgres wire-protocol front-end for Ferrosa.
//!
//! This crate implements the Postgres frontend/backend protocol (v3) and the
//! connection/session state machine for a Postgres listener that shares
//! Ferrosa's storage, schema, auth, and Accord core (via `ferrosa-session`,
//! per decision D10). It does **not** contain the relational query engine —
//! that lives in `ferrosa-sql`.
//!
//! Blueprint: `specs/proposed/postgres-frontend/`.
//!
//! The first implemented slice is the wire **codec** (`codec`) and message
//! **types** (`messages`) — the pure, infra-free foundation (harness layer H1)
//! that the connection state machine and SCRAM exchange build on.

pub mod codec;
pub mod handshake;
pub mod messages;
pub mod scram;

pub use handshake::{Handshake, HandshakeError, VerifierStore};

pub use codec::{CodecError, MAX_MESSAGE_LEN};
pub use messages::{
    BackendMessage, FrontendMessage, StartupFrame, StartupMessage, TransactionStatus,
};
pub use scram::{ScramError, ScramServerFirst, ScramVerifier};
