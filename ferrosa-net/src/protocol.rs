//! Generated Cap'n Proto protocol modules for internode messages.
//!
//! The first schema slice covers the stable ADR-019 envelope/common types and a
//! small cluster-control family. Live networking still uses the legacy message
//! enum until later migration cards wire typed bodies into handlers.

capnp::generated_code!(pub mod envelope_capnp);
