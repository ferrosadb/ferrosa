//! Audit subsystem for tracking schema and authentication events.

pub mod event;

pub use event::{AuditContext, AuditEvent, AuditEventKind};
