//! Structured logging audit sink.

use super::{AuditEvent, AuditSink};

/// Emits audit events as structured JSON via `tracing` at INFO level.
///
/// Target: `"ferrosa::audit"`
pub struct LogAuditSink;

impl AuditSink for LogAuditSink {
    fn emit(&self, event: &AuditEvent) {
        let json =
            serde_json::to_string(event).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
        tracing::info!(target: "ferrosa::audit", "{json}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, AuditEventKind, AuditSink};
    use std::time::SystemTime;

    #[test]
    fn log_audit_sink_emits_without_panic() {
        let sink = LogAuditSink;
        let event = AuditEvent {
            timestamp: SystemTime::now(),
            event: AuditEventKind::AuthSuccess {
                role: "admin".into(),
            },
            actor: Some("admin".into()),
            source: None,
            schema_version: None,
        };
        // Should not panic — we just verify it doesn't blow up.
        sink.emit(&event);
    }
}
