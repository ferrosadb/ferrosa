//! Audit subsystem for tracking schema and authentication events.

pub mod event;
pub mod log_sink;
pub mod table_sink;

use std::sync::{Arc, Mutex};

pub use event::{AuditContext, AuditEvent, AuditEventKind, GraphAuditStatus};
pub use log_sink::LogAuditSink;
pub use table_sink::{AuditLogEntry, SystemTableAuditSink};

/// A sink that receives audit events for logging, storage, or forwarding.
pub trait AuditSink: Send + Sync {
    /// Emit an audit event to this sink.
    fn emit(&self, event: &AuditEvent);
}

/// Blanket impl so `Arc<T>` can be used directly as a sink.
impl<T: AuditSink> AuditSink for Arc<T> {
    fn emit(&self, event: &AuditEvent) {
        (**self).emit(event);
    }
}

/// In-memory audit sink for testing.
///
/// Collects all emitted events and allows inspection and clearing.
pub struct TestAuditSink {
    events: Mutex<Vec<AuditEvent>>,
}

impl TestAuditSink {
    /// Create a new empty test sink.
    pub fn new() -> Self {
        Self {
            events: Mutex::new(Vec::new()),
        }
    }

    /// Return a snapshot of all collected events.
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Clear all collected events.
    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }
}

impl Default for TestAuditSink {
    fn default() -> Self {
        Self::new()
    }
}

impl AuditSink for TestAuditSink {
    fn emit(&self, event: &AuditEvent) {
        self.events.lock().unwrap().push(event.clone());
    }
}

/// Fans out audit events to multiple sinks.
pub struct CompositeSink {
    sinks: Vec<Arc<dyn AuditSink>>,
}

impl CompositeSink {
    /// Create a composite sink that forwards to all given sinks.
    pub fn new(sinks: Vec<Arc<dyn AuditSink>>) -> Self {
        Self { sinks }
    }
}

impl AuditSink for CompositeSink {
    fn emit(&self, event: &AuditEvent) {
        for sink in &self.sinks {
            sink.emit(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::SystemTime;
    use uuid::Uuid;

    fn make_test_event(kind: AuditEventKind) -> AuditEvent {
        AuditEvent {
            timestamp: SystemTime::now(),
            event: kind,
            actor: Some("test_user".to_string()),
            source: None,
            schema_version: Some(Uuid::new_v4()),
        }
    }

    #[test]
    fn test_audit_sink_collects_events() {
        let sink = TestAuditSink::new();
        let event1 = make_test_event(AuditEventKind::AuthSuccess {
            role: "admin".into(),
        });
        let event2 = make_test_event(AuditEventKind::KeyspaceCreated {
            keyspace: "ks".into(),
        });

        sink.emit(&event1);
        sink.emit(&event2);

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].actor.as_deref(), Some("test_user"));
    }

    #[test]
    fn test_audit_sink_clear() {
        let sink = TestAuditSink::new();
        sink.emit(&make_test_event(AuditEventKind::SchemaBootstrapped));
        assert_eq!(sink.events().len(), 1);

        sink.clear();
        assert_eq!(sink.events().len(), 0);
    }

    #[test]
    fn composite_sink_fans_out() {
        let sink_a = Arc::new(TestAuditSink::new());
        let sink_b = Arc::new(TestAuditSink::new());

        let composite = CompositeSink::new(vec![sink_a.clone(), sink_b.clone()]);

        let event = make_test_event(AuditEventKind::RoleCreated {
            role: "new_role".into(),
            is_superuser: false,
        });

        composite.emit(&event);

        assert_eq!(sink_a.events().len(), 1);
        assert_eq!(sink_b.events().len(), 1);
    }
}
