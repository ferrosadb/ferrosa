//! System table audit sink with ring buffer storage.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::SystemTime;

use serde::Serialize;
use uuid::Uuid;

use super::{AuditEvent, AuditEventKind, AuditSink};

/// In-memory ring buffer for `system_auth.audit_log`.
///
/// Stores a bounded number of audit log entries, evicting the oldest
/// when capacity is exceeded.
pub struct SystemTableAuditSink {
    log: Mutex<VecDeque<AuditLogEntry>>,
    capacity: usize,
}

/// A flattened audit log entry suitable for storage in a system table.
#[derive(Debug, Clone, Serialize)]
pub struct AuditLogEntry {
    /// When the event occurred.
    pub timestamp: SystemTime,
    /// The event type as a SCREAMING_SNAKE_CASE string.
    pub event_type: String,
    /// The authenticated role that performed the action, if any.
    pub actor: Option<String>,
    /// The source address as a string, if any.
    pub source: Option<String>,
    /// The resource affected, if applicable.
    pub resource: Option<String>,
    /// A human-readable description of the operation.
    pub operation: String,
    /// The schema version at the time of the event, if applicable.
    pub schema_version: Option<Uuid>,
}

impl SystemTableAuditSink {
    /// Create a new ring buffer sink with the given capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            log: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Query all entries, newest first.
    pub fn query(&self) -> Vec<AuditLogEntry> {
        let log = self.log.lock().unwrap();
        log.iter().rev().cloned().collect()
    }
}

/// Extract the event type string, operation description, and optional resource
/// from an `AuditEventKind`.
#[allow(unreachable_patterns)] // catch-all needed for #[non_exhaustive] forward compat
fn decompose_event(kind: &AuditEventKind) -> (&'static str, String, Option<String>) {
    match kind {
        AuditEventKind::AuthSuccess { role } => (
            "AUTH_SUCCESS",
            format!("Authentication succeeded for {role}"),
            None,
        ),
        AuditEventKind::AuthFailed { role } => (
            "AUTH_FAILED",
            format!("Authentication failed for {role}"),
            None,
        ),
        AuditEventKind::AuthThrottled { role } => (
            "AUTH_THROTTLED",
            format!("Authentication throttled for {role}"),
            None,
        ),
        AuditEventKind::AuthLockedOut {
            role,
            failure_count,
        } => (
            "AUTH_LOCKED_OUT",
            format!("Account {role} locked out after {failure_count} failures"),
            None,
        ),
        AuditEventKind::PasswordChanged {
            role,
            upgraded_algorithm,
        } => (
            "PASSWORD_CHANGED",
            if *upgraded_algorithm {
                format!("Password changed for {role} (algorithm upgraded)")
            } else {
                format!("Password changed for {role}")
            },
            None,
        ),
        AuditEventKind::KeyspaceCreated { keyspace } => (
            "KEYSPACE_CREATED",
            format!("Created keyspace {keyspace}"),
            Some(format!("keyspace {keyspace}")),
        ),
        AuditEventKind::KeyspaceAltered { keyspace } => (
            "KEYSPACE_ALTERED",
            format!("Altered keyspace {keyspace}"),
            Some(format!("keyspace {keyspace}")),
        ),
        AuditEventKind::KeyspaceDropped { keyspace } => (
            "KEYSPACE_DROPPED",
            format!("Dropped keyspace {keyspace}"),
            Some(format!("keyspace {keyspace}")),
        ),
        AuditEventKind::TableCreated { keyspace, table } => (
            "TABLE_CREATED",
            format!("Created table {keyspace}.{table}"),
            Some(format!("table {keyspace}.{table}")),
        ),
        AuditEventKind::TableAltered { keyspace, table } => (
            "TABLE_ALTERED",
            format!("Altered table {keyspace}.{table}"),
            Some(format!("table {keyspace}.{table}")),
        ),
        AuditEventKind::TableDropped { keyspace, table } => (
            "TABLE_DROPPED",
            format!("Dropped table {keyspace}.{table}"),
            Some(format!("table {keyspace}.{table}")),
        ),
        AuditEventKind::RoleCreated { role, is_superuser } => (
            "ROLE_CREATED",
            if *is_superuser {
                format!("Created superuser role {role}")
            } else {
                format!("Created role {role}")
            },
            Some(format!("role {role}")),
        ),
        AuditEventKind::RoleAltered { role } => (
            "ROLE_ALTERED",
            format!("Altered role {role}"),
            Some(format!("role {role}")),
        ),
        AuditEventKind::RoleDropped { role } => (
            "ROLE_DROPPED",
            format!("Dropped role {role}"),
            Some(format!("role {role}")),
        ),
        AuditEventKind::PermissionGranted {
            role,
            resource,
            permissions,
        } => {
            let perms: Vec<String> = permissions.iter().map(|p| p.to_string()).collect();
            (
                "PERMISSION_GRANTED",
                format!("Granted {} on {resource} to {role}", perms.join(", ")),
                Some(resource.to_string()),
            )
        }
        AuditEventKind::PermissionRevoked {
            role,
            resource,
            permissions,
        } => {
            let perms: Vec<String> = permissions.iter().map(|p| p.to_string()).collect();
            (
                "PERMISSION_REVOKED",
                format!("Revoked {} on {resource} from {role}", perms.join(", ")),
                Some(resource.to_string()),
            )
        }
        AuditEventKind::SchemaBootstrapped => (
            "SCHEMA_BOOTSTRAPPED",
            "Schema bootstrapped".to_string(),
            None,
        ),
        AuditEventKind::SuperuserPasswordMustChange => (
            "SUPERUSER_PASSWORD_MUST_CHANGE",
            "Superuser password must be changed from default".to_string(),
            None,
        ),
        // non_exhaustive: handle unknown variants gracefully
        _ => ("UNKNOWN", "Unknown audit event".to_string(), None),
    }
}

impl AuditSink for SystemTableAuditSink {
    fn emit(&self, event: &AuditEvent) {
        let (event_type, operation, resource) = decompose_event(&event.event);

        let entry = AuditLogEntry {
            timestamp: event.timestamp,
            event_type: event_type.to_string(),
            actor: event.actor.clone(),
            source: event.source.map(|s| s.to_string()),
            resource,
            operation,
            schema_version: event.schema_version,
        };

        let mut log = self.log.lock().unwrap();
        if log.len() >= self.capacity {
            log.pop_front();
        }
        log.push_back(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::{AuditEvent, AuditEventKind, AuditSink};
    use std::time::SystemTime;
    use uuid::Uuid;

    fn make_event(kind: AuditEventKind) -> AuditEvent {
        AuditEvent {
            timestamp: SystemTime::now(),
            event: kind,
            actor: Some("test".into()),
            source: Some("127.0.0.1:9042".parse().unwrap()),
            schema_version: Some(Uuid::new_v4()),
        }
    }

    #[test]
    fn ring_buffer_stores_events() {
        let sink = SystemTableAuditSink::new(10);
        sink.emit(&make_event(AuditEventKind::AuthSuccess {
            role: "admin".into(),
        }));
        sink.emit(&make_event(AuditEventKind::KeyspaceCreated {
            keyspace: "ks".into(),
        }));

        let log = sink.query();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].event_type, "KEYSPACE_CREATED");
        assert_eq!(log[1].event_type, "AUTH_SUCCESS");
    }

    #[test]
    fn ring_buffer_evicts_oldest_when_full() {
        let sink = SystemTableAuditSink::new(2);
        sink.emit(&make_event(AuditEventKind::AuthSuccess {
            role: "first".into(),
        }));
        sink.emit(&make_event(AuditEventKind::AuthFailed {
            role: "second".into(),
        }));
        sink.emit(&make_event(AuditEventKind::AuthThrottled {
            role: "third".into(),
        }));

        let log = sink.query();
        assert_eq!(log.len(), 2);
        // Newest first
        assert_eq!(log[0].event_type, "AUTH_THROTTLED");
        assert_eq!(log[1].event_type, "AUTH_FAILED");
        // "AUTH_SUCCESS" was evicted
    }

    #[test]
    fn query_returns_newest_first() {
        let sink = SystemTableAuditSink::new(10);
        sink.emit(&make_event(AuditEventKind::SchemaBootstrapped));
        sink.emit(&make_event(AuditEventKind::SuperuserPasswordMustChange));
        sink.emit(&make_event(AuditEventKind::RoleCreated {
            role: "admin".into(),
            is_superuser: true,
        }));

        let log = sink.query();
        assert_eq!(log.len(), 3);
        assert_eq!(log[0].event_type, "ROLE_CREATED");
        assert_eq!(log[1].event_type, "SUPERUSER_PASSWORD_MUST_CHANGE");
        assert_eq!(log[2].event_type, "SCHEMA_BOOTSTRAPPED");
    }
}
