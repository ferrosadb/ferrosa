//! CQL EVENT frame types for push notifications.
//!
//! Clients that send REGISTER can receive asynchronous EVENT frames for
//! schema changes, topology changes, and status changes.

use bytes::{BufMut, BytesMut};

/// CQL event types that clients can register for via REGISTER.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventType {
    SchemaChange,
    TopologyChange,
    StatusChange,
}

impl EventType {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "SCHEMA_CHANGE" => Some(Self::SchemaChange),
            "TOPOLOGY_CHANGE" => Some(Self::TopologyChange),
            "STATUS_CHANGE" => Some(Self::StatusChange),
            _ => None,
        }
    }
}

/// A CQL event that can be pushed to registered clients.
#[derive(Debug, Clone)]
pub enum CqlEvent {
    SchemaChange {
        change_type: SchemaChangeType,
        target: SchemaTarget,
        keyspace: String,
        name: Option<String>,
    },
    TopologyChange {
        change_type: TopologyChangeType,
        address: std::net::SocketAddr,
    },
    StatusChange {
        change_type: StatusChangeType,
        address: std::net::SocketAddr,
    },
}

#[derive(Debug, Clone)]
pub enum SchemaChangeType {
    Created,
    Updated,
    Dropped,
}

#[derive(Debug, Clone)]
pub enum SchemaTarget {
    Keyspace,
    Table,
    Index,
}

#[derive(Debug, Clone)]
pub enum TopologyChangeType {
    NewNode,
    RemovedNode,
}

#[derive(Debug, Clone)]
pub enum StatusChangeType {
    Up,
    Down,
}

impl CqlEvent {
    /// Encode the EVENT frame body for the CQL native protocol.
    pub fn encode_body(&self, _protocol_version: u8) -> BytesMut {
        let mut buf = BytesMut::new();
        match self {
            CqlEvent::SchemaChange {
                change_type,
                target,
                keyspace,
                name,
            } => {
                write_string(&mut buf, "SCHEMA_CHANGE");
                let ct = match change_type {
                    SchemaChangeType::Created => "CREATED",
                    SchemaChangeType::Updated => "UPDATED",
                    SchemaChangeType::Dropped => "DROPPED",
                };
                write_string(&mut buf, ct);
                let tgt = match target {
                    SchemaTarget::Keyspace => "KEYSPACE",
                    SchemaTarget::Table => "TABLE",
                    SchemaTarget::Index => "INDEX",
                };
                write_string(&mut buf, tgt);
                write_string(&mut buf, keyspace);
                if let Some(n) = name {
                    write_string(&mut buf, n);
                }
            }
            CqlEvent::TopologyChange {
                change_type,
                address,
            } => {
                write_string(&mut buf, "TOPOLOGY_CHANGE");
                let ct = match change_type {
                    TopologyChangeType::NewNode => "NEW_NODE",
                    TopologyChangeType::RemovedNode => "REMOVED_NODE",
                };
                write_string(&mut buf, ct);
                write_inet(&mut buf, address);
            }
            CqlEvent::StatusChange {
                change_type,
                address,
            } => {
                write_string(&mut buf, "STATUS_CHANGE");
                let ct = match change_type {
                    StatusChangeType::Up => "UP",
                    StatusChangeType::Down => "DOWN",
                };
                write_string(&mut buf, ct);
                write_inet(&mut buf, address);
            }
        }
        buf
    }

    /// Returns the event type for filtering against client subscriptions.
    pub fn event_type(&self) -> EventType {
        match self {
            CqlEvent::SchemaChange { .. } => EventType::SchemaChange,
            CqlEvent::TopologyChange { .. } => EventType::TopologyChange,
            CqlEvent::StatusChange { .. } => EventType::StatusChange,
        }
    }
}

fn write_string(buf: &mut BytesMut, s: &str) {
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn write_inet(buf: &mut BytesMut, addr: &std::net::SocketAddr) {
    match addr.ip() {
        std::net::IpAddr::V4(v4) => {
            buf.put_u8(4);
            buf.put_slice(&v4.octets());
        }
        std::net::IpAddr::V6(v6) => {
            buf.put_u8(16);
            buf.put_slice(&v6.octets());
        }
    }
    buf.put_i32(addr.port() as i32);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_schema_change_event() {
        let event = CqlEvent::SchemaChange {
            change_type: SchemaChangeType::Created,
            target: SchemaTarget::Table,
            keyspace: "ks1".into(),
            name: Some("t1".into()),
        };
        let body = event.encode_body(3);
        assert!(!body.is_empty());
        assert_eq!(event.event_type(), EventType::SchemaChange);
    }

    #[test]
    fn encode_topology_change_event() {
        let event = CqlEvent::TopologyChange {
            change_type: TopologyChangeType::NewNode,
            address: "10.0.0.1:9042".parse().unwrap(),
        };
        let body = event.encode_body(3);
        assert!(!body.is_empty());
        assert_eq!(event.event_type(), EventType::TopologyChange);
    }

    #[test]
    fn encode_status_change_event() {
        let event = CqlEvent::StatusChange {
            change_type: StatusChangeType::Down,
            address: "10.0.0.2:9042".parse().unwrap(),
        };
        let body = event.encode_body(3);
        assert!(!body.is_empty());
        assert_eq!(event.event_type(), EventType::StatusChange);
    }

    #[test]
    fn event_type_from_str() {
        assert_eq!(
            EventType::parse("SCHEMA_CHANGE"),
            Some(EventType::SchemaChange)
        );
        assert_eq!(
            EventType::parse("TOPOLOGY_CHANGE"),
            Some(EventType::TopologyChange)
        );
        assert_eq!(
            EventType::parse("STATUS_CHANGE"),
            Some(EventType::StatusChange)
        );
        assert_eq!(EventType::parse("UNKNOWN"), None);
    }
}
