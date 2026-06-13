//! CQL protocol error types.
//!
//! Each variant maps to a CQL error code from the native protocol spec.
//! `encode_body()` produces the wire-format error response body.

use bytes::{BufMut, BytesMut};
use ferrosa_cluster::consistency::ConsistencyLevel;

/// CQL protocol error with structured data for each error code.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum CqlError {
    /// 0x0000 — unexpected internal failure.
    ServerError(String),
    /// 0x000A — malformed frame, wrong version, bad opcode.
    Protocol(String),
    /// 0x0100 — authentication rejected.
    BadCredentials,
    /// 0x1000 — not enough replicas.
    Unavailable {
        consistency: ConsistencyLevel,
        required: usize,
        alive: usize,
    },
    /// 0x1100 — replicas alive but not enough acknowledged the write.
    WriteTimeout {
        consistency: ConsistencyLevel,
        received: usize,
        required: usize,
        write_type: &'static str,
    },
    /// 0x1200 — replicas alive but not enough responded to the read.
    ReadTimeout {
        consistency: ConsistencyLevel,
        received: usize,
        required: usize,
        data_present: bool,
    },
    /// 0x1001 — server backpressure.
    Overloaded(String),
    /// 0x2000 — CQL syntax error.
    SyntaxError(String),
    /// 0x2100 — insufficient permissions.
    Unauthorized(String),
    /// 0x2200 — valid syntax but semantic error.
    Invalid(String),
    /// 0x2300 — invalid DDL configuration.
    ConfigError(String),
    /// 0x2400 — object already exists.
    AlreadyExists { keyspace: String, table: String },
    /// 0x2500 — unknown prepared statement ID.
    Unprepared([u8; 16]),
    /// Client requested a protocol version we don't support (e.g., v5).
    /// The connection handler should reply with an ERROR frame using the
    /// supported version so the driver falls back.
    ProtocolVersionMismatch { requested: u8, supported: u8 },
    /// An explicit transaction exceeded its configured timeout and was aborted
    /// (URS-QEC-B03). Reported as a write-timeout class error so drivers
    /// classify it as a transient timeout; the message carries the budget and
    /// the elapsed time. The transaction is aborted: nothing was persisted.
    TransactionTimeout { timeout_ms: u64, elapsed_ms: u64 },
}

impl CqlError {
    /// Returns the CQL error code for this error.
    pub fn error_code(&self) -> u32 {
        match self {
            Self::ServerError(_) => 0x0000,
            Self::Protocol(_) | Self::ProtocolVersionMismatch { .. } => 0x000A,
            Self::BadCredentials => 0x0100,
            Self::Unavailable { .. } => 0x1000,
            Self::Overloaded(_) => 0x1001,
            Self::WriteTimeout { .. } | Self::TransactionTimeout { .. } => 0x1100,
            Self::ReadTimeout { .. } => 0x1200,
            Self::SyntaxError(_) => 0x2000,
            Self::Unauthorized(_) => 0x2100,
            Self::Invalid(_) => 0x2200,
            Self::ConfigError(_) => 0x2300,
            Self::AlreadyExists { .. } => 0x2400,
            Self::Unprepared(_) => 0x2500,
        }
    }

    /// Encode the error body for a CQL ERROR response frame.
    ///
    /// Format: `[int error_code][string message][extra fields...]`
    pub fn encode_body(&self) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u32(self.error_code());

        let msg = self.to_string();
        buf.put_u16(msg.len() as u16);
        buf.put_slice(msg.as_bytes());

        // Extra fields for specific error types.
        match self {
            Self::Unavailable {
                consistency,
                required,
                alive,
            } => {
                put_consistency(&mut buf, *consistency);
                buf.put_i32(saturating_i32(*required));
                buf.put_i32(saturating_i32(*alive));
            }
            Self::WriteTimeout {
                consistency,
                received,
                required,
                write_type,
            } => {
                put_consistency(&mut buf, *consistency);
                buf.put_i32(saturating_i32(*received));
                buf.put_i32(saturating_i32(*required));
                put_string(&mut buf, write_type);
            }
            Self::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => {
                put_consistency(&mut buf, *consistency);
                buf.put_i32(saturating_i32(*received));
                buf.put_i32(saturating_i32(*required));
                buf.put_u8(u8::from(*data_present));
            }
            Self::AlreadyExists { keyspace, table } => {
                buf.put_u16(keyspace.len() as u16);
                buf.put_slice(keyspace.as_bytes());
                buf.put_u16(table.len() as u16);
                buf.put_slice(table.as_bytes());
            }
            Self::Unprepared(id) => {
                buf.put_u16(16);
                buf.put_slice(id);
            }
            _ => {}
        }

        buf
    }
}

fn put_consistency(buf: &mut BytesMut, consistency: ConsistencyLevel) {
    buf.put_u16(consistency.to_wire());
}

fn put_string(buf: &mut BytesMut, value: &str) {
    buf.put_u16(value.len() as u16);
    buf.put_slice(value.as_bytes());
}

fn saturating_i32(value: usize) -> i32 {
    value.min(i32::MAX as usize) as i32
}

fn parse_consistency(value: &str) -> ConsistencyLevel {
    ConsistencyLevel::from_str(value).unwrap_or(ConsistencyLevel::One)
}

impl std::fmt::Display for CqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServerError(msg) => write!(f, "server error: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::BadCredentials => write!(f, "bad credentials"),
            Self::Unavailable {
                consistency,
                required,
                alive,
            } => write!(
                f,
                "unavailable: CL={consistency}, required={required}, alive={alive}"
            ),
            Self::WriteTimeout {
                consistency,
                received,
                required,
                ..
            } => write!(
                f,
                "write timeout: CL={consistency}, received={received}, required={required}"
            ),
            Self::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => write!(
                f,
                "read timeout: CL={consistency}, received={received}, required={required}, data_present={data_present}"
            ),
            Self::Overloaded(msg) => write!(f, "{msg}"),
            Self::SyntaxError(msg) => write!(f, "{msg}"),
            Self::Unauthorized(msg) => write!(f, "unauthorized: {msg}"),
            Self::Invalid(msg) => write!(f, "{msg}"),
            Self::ConfigError(msg) => write!(f, "config error: {msg}"),
            Self::AlreadyExists { keyspace, table } => {
                if table.is_empty() {
                    write!(f, "keyspace already exists: {keyspace}")
                } else {
                    write!(f, "table already exists: {keyspace}.{table}")
                }
            }
            Self::Unprepared(id) => {
                write!(f, "unprepared: ")?;
                for b in id {
                    write!(f, "{b:02x}")?;
                }
                Ok(())
            }
            Self::ProtocolVersionMismatch {
                requested,
                supported,
            } => write!(
                f,
                "Invalid or unsupported protocol version ({requested}); \
                 the lowest supported version is 3 and the greatest is {supported}"
            ),
            Self::TransactionTimeout {
                timeout_ms,
                elapsed_ms,
            } => write!(
                f,
                "transaction timed out and was aborted: budget={timeout_ms}ms, \
                 elapsed={elapsed_ms}ms; nothing was persisted"
            ),
        }
    }
}

impl CqlError {
    /// `true` if this is a transaction-timeout error (URS-QEC-B03). Lets a
    /// transport map a timed-out, aborted transaction to its own FAILURE class.
    pub fn is_transaction_timeout(&self) -> bool {
        matches!(self, Self::TransactionTimeout { .. })
    }
}

impl std::error::Error for CqlError {}

impl From<ferrosa_schema::SchemaError> for CqlError {
    fn from(err: ferrosa_schema::SchemaError) -> Self {
        use ferrosa_schema::SchemaError;
        match err {
            SchemaError::KeyspaceExists(ks) => Self::AlreadyExists {
                keyspace: ks,
                table: String::new(),
            },
            SchemaError::TableExists(ks, t) => Self::AlreadyExists {
                keyspace: ks,
                table: t,
            },
            SchemaError::KeyspaceNotFound(ks) => Self::Invalid(format!("keyspace not found: {ks}")),
            SchemaError::TableNotFound(ks, t) => {
                Self::Invalid(format!("table not found: {ks}.{t}"))
            }
            SchemaError::RoleExists(r) => Self::Invalid(format!("role already exists: {r}")),
            SchemaError::RoleNotFound(r) => Self::Invalid(format!("role not found: {r}")),
            SchemaError::AuthenticationFailed => Self::BadCredentials,
            SchemaError::AuthenticationThrottled => Self::BadCredentials,
            SchemaError::PermissionDenied {
                role,
                permission,
                resource,
            } => Self::Unauthorized(format!("{role} lacks {permission} on {resource}")),
            SchemaError::SystemKeyspaceProtected(ks) => {
                Self::Invalid(format!("cannot modify system keyspace: {ks}"))
            }
            SchemaError::PasswordTooWeak { violations } => {
                Self::Invalid(format!("password too weak: {}", violations.join(", ")))
            }
            SchemaError::RoleCycleDetected(r) => {
                Self::Invalid(format!("role cycle detected involving: {r}"))
            }
            SchemaError::InvalidSchema(msg) => Self::ConfigError(msg),
            _ => {
                tracing::warn!("unmapped schema error variant: {err}");
                Self::ServerError(format!("schema error: {err}"))
            }
        }
    }
}

impl From<ferrosa_common::Error> for CqlError {
    fn from(err: ferrosa_common::Error) -> Self {
        if err.is_backpressure() {
            Self::Overloaded(format!("storage backpressure: {err}"))
        } else {
            Self::ServerError(format!("storage error: {err}"))
        }
    }
}

impl From<std::io::Error> for CqlError {
    fn from(err: std::io::Error) -> Self {
        Self::ServerError(format!("I/O error: {err}"))
    }
}

impl From<ferrosa_cluster::ClusterError> for CqlError {
    fn from(err: ferrosa_cluster::ClusterError) -> Self {
        use ferrosa_cluster::ClusterError;
        match err {
            ClusterError::Unavailable {
                consistency,
                required,
                alive,
            } => Self::Unavailable {
                consistency: parse_consistency(&consistency),
                required,
                alive,
            },
            ClusterError::WriteTimeout {
                consistency,
                received,
                required,
            } => Self::WriteTimeout {
                consistency: parse_consistency(&consistency),
                received,
                required,
                write_type: "SIMPLE",
            },
            ClusterError::ReadTimeout {
                consistency,
                received,
                required,
                data_present,
            } => Self::ReadTimeout {
                consistency: parse_consistency(&consistency),
                received,
                required,
                data_present,
            },
            ClusterError::Overloaded(msg) => Self::Overloaded(msg),
            ClusterError::Storage(e) if e.is_backpressure() => {
                Self::Overloaded(format!("storage backpressure: {e}"))
            }
            other => Self::ServerError(format!("cluster error: {other}")),
        }
    }
}

impl From<ferrosa_udf::UdfError> for CqlError {
    fn from(err: ferrosa_udf::UdfError) -> Self {
        use ferrosa_udf::UdfError;
        match err {
            UdfError::CompilationFailed(msg) => {
                Self::Invalid(format!("UDF compilation failed: {msg}"))
            }
            UdfError::NotFound { keyspace, name } => {
                Self::Invalid(format!("function not found: {keyspace}.{name}"))
            }
            UdfError::BinaryTooLarge { size, max } => Self::Invalid(format!(
                "WASM binary too large: {size} bytes exceeds {max} byte limit"
            )),
            UdfError::TypeMismatch(msg) => Self::Invalid(format!("UDF type mismatch: {msg}")),
            UdfError::ResourceExhausted(msg) => {
                Self::Invalid(format!("UDF resource exhausted: {msg}"))
            }
            UdfError::ExecutionFailed(msg) => Self::Invalid(format!("UDF execution failed: {msg}")),
            UdfError::KeyInvalid => Self::Invalid("UDF function key is invalid or expired".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_values() {
        assert_eq!(CqlError::ServerError("".into()).error_code(), 0x0000);
        assert_eq!(CqlError::Protocol("".into()).error_code(), 0x000A);
        assert_eq!(CqlError::BadCredentials.error_code(), 0x0100);
        assert_eq!(
            CqlError::Unavailable {
                consistency: ConsistencyLevel::Quorum,
                required: 2,
                alive: 1,
            }
            .error_code(),
            0x1000
        );
        assert_eq!(CqlError::Overloaded("test".into()).error_code(), 0x1001);
        assert_eq!(
            CqlError::WriteTimeout {
                consistency: ConsistencyLevel::LocalQuorum,
                received: 1,
                required: 2,
                write_type: "SIMPLE",
            }
            .error_code(),
            0x1100
        );
        assert_eq!(
            CqlError::ReadTimeout {
                consistency: ConsistencyLevel::LocalQuorum,
                received: 1,
                required: 2,
                data_present: false,
            }
            .error_code(),
            0x1200
        );
        assert_eq!(CqlError::SyntaxError("".into()).error_code(), 0x2000);
        assert_eq!(CqlError::Unauthorized("".into()).error_code(), 0x2100);
        assert_eq!(CqlError::Invalid("".into()).error_code(), 0x2200);
        assert_eq!(CqlError::ConfigError("".into()).error_code(), 0x2300);
        assert_eq!(
            CqlError::AlreadyExists {
                keyspace: "ks".into(),
                table: "t".into()
            }
            .error_code(),
            0x2400
        );
        assert_eq!(CqlError::Unprepared([0u8; 16]).error_code(), 0x2500);
    }

    #[test]
    fn encode_error_frame_body() {
        let err = CqlError::SyntaxError("bad query".into());
        let body = err.encode_body();
        // 4-byte error code + 2-byte string length + "bad query" (9 bytes)
        assert_eq!(&body[..4], &0x2000u32.to_be_bytes());
        let str_len = u16::from_be_bytes([body[4], body[5]]) as usize;
        let msg = std::str::from_utf8(&body[6..6 + str_len]).unwrap();
        assert_eq!(msg, "bad query");
    }

    #[test]
    fn encode_write_timeout_includes_write_type() {
        let err = CqlError::WriteTimeout {
            consistency: ConsistencyLevel::LocalQuorum,
            received: 1,
            required: 2,
            write_type: "SIMPLE",
        };
        let body = err.encode_body();

        assert_eq!(&body[..4], &0x1100u32.to_be_bytes());
        let msg_len = u16::from_be_bytes([body[4], body[5]]) as usize;
        let mut offset = 6 + msg_len;
        assert_eq!(
            u16::from_be_bytes([body[offset], body[offset + 1]]),
            ConsistencyLevel::LocalQuorum.to_wire()
        );
        offset += 2;
        assert_eq!(
            i32::from_be_bytes([
                body[offset],
                body[offset + 1],
                body[offset + 2],
                body[offset + 3],
            ]),
            1
        );
        offset += 4;
        assert_eq!(
            i32::from_be_bytes([
                body[offset],
                body[offset + 1],
                body[offset + 2],
                body[offset + 3],
            ]),
            2
        );
        offset += 4;
        let write_type_len = u16::from_be_bytes([body[offset], body[offset + 1]]) as usize;
        offset += 2;
        assert_eq!(&body[offset..offset + write_type_len], b"SIMPLE");
        assert_eq!(offset + write_type_len, body.len());
    }

    #[test]
    fn from_schema_error_keyspace_exists() {
        let schema_err = ferrosa_schema::SchemaError::KeyspaceExists("ks1".into());
        let cql_err: CqlError = schema_err.into();
        assert_eq!(cql_err.error_code(), 0x2400);
    }

    #[test]
    fn from_schema_error_permission_denied() {
        let schema_err = ferrosa_schema::SchemaError::PermissionDenied {
            role: "user".into(),
            permission: ferrosa_schema::Permission::Select,
            resource: ferrosa_schema::Resource::AllKeyspaces,
        };
        let cql_err: CqlError = schema_err.into();
        assert_eq!(cql_err.error_code(), 0x2100);
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<CqlError>();
        assert_sync::<CqlError>();
    }
}
