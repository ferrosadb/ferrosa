//! CQL protocol error types.
//!
//! Each variant maps to a CQL error code from the native protocol spec.
//! `encode_body()` produces the wire-format error response body.

use bytes::{BufMut, BytesMut};

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
    Unavailable,
    /// 0x1100 — server backpressure.
    Overloaded,
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
}

impl CqlError {
    /// Returns the CQL error code for this error.
    pub fn error_code(&self) -> u32 {
        match self {
            Self::ServerError(_) => 0x0000,
            Self::Protocol(_) => 0x000A,
            Self::BadCredentials => 0x0100,
            Self::Unavailable => 0x1000,
            Self::Overloaded => 0x1100,
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

impl std::fmt::Display for CqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServerError(msg) => write!(f, "server error: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::BadCredentials => write!(f, "bad credentials"),
            Self::Unavailable => write!(f, "unavailable"),
            Self::Overloaded => write!(f, "overloaded"),
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
        }
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
        Self::ServerError(format!("storage error: {err}"))
    }
}

impl From<std::io::Error> for CqlError {
    fn from(err: std::io::Error) -> Self {
        Self::ServerError(format!("I/O error: {err}"))
    }
}

impl From<ferrosa_cluster::ClusterError> for CqlError {
    fn from(err: ferrosa_cluster::ClusterError) -> Self {
        Self::ServerError(format!("cluster error: {err}"))
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
        assert_eq!(CqlError::Unavailable.error_code(), 0x1000);
        assert_eq!(CqlError::Overloaded.error_code(), 0x1100);
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
