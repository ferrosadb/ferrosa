//! Bolt v5 protocol message types.
//!
//! Each message is a PackStream structure with a specific tag byte.
//! Client messages flow from the driver to the server; server messages
//! flow from the server back to the driver.

use super::codec::{self, CodecError, PackValue};

// ── Tag Constants ───────────────────────────────────────────────────

/// HELLO — client identifies itself and negotiates capabilities.
pub const TAG_HELLO: u8 = 0x01;
/// LOGON — client provides authentication credentials (Bolt 5+).
pub const TAG_LOGON: u8 = 0x6A;
/// LOGOFF — client de-authenticates (Bolt 5+).
pub const TAG_LOGOFF: u8 = 0x6B;
/// BEGIN — open an explicit transaction (Bolt 3+).
pub const TAG_BEGIN: u8 = 0x11;
/// COMMIT — commit the open explicit transaction (Bolt 3+).
pub const TAG_COMMIT: u8 = 0x12;
/// ROLLBACK — abort the open explicit transaction (Bolt 3+).
pub const TAG_ROLLBACK: u8 = 0x13;
/// RUN — execute a Cypher query.
pub const TAG_RUN: u8 = 0x10;
/// PULL — pull results from the last RUN.
pub const TAG_PULL: u8 = 0x3F;
/// DISCARD — discard results from the last RUN.
pub const TAG_DISCARD: u8 = 0x2F;
/// RESET — return the connection to a clean state.
pub const TAG_RESET: u8 = 0x0F;
/// GOODBYE — gracefully close the connection.
pub const TAG_GOODBYE: u8 = 0x02;

/// SUCCESS — server reports successful completion.
pub const TAG_SUCCESS: u8 = 0x70;
/// FAILURE — server reports an error.
pub const TAG_FAILURE: u8 = 0x7F;
/// RECORD — a single result row.
pub const TAG_RECORD: u8 = 0x71;
/// IGNORED — server ignored a message (e.g. after FAILURE).
pub const TAG_IGNORED: u8 = 0x7E;

// ── Message Enum ────────────────────────────────────────────────────

/// A Bolt protocol message.
#[derive(Debug, Clone)]
pub enum BoltMessage {
    // ── Client messages ──
    /// Client identification and capability negotiation.
    Hello { extra: Vec<(String, PackValue)> },
    /// Authentication credentials (Bolt 5+).
    Logon { auth: Vec<(String, PackValue)> },
    /// De-authenticate the current identity (Bolt 5+).
    Logoff,
    /// Begin an explicit transaction with optional metadata.
    Begin { extra: Vec<(String, PackValue)> },
    /// Commit the open explicit transaction.
    Commit,
    /// Roll back the open explicit transaction.
    Rollback,
    /// Execute a query with parameters and extra metadata.
    Run {
        query: String,
        params: Vec<(String, PackValue)>,
        extra: Vec<(String, PackValue)>,
    },
    /// Pull result records.
    Pull { extra: Vec<(String, PackValue)> },
    /// Discard result records.
    Discard { extra: Vec<(String, PackValue)> },
    /// Reset connection to a clean state.
    Reset,
    /// Graceful connection close.
    Goodbye,

    // ── Server messages ──
    /// Successful operation with metadata.
    Success { metadata: Vec<(String, PackValue)> },
    /// Operation failed with error metadata.
    Failure { metadata: Vec<(String, PackValue)> },
    /// A single result record.
    Record { values: Vec<PackValue> },
    /// Message was ignored (connection in failed state).
    Ignored,
}

impl BoltMessage {
    /// Encode this message into PackStream bytes (without chunked framing).
    pub fn encode(&self) -> Vec<u8> {
        let structure = self.to_pack_structure();
        let mut buf = Vec::new();
        codec::encode(&structure, &mut buf);
        buf
    }

    /// Decode a message from raw PackStream bytes.
    pub fn decode(data: &[u8]) -> Result<Self, CodecError> {
        let (value, _consumed) = codec::decode(data)?;
        Self::from_pack_value(value)
    }

    /// Convert this message to a PackStream structure value.
    fn to_pack_structure(&self) -> PackValue {
        match self {
            Self::Hello { extra } => PackValue::Structure {
                tag: TAG_HELLO,
                fields: vec![PackValue::Map(extra.clone())],
            },
            Self::Logon { auth } => PackValue::Structure {
                tag: TAG_LOGON,
                fields: vec![PackValue::Map(auth.clone())],
            },
            Self::Logoff => PackValue::Structure {
                tag: TAG_LOGOFF,
                fields: vec![],
            },
            Self::Begin { extra } => PackValue::Structure {
                tag: TAG_BEGIN,
                fields: vec![PackValue::Map(extra.clone())],
            },
            Self::Commit => PackValue::Structure {
                tag: TAG_COMMIT,
                fields: vec![],
            },
            Self::Rollback => PackValue::Structure {
                tag: TAG_ROLLBACK,
                fields: vec![],
            },
            Self::Run {
                query,
                params,
                extra,
            } => PackValue::Structure {
                tag: TAG_RUN,
                fields: vec![
                    PackValue::String(query.clone()),
                    PackValue::Map(params.clone()),
                    PackValue::Map(extra.clone()),
                ],
            },
            Self::Pull { extra } => PackValue::Structure {
                tag: TAG_PULL,
                fields: vec![PackValue::Map(extra.clone())],
            },
            Self::Discard { extra } => PackValue::Structure {
                tag: TAG_DISCARD,
                fields: vec![PackValue::Map(extra.clone())],
            },
            Self::Reset => PackValue::Structure {
                tag: TAG_RESET,
                fields: vec![],
            },
            Self::Goodbye => PackValue::Structure {
                tag: TAG_GOODBYE,
                fields: vec![],
            },
            Self::Success { metadata } => PackValue::Structure {
                tag: TAG_SUCCESS,
                fields: vec![PackValue::Map(metadata.clone())],
            },
            Self::Failure { metadata } => PackValue::Structure {
                tag: TAG_FAILURE,
                fields: vec![PackValue::Map(metadata.clone())],
            },
            Self::Record { values } => PackValue::Structure {
                tag: TAG_RECORD,
                fields: vec![PackValue::List(values.clone())],
            },
            Self::Ignored => PackValue::Structure {
                tag: TAG_IGNORED,
                fields: vec![],
            },
        }
    }

    /// Parse a PackStream value (expected to be a Structure) into a `BoltMessage`.
    fn from_pack_value(value: PackValue) -> Result<Self, CodecError> {
        match value {
            PackValue::Structure { tag, fields } => Self::from_tag_and_fields(tag, fields),
            _ => Err(CodecError::InvalidMarker(0)),
        }
    }

    fn from_tag_and_fields(tag: u8, mut fields: Vec<PackValue>) -> Result<Self, CodecError> {
        match tag {
            TAG_HELLO => {
                let extra = take_map(&mut fields, 0)?;
                Ok(Self::Hello { extra })
            }
            TAG_LOGON => {
                let auth = take_map(&mut fields, 0)?;
                Ok(Self::Logon { auth })
            }
            TAG_LOGOFF => Ok(Self::Logoff),
            TAG_BEGIN => {
                let extra = take_map(&mut fields, 0)?;
                Ok(Self::Begin { extra })
            }
            TAG_COMMIT => Ok(Self::Commit),
            TAG_ROLLBACK => Ok(Self::Rollback),
            TAG_RUN => {
                let query = take_string(&mut fields, 0)?;
                let params = take_map(&mut fields, 1)?;
                let extra = take_map(&mut fields, 2)?;
                Ok(Self::Run {
                    query,
                    params,
                    extra,
                })
            }
            TAG_PULL => {
                let extra = take_map(&mut fields, 0)?;
                Ok(Self::Pull { extra })
            }
            TAG_DISCARD => {
                let extra = take_map(&mut fields, 0)?;
                Ok(Self::Discard { extra })
            }
            TAG_RESET => Ok(Self::Reset),
            TAG_GOODBYE => Ok(Self::Goodbye),
            TAG_SUCCESS => {
                let metadata = take_map(&mut fields, 0)?;
                Ok(Self::Success { metadata })
            }
            TAG_FAILURE => {
                let metadata = take_map(&mut fields, 0)?;
                Ok(Self::Failure { metadata })
            }
            TAG_RECORD => {
                let values = take_list(&mut fields, 0)?;
                Ok(Self::Record { values })
            }
            TAG_IGNORED => Ok(Self::Ignored),
            _ => Err(CodecError::InvalidMarker(tag)),
        }
    }
}

// ── Field extraction helpers ────────────────────────────────────────

fn take_map(
    fields: &mut [PackValue],
    index: usize,
) -> Result<Vec<(String, PackValue)>, CodecError> {
    if index >= fields.len() {
        return Err(CodecError::UnexpectedEnd);
    }
    match std::mem::replace(&mut fields[index], PackValue::Null) {
        PackValue::Map(m) => Ok(m),
        _ => Err(CodecError::InvalidMarker(0)),
    }
}

fn take_string(fields: &mut [PackValue], index: usize) -> Result<String, CodecError> {
    if index >= fields.len() {
        return Err(CodecError::UnexpectedEnd);
    }
    match std::mem::replace(&mut fields[index], PackValue::Null) {
        PackValue::String(s) => Ok(s),
        _ => Err(CodecError::InvalidMarker(0)),
    }
}

fn take_list(fields: &mut [PackValue], index: usize) -> Result<Vec<PackValue>, CodecError> {
    if index >= fields.len() {
        return Err(CodecError::UnexpectedEnd);
    }
    match std::mem::replace(&mut fields[index], PackValue::Null) {
        PackValue::List(l) => Ok(l),
        _ => Err(CodecError::InvalidMarker(0)),
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: encode then decode a message and return the decoded form.
    fn roundtrip(msg: &BoltMessage) -> BoltMessage {
        let bytes = msg.encode();
        BoltMessage::decode(&bytes).expect("failed to decode")
    }

    /// Helper: compare map entries (order-preserving).
    fn assert_maps_eq(a: &[(String, PackValue)], b: &[(String, PackValue)]) {
        assert_eq!(a.len(), b.len(), "map length mismatch");
        for (i, (ak, av)) in a.iter().enumerate() {
            assert_eq!(ak, &b[i].0, "key mismatch at {i}");
            assert_eq!(av, &b[i].1, "value mismatch at {i}");
        }
    }

    #[test]
    fn encode_decode_hello() {
        let msg = BoltMessage::Hello {
            extra: vec![
                (
                    "user_agent".into(),
                    PackValue::String("ferrosa-test/1.0".into()),
                ),
                (
                    "routing".into(),
                    PackValue::Map(vec![(
                        "address".into(),
                        PackValue::String("localhost:7687".into()),
                    )]),
                ),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Hello { extra } => {
                assert_eq!(extra.len(), 2);
                assert_eq!(extra[0].0, "user_agent");
            }
            other => panic!("expected Hello, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_run() {
        let msg = BoltMessage::Run {
            query: "MATCH (n:Person) RETURN n.name".into(),
            params: vec![("limit".into(), PackValue::Int(10))],
            extra: vec![("db".into(), PackValue::String("neo4j".into()))],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Run {
                query,
                params,
                extra,
            } => {
                assert_eq!(query, "MATCH (n:Person) RETURN n.name");
                assert_maps_eq(&params, &[("limit".into(), PackValue::Int(10))]);
                assert_maps_eq(&extra, &[("db".into(), PackValue::String("neo4j".into()))]);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_pull() {
        let msg = BoltMessage::Pull {
            extra: vec![("n".into(), PackValue::Int(-1))],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Pull { extra } => {
                assert_maps_eq(&extra, &[("n".into(), PackValue::Int(-1))]);
            }
            other => panic!("expected Pull, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_success() {
        let msg = BoltMessage::Success {
            metadata: vec![
                (
                    "fields".into(),
                    PackValue::List(vec![PackValue::String("n.name".into())]),
                ),
                ("t_first".into(), PackValue::Int(5)),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Success { metadata } => {
                assert_eq!(metadata.len(), 2);
                assert_eq!(metadata[0].0, "fields");
            }
            other => panic!("expected Success, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_failure() {
        let msg = BoltMessage::Failure {
            metadata: vec![
                (
                    "code".into(),
                    PackValue::String("Neo.ClientError.Statement.SyntaxError".into()),
                ),
                ("message".into(), PackValue::String("Invalid query".into())),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Failure { metadata } => {
                assert_eq!(metadata.len(), 2);
                assert_eq!(metadata[0].0, "code");
            }
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_record() {
        let msg = BoltMessage::Record {
            values: vec![
                PackValue::String("Alice".into()),
                PackValue::Int(30),
                PackValue::Bool(true),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Record { values } => {
                assert_eq!(values.len(), 3);
                assert_eq!(values[0], PackValue::String("Alice".into()));
                assert_eq!(values[1], PackValue::Int(30));
                assert_eq!(values[2], PackValue::Bool(true));
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_reset() {
        let decoded = roundtrip(&BoltMessage::Reset);
        assert!(matches!(decoded, BoltMessage::Reset));
    }

    #[test]
    fn encode_decode_goodbye() {
        let decoded = roundtrip(&BoltMessage::Goodbye);
        assert!(matches!(decoded, BoltMessage::Goodbye));
    }

    #[test]
    fn encode_decode_logon() {
        let msg = BoltMessage::Logon {
            auth: vec![
                ("scheme".into(), PackValue::String("basic".into())),
                ("principal".into(), PackValue::String("neo4j".into())),
                ("credentials".into(), PackValue::String("password".into())),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Logon { auth } => {
                assert_eq!(auth.len(), 3);
                assert_eq!(auth[0].0, "scheme");
            }
            other => panic!("expected Logon, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_logoff() {
        let decoded = roundtrip(&BoltMessage::Logoff);
        assert!(matches!(decoded, BoltMessage::Logoff));
    }

    #[test]
    fn encode_decode_discard() {
        let msg = BoltMessage::Discard {
            extra: vec![("n".into(), PackValue::Int(-1))],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Discard { extra } => {
                assert_maps_eq(&extra, &[("n".into(), PackValue::Int(-1))]);
            }
            other => panic!("expected Discard, got {other:?}"),
        }
    }

    #[test]
    fn encode_decode_ignored() {
        let decoded = roundtrip(&BoltMessage::Ignored);
        assert!(matches!(decoded, BoltMessage::Ignored));
    }

    #[test]
    fn encode_decode_begin() {
        let msg = BoltMessage::Begin {
            extra: vec![
                ("tx_timeout".into(), PackValue::Int(30_000)),
                ("mode".into(), PackValue::String("w".into())),
            ],
        };
        let decoded = roundtrip(&msg);
        match decoded {
            BoltMessage::Begin { extra } => {
                assert_maps_eq(
                    &extra,
                    &[
                        ("tx_timeout".into(), PackValue::Int(30_000)),
                        ("mode".into(), PackValue::String("w".into())),
                    ],
                );
            }
            other => panic!("expected Begin, got {other:?}"),
        }
    }

    #[test]
    fn begin_uses_tag_0x11() {
        let bytes = BoltMessage::Begin { extra: vec![] }.encode();
        // PackStream structure marker for 1 field is 0xB1, followed by the tag.
        assert_eq!(bytes[0], 0xB1, "expected 1-field structure marker");
        assert_eq!(bytes[1], TAG_BEGIN, "expected BEGIN tag");
        assert_eq!(TAG_BEGIN, 0x11);
    }

    #[test]
    fn encode_decode_commit() {
        let decoded = roundtrip(&BoltMessage::Commit);
        assert!(matches!(decoded, BoltMessage::Commit));
    }

    #[test]
    fn commit_uses_tag_0x12() {
        let bytes = BoltMessage::Commit.encode();
        // PackStream structure marker for 0 fields is 0xB0, followed by the tag.
        assert_eq!(bytes[0], 0xB0, "expected 0-field structure marker");
        assert_eq!(bytes[1], TAG_COMMIT, "expected COMMIT tag");
        assert_eq!(TAG_COMMIT, 0x12);
    }

    #[test]
    fn encode_decode_rollback() {
        let decoded = roundtrip(&BoltMessage::Rollback);
        assert!(matches!(decoded, BoltMessage::Rollback));
    }

    #[test]
    fn rollback_uses_tag_0x13() {
        let bytes = BoltMessage::Rollback.encode();
        assert_eq!(bytes[0], 0xB0, "expected 0-field structure marker");
        assert_eq!(bytes[1], TAG_ROLLBACK, "expected ROLLBACK tag");
        assert_eq!(TAG_ROLLBACK, 0x13);
    }
}
