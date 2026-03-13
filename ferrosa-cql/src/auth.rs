//! SASL PLAIN authentication for CQL connections.
//!
//! The auth flow: server sends AUTHENTICATE with the authenticator class
//! name, client responds with AUTH_RESPONSE containing a SASL PLAIN
//! payload (`\0username\0password`), server validates via
//! `ferrosa-schema::Schema::authenticate()`.

use bytes::BufMut;

use crate::error::CqlError;

/// The authenticator class name sent to drivers.
pub const AUTHENTICATOR_CLASS: &str = "org.apache.cassandra.auth.PasswordAuthenticator";

/// Maximum auth attempts before closing the connection.
pub const MAX_AUTH_ATTEMPTS: u32 = 3;

/// Parse a SASL PLAIN payload into (username, password).
///
/// SASL PLAIN format: `[authzid]\0<username>\0<password>`
/// The authzid (authorization identity) is ignored.
pub fn parse_sasl_plain(payload: &[u8]) -> Result<(&str, &str), CqlError> {
    let mut nulls = payload
        .iter()
        .enumerate()
        .filter(|(_, &b)| b == 0)
        .map(|(i, _)| i);

    let first = nulls
        .next()
        .ok_or_else(|| CqlError::Protocol("SASL PLAIN: missing null separator".into()))?;
    let second = nulls
        .next()
        .ok_or_else(|| CqlError::Protocol("SASL PLAIN: missing second null separator".into()))?;

    let username = std::str::from_utf8(&payload[first + 1..second])
        .map_err(|e| CqlError::Protocol(format!("SASL PLAIN: invalid UTF-8 username: {e}")))?;
    let password = std::str::from_utf8(&payload[second + 1..])
        .map_err(|e| CqlError::Protocol(format!("SASL PLAIN: invalid UTF-8 password: {e}")))?;

    Ok((username, password))
}

/// Encode the body of an AUTHENTICATE response frame.
///
/// Format: `[string authenticator_class]`
pub fn encode_authenticate_response() -> Vec<u8> {
    let mut buf = Vec::with_capacity(2 + AUTHENTICATOR_CLASS.len());
    buf.put_u16(AUTHENTICATOR_CLASS.len() as u16);
    buf.extend_from_slice(AUTHENTICATOR_CLASS.as_bytes());
    buf
}

/// Encode the body of an AUTH_SUCCESS response frame (empty token).
pub fn encode_auth_success() -> Vec<u8> {
    // [int length][-1 for null token]
    (-1i32).to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sasl_plain_valid() {
        let payload = b"\0cassandra\0cassandra";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "cassandra");
        assert_eq!(pass, "cassandra");
    }

    #[test]
    fn parse_sasl_plain_with_authzid() {
        let payload = b"ignored\0user\0pass";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "pass");
    }

    #[test]
    fn parse_sasl_plain_empty_password() {
        let payload = b"\0user\0";
        let (user, pass) = parse_sasl_plain(payload).unwrap();
        assert_eq!(user, "user");
        assert_eq!(pass, "");
    }

    #[test]
    fn parse_sasl_plain_no_null() {
        let payload = b"no nulls here";
        assert!(parse_sasl_plain(payload).is_err());
    }

    #[test]
    fn authenticator_class_name() {
        assert_eq!(
            AUTHENTICATOR_CLASS,
            "org.apache.cassandra.auth.PasswordAuthenticator"
        );
    }

    #[test]
    fn encode_authenticate_body() {
        let body = encode_authenticate_response();
        let len = u16::from_be_bytes([body[0], body[1]]) as usize;
        let class = std::str::from_utf8(&body[2..2 + len]).unwrap();
        assert_eq!(class, AUTHENTICATOR_CLASS);
    }
}
