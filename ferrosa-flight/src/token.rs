//! Signed bearer tokens for the Flight endpoint (decision D4).
//!
//! A `Handshake` validates CQL credentials and issues a token carrying the
//! authenticated role; subsequent RPCs present it as a bearer credential and
//! the server derives the request's `AuthContext` from the verified claims —
//! no anonymous access. Tokens are HMAC-SHA256 signed over a compact payload
//! and carry an absolute expiry.
//!
//! Format: `hex(payload) "." hex(hmac_sha256(key, payload))`, where
//! `payload = "{expires_at}:{is_superuser as 0|1}:{role}"`. Hex keeps the token
//! header-safe without coupling to a specific base64 API version. Keys are
//! process-held; [`verify_with_keys`] accepts a set of keys (current + recently
//! retired), so rotation has an overlap window rather than invalidating every
//! outstanding token at once.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Verified identity carried by a bearer token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub role: String,
    pub is_superuser: bool,
    /// Absolute expiry, seconds since the Unix epoch.
    pub expires_at: u64,
}

/// Why a token was rejected. Never leaks which check failed to the wire beyond
/// a generic "unauthenticated"; the variants exist for server-side logging.
#[derive(Debug, PartialEq, Eq)]
pub enum TokenError {
    /// Structurally invalid (no separator, bad hex, non-UTF8/garbled payload).
    Malformed,
    /// HMAC did not match — forged or tampered.
    BadSignature,
    /// Past its `expires_at`.
    Expired,
}

impl std::fmt::Display for TokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TokenError::Malformed => write!(f, "malformed bearer token"),
            TokenError::BadSignature => write!(f, "bearer token signature mismatch"),
            TokenError::Expired => write!(f, "bearer token expired"),
        }
    }
}

impl std::error::Error for TokenError {}

fn payload_bytes(claims: &Claims) -> Vec<u8> {
    format!(
        "{}:{}:{}",
        claims.expires_at,
        u8::from(claims.is_superuser),
        claims.role
    )
    .into_bytes()
}

fn mac(key: &[u8], msg: &[u8]) -> HmacSha256 {
    // HMAC accepts a key of any length, so this never errors.
    let mut m = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    m.update(msg);
    m
}

/// Issue a signed bearer token for `claims`.
pub fn issue(key: &[u8], claims: &Claims) -> String {
    let payload = payload_bytes(claims);
    let sig = mac(key, &payload).finalize().into_bytes();
    format!("{}.{}", hex::encode(&payload), hex::encode(sig))
}

/// Verify a bearer token against `key` at `now_secs`, returning its claims.
///
/// Signature is checked in constant time (`verify_slice`) before the payload is
/// trusted; expiry is checked last so a forged token never reports "expired".
pub fn verify(key: &[u8], token: &str, now_secs: u64) -> Result<Claims, TokenError> {
    let (payload_hex, sig_hex) = token.split_once('.').ok_or(TokenError::Malformed)?;
    let payload = hex::decode(payload_hex).map_err(|_| TokenError::Malformed)?;
    let sig = hex::decode(sig_hex).map_err(|_| TokenError::Malformed)?;

    mac(key, &payload)
        .verify_slice(&sig)
        .map_err(|_| TokenError::BadSignature)?;

    let text = std::str::from_utf8(&payload).map_err(|_| TokenError::Malformed)?;
    let mut parts = text.splitn(3, ':');
    let expires_at: u64 = parts
        .next()
        .ok_or(TokenError::Malformed)?
        .parse()
        .map_err(|_| TokenError::Malformed)?;
    let is_superuser = match parts.next() {
        Some("1") => true,
        Some("0") => false,
        _ => return Err(TokenError::Malformed),
    };
    let role = parts.next().ok_or(TokenError::Malformed)?.to_string();

    if now_secs >= expires_at {
        return Err(TokenError::Expired);
    }
    Ok(Claims {
        role,
        is_superuser,
        expires_at,
    })
}

/// Verify against any of `keys` — supporting key rotation with an overlap
/// window: tokens are signed with the current key but stay valid while a
/// previous key is still in the set. Returns the claims from the first key that
/// validates. Error precedence reflects intent: `Malformed` (structural, key-
/// independent) short-circuits; a key whose signature matches but is expired
/// yields `Expired`; otherwise `BadSignature` (matched no key).
pub fn verify_with_keys(
    keys: &[Vec<u8>],
    token: &str,
    now_secs: u64,
) -> Result<Claims, TokenError> {
    let mut expired = false;
    for key in keys {
        match verify(key, token, now_secs) {
            Ok(claims) => return Ok(claims),
            Err(TokenError::Malformed) => return Err(TokenError::Malformed),
            Err(TokenError::Expired) => expired = true,
            Err(TokenError::BadSignature) => {}
        }
    }
    Err(if expired {
        TokenError::Expired
    } else {
        TokenError::BadSignature
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claims() -> Claims {
        Claims {
            role: "alice".to_string(),
            is_superuser: false,
            expires_at: 2_000,
        }
    }

    #[test]
    fn issue_then_verify_round_trips() {
        let key = b"secret-signing-key";
        let token = issue(key, &claims());
        assert_eq!(verify(key, &token, 1_000).unwrap(), claims());
    }

    #[test]
    fn role_with_colons_survives() {
        // role is the splitn(3) remainder, so embedded ':' is preserved.
        let c = Claims {
            role: "ns:team:svc".to_string(),
            is_superuser: true,
            expires_at: 2_000,
        };
        let key = b"k";
        assert_eq!(verify(key, &issue(key, &c), 1).unwrap(), c);
    }

    #[test]
    fn tampered_signature_is_rejected() {
        let key = b"secret-signing-key";
        let mut token = issue(key, &claims());
        // Flip the last hex nibble of the signature.
        let last = token.pop().unwrap();
        token.push(if last == 'a' { 'b' } else { 'a' });
        assert_eq!(verify(key, &token, 1_000), Err(TokenError::BadSignature));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let token = issue(b"key-one", &claims());
        assert_eq!(
            verify(b"key-two", &token, 1_000),
            Err(TokenError::BadSignature)
        );
    }

    #[test]
    fn expired_token_is_rejected() {
        let key = b"secret-signing-key";
        let token = issue(key, &claims()); // expires_at = 2000
        assert_eq!(verify(key, &token, 2_000), Err(TokenError::Expired));
        assert_eq!(verify(key, &token, 2_001), Err(TokenError::Expired));
    }

    #[test]
    fn verify_with_keys_supports_rotation() {
        let old = b"old-key".to_vec();
        let new = b"new-key".to_vec();
        // Token issued under the OLD key still validates while old is in the set.
        let token = issue(&old, &claims());
        let keys = vec![new.clone(), old.clone()]; // current first, previous second
        assert_eq!(verify_with_keys(&keys, &token, 1_000).unwrap(), claims());

        // Once the old key is rotated out, the token no longer validates.
        assert_eq!(
            verify_with_keys(&[new], &token, 1_000),
            Err(TokenError::BadSignature)
        );

        // A signature-valid-but-expired token reports Expired, not BadSignature,
        // even when other (non-matching) keys are present.
        assert_eq!(
            verify_with_keys(&[b"other".to_vec(), old], &token, 2_000),
            Err(TokenError::Expired)
        );
    }

    #[test]
    fn malformed_tokens_are_rejected() {
        let key = b"k";
        assert_eq!(verify(key, "no-separator", 1), Err(TokenError::Malformed));
        assert_eq!(verify(key, "zz.zz", 1), Err(TokenError::Malformed));
    }
}
