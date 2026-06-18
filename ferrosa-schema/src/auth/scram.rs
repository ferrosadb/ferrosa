//! SCRAM-SHA-256 credential derivation (RFC 5802 / RFC 7677) for role auth.
//!
//! Per decision **D4**, ferrosa stores a SCRAM verifier alongside the existing
//! bcrypt/Argon2 salted hash so Postgres clients can authenticate with their
//! driver-default mechanism. A bcrypt hash cannot produce a SCRAM verifier
//! (the derivations are incompatible), so the verifier must be derived at
//! password-set time from the cleartext password and persisted on the role.
//!
//! This module owns only the *derivation* (the password-set side). The
//! *server-side exchange* (recovering ClientKey from the client proof and
//! emitting the server signature) lives in `ferrosa-postgres::scram`, which
//! converts a [`ScramCredential`] into its own `ScramVerifier`. The two share
//! identical fields so the conversion is a field-for-field copy.

use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Default PBKDF2 iteration count for newly-derived credentials (RFC 7677 §3
/// worked example uses 4096; matches libpq's historical default).
const DEFAULT_ITERATIONS: u32 = 4096;

/// Length of a freshly-generated random salt, in bytes.
const SALT_LEN: usize = 16;

/// HMAC-SHA-256(key, msg) → 32-byte tag.
fn hmac256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// A stored SCRAM-SHA-256 verifier persisted per role (D4).
///
/// Holds everything the server needs to validate a client proof without ever
/// seeing the password: the `salt`, `iterations`, `stored_key`
/// (`SHA256(ClientKey)`), and `server_key` (`HMAC(SaltedPassword, "Server
/// Key")`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramCredential {
    /// PBKDF2 salt.
    pub salt: Vec<u8>,
    /// PBKDF2 iteration count.
    pub iterations: u32,
    /// `SHA256(ClientKey)` — used to validate the client proof.
    pub stored_key: [u8; 32],
    /// `HMAC(SaltedPassword, "Server Key")` — used to sign the server-final.
    pub server_key: [u8; 32],
}

/// Derive a [`ScramCredential`] from a cleartext password and a fixed salt.
///
/// Implements the standard SCRAM-SHA-256 derivation:
/// - `SaltedPassword = PBKDF2-HMAC-SHA256(password, salt, iterations, dkLen=32)`
/// - `ClientKey = HMAC-SHA256(SaltedPassword, "Client Key")`
/// - `StoredKey = SHA256(ClientKey)`
/// - `ServerKey = HMAC-SHA256(SaltedPassword, "Server Key")`
pub fn derive(password: &str, salt: &[u8], iterations: u32) -> ScramCredential {
    debug_assert!(iterations > 0, "SCRAM iteration count must be positive");
    let mut salted = [0u8; 32];
    pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac256(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let server_key = hmac256(&salted, b"Server Key");
    ScramCredential {
        salt: salt.to_vec(),
        iterations,
        stored_key,
        server_key,
    }
}

/// Derive a [`ScramCredential`] using a freshly-generated 16-byte random salt
/// and the default iteration count (4096). Called on the plaintext-password
/// path of `CREATE ROLE` / `ALTER ROLE` at password-set time.
pub fn derive_with_random_salt(password: &str) -> ScramCredential {
    let mut salt = [0u8; SALT_LEN];
    rand::thread_rng().fill_bytes(&mut salt);
    derive(password, &salt, DEFAULT_ITERATIONS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    // RFC 7677 §3 worked example (mechanism SCRAM-SHA-256).
    const PASSWORD: &str = "pencil";
    const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
    const ITERATIONS: u32 = 4096;

    fn rfc_salt() -> Vec<u8> {
        STANDARD.decode(SALT_B64).unwrap()
    }

    #[test]
    fn derive_keys_are_32_bytes_and_round_trip_salt() {
        let c = derive(PASSWORD, &rfc_salt(), ITERATIONS);
        assert_eq!(c.iterations, ITERATIONS);
        assert_eq!(STANDARD.encode(&c.salt), SALT_B64);
        assert_eq!(c.stored_key.len(), 32);
        assert_eq!(c.server_key.len(), 32);
    }

    #[test]
    fn derive_is_deterministic_for_fixed_inputs() {
        let a = derive(PASSWORD, &rfc_salt(), ITERATIONS);
        let b = derive(PASSWORD, &rfc_salt(), ITERATIONS);
        assert_eq!(a, b, "same password/salt/iters must derive identically");
    }

    #[test]
    fn derive_differs_for_differing_passwords() {
        let a = derive(PASSWORD, &rfc_salt(), ITERATIONS);
        let b = derive("not-pencil", &rfc_salt(), ITERATIONS);
        assert_ne!(
            a, b,
            "different passwords must derive different stored/server keys"
        );
        assert_ne!(a.stored_key, b.stored_key);
        assert_ne!(a.server_key, b.server_key);
    }

    #[test]
    fn server_signature_relationship_is_self_consistent() {
        // Reconstruct the server-signature/stored-key relationship from a known
        // SaltedPassword: derive() must produce keys that satisfy the SCRAM
        // identities StoredKey == SHA256(ClientKey) and
        // ServerKey == HMAC(SaltedPassword, "Server Key").
        let salt = rfc_salt();
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(PASSWORD.as_bytes(), &salt, ITERATIONS, &mut salted);
        let client_key = hmac256(&salted, b"Client Key");
        let expected_stored: [u8; 32] = Sha256::digest(client_key).into();
        let expected_server = hmac256(&salted, b"Server Key");

        let c = derive(PASSWORD, &salt, ITERATIONS);
        assert_eq!(c.stored_key, expected_stored);
        assert_eq!(c.server_key, expected_server);
    }

    #[test]
    fn random_salt_derivation_uses_defaults_and_varies() {
        let a = derive_with_random_salt(PASSWORD);
        let b = derive_with_random_salt(PASSWORD);
        assert_eq!(a.iterations, ITERATIONS);
        assert_eq!(a.salt.len(), SALT_LEN);
        // Two random salts collide with negligible probability.
        assert_ne!(a.salt, b.salt, "random salts must differ");
        assert_ne!(
            a.stored_key, b.stored_key,
            "distinct salts must yield distinct stored keys"
        );
    }

    #[test]
    fn credential_serde_round_trips() {
        let c = derive(PASSWORD, &rfc_salt(), ITERATIONS);
        let json = serde_json::to_string(&c).unwrap();
        let back: ScramCredential = serde_json::from_str(&json).unwrap();
        assert_eq!(c, back);
    }
}
