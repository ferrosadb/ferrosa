//! SCRAM-SHA-256 server-side exchange (RFC 5802 / RFC 7677) for Postgres auth.
//!
//! Per decision **D4**, ferrosa stores a SCRAM verifier alongside the existing
//! bcrypt hash so Postgres clients can authenticate with their driver-default
//! mechanism. This module implements the *server* half of the exchange:
//!
//! 1. client → `client-first`: `n,,n=user,r=<client-nonce>`
//! 2. server → `server-first`: `r=<client-nonce><server-nonce>,s=<salt>,i=<iters>`
//! 3. client → `client-final`: `c=<gs2>,r=<combined-nonce>,p=<client-proof>`
//! 4. server → `server-final`: `v=<server-signature>` (after verifying the proof)
//!
//! The server never sees the password; it stores `{salt, iterations, StoredKey,
//! ServerKey}` and recovers/validates `ClientKey` from the client proof.
//!
//! Channel binding (`SCRAM-SHA-256-PLUS`) is out of scope for v1 (blueprint Q4)
//! and a `p=`-flagged client-first is rejected fail-loud.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// HMAC-SHA-256(key, msg) → 32-byte tag.
fn hmac256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

/// Split a `client-first` into its gs2 header and the `client-first-bare`.
/// The gs2 header is `gs2-cbind-flag "," [authzid] ","` — i.e. everything up
/// to and including the second comma.
fn split_gs2(client_first: &str) -> Result<(&str, &str), ScramError> {
    let mut commas = client_first.match_indices(',').map(|(i, _)| i);
    commas
        .next()
        .ok_or(ScramError::Malformed("missing gs2 header"))?;
    let second = commas
        .next()
        .ok_or(ScramError::Malformed("missing gs2 header"))?;
    Ok((&client_first[..=second], &client_first[second + 1..]))
}

/// Find the value of a comma-separated `key=value` attribute.
fn field<'a>(msg: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    msg.split(',').find_map(|f| f.strip_prefix(prefix.as_str()))
}

/// A stored SCRAM verifier — what `ferrosa-schema` persists per role (D4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramVerifier {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl From<&ferrosa_schema::ScramCredential> for ScramVerifier {
    /// Adapt a schema-stored SCRAM credential (derived at password-set time in
    /// `ferrosa-schema`, per D4) into the postgres wire verifier. The fields are
    /// identical, so this is a field-for-field copy — the server half of the
    /// exchange (`server_first`/`verify_client_final`) then operates on it.
    fn from(c: &ferrosa_schema::ScramCredential) -> Self {
        ScramVerifier {
            salt: c.salt.clone(),
            iterations: c.iterations,
            stored_key: c.stored_key,
            server_key: c.server_key,
        }
    }
}

impl ScramVerifier {
    /// Derive a verifier from a cleartext password (called at password-set time,
    /// on every CQL/Postgres password path per D4).
    pub fn from_password(password: &str, salt: &[u8], iterations: u32) -> ScramVerifier {
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
        let client_key = hmac256(&salted, b"Client Key");
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let server_key = hmac256(&salted, b"Server Key");
        ScramVerifier {
            salt: salt.to_vec(),
            iterations,
            stored_key,
            server_key,
        }
    }
}

/// Carried context between the server-first and server-final steps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramServerFirst {
    /// `client-first-bare` (the client-first without the gs2 header) — part of
    /// the AuthMessage.
    pub client_first_bare: String,
    /// The `server-first` message to send to the client.
    pub server_first: String,
    /// `<client-nonce><server-nonce>` — must be echoed by the client.
    pub combined_nonce: String,
}

/// A SCRAM protocol failure (fail loud — never silently accept).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramError {
    /// Structurally malformed message (static reason).
    Malformed(&'static str),
    /// The client proof did not validate against the stored key.
    ProofMismatch,
    /// Client requested channel binding (`p=`), unsupported in v1.
    UnsupportedChannelBinding,
    /// A base64 field failed to decode.
    InvalidBase64,
}

/// Build the `server-first` message from the client's `client-first` and this
/// server's nonce contribution.
pub fn server_first(
    client_first: &str,
    server_nonce: &str,
    verifier: &ScramVerifier,
) -> Result<ScramServerFirst, ScramError> {
    let (gs2, bare) = split_gs2(client_first)?;
    if gs2.starts_with('p') {
        return Err(ScramError::UnsupportedChannelBinding);
    }
    let client_nonce = field(bare, "r").ok_or(ScramError::Malformed("missing client nonce"))?;
    let combined_nonce = format!("{client_nonce}{server_nonce}");
    let server_first = format!(
        "r={combined_nonce},s={},i={}",
        STANDARD.encode(&verifier.salt),
        verifier.iterations
    );
    Ok(ScramServerFirst {
        client_first_bare: bare.to_string(),
        server_first,
        combined_nonce,
    })
}

/// Verify the client's `client-final` proof; on success return the
/// `server-final` (`v=...`) message to send back.
pub fn verify_client_final(
    ctx: &ScramServerFirst,
    client_final: &str,
    verifier: &ScramVerifier,
) -> Result<String, ScramError> {
    let combined = field(client_final, "r").ok_or(ScramError::Malformed("missing nonce"))?;
    if combined != ctx.combined_nonce {
        return Err(ScramError::Malformed("nonce mismatch"));
    }
    let proof_b64 = field(client_final, "p").ok_or(ScramError::Malformed("missing proof"))?;
    let proof = STANDARD
        .decode(proof_b64)
        .map_err(|_| ScramError::InvalidBase64)?;
    if proof.len() != 32 {
        return Err(ScramError::Malformed("client proof must be 32 bytes"));
    }

    // client-final-without-proof is everything before the ",p=" attribute.
    let without_proof = client_final
        .split(",p=")
        .next()
        .ok_or(ScramError::Malformed("missing proof delimiter"))?;
    let auth_message = format!(
        "{},{},{}",
        ctx.client_first_bare, ctx.server_first, without_proof
    );

    // Recover ClientKey = ClientProof XOR ClientSignature and check its hash.
    let client_signature = hmac256(&verifier.stored_key, auth_message.as_bytes());
    let mut client_key = [0u8; 32];
    for i in 0..32 {
        client_key[i] = proof[i] ^ client_signature[i];
    }
    let recomputed_stored: [u8; 32] = Sha256::digest(client_key).into();
    if recomputed_stored != verifier.stored_key {
        return Err(ScramError::ProofMismatch);
    }

    let server_signature = hmac256(&verifier.server_key, auth_message.as_bytes());
    Ok(format!("v={}", STANDARD.encode(server_signature)))
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 7677 §3 worked example (mechanism SCRAM-SHA-256).
    const PASSWORD: &str = "pencil";
    const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
    const ITERATIONS: u32 = 4096;
    const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    fn rfc_verifier() -> ScramVerifier {
        let salt = STANDARD.decode(SALT_B64).unwrap();
        ScramVerifier::from_password(PASSWORD, &salt, ITERATIONS)
    }

    #[test]
    fn verifier_keys_are_32_bytes_and_round_trip_salt() {
        let v = rfc_verifier();
        assert_eq!(v.iterations, ITERATIONS);
        assert_eq!(STANDARD.encode(&v.salt), SALT_B64);
        assert_eq!(v.stored_key.len(), 32);
        assert_eq!(v.server_key.len(), 32);
    }

    #[test]
    fn server_first_matches_rfc_vector() {
        let v = rfc_verifier();
        let sf = server_first(CLIENT_FIRST, SERVER_NONCE, &v).unwrap();
        assert_eq!(sf.server_first, SERVER_FIRST);
        assert_eq!(sf.client_first_bare, "n=user,r=rOprNGfwEbeRWgbNEkqO");
        assert_eq!(
            sf.combined_nonce,
            "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
        );
    }

    #[test]
    fn client_final_verifies_and_server_final_matches_rfc_vector() {
        let v = rfc_verifier();
        let sf = server_first(CLIENT_FIRST, SERVER_NONCE, &v).unwrap();
        let server_final = verify_client_final(&sf, CLIENT_FINAL, &v).unwrap();
        assert_eq!(server_final, SERVER_FINAL);
    }

    #[test]
    fn tampered_proof_is_rejected() {
        let v = rfc_verifier();
        let sf = server_first(CLIENT_FIRST, SERVER_NONCE, &v).unwrap();
        // flip one base64 char in the proof
        let bad = CLIENT_FINAL.replace("dHzb", "DHzb");
        assert_eq!(
            verify_client_final(&sf, &bad, &v),
            Err(ScramError::ProofMismatch)
        );
    }

    #[test]
    fn nonce_mismatch_is_rejected() {
        let v = rfc_verifier();
        let sf = server_first(CLIENT_FIRST, SERVER_NONCE, &v).unwrap();
        let wrong_nonce = CLIENT_FINAL.replace("hNlF$k0", "hNlF$kX");
        assert!(matches!(
            verify_client_final(&sf, &wrong_nonce, &v),
            Err(ScramError::Malformed(_))
        ));
    }

    #[test]
    fn channel_binding_request_is_rejected() {
        let v = rfc_verifier();
        // gs2 flag 'p=tls-server-end-point' signals channel binding (unsupported v1)
        let cb_first = "p=tls-server-end-point,,n=user,r=rOprNGfwEbeRWgbNEkqO";
        assert_eq!(
            server_first(cb_first, SERVER_NONCE, &v),
            Err(ScramError::UnsupportedChannelBinding)
        );
    }

    #[test]
    fn wrong_password_fails_proof() {
        // A verifier for a different password must reject the RFC proof.
        let salt = STANDARD.decode(SALT_B64).unwrap();
        let v = ScramVerifier::from_password("not-pencil", &salt, ITERATIONS);
        let sf = server_first(CLIENT_FIRST, SERVER_NONCE, &v).unwrap();
        assert_eq!(
            verify_client_final(&sf, CLIENT_FINAL, &v),
            Err(ScramError::ProofMismatch)
        );
    }
}
