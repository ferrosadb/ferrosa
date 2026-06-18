//! Schema-backed [`VerifierStore`] adapter.
//!
//! Bridges the pure SCRAM handshake (`handshake::VerifierStore`) to the real
//! `ferrosa-schema` role registry: the Postgres server resolves a login user to
//! its stored SCRAM-SHA-256 credential (derived at password-set time, D4) and
//! authenticates against it. Roles without a verifier (e.g. `HASHED PASSWORD`
//! roles that have not had a plaintext reset) return `None`, which the handshake
//! surfaces as `UnknownRole` — fail loud, never authenticate on doubt.

use std::sync::Arc;

use ferrosa_schema::Schema;

use crate::handshake::VerifierStore;
use crate::scram::ScramVerifier;

/// A [`VerifierStore`] backed by the live `ferrosa-schema` registry.
pub struct SchemaVerifierStore {
    schema: Arc<Schema>,
}

impl SchemaVerifierStore {
    /// Wrap a shared schema registry as a verifier store.
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

impl VerifierStore for SchemaVerifierStore {
    fn verifier(&self, user: &str) -> Option<ScramVerifier> {
        self.schema.scram_credential(user).map(|c| (&c).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scram;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use ferrosa_schema::auth::role::RoleMetadata;
    use ferrosa_schema::{
        AuthContext, AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher,
        PasswordPolicy, RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    fn hmac256(key: &[u8], msg: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(msg);
        mac.finalize().into_bytes().into()
    }

    fn superuser() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    /// Build a real schema, create a login role with a known password through
    /// the public role API (which derives the SCRAM verifier, D4), and return a
    /// store over it.
    fn test_config() -> SchemaConfig {
        SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        }
    }

    fn store_with_role(user: &str, password: &str) -> SchemaVerifierStore {
        let schema = Schema::new(test_config()).expect("schema bootstraps");
        let role = RoleMetadata {
            name: user.to_string(),
            is_superuser: false,
            can_login: true,
            salted_hash: None,
            member_of: Default::default(),
            scram: None,
        };
        schema
            .create_role(role, Some(password), &superuser())
            .expect("create_role with password");
        SchemaVerifierStore::new(Arc::new(schema))
    }

    #[test]
    fn known_role_resolves_to_a_verifier() {
        let s = store_with_role("alice", "s3cretPwd!42");
        assert!(s.verifier("alice").is_some());
    }

    #[test]
    fn unknown_role_resolves_to_none() {
        let s = store_with_role("alice", "s3cretPwd!42");
        assert!(s.verifier("bob").is_none());
    }

    #[test]
    fn adapter_verifier_matches_from_password_on_stored_salt() {
        // The adapter's verifier must equal one derived directly from the same
        // password using the salt/iterations the schema stored — proving the
        // From<&ScramCredential> conversion is lossless and uses the persisted
        // salt rather than a fresh one.
        let password = "s3cretPwd!42";
        let s = store_with_role("alice", password);
        let v = s.verifier("alice").expect("verifier present");
        let direct = ScramVerifier::from_password(password, &v.salt, v.iterations);
        assert_eq!(v, direct);
    }

    #[test]
    fn schema_derived_verifier_completes_the_postgres_exchange() {
        // End-to-end proof: a verifier derived in ferrosa-schema interoperates
        // with ferrosa-postgres's own server-side SCRAM exchange. We compute the
        // client side here using the postgres scram primitives plus the standard
        // ClientProof = ClientKey XOR HMAC(StoredKey, AuthMessage) identity.
        let password = "corre1t-h0rse!";
        let s = store_with_role("dbuser", password);
        let verifier = s.verifier("dbuser").expect("verifier present");

        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let server_nonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let client_first = format!("n,,n=dbuser,r={client_nonce}");

        // Server: build server-first from the stored verifier.
        let sf = scram::server_first(&client_first, server_nonce, &verifier)
            .expect("server_first succeeds");

        // Client: recompute SaltedPassword/ClientKey from the password and the
        // server-advertised salt/iterations, then form the client proof.
        let mut salted = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            password.as_bytes(),
            &verifier.salt,
            verifier.iterations,
            &mut salted,
        );
        let client_key = hmac256(&salted, b"Client Key");
        let stored_key: [u8; 32] = <Sha256 as sha2::Digest>::digest(client_key).into();

        let client_first_bare = format!("n=dbuser,r={client_nonce}");
        let channel_binding = STANDARD.encode("n,,"); // gs2 header for "no binding"
        let client_final_without_proof = format!("c={channel_binding},r={}", sf.combined_nonce);
        let auth_message = format!(
            "{client_first_bare},{},{client_final_without_proof}",
            sf.server_first
        );

        let client_signature = hmac256(&stored_key, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!("{client_final_without_proof},p={}", STANDARD.encode(proof));

        // Server: verify the client proof against the schema-derived verifier.
        let server_final = scram::verify_client_final(&sf, &client_final, &verifier)
            .expect("schema-derived verifier must accept a correct client proof");
        assert!(server_final.starts_with("v="));
    }
}
