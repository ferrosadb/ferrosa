//! Password hashing and policy types.

use argon2::Argon2;
use password_hash::rand_core::OsRng;
use password_hash::SaltString;
use serde::{Deserialize, Serialize};

use crate::error::SchemaError;

/// Configures the password hashing algorithm.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PasswordHasher {
    /// Bcrypt with configurable cost factor.
    Bcrypt { cost: u32 },
    /// Argon2id with configurable memory, iterations, and parallelism.
    Argon2id {
        memory_kib: u32,
        iterations: u32,
        parallelism: u32,
    },
}

impl Default for PasswordHasher {
    fn default() -> Self {
        PasswordHasher::Bcrypt { cost: 12 }
    }
}

impl PasswordHasher {
    /// Hash a password using the configured algorithm.
    pub fn hash_password(&self, password: &str) -> crate::Result<String> {
        match self {
            PasswordHasher::Bcrypt { cost } => bcrypt::hash(password, *cost)
                .map_err(|e| SchemaError::InvalidSchema(format!("bcrypt hash failed: {e}"))),
            PasswordHasher::Argon2id {
                memory_kib,
                iterations,
                parallelism,
            } => {
                use password_hash::PasswordHasher as _;

                let params = argon2::Params::new(*memory_kib, *iterations, *parallelism, None)
                    .map_err(|e| {
                        SchemaError::InvalidSchema(format!("argon2 params invalid: {e}"))
                    })?;
                let argon2 =
                    Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
                let salt = SaltString::generate(&mut OsRng);
                let hash = argon2
                    .hash_password(password.as_bytes(), &salt)
                    .map_err(|e| SchemaError::InvalidSchema(format!("argon2 hash failed: {e}")))?;
                Ok(hash.to_string())
            }
        }
    }

    /// Verify a password against any hash (auto-detect from prefix).
    /// `$2b$` -> bcrypt, `$argon2id$` -> argon2id.
    pub fn verify_password_any(password: &str, hash: &str) -> crate::Result<bool> {
        if hash.starts_with("$2b$") || hash.starts_with("$2a$") || hash.starts_with("$2y$") {
            // Bcrypt hash
            bcrypt::verify(password, hash)
                .map_err(|e| SchemaError::InvalidSchema(format!("bcrypt verify failed: {e}")))
        } else if hash.starts_with("$argon2id$") {
            // Argon2id hash
            use password_hash::PasswordVerifier;

            let parsed = password_hash::PasswordHash::new(hash).map_err(|e| {
                SchemaError::InvalidSchema(format!("argon2 hash parse failed: {e}"))
            })?;
            // Use default Argon2 — params are embedded in the hash
            let argon2 = Argon2::default();
            match argon2.verify_password(password.as_bytes(), &parsed) {
                Ok(()) => Ok(true),
                Err(password_hash::Error::Password) => Ok(false),
                Err(e) => Err(SchemaError::InvalidSchema(format!(
                    "argon2 verify failed: {e}"
                ))),
            }
        } else {
            Err(SchemaError::InvalidSchema(format!(
                "unrecognized hash prefix in: {}...",
                &hash[..hash.len().min(10)]
            )))
        }
    }

    /// Returns true if the hash algorithm differs from the configured one.
    pub fn needs_rehash(&self, hash: &str) -> bool {
        match self {
            PasswordHasher::Bcrypt { .. } => {
                !hash.starts_with("$2b$") && !hash.starts_with("$2a$") && !hash.starts_with("$2y$")
            }
            PasswordHasher::Argon2id { .. } => !hash.starts_with("$argon2id$"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bcrypt_hash_and_verify() {
        let hasher = PasswordHasher::Bcrypt { cost: 4 }; // low cost for fast tests
        let hash = hasher.hash_password("hunter2").unwrap();
        assert!(hash.starts_with("$2b$"));
        assert!(PasswordHasher::verify_password_any("hunter2", &hash).unwrap());
    }

    #[test]
    fn bcrypt_wrong_password_fails() {
        let hasher = PasswordHasher::Bcrypt { cost: 4 };
        let hash = hasher.hash_password("correct").unwrap();
        assert!(!PasswordHasher::verify_password_any("wrong", &hash).unwrap());
    }

    #[test]
    fn argon2id_hash_and_verify() {
        let hasher = PasswordHasher::Argon2id {
            memory_kib: 256,
            iterations: 1,
            parallelism: 1,
        };
        let hash = hasher.hash_password("s3cret!").unwrap();
        assert!(hash.starts_with("$argon2id$"));
        assert!(PasswordHasher::verify_password_any("s3cret!", &hash).unwrap());
    }

    #[test]
    fn auto_detect_algorithm_from_prefix() {
        // Bcrypt hash verified without knowing config
        let bcrypt_hasher = PasswordHasher::Bcrypt { cost: 4 };
        let bcrypt_hash = bcrypt_hasher.hash_password("password1").unwrap();
        assert!(PasswordHasher::verify_password_any("password1", &bcrypt_hash).unwrap());

        // Argon2id hash verified without knowing config
        let argon2_hasher = PasswordHasher::Argon2id {
            memory_kib: 256,
            iterations: 1,
            parallelism: 1,
        };
        let argon2_hash = argon2_hasher.hash_password("password2").unwrap();
        assert!(PasswordHasher::verify_password_any("password2", &argon2_hash).unwrap());
    }

    #[test]
    fn needs_upgrade_detects_algorithm_mismatch() {
        let bcrypt_hasher = PasswordHasher::Bcrypt { cost: 4 };
        let argon2_hasher = PasswordHasher::Argon2id {
            memory_kib: 256,
            iterations: 1,
            parallelism: 1,
        };

        let bcrypt_hash = bcrypt_hasher.hash_password("pw").unwrap();
        let argon2_hash = argon2_hasher.hash_password("pw").unwrap();

        // Bcrypt hasher sees bcrypt hash — no rehash needed
        assert!(!bcrypt_hasher.needs_rehash(&bcrypt_hash));
        // Bcrypt hasher sees argon2 hash — rehash needed
        assert!(bcrypt_hasher.needs_rehash(&argon2_hash));
        // Argon2 hasher sees argon2 hash — no rehash needed
        assert!(!argon2_hasher.needs_rehash(&argon2_hash));
        // Argon2 hasher sees bcrypt hash — rehash needed
        assert!(argon2_hasher.needs_rehash(&bcrypt_hash));
    }

    #[test]
    fn default_hasher_is_bcrypt_12() {
        let hasher = PasswordHasher::default();
        match hasher {
            PasswordHasher::Bcrypt { cost } => assert_eq!(cost, 12),
            _ => panic!("default hasher should be Bcrypt"),
        }
    }
}
