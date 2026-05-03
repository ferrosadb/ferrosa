//! Password hashing and policy types.

use argon2::Argon2;
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
                use argon2::password_hash::PasswordHasher as _;

                let params = argon2::Params::new(*memory_kib, *iterations, *parallelism, None)
                    .map_err(|e| {
                        SchemaError::InvalidSchema(format!("argon2 params invalid: {e}"))
                    })?;
                let argon2 =
                    Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
                // In argon2 0.6 + password-hash 0.6, hash_password generates its own
                // random salt internally and returns a PHC string directly.
                let hash = argon2
                    .hash_password(password.as_bytes())
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
            use argon2::password_hash::phc::PasswordHash;
            use argon2::password_hash::PasswordVerifier;

            let parsed = PasswordHash::new(hash).map_err(|e| {
                SchemaError::InvalidSchema(format!("argon2 hash parse failed: {e}"))
            })?;
            // Use default Argon2 — params are embedded in the hash
            let argon2 = Argon2::default();
            match argon2.verify_password(password.as_bytes(), &parsed) {
                Ok(()) => Ok(true),
                Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
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

/// Password strength policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PasswordPolicy {
    /// Minimum password length.
    pub min_length: usize,
    /// Require at least one uppercase letter.
    pub require_uppercase: bool,
    /// Require at least one lowercase letter.
    pub require_lowercase: bool,
    /// Require at least one digit.
    pub require_digit: bool,
    /// Require at least one special character.
    pub require_special: bool,
    /// Reject passwords that match the username.
    pub reject_username_as_password: bool,
}

impl PasswordPolicy {
    /// A permissive policy that accepts almost anything.
    pub fn permissive() -> Self {
        Self {
            min_length: 1,
            require_uppercase: false,
            require_lowercase: false,
            require_digit: false,
            require_special: false,
            reject_username_as_password: false,
        }
    }

    /// A strict policy aligned with ISO 27001 recommendations.
    pub fn iso27001() -> Self {
        Self {
            min_length: 12,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: true,
            reject_username_as_password: true,
        }
    }

    /// Validate a password against this policy.
    /// Returns `PasswordTooWeak` with violation descriptions on failure.
    pub fn validate(&self, password: &str, username: &str) -> crate::Result<()> {
        let mut violations = Vec::new();

        if password.len() < self.min_length {
            violations.push(format!("must be at least {} characters", self.min_length));
        }
        if self.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            violations.push("must contain at least one uppercase letter".to_string());
        }
        if self.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            violations.push("must contain at least one lowercase letter".to_string());
        }
        if self.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            violations.push("must contain at least one digit".to_string());
        }
        if self.require_special
            && !password
                .chars()
                .any(|c| !c.is_alphanumeric() && !c.is_whitespace())
        {
            violations.push("must contain at least one special character".to_string());
        }
        if self.reject_username_as_password && password == username {
            violations.push("password must not match username".to_string());
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(SchemaError::PasswordTooWeak { violations })
        }
    }

    /// Returns true if this policy is at least as strict as `other`.
    pub fn is_at_least_as_strong_as(&self, other: &PasswordPolicy) -> bool {
        self.min_length >= other.min_length
            && (self.require_uppercase || !other.require_uppercase)
            && (self.require_lowercase || !other.require_lowercase)
            && (self.require_digit || !other.require_digit)
            && (self.require_special || !other.require_special)
            && (self.reject_username_as_password || !other.reject_username_as_password)
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

    // --- PasswordPolicy tests ---

    #[test]
    fn permissive_policy_accepts_anything() {
        let policy = PasswordPolicy::permissive();
        assert!(policy.validate("a", "admin").is_ok());
        assert!(policy.validate("123", "admin").is_ok());
        assert!(policy.validate("admin", "admin").is_ok()); // even username as password
    }

    #[test]
    fn iso27001_rejects_short_password() {
        let policy = PasswordPolicy::iso27001();
        let err = policy.validate("Short1!", "user").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("12"), "should mention 12 characters: {msg}");
    }

    #[test]
    fn iso27001_rejects_missing_uppercase() {
        let policy = PasswordPolicy::iso27001();
        let err = policy.validate("alllowercase1!", "user").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("uppercase"), "should mention uppercase: {msg}");
    }

    #[test]
    fn iso27001_rejects_password_matching_username() {
        let policy = PasswordPolicy::iso27001();
        // Password matches username (case-insensitive would be a bonus but not required)
        let err = policy.validate("admin", "admin").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("username"), "should mention username: {msg}");
    }

    #[test]
    fn iso27001_accepts_strong_password() {
        let policy = PasswordPolicy::iso27001();
        assert!(policy.validate("MyStr0ng!Pass", "user").is_ok());
    }

    #[test]
    fn is_at_least_as_strong_as_self() {
        let iso = PasswordPolicy::iso27001();
        assert!(iso.is_at_least_as_strong_as(&iso));

        let permissive = PasswordPolicy::permissive();
        assert!(permissive.is_at_least_as_strong_as(&permissive));
    }

    #[test]
    fn permissive_not_as_strong_as_iso27001() {
        let permissive = PasswordPolicy::permissive();
        let iso = PasswordPolicy::iso27001();
        assert!(!permissive.is_at_least_as_strong_as(&iso));
    }
}
