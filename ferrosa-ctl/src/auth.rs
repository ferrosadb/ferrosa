//! `ferrosa-ctl auth set-password` — seed initial admin credentials.
//!
//! Used by the post-install prompt to write the CQL admin and graph
//! HTTP/Bolt basic-auth password hashes into
//! `~/.ferrosa/config/auth.yaml`.
//!
//! # File layout
//!
//! ```yaml
//! cql:
//!   admin:
//!     hash: "$argon2id$v=19$m=19456,t=2,p=1$..."
//!     created_at: "2026-05-08T20:30:00Z"
//! graph:
//!   admin:
//!     hash: "..."
//!     created_at: "..."
//! ```
//!
//! # Hashing scheme
//!
//! Argon2id with the OWASP-recommended parameters
//! (m = 19 456 KiB, t = 2, p = 1). The PHC string is self-describing,
//! and `ferrosa-schema::auth::password::PasswordHasher::verify_password_any`
//! already auto-detects an `$argon2id$` prefix, so a hash produced here
//! is verifiable by the server's existing authenticator path.
//!
//! Note: the running server does NOT currently read `auth.yaml` — its
//! authoritative store is `system_auth.roles.salted_hash`. Wiring the
//! installer's first-touch hash into the server is tracked as a
//! follow-up (see PR description).

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use argon2::password_hash::PasswordHasher as _;
use argon2::Argon2;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Argon2id memory cost in KiB. OWASP 2024 recommendation.
const ARGON2_MEMORY_KIB: u32 = 19_456;
/// Argon2id iterations. OWASP 2024 recommendation.
const ARGON2_ITERATIONS: u32 = 2;
/// Argon2id parallelism. OWASP 2024 recommendation.
const ARGON2_PARALLELISM: u32 = 1;

/// Realm a credential applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Realm {
    /// CQL native protocol authenticator (port 9042).
    Cql,
    /// Graph HTTP/Bolt basic-auth credential (ports 7474 / 7687).
    Graph,
}

impl Realm {
    fn as_yaml_key(self) -> &'static str {
        match self {
            Realm::Cql => "cql",
            Realm::Graph => "graph",
        }
    }
}

impl std::str::FromStr for Realm {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cql" => Ok(Realm::Cql),
            "graph" => Ok(Realm::Graph),
            other => Err(format!(
                "unknown realm '{other}' — expected 'cql' or 'graph'"
            )),
        }
    }
}

/// One credential entry in the YAML file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialEntry {
    /// PHC-format password hash (e.g. `$argon2id$v=19$m=...$...$...`).
    pub hash: String,
    /// RFC 3339 timestamp recording when this hash was written.
    pub created_at: String,
}

/// On-disk representation of `auth.yaml`. Realms are top-level keys,
/// each mapping `username -> CredentialEntry`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AuthFile {
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub cql: BTreeMap<String, CredentialEntry>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub graph: BTreeMap<String, CredentialEntry>,
}

impl AuthFile {
    fn realm_mut(&mut self, realm: Realm) -> &mut BTreeMap<String, CredentialEntry> {
        match realm {
            Realm::Cql => &mut self.cql,
            Realm::Graph => &mut self.graph,
        }
    }

    fn realm(&self, realm: Realm) -> &BTreeMap<String, CredentialEntry> {
        match realm {
            Realm::Cql => &self.cql,
            Realm::Graph => &self.graph,
        }
    }
}

/// Errors produced by the `auth` subcommand.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("password mismatch — please try again")]
    Mismatch,
    #[error("operation aborted by user")]
    Aborted,
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("yaml error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("hash error: {0}")]
    Hash(String),
    #[error("home directory could not be resolved")]
    NoHome,
}

/// Default config path under `$HOME/.ferrosa/config/auth.yaml`.
pub fn default_config_path() -> Result<PathBuf, AuthError> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or(AuthError::NoHome)?;
    Ok(home.join(".ferrosa").join("config").join("auth.yaml"))
}

/// Hash a plaintext password with Argon2id and OWASP parameters.
/// Returns a PHC-format string. `password-hash` 0.6 generates the salt
/// internally via the OS RNG.
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let params = argon2::Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_ITERATIONS,
        ARGON2_PARALLELISM,
        None,
    )
    .map_err(|e| AuthError::Hash(format!("invalid argon2 params: {e}")))?;
    let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let hash = argon2
        .hash_password(password.as_bytes())
        .map_err(|e| AuthError::Hash(format!("argon2 hash failed: {e}")))?;
    Ok(hash.to_string())
}

/// Verify a plaintext password against a PHC-format Argon2id hash.
/// Used by tests; mirrors the server-side `verify_password_any` path.
#[cfg(test)]
pub fn verify_password(password: &str, phc_hash: &str) -> Result<bool, AuthError> {
    use argon2::password_hash::phc::PasswordHash;
    use argon2::password_hash::PasswordVerifier;

    let parsed = PasswordHash::new(phc_hash)
        .map_err(|e| AuthError::Hash(format!("phc parse failed: {e}")))?;
    let argon2 = Argon2::default();
    match argon2.verify_password(password.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
        Err(e) => Err(AuthError::Hash(format!("verify failed: {e}"))),
    }
}

/// Read an existing `auth.yaml` if present, otherwise return a default
/// (empty) struct. Bubbles up parse errors so we never silently clobber
/// a broken file.
pub fn load_or_default(path: &Path) -> Result<AuthFile, AuthError> {
    match fs::read_to_string(path) {
        Ok(text) => {
            let file: AuthFile = serde_yaml::from_str(&text)?;
            Ok(file)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(AuthFile::default()),
        Err(e) => Err(AuthError::Io(e)),
    }
}

/// Atomically write `auth.yaml` to `path`, creating parent directories
/// and setting mode 0600 on Unix.
pub fn save(path: &Path, file: &AuthFile) -> Result<(), AuthError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let yaml = serde_yaml::to_string(file)?;
    let tmp = path.with_extension("yaml.tmp");
    {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)?;
        f.write_all(yaml.as_bytes())?;
        f.sync_all()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

/// Insert or replace a credential, producing the merged `AuthFile`
/// without touching disk. Pure function — easy to test.
pub fn merge_credential(
    mut file: AuthFile,
    realm: Realm,
    user: &str,
    hash: String,
    created_at: DateTime<Utc>,
) -> AuthFile {
    file.realm_mut(realm).insert(
        user.to_string(),
        CredentialEntry {
            hash,
            created_at: created_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        },
    );
    file
}

/// Returns true if the file already has an entry for (realm, user).
pub fn has_credential(file: &AuthFile, realm: Realm, user: &str) -> bool {
    file.realm(realm).contains_key(user)
}

// ── I/O abstraction so tests can drive the prompt without a TTY ─────────────

/// Trait for reading one secret line from a user. Test impls return a
/// pre-canned password; the production impl uses `rpassword::prompt_password`.
pub trait PasswordSource {
    fn read_password(&mut self, prompt: &str) -> io::Result<String>;
    fn read_confirm_line(&mut self, prompt: &str) -> io::Result<String>;
    fn write_stderr(&mut self, msg: &str) -> io::Result<()>;
}

/// Production implementation: rpassword for hidden input, stdin/stderr.
pub struct StdioPasswordSource;

impl PasswordSource for StdioPasswordSource {
    fn read_password(&mut self, prompt: &str) -> io::Result<String> {
        rpassword::prompt_password(prompt)
    }
    fn read_confirm_line(&mut self, prompt: &str) -> io::Result<String> {
        eprint!("{prompt}");
        io::stderr().flush()?;
        let mut line = String::new();
        io::stdin().lock().read_line(&mut line)?;
        Ok(line.trim().to_string())
    }
    fn write_stderr(&mut self, msg: &str) -> io::Result<()> {
        let mut err = io::stderr();
        err.write_all(msg.as_bytes())?;
        err.flush()
    }
}

/// Top-level options for `set-password`.
#[derive(Debug, Clone)]
pub struct SetPasswordOpts {
    pub realm: Realm,
    pub user: String,
    pub config_path: PathBuf,
    pub force: bool,
}

/// Run `auth set-password`. Returns Ok on success; AuthError otherwise.
/// The caller (`main`) is responsible for mapping the error to an exit
/// code and a stderr message.
pub fn run_set_password<P: PasswordSource>(
    opts: &SetPasswordOpts,
    source: &mut P,
    now: DateTime<Utc>,
) -> Result<(), AuthError> {
    let mut file = load_or_default(&opts.config_path)?;

    if has_credential(&file, opts.realm, &opts.user) && !opts.force {
        let prompt = format!(
            "credential already exists for {}/{}; overwrite? [y/N]: ",
            opts.realm.as_yaml_key(),
            opts.user
        );
        let answer = source.read_confirm_line(&prompt)?;
        if !matches!(answer.to_ascii_lowercase().as_str(), "y" | "yes") {
            return Err(AuthError::Aborted);
        }
    }

    let pw1 = source.read_password(&format!(
        "Enter password for {}/{}: ",
        opts.realm.as_yaml_key(),
        opts.user
    ))?;
    let pw2 = source.read_password("Confirm password: ")?;
    if pw1 != pw2 {
        return Err(AuthError::Mismatch);
    }
    if pw1.is_empty() {
        return Err(AuthError::Hash("password must not be empty".into()));
    }

    let hash = hash_password(&pw1)?;
    file = merge_credential(file, opts.realm, &opts.user, hash, now);
    save(&opts.config_path, &file)?;
    source.write_stderr(&format!(
        "wrote credential for {}/{} to {}\n",
        opts.realm.as_yaml_key(),
        opts.user,
        opts.config_path.display()
    ))?;
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use tempfile::tempdir;

    /// In-memory `PasswordSource` for tests. `passwords` is a queue —
    /// each `read_password` call dequeues one entry.
    struct ScriptedSource {
        passwords: RefCell<Vec<String>>,
        confirm_answers: RefCell<Vec<String>>,
        stderr: RefCell<String>,
    }

    impl ScriptedSource {
        fn new(passwords: Vec<&str>, confirms: Vec<&str>) -> Self {
            Self {
                passwords: RefCell::new(passwords.into_iter().map(String::from).collect()),
                confirm_answers: RefCell::new(confirms.into_iter().map(String::from).collect()),
                stderr: RefCell::new(String::new()),
            }
        }
    }

    impl PasswordSource for ScriptedSource {
        fn read_password(&mut self, _prompt: &str) -> io::Result<String> {
            self.passwords
                .borrow_mut()
                .pop()
                .ok_or_else(|| io::Error::other("no scripted password left"))
        }
        fn read_confirm_line(&mut self, _prompt: &str) -> io::Result<String> {
            self.confirm_answers
                .borrow_mut()
                .pop()
                .ok_or_else(|| io::Error::other("no scripted confirm left"))
        }
        fn write_stderr(&mut self, msg: &str) -> io::Result<()> {
            self.stderr.borrow_mut().push_str(msg);
            Ok(())
        }
    }

    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-08T20:30:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    // Realm parsing
    #[test]
    fn realm_parses_cql_and_graph() {
        assert_eq!("cql".parse::<Realm>().unwrap(), Realm::Cql);
        assert_eq!("graph".parse::<Realm>().unwrap(), Realm::Graph);
    }

    #[test]
    fn realm_rejects_unknown() {
        assert!("ldap".parse::<Realm>().is_err());
    }

    // Hashing
    #[test]
    fn hash_password_produces_argon2id_phc_string() {
        let h = hash_password("hunter2").unwrap();
        assert!(
            h.starts_with("$argon2id$"),
            "expected argon2id PHC prefix, got: {h}"
        );
    }

    #[test]
    fn hash_password_verifies_correct_plaintext() {
        let h = hash_password("correct horse").unwrap();
        assert!(verify_password("correct horse", &h).unwrap());
    }

    #[test]
    fn hash_password_rejects_wrong_plaintext() {
        let h = hash_password("right").unwrap();
        assert!(!verify_password("wrong", &h).unwrap());
    }

    #[test]
    fn hash_password_uses_random_salt() {
        let a = hash_password("same").unwrap();
        let b = hash_password("same").unwrap();
        assert_ne!(a, b, "two hashes of the same plaintext must differ");
    }

    // Wire compatibility with ferrosa-schema's verifier.
    #[test]
    fn hash_is_wire_compatible_with_schema_verify_password_any() {
        let h = hash_password("ferrosa_admin").unwrap();
        // Mirrors what server-side `PasswordHasher::verify_password_any` does
        // for argon2id-prefixed strings.
        use argon2::password_hash::phc::PasswordHash;
        use argon2::password_hash::PasswordVerifier;
        let parsed = PasswordHash::new(&h).unwrap();
        let argon2 = Argon2::default();
        assert!(argon2
            .verify_password("ferrosa_admin".as_bytes(), &parsed)
            .is_ok());
    }

    // YAML round trip
    #[test]
    fn auth_file_round_trips_yaml() {
        let mut f = AuthFile::default();
        f.cql.insert(
            "admin".into(),
            CredentialEntry {
                hash: "$argon2id$x".into(),
                created_at: "2026-05-08T20:30:00Z".into(),
            },
        );
        let s = serde_yaml::to_string(&f).unwrap();
        let back: AuthFile = serde_yaml::from_str(&s).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn auth_file_omits_empty_realms() {
        let f = AuthFile::default();
        let s = serde_yaml::to_string(&f).unwrap();
        // Both maps empty → emit "{}" or empty doc, but never literal cql/graph keys.
        assert!(!s.contains("cql:"), "unexpected cql key in: {s}");
        assert!(!s.contains("graph:"), "unexpected graph key in: {s}");
    }

    // load_or_default
    #[test]
    fn load_or_default_returns_default_when_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let f = load_or_default(&path).unwrap();
        assert_eq!(f, AuthFile::default());
    }

    #[test]
    fn load_or_default_reads_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let yaml = "cql:\n  admin:\n    hash: \"$argon2id$abc\"\n    created_at: \"2026-01-01T00:00:00Z\"\n";
        fs::write(&path, yaml).unwrap();
        let f = load_or_default(&path).unwrap();
        assert_eq!(f.cql.get("admin").unwrap().hash, "$argon2id$abc");
    }

    // save
    #[test]
    fn save_creates_parent_directories() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("a/b/c/auth.yaml");
        save(&path, &AuthFile::default()).unwrap();
        assert!(path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn save_sets_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        save(&path, &AuthFile::default()).unwrap();
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "auth.yaml must be 0600, got {mode:o}");
    }

    // merge_credential
    #[test]
    fn merge_credential_adds_new_entry() {
        let f = AuthFile::default();
        let f = merge_credential(f, Realm::Cql, "admin", "h1".into(), fixed_now());
        assert_eq!(f.cql.get("admin").unwrap().hash, "h1");
    }

    #[test]
    fn merge_credential_overwrites_same_user_same_realm() {
        let f = AuthFile::default();
        let f = merge_credential(f, Realm::Cql, "admin", "h1".into(), fixed_now());
        let f = merge_credential(f, Realm::Cql, "admin", "h2".into(), fixed_now());
        assert_eq!(f.cql.get("admin").unwrap().hash, "h2");
    }

    #[test]
    fn merge_credential_preserves_other_realms() {
        let f = AuthFile::default();
        let f = merge_credential(f, Realm::Cql, "admin", "cql_hash".into(), fixed_now());
        let f = merge_credential(f, Realm::Graph, "admin", "graph_hash".into(), fixed_now());
        assert_eq!(f.cql.get("admin").unwrap().hash, "cql_hash");
        assert_eq!(f.graph.get("admin").unwrap().hash, "graph_hash");
    }

    #[test]
    fn merge_credential_preserves_other_users_in_same_realm() {
        let f = AuthFile::default();
        let f = merge_credential(f, Realm::Cql, "alice", "ha".into(), fixed_now());
        let f = merge_credential(f, Realm::Cql, "bob", "hb".into(), fixed_now());
        assert_eq!(f.cql.get("alice").unwrap().hash, "ha");
        assert_eq!(f.cql.get("bob").unwrap().hash, "hb");
    }

    // run_set_password — happy path
    #[test]
    fn run_set_password_writes_argon2_hash_to_disk() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path.clone(),
            force: false,
        };
        let mut src = ScriptedSource::new(vec!["s3cret!", "s3cret!"], vec![]);
        run_set_password(&opts, &mut src, fixed_now()).unwrap();

        let f = load_or_default(&path).unwrap();
        let entry = f.cql.get("admin").expect("admin entry exists");
        assert!(entry.hash.starts_with("$argon2id$"));
        assert!(verify_password("s3cret!", &entry.hash).unwrap());
        assert_eq!(entry.created_at, "2026-05-08T20:30:00Z");
    }

    // Mismatch path: must error AND not modify the file.
    #[test]
    fn run_set_password_mismatch_errors_and_does_not_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path.clone(),
            force: false,
        };
        let mut src = ScriptedSource::new(vec!["one", "two"], vec![]);
        let err = run_set_password(&opts, &mut src, fixed_now()).unwrap_err();
        assert!(matches!(err, AuthError::Mismatch));
        assert!(!path.exists(), "auth.yaml must not be created on mismatch");
    }

    // Mismatch must NOT clobber an existing file.
    #[test]
    fn run_set_password_mismatch_preserves_existing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let mut existing = AuthFile::default();
        existing.cql.insert(
            "alice".into(),
            CredentialEntry {
                hash: "$argon2id$preserved".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
            },
        );
        save(&path, &existing).unwrap();

        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "bob".into(),
            config_path: path.clone(),
            force: true, // force=true so we don't need a confirm prompt
        };
        let mut src = ScriptedSource::new(vec!["a", "b"], vec![]);
        let err = run_set_password(&opts, &mut src, fixed_now()).unwrap_err();
        assert!(matches!(err, AuthError::Mismatch));
        let f = load_or_default(&path).unwrap();
        assert_eq!(f.cql.get("alice").unwrap().hash, "$argon2id$preserved");
        assert!(!f.cql.contains_key("bob"));
    }

    // Merge: existing file with another realm's entry must be preserved.
    #[test]
    fn run_set_password_merges_other_realm_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let mut existing = AuthFile::default();
        existing.graph.insert(
            "admin".into(),
            CredentialEntry {
                hash: "$argon2id$graph_existing".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
            },
        );
        save(&path, &existing).unwrap();

        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path.clone(),
            force: false,
        };
        let mut src = ScriptedSource::new(vec!["pw", "pw"], vec![]);
        run_set_password(&opts, &mut src, fixed_now()).unwrap();

        let f = load_or_default(&path).unwrap();
        assert_eq!(
            f.graph.get("admin").unwrap().hash,
            "$argon2id$graph_existing",
            "graph realm must be preserved across writes"
        );
        assert!(verify_password("pw", &f.cql.get("admin").unwrap().hash).unwrap());
    }

    // Existing user/realm without --force prompts; "n" aborts.
    #[test]
    fn run_set_password_prompts_on_existing_and_aborts_on_no() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let mut existing = AuthFile::default();
        existing.cql.insert(
            "admin".into(),
            CredentialEntry {
                hash: "$argon2id$old".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
            },
        );
        save(&path, &existing).unwrap();

        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path.clone(),
            force: false,
        };
        let mut src = ScriptedSource::new(vec![], vec!["n"]);
        let err = run_set_password(&opts, &mut src, fixed_now()).unwrap_err();
        assert!(matches!(err, AuthError::Aborted));
        // Original hash unchanged.
        let f = load_or_default(&path).unwrap();
        assert_eq!(f.cql.get("admin").unwrap().hash, "$argon2id$old");
    }

    // --force skips the prompt.
    #[test]
    fn run_set_password_force_overwrites_without_prompt() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let mut existing = AuthFile::default();
        existing.cql.insert(
            "admin".into(),
            CredentialEntry {
                hash: "$argon2id$old".into(),
                created_at: "2026-01-01T00:00:00Z".into(),
            },
        );
        save(&path, &existing).unwrap();

        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path.clone(),
            force: true,
        };
        // No confirm answer scripted — proves no prompt was issued.
        let mut src = ScriptedSource::new(vec!["new", "new"], vec![]);
        run_set_password(&opts, &mut src, fixed_now()).unwrap();

        let f = load_or_default(&path).unwrap();
        let entry = f.cql.get("admin").unwrap();
        assert_ne!(entry.hash, "$argon2id$old");
        assert!(verify_password("new", &entry.hash).unwrap());
    }

    // Writes a brief confirmation line to stderr on success.
    #[test]
    fn run_set_password_writes_confirmation_to_stderr() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let opts = SetPasswordOpts {
            realm: Realm::Graph,
            user: "admin".into(),
            config_path: path,
            force: false,
        };
        let mut src = ScriptedSource::new(vec!["pw", "pw"], vec![]);
        run_set_password(&opts, &mut src, fixed_now()).unwrap();
        assert!(src.stderr.borrow().contains("graph/admin"));
    }

    // Empty password rejected.
    #[test]
    fn run_set_password_rejects_empty_password() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        let opts = SetPasswordOpts {
            realm: Realm::Cql,
            user: "admin".into(),
            config_path: path,
            force: false,
        };
        let mut src = ScriptedSource::new(vec!["", ""], vec![]);
        let err = run_set_password(&opts, &mut src, fixed_now()).unwrap_err();
        assert!(matches!(err, AuthError::Hash(_)));
    }

    #[test]
    fn default_config_path_is_under_home() {
        // Force a known HOME for determinism
        let dir = tempdir().unwrap();
        let prev = std::env::var_os("HOME");
        std::env::set_var("HOME", dir.path());
        let p = default_config_path().unwrap();
        assert!(p.ends_with(".ferrosa/config/auth.yaml"));
        if let Some(h) = prev {
            std::env::set_var("HOME", h);
        }
    }
}
