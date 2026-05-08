//! `ferrosa-ctl auth` subcommands.
//!
//! Provides administrative operations against the running Ferrosa server's
//! `system_auth` keyspace via the CQL native protocol. The server stores
//! Argon2 password hashes — this CLI never touches the hash store directly.
//!
//! The current implementation issues `ALTER ROLE <name> WITH PASSWORD = '<plain>'`,
//! letting the server hash the cleartext via its own `PasswordHasher`.
//!
//! # Password policy
//!
//! - non-empty
//! - at least 8 characters
//!
//! These are baseline checks; the server enforces additional invariants
//! (role exists, caller has ALTER permission).

use std::io::{self, Write};
use std::net::SocketAddr;

use ferrosa_cql::client::CqlClient;
use ferrosa_cql::error::CqlError;

/// Exit codes used by the auth subcommands.
pub const EXIT_BAD_INPUT: i32 = 2;
pub const EXIT_OPERATION_FAILED: i32 = 1;

/// Minimum length of a new password. Matches a common baseline; the server
/// owns the authoritative policy.
pub const MIN_PASSWORD_LEN: usize = 8;

/// Default seed admin password — used as the first attempt for the admin
/// connection so `ferrosa-ctl auth set-password` works on a fresh install
/// without prompting twice.
const SEED_ADMIN_PASSWORD: &str = "ferrosa_admin";

/// Errors that can be produced by the `auth set-password` flow.
#[derive(Debug)]
pub enum SetPasswordError {
    /// User input was rejected (mismatch, empty, too short). Exits 2.
    BadInput(String),
    /// The CQL operation failed (connect, auth, ALTER ROLE). Exits 1.
    Operation(String),
}

impl SetPasswordError {
    pub fn exit_code(&self) -> i32 {
        match self {
            Self::BadInput(_) => EXIT_BAD_INPUT,
            Self::Operation(_) => EXIT_OPERATION_FAILED,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::BadInput(m) | Self::Operation(m) => m,
        }
    }
}

/// Trait for the CQL action `ferrosa-ctl auth set-password` performs against
/// the server. Allows unit tests to substitute a mock client.
#[async_trait::async_trait]
pub trait CqlExecutor: Send {
    /// Issue a single CQL statement; return `Ok(())` on VOID/SCHEMA_CHANGE.
    async fn execute(&mut self, cql: &str) -> Result<(), String>;
}

/// CQL executor backed by a real authenticated [`CqlClient`].
pub struct LiveCqlExecutor {
    client: CqlClient,
}

impl LiveCqlExecutor {
    /// Connect to `addr` as `admin_user` with `admin_password` and prepare
    /// the executor.
    pub async fn connect(
        addr: SocketAddr,
        admin_user: &str,
        admin_password: &str,
    ) -> Result<Self, CqlError> {
        let client = CqlClient::connect_with_credentials(addr, admin_user, admin_password).await?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl CqlExecutor for LiveCqlExecutor {
    async fn execute(&mut self, cql: &str) -> Result<(), String> {
        self.client
            .query(cql)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Validate a new password and produce a [`SetPasswordError::BadInput`] on
/// rejection.
pub fn validate_new_password(p1: &str, p2: &str) -> Result<(), SetPasswordError> {
    if p1 != p2 {
        return Err(SetPasswordError::BadInput(
            "passwords did not match".to_string(),
        ));
    }
    if p1.is_empty() {
        return Err(SetPasswordError::BadInput(
            "new password must not be empty".to_string(),
        ));
    }
    if p1.len() < MIN_PASSWORD_LEN {
        return Err(SetPasswordError::BadInput(format!(
            "new password must be at least {MIN_PASSWORD_LEN} characters",
        )));
    }
    Ok(())
}

/// Build the exact CQL `ALTER ROLE` statement for the given role and
/// cleartext password. Both the role identifier and the password literal
/// are quoted/escaped so embedded special characters can't break out of
/// the statement.
pub fn build_alter_role_statement(role: &str, new_password: &str) -> String {
    format!(
        "ALTER ROLE \"{}\" WITH PASSWORD = '{}'",
        escape_double_quotes(role),
        escape_single_quotes(new_password),
    )
}

/// Escape `"` as `""` for CQL quoted identifiers.
fn escape_double_quotes(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Escape `'` as `''` for CQL string literals.
fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Run the `set-password` operation, end-to-end, against the supplied
/// executor. Splitting this from connect/IO keeps the function unit-testable.
pub async fn run_set_password(
    executor: &mut dyn CqlExecutor,
    target_user: &str,
    new_password: &str,
    host: SocketAddr,
) -> Result<(), SetPasswordError> {
    let stmt = build_alter_role_statement(target_user, new_password);
    executor
        .execute(&stmt)
        .await
        .map_err(|e| SetPasswordError::Operation(format_cql_error(&e, host)))
}

/// Format a CQL error string with a hint about the host and a recovery
/// suggestion. Used for both connect failures and ALTER errors.
pub fn format_cql_error(err: &str, host: SocketAddr) -> String {
    format!(
        "CQL operation against {host} failed: {err}\n\
         hint: is ferrosa running? Try `systemctl --user status ferrosa` or \
         `launchctl print gui/$(id -u)/com.ferrosadb.ferrosa`."
    )
}

// ── prompt helpers ──────────────────────────────────────────────────────

/// Prompt for a password without echoing to the terminal.
pub fn prompt_password(label: &str) -> io::Result<String> {
    rpassword::prompt_password(format!("{label}: "))
}

/// Read a new password from the terminal, prompting twice unless
/// `confirm == false`. Validates the result.
pub fn read_new_password_interactive(confirm: bool) -> Result<String, SetPasswordError> {
    let p1 = prompt_password("New password")
        .map_err(|e| SetPasswordError::BadInput(format!("could not read password: {e}")))?;
    let p2 = if confirm {
        prompt_password("Confirm new password")
            .map_err(|e| SetPasswordError::BadInput(format!("could not read password: {e}")))?
    } else {
        p1.clone()
    };
    validate_new_password(&p1, &p2)?;
    Ok(p1)
}

/// Resolve the admin password using, in order: `--admin-password-env`,
/// the well-known seed default, then an interactive prompt.
///
/// Returns the password to try first AND a flag indicating whether the
/// caller should re-prompt on auth failure (true when we used the seed
/// default and have not yet asked the operator).
#[derive(Debug, Clone)]
pub struct AdminPasswordChoice {
    pub password: String,
    pub may_retry_with_prompt: bool,
}

pub fn resolve_admin_password(
    admin_password_env_var: Option<&str>,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> io::Result<AdminPasswordChoice> {
    if let Some(var) = admin_password_env_var {
        match env_lookup(var) {
            Some(v) => {
                return Ok(AdminPasswordChoice {
                    password: v,
                    may_retry_with_prompt: false,
                });
            }
            None => {
                // Variable was specified but unset — fail loud rather than
                // silently falling back. Per CLAUDE.md "fail loud".
                return Err(io::Error::other(format!(
                    "--admin-password-env={var} is set but no value found in the environment"
                )));
            }
        }
    }
    Ok(AdminPasswordChoice {
        password: SEED_ADMIN_PASSWORD.to_string(),
        may_retry_with_prompt: true,
    })
}

/// Print a SUPERUSER warning to stderr and require an explicit "yes" on
/// stdin to proceed. Used unless `--force` is supplied.
pub fn confirm_superuser_change<R: io::BufRead, W: Write>(
    target_user: &str,
    input: &mut R,
    output: &mut W,
) -> Result<(), SetPasswordError> {
    writeln!(
        output,
        "About to change the password for SUPERUSER role '{target_user}'."
    )
    .map_err(|e| SetPasswordError::BadInput(format!("could not write prompt: {e}")))?;
    write!(output, "Continue? [y/N] ")
        .map_err(|e| SetPasswordError::BadInput(format!("could not write prompt: {e}")))?;
    output
        .flush()
        .map_err(|e| SetPasswordError::BadInput(format!("could not flush prompt: {e}")))?;
    let mut answer = String::new();
    input
        .read_line(&mut answer)
        .map_err(|e| SetPasswordError::BadInput(format!("could not read answer: {e}")))?;
    let trimmed = answer.trim().to_lowercase();
    if trimmed == "y" || trimmed == "yes" {
        Ok(())
    } else {
        Err(SetPasswordError::BadInput("aborted by user".to_string()))
    }
}

// ── tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    use std::sync::Mutex;

    /// Captures statements executed against the mock executor.
    #[derive(Default)]
    struct MockExecutor {
        calls: Mutex<Vec<String>>,
        fail_with: Option<String>,
    }

    #[async_trait::async_trait]
    impl CqlExecutor for MockExecutor {
        async fn execute(&mut self, cql: &str) -> Result<(), String> {
            self.calls.lock().unwrap().push(cql.to_string());
            match &self.fail_with {
                Some(msg) => Err(msg.clone()),
                None => Ok(()),
            }
        }
    }

    fn host() -> SocketAddr {
        "127.0.0.1:9042".parse().unwrap()
    }

    #[test]
    fn validate_rejects_mismatch() {
        let err = validate_new_password("hunter2foo", "hunter2bar").unwrap_err();
        assert_eq!(err.exit_code(), EXIT_BAD_INPUT);
        assert!(err.message().contains("did not match"));
    }

    #[test]
    fn validate_rejects_empty() {
        let err = validate_new_password("", "").unwrap_err();
        assert_eq!(err.exit_code(), EXIT_BAD_INPUT);
        assert!(err.message().contains("empty"));
    }

    #[test]
    fn validate_rejects_short() {
        let err = validate_new_password("short", "short").unwrap_err();
        assert_eq!(err.exit_code(), EXIT_BAD_INPUT);
        assert!(err.message().contains("at least"));
    }

    #[test]
    fn validate_accepts_strong_password() {
        validate_new_password("hunter2foo", "hunter2foo").unwrap();
    }

    #[test]
    fn build_alter_role_quotes_role_name() {
        let stmt = build_alter_role_statement("ferrosa_admin", "hunter2foo");
        assert_eq!(
            stmt,
            "ALTER ROLE \"ferrosa_admin\" WITH PASSWORD = 'hunter2foo'"
        );
    }

    #[test]
    fn build_alter_role_escapes_single_quote_in_password() {
        let stmt = build_alter_role_statement("ferrosa_admin", "ab'cd");
        assert_eq!(
            stmt,
            "ALTER ROLE \"ferrosa_admin\" WITH PASSWORD = 'ab''cd'"
        );
    }

    #[test]
    fn build_alter_role_escapes_double_quote_in_role() {
        let stmt = build_alter_role_statement("weird\"name", "hunter2foo");
        assert_eq!(
            stmt,
            "ALTER ROLE \"weird\"\"name\" WITH PASSWORD = 'hunter2foo'"
        );
    }

    #[tokio::test]
    async fn run_set_password_issues_alter_role() {
        let mut exec = MockExecutor::default();
        run_set_password(&mut exec, "ferrosa_admin", "hunter2foo", host())
            .await
            .unwrap();
        let calls = exec.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            "ALTER ROLE \"ferrosa_admin\" WITH PASSWORD = 'hunter2foo'"
        );
    }

    #[tokio::test]
    async fn run_set_password_propagates_cql_error_with_host_hint() {
        let mut exec = MockExecutor {
            fail_with: Some("connection refused".to_string()),
            ..Default::default()
        };
        let err = run_set_password(&mut exec, "ferrosa_admin", "hunter2foo", host())
            .await
            .unwrap_err();
        assert_eq!(err.exit_code(), EXIT_OPERATION_FAILED);
        assert!(err.message().contains("127.0.0.1:9042"));
        assert!(err.message().contains("connection refused"));
        assert!(err.message().contains("hint:"));
    }

    #[test]
    fn resolve_admin_password_uses_env_var_when_set() {
        let env = |k: &str| {
            if k == "MY_PW" {
                Some("from-env".to_string())
            } else {
                None
            }
        };
        let res = resolve_admin_password(Some("MY_PW"), env).unwrap();
        assert_eq!(res.password, "from-env");
        assert!(!res.may_retry_with_prompt);
    }

    #[test]
    fn resolve_admin_password_errors_when_env_var_unset() {
        let env = |_: &str| None;
        let err = resolve_admin_password(Some("MISSING"), env).unwrap_err();
        assert!(err.to_string().contains("MISSING"));
    }

    #[test]
    fn resolve_admin_password_falls_back_to_seed_default() {
        let env = |_: &str| None;
        let res = resolve_admin_password(None, env).unwrap();
        assert_eq!(res.password, "ferrosa_admin");
        assert!(res.may_retry_with_prompt);
    }

    #[test]
    fn confirm_superuser_change_accepts_yes() {
        let mut input = Cursor::new(b"yes\n");
        let mut output = Vec::new();
        confirm_superuser_change("ferrosa_admin", &mut input, &mut output).unwrap();
        let s = String::from_utf8(output).unwrap();
        assert!(s.contains("ferrosa_admin"));
        assert!(s.contains("SUPERUSER"));
    }

    #[test]
    fn confirm_superuser_change_accepts_y() {
        let mut input = Cursor::new(b"y\n");
        let mut output = Vec::new();
        confirm_superuser_change("ferrosa_admin", &mut input, &mut output).unwrap();
    }

    #[test]
    fn confirm_superuser_change_aborts_on_no() {
        let mut input = Cursor::new(b"n\n");
        let mut output = Vec::new();
        let err = confirm_superuser_change("ferrosa_admin", &mut input, &mut output).unwrap_err();
        assert_eq!(err.exit_code(), EXIT_BAD_INPUT);
        assert!(err.message().contains("aborted"));
    }

    #[test]
    fn confirm_superuser_change_aborts_on_empty() {
        let mut input = Cursor::new(b"\n");
        let mut output = Vec::new();
        let err = confirm_superuser_change("ferrosa_admin", &mut input, &mut output).unwrap_err();
        assert_eq!(err.exit_code(), EXIT_BAD_INPUT);
    }

    #[test]
    fn format_cql_error_includes_host_and_hint() {
        let s = format_cql_error("io error: refused", host());
        assert!(s.contains("127.0.0.1:9042"));
        assert!(s.contains("io error: refused"));
        assert!(s.contains("hint:"));
        assert!(s.contains("ferrosa"));
    }
}
