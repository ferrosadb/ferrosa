//! One credential source for every command that talks to a node.
//!
//! A node with auth enabled refuses CQL STARTUP without SASL PLAIN and answers
//! the web admin API with `401 missing Authorization header`. Before this
//! module each command resolved (or, mostly, did not resolve) credentials on
//! its own, so `status`, `ring`, `repair` and the rest failed against every
//! auth-enabled cluster (2026-11-06). Credentials are now resolved once in
//! `main` and passed to every CQL and HTTP call.
//!
//! Sources, in precedence order:
//!
//! - username: `--username`, else `FERROSA_CTL_USERNAME`. Neither means
//!   anonymous, which is what an auth-disabled node expects.
//! - password: `--password-stdin` (one line), else `FERROSA_CTL_PASSWORD`,
//!   else an interactive no-echo prompt.
//!
//! A password supplied without a username is an error, not ignored: there is
//! nobody to authenticate as, and dropping it silently turns into a confusing
//! 401 later.

use std::io::{self, BufRead};
use std::net::SocketAddr;

use ferrosa_cql::client::CqlClient;
use ferrosa_cql::error::CqlError;

/// Environment variable holding the username when `--username` is absent.
pub const USERNAME_ENV: &str = "FERROSA_CTL_USERNAME";
/// Environment variable holding the password.
pub const PASSWORD_ENV: &str = "FERROSA_CTL_PASSWORD";
/// Longest password line accepted from stdin.
const MAX_PASSWORD_BYTES: usize = 4096;

/// A username and password for a node's CQL and web admin endpoints.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub username: String,
    password: String,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Credentials {
    /// Credentials from known values. Production code resolves them with
    /// [`resolve`]; only tests construct them directly.
    #[cfg(test)]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// The password, for building an authentication frame or header.
    pub fn password(&self) -> &str {
        &self.password
    }
}

/// Resolve the credentials for this invocation. See the module docs for the
/// precedence. `env`, `stdin` and `prompt` are injected so tests never touch
/// the real environment or terminal.
pub fn resolve(
    username: Option<&str>,
    password_stdin: bool,
    env: impl Fn(&str) -> Option<String>,
    stdin: impl BufRead,
    prompt: impl FnOnce() -> io::Result<String>,
) -> io::Result<Option<Credentials>> {
    let username = username.map(str::to_owned).or_else(|| env(USERNAME_ENV));
    let Some(username) = username else {
        if password_stdin {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--password-stdin needs a user: pass --username or set {USERNAME_ENV}"),
            ));
        }
        if env(PASSWORD_ENV).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "{PASSWORD_ENV} is set but no user is: pass --username or set {USERNAME_ENV}"
                ),
            ));
        }
        return Ok(None);
    };
    let password = if password_stdin {
        read_password_line(stdin)?
    } else if let Some(password) = env(PASSWORD_ENV) {
        password
    } else {
        prompt()?
    };
    Ok(Some(Credentials { username, password }))
}

/// [`resolve`] against the process environment, stdin and terminal.
pub fn resolve_from_process(
    username: Option<&str>,
    password_stdin: bool,
) -> io::Result<Option<Credentials>> {
    resolve(
        username,
        password_stdin,
        |name| std::env::var(name).ok(),
        io::stdin().lock(),
        || crate::auth::prompt_password("Password"),
    )
}

/// Attach HTTP basic authentication when credentials are present.
pub fn authorize(
    request: reqwest::RequestBuilder,
    credentials: Option<&Credentials>,
) -> reqwest::RequestBuilder {
    match credentials {
        Some(c) => request.basic_auth(&c.username, Some(&c.password)),
        None => request,
    }
}

/// Open a CQL connection, authenticating when credentials are present.
pub async fn connect_cql(
    addr: SocketAddr,
    credentials: Option<&Credentials>,
) -> Result<CqlClient, CqlError> {
    match credentials {
        Some(c) => CqlClient::connect_with_credentials(addr, &c.username, &c.password).await,
        None => CqlClient::connect(addr).await,
    }
}

/// Read one password line from `stdin`, bounded so a mistakenly piped file is
/// refused rather than slurped.
fn read_password_line(stdin: impl BufRead) -> io::Result<String> {
    let mut line = String::new();
    stdin
        .take(MAX_PASSWORD_BYTES as u64 + 1)
        .read_line(&mut line)?;
    if line.len() > MAX_PASSWORD_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("password input exceeds {MAX_PASSWORD_BYTES} bytes"),
        ));
    }
    Ok(line.trim_end_matches(['\r', '\n']).to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn no_prompt() -> io::Result<String> {
        panic!("must not prompt")
    }

    fn stdin(text: &str) -> io::Cursor<Vec<u8>> {
        io::Cursor::new(text.as_bytes().to_vec())
    }

    #[test]
    fn no_username_anywhere_is_anonymous() {
        let creds = resolve(None, false, env(&[]), stdin(""), no_prompt).unwrap();
        assert_eq!(creds, None);
    }

    #[test]
    fn username_flag_with_password_env() {
        let creds = resolve(
            Some("admin"),
            false,
            env(&[(PASSWORD_ENV, "s3cret")]),
            stdin(""),
            no_prompt,
        )
        .unwrap();
        assert_eq!(creds, Some(Credentials::new("admin", "s3cret")));
    }

    #[test]
    fn username_env_is_used_when_flag_absent() {
        let creds = resolve(
            None,
            false,
            env(&[(USERNAME_ENV, "envuser"), (PASSWORD_ENV, "pw")]),
            stdin(""),
            no_prompt,
        )
        .unwrap();
        assert_eq!(creds, Some(Credentials::new("envuser", "pw")));
    }

    #[test]
    fn username_flag_beats_username_env() {
        let creds = resolve(
            Some("flaguser"),
            false,
            env(&[(USERNAME_ENV, "envuser"), (PASSWORD_ENV, "pw")]),
            stdin(""),
            no_prompt,
        )
        .unwrap();
        assert_eq!(creds.unwrap().username, "flaguser");
    }

    #[test]
    fn password_stdin_beats_password_env() {
        let creds = resolve(
            Some("admin"),
            true,
            env(&[(PASSWORD_ENV, "from-env")]),
            stdin("from-stdin\n"),
            no_prompt,
        )
        .unwrap();
        assert_eq!(creds.unwrap().password(), "from-stdin");
    }

    #[test]
    fn prompts_when_no_other_password_source() {
        let creds = resolve(Some("admin"), false, env(&[]), stdin(""), || {
            Ok("typed".to_string())
        })
        .unwrap();
        assert_eq!(creds.unwrap().password(), "typed");
    }

    #[test]
    fn password_env_without_username_fails_loud() {
        let err = resolve(
            None,
            false,
            env(&[(PASSWORD_ENV, "pw")]),
            stdin(""),
            no_prompt,
        )
        .expect_err("a password with nobody to authenticate as must be refused");
        assert!(err.to_string().contains(USERNAME_ENV), "got: {err}");
    }

    #[test]
    fn password_stdin_without_username_fails_loud() {
        let err = resolve(None, true, env(&[]), stdin("pw\n"), no_prompt)
            .expect_err("--password-stdin without a username must be refused");
        assert!(err.to_string().contains("--username"), "got: {err}");
    }

    #[test]
    fn oversized_stdin_password_is_refused() {
        let big = "x".repeat(MAX_PASSWORD_BYTES + 10);
        let err = resolve(Some("admin"), true, env(&[]), stdin(&big), no_prompt)
            .expect_err("an oversized password line must be refused");
        assert!(err.to_string().contains("exceeds"), "got: {err}");
    }

    #[test]
    fn debug_never_prints_the_password() {
        let shown = format!("{:?}", Credentials::new("admin", "hunter2"));
        assert!(!shown.contains("hunter2"), "got: {shown}");
        assert!(shown.contains("admin"));
    }
}
