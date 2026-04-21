//! CQL-layer auth glue. Two concerns live here:
//!
//! 1. SASL PLAIN decode + role lookup (called from the STARTUP/AUTH_RESPONSE
//!    frame handlers in `server.rs`). The server sends AUTHENTICATE with
//!    the authenticator class name, the client responds with
//!    AUTH_RESPONSE containing a SASL PLAIN payload
//!    (`\0username\0password`), and the server validates via
//!    `ferrosa_schema::Schema::authenticate()`.
//! 2. Permission enforcement helper [`enforce_permission`] that wraps
//!    `ferrosa_schema::Schema::check_permission` with a warn-mode
//!    escape hatch controlled by `StorageEngineConfig.auth_warn`. When
//!    warn mode is on, denials are logged at `WARN` level and counted
//!    (see [`warn_denial_stats`]) but the request still proceeds.
//!    This is the soak stage of the rollout described in
//!    `specs/decisions/design-cql-role-auth-rollout.md` Sprint D.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use bytes::BufMut;
use dashmap::DashMap;

use ferrosa_schema::auth::permission::{Permission, Resource};
use ferrosa_schema::auth::role::AuthContext;
use ferrosa_schema::Schema;

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

// ── Permission enforcement with warn-mode escape hatch ──────────────────

/// Counter registry for would-be denials in warn mode, keyed by
/// `role|resource|perm`. Lives at module scope so integrations (router,
/// observability handlers) can all share the same view.
fn warn_counters() -> &'static DashMap<String, AtomicU64> {
    static C: OnceLock<DashMap<String, AtomicU64>> = OnceLock::new();
    C.get_or_init(DashMap::new)
}

fn counter_key(role: &str, resource: &Resource, perm: Permission) -> String {
    // `Resource` and `Permission` already have human-friendly Display
    // impls — reuse them so the counter keys and log lines match.
    format!("{role}|{resource}|{perm}")
}

/// Bump the warn-mode denial counter for a (role, resource, perm) triple.
///
/// Exposed so integration code in sibling crates can write to the same
/// counters without re-implementing the key format. Returns the new
/// total for that triple.
pub fn record_warn_denial(role: &str, resource: &Resource, perm: Permission) -> u64 {
    let key = counter_key(role, resource, perm);
    let cell = warn_counters()
        .entry(key)
        .or_insert_with(|| AtomicU64::new(0));
    cell.fetch_add(1, Ordering::Relaxed) + 1
}

/// Read-only snapshot of the warn-mode denial counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WarnDenialSnapshot {
    /// Total number of would-be denials, across all roles and resources.
    pub total: u64,
    /// `role -> count`.
    pub by_role: std::collections::BTreeMap<String, u64>,
    /// `resource -> count` (Display form, e.g. `table ks.tbl`).
    pub by_resource: std::collections::BTreeMap<String, u64>,
}

/// Take an atomic-consistent-ish snapshot of the current counter state.
///
/// Racy — counters may change mid-read — but the counters themselves are
/// additive so the total is at most slightly stale. This matches the
/// semantics of the other `/api/observability/*` endpoints.
pub fn warn_denial_stats() -> WarnDenialSnapshot {
    let mut snap = WarnDenialSnapshot::default();
    for entry in warn_counters().iter() {
        let key = entry.key();
        let count = entry.value().load(Ordering::Relaxed);
        snap.total = snap.total.saturating_add(count);
        // key format: `role|resource|perm`.
        let mut parts = key.splitn(3, '|');
        let role = parts.next().unwrap_or("").to_string();
        let resource = parts.next().unwrap_or("").to_string();
        *snap.by_role.entry(role).or_insert(0) += count;
        *snap.by_resource.entry(resource).or_insert(0) += count;
    }
    snap
}

/// Test-only: clear all counters. Exposed via a `__`-prefixed name so
/// callers know this is not part of the stable API.
#[doc(hidden)]
pub fn __clear_warn_denial_counters_for_tests() {
    warn_counters().clear();
}

/// Enforce a CQL permission check with an optional warn-mode escape
/// hatch. The caller passes the current [`AuthContext`], the required
/// [`Permission`], the [`Resource`], and the `auth_warn` flag read from
/// `StorageEngineConfig.auth_warn`.
///
/// - If the schema grants the permission: `Ok(())`.
/// - If denied and `auth_warn == false`: `Err(CqlError::Unauthorized(..))`.
/// - If denied and `auth_warn == true`: emit a `WARN` log line, bump
///   the counter, and return `Ok(())`. The request then proceeds as if
///   permitted. This is the soak mode described in Sprint D of
///   `specs/decisions/design-cql-role-auth-rollout.md`.
///
/// The warn-log line is shaped for easy `grep`: it always contains
/// `auth: WARN MODE` plus the role/perm/resource triple so operators
/// can watch for unexpected consumers before flipping enforcement on.
pub fn enforce_permission(
    schema: &Schema,
    ctx: &AuthContext,
    perm: Permission,
    resource: &Resource,
    auth_warn: bool,
) -> Result<(), CqlError> {
    match schema.check_permission(ctx, perm, resource) {
        Ok(()) => Ok(()),
        Err(denied) if auth_warn => {
            let new_count = record_warn_denial(&ctx.role, resource, perm);
            tracing::warn!(
                role = %ctx.role,
                perm = %perm,
                resource = %resource,
                warn_denials = new_count,
                "auth: WARN MODE — would deny but permitting \
                 (FERROSA_AUTH_WARN=true). This is a soak; denials \
                 become errors once FERROSA_AUTH_WARN=false. \
                 Underlying error: {denied}"
            );
            Ok(())
        }
        Err(denied) => Err(denied.into()),
    }
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
