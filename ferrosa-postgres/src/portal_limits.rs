//! Module: Bound how many portals may sit suspended, per connection and per
//! node, and for how long.
//! Correctness: Correct when (1) a portal is admitted to suspension only while
//! both its connection and the node are under their limits, and the
//! (limit+1)th is refused with SQLSTATE 53000; (2) the node count equals the
//! live [`PortalSlot`]s, so every way a suspended portal ends (Execute to the
//! end, Close, rebind, Sync, idle expiry, disconnect) gives its slot back; and
//! (3) refusals are reported on their edges, not once per refusal.
//! Last revised: 2026-10-03
//! Last changed: Created (missing-guards entry 8).
//!
//! A suspended portal holds no thread (see `result_stream`), but it still
//! holds its query: a few batches of rows, the storage scan's open SSTable
//! readers (which keep compacted-away files on disk), and any spilled sort
//! runs. Nothing else bounds how many a client may leave open, or for how
//! long, so these limits do. They are a safety net under the no-thread design,
//! not a substitute for it.
//!
//! Configuration (`[postgres]` TOML wins over the environment):
//!
//! | TOML key | Environment | Default |
//! |---|---|---|
//! | `max_suspended_portals_per_connection` | `FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS_PER_CONNECTION` | 64 |
//! | `max_suspended_portals` | `FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS` | 2048 |
//! | `suspended_portal_idle_timeout_ms` | `FERROSA_POSTGRES_SUSPENDED_PORTAL_IDLE_TIMEOUT_MS` | 600000 (10 min, the MVCC snapshot age) |

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::messages::BackendMessage;
use crate::query::error_response;

pub const MAX_PER_CONNECTION_ENV: &str = "FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS_PER_CONNECTION";
pub const MAX_PER_NODE_ENV: &str = "FERROSA_POSTGRES_MAX_SUSPENDED_PORTALS";
pub const IDLE_TIMEOUT_MS_ENV: &str = "FERROSA_POSTGRES_SUSPENDED_PORTAL_IDLE_TIMEOUT_MS";

const DEFAULT_MAX_PER_CONNECTION: usize = 64;
const DEFAULT_MAX_PER_NODE: usize = 2048;
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(600);

/// SQLSTATE `insufficient_resources`.
const INSUFFICIENT_RESOURCES: &str = "53000";
/// SQLSTATE `query_canceled`, for a portal the server closed while idle.
const QUERY_CANCELED: &str = "57014";

/// The three limits on suspended portals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortalLimits {
    pub per_connection: usize,
    pub per_node: usize,
    pub idle_timeout: Duration,
}

impl Default for PortalLimits {
    fn default() -> Self {
        Self {
            per_connection: DEFAULT_MAX_PER_CONNECTION,
            per_node: DEFAULT_MAX_PER_NODE,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
        }
    }
}

/// A malformed limit setting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortalLimitsError(String);

impl fmt::Display for PortalLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PortalLimitsError {}

fn positive(name: &str, value: Option<&str>, default: u64) -> Result<u64, PortalLimitsError> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.trim().parse::<u64>() {
        Ok(0) => Err(PortalLimitsError(format!(
            "{name} must be greater than zero"
        ))),
        Ok(parsed) => Ok(parsed),
        Err(error) => Err(PortalLimitsError(format!(
            "invalid {name} value {value:?}: {error}"
        ))),
    }
}

impl PortalLimits {
    /// Build from optional raw settings; an absent one takes its default.
    ///
    /// # Errors
    ///
    /// A setting that is not a positive integer.
    pub fn from_overrides(
        per_connection: Option<&str>,
        per_node: Option<&str>,
        idle_timeout_ms: Option<&str>,
    ) -> Result<Self, PortalLimitsError> {
        let defaults = Self::default();
        let per_connection = positive(
            MAX_PER_CONNECTION_ENV,
            per_connection,
            defaults.per_connection as u64,
        )?;
        let per_node = positive(MAX_PER_NODE_ENV, per_node, defaults.per_node as u64)?;
        let idle_ms = positive(
            IDLE_TIMEOUT_MS_ENV,
            idle_timeout_ms,
            defaults.idle_timeout.as_millis() as u64,
        )?;
        Ok(Self {
            per_connection: usize::try_from(per_connection).unwrap_or(usize::MAX),
            per_node: usize::try_from(per_node).unwrap_or(usize::MAX),
            idle_timeout: Duration::from_millis(idle_ms),
        })
    }

    /// [`Self::from_overrides`], falling back to the defaults on a malformed
    /// setting. Designed fallback: logged at ERROR naming the setting, so a
    /// typo is visible, and the defaults are safe limits rather than none.
    pub fn resolve_or_default(
        per_connection: Option<&str>,
        per_node: Option<&str>,
        idle_timeout_ms: Option<&str>,
    ) -> Self {
        match Self::from_overrides(per_connection, per_node, idle_timeout_ms) {
            Ok(limits) => limits,
            Err(error) => {
                tracing::error!(%error, "invalid PostgreSQL suspended-portal limit; using the defaults");
                Self::default()
            }
        }
    }
}

static SUSPENDED_GAUGE: AtomicUsize = AtomicUsize::new(0);
static REFUSALS_TOTAL: AtomicU64 = AtomicU64::new(0);
static EXPIRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Append the suspended-portal metrics in Prometheus text format.
pub fn render_prometheus(out: &mut String) {
    out.push_str(
        "# HELP ferrosa_pg_suspended_portals PostgreSQL portals suspended by max_rows and still open.\n\
         # TYPE ferrosa_pg_suspended_portals gauge\n",
    );
    out.push_str(&format!(
        "ferrosa_pg_suspended_portals {}\n",
        SUSPENDED_GAUGE.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_pg_suspended_portal_refusals_total Portals refused suspension because a connection or the node was at its limit (SQLSTATE 53000).\n\
         # TYPE ferrosa_pg_suspended_portal_refusals_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_pg_suspended_portal_refusals_total {}\n",
        REFUSALS_TOTAL.load(Ordering::Relaxed)
    ));
    out.push_str(
        "# HELP ferrosa_pg_suspended_portal_expiries_total Suspended portals closed because no Execute touched them within the idle timeout.\n\
         # TYPE ferrosa_pg_suspended_portal_expiries_total counter\n",
    );
    out.push_str(&format!(
        "ferrosa_pg_suspended_portal_expiries_total {}\n",
        EXPIRIES_TOTAL.load(Ordering::Relaxed)
    ));
}

/// Node-wide accounting of suspended portals. One per PostgreSQL listener,
/// shared by every connection through [`crate::QueryContext`].
#[derive(Debug)]
pub struct SuspendedPortals {
    limits: PortalLimits,
    suspended: AtomicUsize,
    /// Set by a refusal, cleared by the next admission: refusals are logged
    /// on these edges.
    refusing: AtomicBool,
}

impl Default for SuspendedPortals {
    fn default() -> Self {
        Self::new(PortalLimits::default())
    }
}

/// Which limit refused a portal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scope {
    Connection,
    Node,
}

impl SuspendedPortals {
    pub fn new(limits: PortalLimits) -> Self {
        assert!(
            limits.per_connection > 0 && limits.per_node > 0,
            "limits must be positive"
        );
        Self {
            limits,
            suspended: AtomicUsize::new(0),
            refusing: AtomicBool::new(false),
        }
    }

    pub fn limits(&self) -> PortalLimits {
        self.limits
    }

    /// Portals suspended on this node right now.
    pub fn suspended(&self) -> usize {
        self.suspended.load(Ordering::Acquire)
    }

    /// Admit one more suspended portal on a connection that already holds
    /// `held`, or refuse it with the `ErrorResponse` (SQLSTATE 53000) to send.
    pub(crate) fn admit(self: &Arc<Self>, held: usize) -> Result<PortalSlot, BackendMessage> {
        if held >= self.limits.per_connection {
            return Err(self.refuse(Scope::Connection, held));
        }
        let per_node = self.limits.per_node;
        let reserved = self
            .suspended
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < per_node).then_some(n + 1)
            });
        match reserved {
            Ok(_) => {
                SUSPENDED_GAUGE.fetch_add(1, Ordering::Relaxed);
                if self.refusing.swap(false, Ordering::AcqRel) {
                    tracing::info!(
                        suspended = self.suspended(),
                        "PostgreSQL portals are being admitted to suspension again"
                    );
                }
                Ok(PortalSlot(Arc::clone(self)))
            }
            Err(now) => Err(self.refuse(Scope::Node, now)),
        }
    }

    fn refuse(&self, scope: Scope, held: usize) -> BackendMessage {
        REFUSALS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let (what, limit, setting) = match scope {
            Scope::Connection => (
                "this connection",
                self.limits.per_connection,
                MAX_PER_CONNECTION_ENV,
            ),
            Scope::Node => ("this node", self.limits.per_node, MAX_PER_NODE_ENV),
        };
        if !self.refusing.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                scope = ?scope,
                held,
                limit,
                "refusing to suspend PostgreSQL portals: at the suspended-portal limit"
            );
        }
        error_response(
            INSUFFICIENT_RESOURCES,
            &format!(
                "too many suspended portals: {what} already holds {held} (limit {limit}, \
                 set by {setting}); fetch a portal to its end or close it"
            ),
        )
    }

    /// Count `n` suspended portals closed by the idle timeout.
    pub(crate) fn record_expiries(&self, n: usize) {
        EXPIRIES_TOTAL.fetch_add(n as u64, Ordering::Relaxed);
    }
}

/// One admitted suspended portal. Dropping it gives the slot back, however the
/// portal ends.
#[derive(Debug)]
pub(crate) struct PortalSlot(Arc<SuspendedPortals>);

impl Drop for PortalSlot {
    fn drop(&mut self) {
        let before = self.0.suspended.fetch_sub(1, Ordering::AcqRel);
        assert!(before > 0, "a portal slot was released twice");
        SUSPENDED_GAUGE.fetch_sub(1, Ordering::Relaxed);
    }
}

/// The error a later `Execute` gets for a portal the server closed because it
/// sat suspended past the idle timeout.
pub(crate) fn expired_portal_error(portal: &str, idle_timeout: Duration) -> BackendMessage {
    error_response(
        QUERY_CANCELED,
        &format!(
            "portal \"{portal}\" was closed after it sat suspended for {} ms with no Execute \
             (set by {IDLE_TIMEOUT_MS_ENV}); run the query again",
            idle_timeout.as_millis()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(message: &BackendMessage) -> &str {
        match message {
            BackendMessage::ErrorResponse { fields } => fields
                .iter()
                .find(|(tag, _)| *tag == b'C')
                .map(|(_, code)| code.as_str())
                .expect("an ErrorResponse carries a code"),
            other => panic!("expected an ErrorResponse, got {other:?}"),
        }
    }

    fn limits(per_connection: usize, per_node: usize) -> Arc<SuspendedPortals> {
        Arc::new(SuspendedPortals::new(PortalLimits {
            per_connection,
            per_node,
            idle_timeout: Duration::from_secs(60),
        }))
    }

    #[test]
    fn the_connection_limit_refuses_the_next_portal_with_53000() {
        let portals = limits(2, 100);
        let first = portals.admit(0).expect("first");
        let _second = portals.admit(1).expect("second");
        let refusal = portals.admit(2).expect_err("the third is over the limit");
        assert_eq!(code(&refusal), "53000");
        assert_eq!(portals.suspended(), 2, "a refusal reserves nothing");
        drop(first);
        assert_eq!(portals.suspended(), 1);
        portals.admit(1).expect("closing one frees a slot");
    }

    #[test]
    fn the_node_limit_refuses_across_connections() {
        let portals = limits(10, 3);
        let held: Vec<PortalSlot> = (0..3).map(|_| portals.admit(0).expect("under")).collect();
        let refusal = portals.admit(0).expect_err("the node is full");
        assert_eq!(code(&refusal), "53000");
        drop(held);
        assert_eq!(portals.suspended(), 0, "every slot came back");
        portals.admit(0).expect("a freed node admits again");
    }

    #[test]
    fn malformed_settings_are_refused_and_absent_ones_default() {
        assert_eq!(
            PortalLimits::from_overrides(None, None, None).unwrap(),
            PortalLimits::default()
        );
        let set = PortalLimits::from_overrides(Some("3"), Some("9"), Some("250")).unwrap();
        assert_eq!(
            set,
            PortalLimits {
                per_connection: 3,
                per_node: 9,
                idle_timeout: Duration::from_millis(250)
            }
        );
        assert!(PortalLimits::from_overrides(Some("0"), None, None).is_err());
        assert!(PortalLimits::from_overrides(None, Some("lots"), None).is_err());
        assert!(PortalLimits::from_overrides(None, None, Some("-1")).is_err());
        assert_eq!(
            PortalLimits::resolve_or_default(Some("x"), None, None),
            PortalLimits::default()
        );
    }
}
