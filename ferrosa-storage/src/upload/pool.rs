//! Object-store connection-pool and dial tunables.
//!
//! The pool is sized from the in-flight request target, which is a property of
//! the link (bandwidth x round-trip time), never from core counts or worker
//! counts. See [`DEFAULT_MAX_IN_FLIGHT`] for the arithmetic.
//!
//! Variables (invalid values are rejected naming the variable):
//! - `FERROSA_S3_MAX_CONCURRENT_REQUESTS`: the in-flight target (see
//!   [`super::config::ObjectStoreConfig::effective_max_in_flight`]);
//! - `FERROSA_S3_POOL_MAX_IDLE_PER_HOST`: overrides the pool size; may not be
//!   below the in-flight target;
//! - `FERROSA_S3_POOL_IDLE_TIMEOUT_SECS`: default 90;
//! - `FERROSA_S3_CONNECT_TIMEOUT_SECS`: default 10. This is the object-store
//!   dial, not the internode `FERROSA_CONNECT_TIMEOUT_MS`.

use std::sync::OnceLock;
use std::time::Duration;

use super::config::parse_request_limit;

/// Default cap on concurrent object-store requests, chosen for a WAN.
///
/// In-flight data needed to fill a link is bandwidth x RTT. Assuming 1 Gbit/s
/// (125 MB/s) to R2/S3 at 100 ms RTT, that is 12.5 MB; at a typical 256 KiB
/// request (index and small components, ranged reads) it is about 50
/// requests, rounded up to 64. A LAN needs far fewer and a faster or longer
/// link more: scale linearly (`requests = bandwidth x RTT / request size`).
/// Set `FERROSA_S3_MAX_CONCURRENT_REQUESTS` to the value your link needs.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 64;
/// Default idle lifetime of a pooled connection.
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 90;
/// Default object-store connect (dial) timeout.
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 10;

/// Pool and dial tunables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolConfig {
    /// Explicit pool size; `None` derives it from the in-flight target.
    pub max_idle_override: Option<usize>,
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
}

impl PoolConfig {
    /// Parse the three optional variables.
    pub fn parse(
        max_idle: Option<&str>,
        idle_timeout_secs: Option<&str>,
        connect_timeout_secs: Option<&str>,
    ) -> ferrosa_common::Result<Self> {
        let idle =
            parse_request_limit::<u64>("FERROSA_S3_POOL_IDLE_TIMEOUT_SECS", idle_timeout_secs)?
                .unwrap_or(DEFAULT_IDLE_TIMEOUT_SECS);
        let connect =
            parse_request_limit::<u64>("FERROSA_S3_CONNECT_TIMEOUT_SECS", connect_timeout_secs)?
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS);
        Ok(Self {
            max_idle_override: parse_request_limit::<usize>(
                "FERROSA_S3_POOL_MAX_IDLE_PER_HOST",
                max_idle,
            )?,
            idle_timeout: Duration::from_secs(idle),
            connect_timeout: Duration::from_secs(connect),
        })
    }

    /// Read the variables from the process environment.
    pub fn from_env() -> ferrosa_common::Result<Self> {
        Self::parse(
            std::env::var("FERROSA_S3_POOL_MAX_IDLE_PER_HOST")
                .ok()
                .as_deref(),
            std::env::var("FERROSA_S3_POOL_IDLE_TIMEOUT_SECS")
                .ok()
                .as_deref(),
            std::env::var("FERROSA_S3_CONNECT_TIMEOUT_SECS")
                .ok()
                .as_deref(),
        )
    }

    /// Idle connections kept per host for a given in-flight target: the
    /// override when set, else the target itself. A pool below the target
    /// forces a reconnect on every burst, so an override below it is an error.
    pub fn max_idle_per_host(&self, max_in_flight: usize) -> ferrosa_common::Result<usize> {
        match self.max_idle_override {
            Some(over) if over < max_in_flight => {
                Err(ferrosa_common::Error::InvalidFormat(format!(
                    "FERROSA_S3_POOL_MAX_IDLE_PER_HOST={over} is below the in-flight limit \
                     {max_in_flight} (FERROSA_S3_MAX_CONCURRENT_REQUESTS); a smaller pool \
                     reconnects on every burst"
                )))
            }
            Some(over) => Ok(over),
            None => Ok(max_in_flight),
        }
    }
}

static CONFIG: OnceLock<PoolConfig> = OnceLock::new();

/// The process-wide pool config, read from the environment on first use. An
/// invalid variable is an error on every call; it is never replaced by a
/// default.
pub fn config() -> ferrosa_common::Result<PoolConfig> {
    if let Some(config) = CONFIG.get() {
        return Ok(*config);
    }
    let parsed = PoolConfig::from_env()?;
    Ok(*CONFIG.get_or_init(|| parsed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_when_unset_or_empty() {
        let cfg = PoolConfig::parse(None, Some(""), None).unwrap();
        assert_eq!(cfg.max_idle_override, None);
        assert_eq!(cfg.idle_timeout, Duration::from_secs(90));
        assert_eq!(cfg.connect_timeout, Duration::from_secs(10));
    }

    #[test]
    fn explicit_values_parse() {
        let cfg = PoolConfig::parse(Some("200"), Some("30"), Some("5")).unwrap();
        assert_eq!(cfg.max_idle_override, Some(200));
        assert_eq!(cfg.idle_timeout, Duration::from_secs(30));
        assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    }

    #[test]
    fn invalid_values_are_rejected_naming_the_variable() {
        for (args, name) in [
            ((Some("0"), None, None), "FERROSA_S3_POOL_MAX_IDLE_PER_HOST"),
            (
                (Some("many"), None, None),
                "FERROSA_S3_POOL_MAX_IDLE_PER_HOST",
            ),
            ((None, Some("0"), None), "FERROSA_S3_POOL_IDLE_TIMEOUT_SECS"),
            ((None, None, Some("-1")), "FERROSA_S3_CONNECT_TIMEOUT_SECS"),
        ] {
            let err = PoolConfig::parse(args.0, args.1, args.2)
                .unwrap_err()
                .to_string();
            assert!(err.contains(name), "{err}");
        }
    }

    #[test]
    fn the_pool_follows_the_in_flight_target_not_the_machine() {
        let cfg = PoolConfig::parse(None, None, None).unwrap();
        assert_eq!(cfg.max_idle_per_host(64).unwrap(), 64);
        assert_eq!(cfg.max_idle_per_host(300).unwrap(), 300);
    }

    #[test]
    fn an_override_wins_but_may_not_undercut_the_in_flight_target() {
        let cfg = PoolConfig::parse(Some("128"), None, None).unwrap();
        assert_eq!(cfg.max_idle_per_host(64).unwrap(), 128);
        let err = cfg.max_idle_per_host(256).unwrap_err().to_string();
        assert!(err.contains("FERROSA_S3_POOL_MAX_IDLE_PER_HOST"), "{err}");
    }
}
