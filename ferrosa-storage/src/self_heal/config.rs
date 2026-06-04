//! Configuration for the self-healing controller.
//!
//! Every knob is **fixed** (env-or-default) and read once at construction.
//! Nothing here is derived from wall-clock or RNG — the controller's
//! determinism contract (design doc, "Determinism guarantees") requires that
//! detection, action selection and scheduling be pure functions of the
//! observable [`HealthSnapshot`](super::HealthSnapshot) plus this config.

use std::time::Duration;

/// Master switch env var. Default ON per owner decision (FMEA Q2).
pub const ENV_ENABLED: &str = "FERROSA_SELFHEAL_ENABLED";
/// Tick interval (seconds).
pub const ENV_TICK_SECS: &str = "FERROSA_SELFHEAL_TICK_SECS";
/// Max remediation attempts per (table, issue) before escalate-and-stop.
pub const ENV_MAX_ATTEMPTS: &str = "FERROSA_SELFHEAL_MAX_ATTEMPTS";
/// Per-(table, issue) cooldown in ticks between attempts.
pub const ENV_COOLDOWN_TICKS: &str = "FERROSA_SELFHEAL_COOLDOWN_TICKS";

/// Deterministic, fixed configuration for the self-heal control loop.
///
/// `Clone`/`PartialEq` so unit tests can pin a config and assert that
/// `decide` is a pure function of `(snapshot, config)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelfHealConfig {
    /// Master switch. When `false`, the loop still **detects and warns**
    /// (loud-on-issue is unconditional) but never selects a remediation.
    pub enabled: bool,
    /// Wall-clock interval between ticks. Used only by the loop's sleep —
    /// never inside `decide`, which is driven by the snapshot's logical
    /// tick counter.
    pub tick_interval: Duration,
    /// Maximum attempts per (table, issue) before the controller escalates
    /// to `health = degraded` and stops retrying that issue (FMEA #5).
    pub max_attempts: u32,
    /// Number of ticks that must elapse after an attempt before the same
    /// (table, issue) is eligible again (FMEA #5 / #10 deterministic
    /// backoff — keyed by attempt count below).
    pub cooldown_ticks: u64,
}

impl Default for SelfHealConfig {
    fn default() -> Self {
        // Conservative defaults (FMEA Q2): slow tick, few attempts, real
        // cooldown so a flapping detector cannot thrash the node.
        Self {
            enabled: true,
            tick_interval: Duration::from_secs(30),
            max_attempts: 3,
            cooldown_ticks: 4,
        }
    }
}

impl SelfHealConfig {
    /// Build from environment, falling back to [`Default`] for any unset or
    /// unparseable knob. Logs the resolved posture loudly so an operator can
    /// never silently disable the subsystem without a record (fail-loud rule).
    pub fn from_env() -> Self {
        let d = Self::default();
        let enabled = std::env::var(ENV_ENABLED)
            .ok()
            .map(|v| parse_bool(&v, d.enabled))
            .unwrap_or(d.enabled);
        let tick_secs = parse_u64(ENV_TICK_SECS, d.tick_interval.as_secs()).max(1);
        let max_attempts = parse_u64(ENV_MAX_ATTEMPTS, d.max_attempts as u64).max(1) as u32;
        let cooldown_ticks = parse_u64(ENV_COOLDOWN_TICKS, d.cooldown_ticks);

        let cfg = Self {
            enabled,
            tick_interval: Duration::from_secs(tick_secs),
            max_attempts,
            cooldown_ticks,
        };
        tracing::info!(
            enabled = cfg.enabled,
            tick_secs = cfg.tick_interval.as_secs(),
            max_attempts = cfg.max_attempts,
            cooldown_ticks = cfg.cooldown_ticks,
            "self-heal: controller configuration resolved"
        );
        cfg
    }
}

fn parse_bool(v: &str, default: bool) -> bool {
    match v.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn parse_u64(env: &str, default: u64) -> u64 {
    std::env::var(env)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_enabled_and_conservative() {
        let d = SelfHealConfig::default();
        assert!(d.enabled, "master switch defaults ON per FMEA Q2");
        assert!(d.max_attempts >= 1);
        assert!(d.tick_interval.as_secs() >= 1);
    }

    #[test]
    fn parse_bool_handles_common_forms() {
        assert!(parse_bool("true", false));
        assert!(parse_bool("ON", false));
        assert!(!parse_bool("0", true));
        assert!(!parse_bool("off", true));
        assert!(parse_bool("garbage", true), "unparseable keeps default");
    }
}
