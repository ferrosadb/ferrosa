//! Restore-on-boot intent: the link between "a restore was requested" and
//! "this node actually restores when it next starts".
//!
//! # Why this exists
//!
//! [`StorageEngine::open_from_snapshot_with_store`] implements restore, and
//! `POST /api/restore` validates a restore request and replies
//! "restart the node to complete restore". Nothing connected the two: the HTTP
//! handler persisted nothing, and startup always took the ordinary open path.
//! A node would restart cleanly and come back serving pre-restore data while
//! the caller believed the restore had happened.
//!
//! This module supplies the missing link, using the env-var contract already
//! documented by the DBaaS layer's `GuestMetadata`:
//!
//! - `FERROSA_RESTORE_SNAPSHOT` — snapshot name to restore from
//! - `FERROSA_RESTORE_POINT_IN_TIME` — optional RFC 3339 UTC replay cutoff
//! - `FERROSA_RESTORE_FORCE` — allow a snapshot taken by a different node
//!
//! # Applying at most once
//!
//! An env var survives a reboot, so a naive implementation would re-restore on
//! every start and silently discard everything written after the first
//! restore. [`RestoreIntent::already_applied`] guards against that: after a
//! successful restore the node records the intent in a marker file, and a
//! later boot carrying the same intent skips it.
//!
//! A *different* intent (new snapshot, or a new point in time) is applied
//! normally — that is a fresh restore request, not a repeat of the last one.
//!
//! [`StorageEngine::open_from_snapshot_with_store`]: crate::StorageEngine::open_from_snapshot_with_store

use std::path::{Path, PathBuf};

/// Env var naming the snapshot to restore from at startup.
pub const ENV_RESTORE_SNAPSHOT: &str = "FERROSA_RESTORE_SNAPSHOT";
/// Env var carrying the RFC 3339 UTC point-in-time replay cutoff.
pub const ENV_RESTORE_POINT_IN_TIME: &str = "FERROSA_RESTORE_POINT_IN_TIME";
/// Env var allowing restore of a snapshot taken by a different node.
pub const ENV_RESTORE_FORCE: &str = "FERROSA_RESTORE_FORCE";

/// Name of the marker recording the last successfully applied intent.
const MARKER_FILE: &str = ".restore-applied";

/// A restore requested for the next node start.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreIntent {
    /// Snapshot to restore from.
    pub snapshot: String,
    /// Optional replay cutoff, as originally supplied (RFC 3339 UTC).
    pub point_in_time: Option<String>,
    /// Allow a snapshot whose `node_id` differs from this node's.
    pub force: bool,
}

impl RestoreIntent {
    /// Read the intent from the process environment.
    ///
    /// Returns `Ok(None)` when no restore is requested. Returns an error when
    /// the request is present but malformed — a restore asked for with an
    /// unparsable timestamp must fail loudly, not silently restore to the
    /// snapshot boundary instead.
    pub fn from_env() -> ferrosa_common::Result<Option<Self>> {
        Self::from_vars(
            std::env::var(ENV_RESTORE_SNAPSHOT).ok(),
            std::env::var(ENV_RESTORE_POINT_IN_TIME).ok(),
            std::env::var(ENV_RESTORE_FORCE).ok(),
        )
    }

    /// Build an intent from explicit values. Pure, so the rules are testable
    /// without mutating (and racing on) process environment.
    pub fn from_vars(
        snapshot: Option<String>,
        point_in_time: Option<String>,
        force: Option<String>,
    ) -> ferrosa_common::Result<Option<Self>> {
        let Some(snapshot) = snapshot
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
        else {
            // A point-in-time without a snapshot is a misconfiguration, not a
            // restore. Say so rather than ignoring it.
            if point_in_time.is_some_and(|p| !p.trim().is_empty()) {
                return Err(ferrosa_common::Error::InvalidFormat(format!(
                    "{ENV_RESTORE_POINT_IN_TIME} is set but {ENV_RESTORE_SNAPSHOT} is not; \
                     a point-in-time restore needs a snapshot to replay from"
                )));
            }
            return Ok(None);
        };

        let point_in_time = point_in_time
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty());

        // Validate now, at startup, rather than after downloading SSTables.
        if let Some(pit) = &point_in_time {
            parse_rfc3339_micros(pit)?;
        }

        let force = force.is_some_and(|v| {
            let v = v.trim().to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes" || v == "on"
        });

        Ok(Some(Self {
            snapshot,
            point_in_time,
            force,
        }))
    }

    /// The replay cutoff as Unix-epoch microseconds, which is what
    /// `open_from_snapshot_with_store` expects.
    pub fn point_in_time_micros(&self) -> ferrosa_common::Result<Option<i64>> {
        self.point_in_time
            .as_deref()
            .map(parse_rfc3339_micros)
            .transpose()
    }

    /// A stable identity for this intent, used as the marker contents.
    fn fingerprint(&self) -> String {
        format!(
            "{}\n{}\n{}",
            self.snapshot,
            self.point_in_time.as_deref().unwrap_or(""),
            self.force
        )
    }

    fn marker_path(data_dir: &Path) -> PathBuf {
        data_dir.join(MARKER_FILE)
    }

    /// Whether this exact intent has already been applied in `data_dir`.
    ///
    /// Without this check the env var would re-trigger a restore on every
    /// boot, discarding all writes made since the first one.
    pub fn already_applied(&self, data_dir: &Path) -> bool {
        std::fs::read_to_string(Self::marker_path(data_dir))
            .map(|recorded| recorded == self.fingerprint())
            .unwrap_or(false)
    }

    /// Record this intent as applied so later boots skip it.
    pub fn mark_applied(&self, data_dir: &Path) -> ferrosa_common::Result<()> {
        std::fs::create_dir_all(data_dir).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "failed to create data dir {}: {e}",
                data_dir.display()
            ))
        })?;
        std::fs::write(Self::marker_path(data_dir), self.fingerprint()).map_err(|e| {
            ferrosa_common::Error::InvalidFormat(format!(
                "restore succeeded but the applied-marker could not be written to {}: {e}. \
                 Refusing to continue would lose the restore; but leaving this unwritten \
                 means the next boot would restore again and discard subsequent writes.",
                Self::marker_path(data_dir).display()
            ))
        })
    }
}

/// Parse an RFC 3339 UTC timestamp into Unix-epoch microseconds.
///
/// Deliberately strict: accepts `YYYY-MM-DDTHH:MM:SS[.fraction](Z|+00:00)`,
/// which is the form the DBaaS layer emits. Anything else is rejected rather
/// than coerced, because a silently mis-parsed cutoff would restore to the
/// wrong moment — worse than refusing.
///
/// Implemented locally because `ferrosa-storage` does not depend on `chrono`
/// and a date crate is not worth pulling in for one parse.
pub fn parse_rfc3339_micros(s: &str) -> ferrosa_common::Result<i64> {
    let bad = |why: &str| {
        ferrosa_common::Error::InvalidFormat(format!(
            "invalid {ENV_RESTORE_POINT_IN_TIME} {s:?}: {why}. \
             Expected RFC 3339 UTC, e.g. 2026-08-05T12:00:00Z"
        ))
    };

    // Split off the zone; only UTC is accepted.
    let body = if let Some(rest) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        rest
    } else if let Some(rest) = s.strip_suffix("+00:00").or_else(|| s.strip_suffix("+0000")) {
        rest
    } else {
        return Err(bad("must be UTC (trailing 'Z' or '+00:00')"));
    };

    let (date, time) = body
        .split_once('T')
        .or_else(|| body.split_once('t'))
        .ok_or_else(|| bad("missing 'T' between date and time"))?;

    let mut d = date.split('-');
    let (y, mo, da) = match (d.next(), d.next(), d.next(), d.next()) {
        (Some(y), Some(mo), Some(da), None) => (y, mo, da),
        _ => return Err(bad("date must be YYYY-MM-DD")),
    };

    // Fractional seconds are optional.
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, Some(frac)),
        None => (time, None),
    };
    let mut t = hms.split(':');
    let (h, mi, se) = match (t.next(), t.next(), t.next(), t.next()) {
        (Some(h), Some(mi), Some(se), None) => (h, mi, se),
        _ => return Err(bad("time must be HH:MM:SS")),
    };

    let num = |v: &str, what: &str, width: usize| -> ferrosa_common::Result<i64> {
        if v.len() != width || !v.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad(&format!("{what} must be {width} digits")));
        }
        v.parse::<i64>().map_err(|_| bad(what))
    };

    let (y, mo, da) = (num(y, "year", 4)?, num(mo, "month", 2)?, num(da, "day", 2)?);
    let (h, mi, se) = (
        num(h, "hour", 2)?,
        num(mi, "minute", 2)?,
        num(se, "second", 2)?,
    );

    if !(1..=12).contains(&mo) {
        return Err(bad("month out of range"));
    }
    if !(1..=31).contains(&da) {
        return Err(bad("day out of range"));
    }
    if h > 23 || mi > 59 || se > 60 {
        return Err(bad("time component out of range"));
    }

    // Fraction -> microseconds, padded/truncated to 6 digits.
    let micros_frac = match frac {
        Some(f) => {
            if f.is_empty() || !f.bytes().all(|b| b.is_ascii_digit()) {
                return Err(bad("fractional seconds must be digits"));
            }
            let mut six = f.to_string();
            six.truncate(6);
            while six.len() < 6 {
                six.push('0');
            }
            six.parse::<i64>().unwrap_or(0)
        }
        None => 0,
    };

    let days = days_from_civil(y, mo, da);
    let secs = days * 86_400 + h * 3_600 + mi * 60 + se;
    Ok(secs * 1_000_000 + micros_frac)
}

/// Days since 1970-01-01 for a proleptic Gregorian date.
///
/// Howard Hinnant's `days_from_civil`, which is exact for all years in range
/// and avoids a calendar dependency.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_and_known_instants() {
        assert_eq!(parse_rfc3339_micros("1970-01-01T00:00:00Z").unwrap(), 0);
        // 2026-08-05T12:00:00Z — cross-checked against `date -u -d ... +%s`.
        assert_eq!(
            parse_rfc3339_micros("2026-08-05T12:00:00Z").unwrap(),
            1_785_931_200_000_000
        );
        // Leap day must not shift the result.
        assert_eq!(
            parse_rfc3339_micros("2024-02-29T00:00:00Z").unwrap(),
            1_709_164_800_000_000
        );
    }

    #[test]
    fn fractional_seconds_pad_and_truncate() {
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T00:00:00.5Z").unwrap(),
            500_000
        );
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T00:00:00.123456Z").unwrap(),
            123_456
        );
        // Sub-microsecond precision is truncated, not rounded.
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T00:00:00.1234569Z").unwrap(),
            123_456
        );
    }

    #[test]
    fn accepts_explicit_utc_offset() {
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T00:00:00+00:00").unwrap(),
            0
        );
    }

    #[test]
    fn rejects_malformed_and_non_utc() {
        for bad in [
            "2026-08-05T12:00:00",       // no zone
            "2026-08-05T12:00:00-05:00", // not UTC
            "2026-08-05 12:00:00Z",      // space instead of T
            "2026-8-5T12:00:00Z",        // unpadded
            "2026-13-01T00:00:00Z",      // month out of range
            "2026-08-05T25:00:00Z",      // hour out of range
            "not-a-date",
        ] {
            assert!(
                parse_rfc3339_micros(bad).is_err(),
                "should have rejected {bad:?}"
            );
        }
    }

    #[test]
    fn no_snapshot_means_no_intent() {
        assert_eq!(RestoreIntent::from_vars(None, None, None).unwrap(), None);
        // Whitespace-only is treated as unset.
        assert_eq!(
            RestoreIntent::from_vars(Some("  ".into()), None, None).unwrap(),
            None
        );
    }

    #[test]
    fn point_in_time_without_snapshot_is_an_error() {
        let err =
            RestoreIntent::from_vars(None, Some("2026-08-05T12:00:00Z".into()), None).unwrap_err();
        assert!(err.to_string().contains(ENV_RESTORE_SNAPSHOT));
    }

    #[test]
    fn malformed_point_in_time_is_rejected_at_startup() {
        let err = RestoreIntent::from_vars(Some("snap".into()), Some("yesterday".into()), None)
            .unwrap_err();
        assert!(err.to_string().contains("RFC 3339"));
    }

    #[test]
    fn force_parsing() {
        let mk = |f: Option<&str>| {
            RestoreIntent::from_vars(Some("s".into()), None, f.map(String::from))
                .unwrap()
                .unwrap()
                .force
        };
        assert!(!mk(None));
        assert!(!mk(Some("0")));
        assert!(!mk(Some("false")));
        assert!(mk(Some("1")));
        assert!(mk(Some("true")));
        assert!(mk(Some("TRUE")));
    }

    #[test]
    fn marker_makes_the_same_intent_apply_once() {
        let dir = std::env::temp_dir().join(format!("restore-intent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let intent = RestoreIntent::from_vars(Some("snap-1".into()), None, None)
            .unwrap()
            .unwrap();
        assert!(!intent.already_applied(&dir), "not applied yet");

        intent.mark_applied(&dir).unwrap();
        assert!(intent.already_applied(&dir), "should be marked applied");

        // A different snapshot is a new request and must still run.
        let other = RestoreIntent::from_vars(Some("snap-2".into()), None, None)
            .unwrap()
            .unwrap();
        assert!(
            !other.already_applied(&dir),
            "different snapshot must apply"
        );

        // Same snapshot but a new point-in-time is also a new request.
        let repointed = RestoreIntent::from_vars(
            Some("snap-1".into()),
            Some("2026-08-05T12:00:00Z".into()),
            None,
        )
        .unwrap()
        .unwrap();
        assert!(
            !repointed.already_applied(&dir),
            "new point-in-time must apply"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
