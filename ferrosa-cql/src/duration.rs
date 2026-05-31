//! CQL `duration` literal parsing and temporal (date/timestamp) arithmetic.
//!
//! A CQL duration is three signed components — months, days, and nanoseconds —
//! because months and days are calendar-relative (a month is not a fixed number
//! of seconds). Literals come in two forms:
//!
//! * **compact**: `1y2mo3w4d5h6m7s8ms9us10ns` (units: y, mo, w, d, h, m, s, ms,
//!   us/µs, ns), and
//! * **ISO-8601**: `P1Y2M3D`, `P1Y2M3DT4H5M6S`, `P1W`, `PT4H`.
//!
//! Arithmetic adds months calendar-aware (clamping to month end), days as
//! calendar days, and nanoseconds as a fixed offset.

use chrono::{DateTime, Days, Months, NaiveDate, TimeZone, Utc};

const NANOS_PER_MICRO: i64 = 1_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MIN;
const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;
const MONTHS_PER_YEAR: i64 = 12;
const DAYS_PER_WEEK: i64 = 7;

/// Cassandra's `date` column type is a `u32` counting days from the Unix epoch
/// with the epoch shifted to the center of the range: `2^31` == 1970-01-01.
const DATE_EPOCH_OFFSET: i64 = 1 << 31;

/// Parsed duration components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DurationComponents {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

/// Add the contribution of `value` of `unit` into the running components.
fn add_unit(acc: &mut (i64, i64, i64), value: i64, unit: &str) -> Result<(), String> {
    let (months, days, nanos) = acc;
    match unit {
        "y" => *months += value * MONTHS_PER_YEAR,
        "mo" => *months += value,
        "w" => *days += value * DAYS_PER_WEEK,
        "d" => *days += value,
        "h" => *nanos += value * NANOS_PER_HOUR,
        "m" => *nanos += value * NANOS_PER_MIN,
        "s" => *nanos += value * NANOS_PER_SEC,
        "ms" => *nanos += value * NANOS_PER_MILLI,
        "us" | "µs" => *nanos += value * NANOS_PER_MICRO,
        "ns" => *nanos += value,
        other => return Err(format!("unknown duration unit '{other}'")),
    }
    Ok(())
}

/// Parse a compact-form duration literal: `<n><unit>` repeated, e.g. `2d`,
/// `1mo3d`, `89h4m48s`, optionally with a leading `-`. The lexer emits this as a
/// single digit-led identifier token.
pub fn parse_compact_duration(input: &str) -> Result<DurationComponents, String> {
    let trimmed = input.trim();
    let negative = trimmed.starts_with('-');
    let s = trimmed.strip_prefix('-').unwrap_or(trimmed);
    let bytes = s.as_bytes();
    if bytes.is_empty() || !bytes[0].is_ascii_digit() {
        return Err(format!("invalid duration literal '{input}'"));
    }
    let mut acc = (0i64, 0i64, 0i64);
    let mut i = 0;
    while i < bytes.len() {
        let num_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == num_start {
            return Err(format!("expected a number in duration '{input}'"));
        }
        let value: i64 = s[num_start..i]
            .parse()
            .map_err(|_| format!("number out of range in '{input}'"))?;
        let unit_start = i;
        while i < bytes.len() && !bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == unit_start {
            return Err(format!("missing unit in duration '{input}'"));
        }
        add_unit(&mut acc, value, &s[unit_start..i].to_ascii_lowercase())?;
    }
    let sign = if negative { -1 } else { 1 };
    Ok(DurationComponents {
        months: i32::try_from(sign * acc.0).map_err(|_| "months out of range".to_string())?,
        days: i32::try_from(sign * acc.1).map_err(|_| "days out of range".to_string())?,
        nanos: sign * acc.2,
    })
}

/// Returns true if `s` is a compact-form duration literal (digit-led and parses
/// cleanly), used to disambiguate `2d` from an ordinary identifier.
pub fn is_compact_duration(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    t.as_bytes().first().is_some_and(u8::is_ascii_digit) && parse_compact_duration(s).is_ok()
}

/// Returns true if `s` is an ISO-8601 duration literal: `P` followed only by
/// digits and the unit letters `YMWDTHS`, with at least one digit AND one unit.
/// The strict check avoids treating column identifiers that merely start with
/// `P` (e.g. `Person`, `PT`, `P0000`) as durations.
pub fn is_iso_duration(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 2 || !(b[0] == b'P' || b[0] == b'p') {
        return false;
    }
    let mut has_digit = false;
    let mut has_unit = false;
    for &c in &b[1..] {
        match c.to_ascii_uppercase() {
            b'0'..=b'9' => has_digit = true,
            b'Y' | b'M' | b'W' | b'D' | b'H' | b'S' => has_unit = true,
            b'T' => {}
            _ => return false,
        }
    }
    has_digit && has_unit
}

/// Parse an ISO-8601 duration: `P[nY][nMo? no — M][nW][nD][T[nH][nM][nS]]`.
/// The `M` before `T` is months; after `T` it is minutes.
pub fn parse_iso_duration(input: &str) -> Result<DurationComponents, String> {
    let s = input.trim();
    let mut chars = s.chars().peekable();
    let negative = matches!(chars.peek(), Some('-'));
    if negative {
        chars.next();
    }
    if chars.next().map(|c| c.eq_ignore_ascii_case(&'P')) != Some(true) {
        return Err(format!("invalid ISO-8601 duration: '{input}'"));
    }

    let mut acc = (0i64, 0i64, 0i64);
    let mut in_time = false;
    let mut num = String::new();
    for c in chars {
        if c == 'T' || c == 't' {
            in_time = true;
            continue;
        }
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        if num.is_empty() {
            return Err(format!("invalid ISO-8601 duration: '{input}'"));
        }
        let value: i64 = num
            .parse()
            .map_err(|_| format!("bad number in '{input}'"))?;
        num.clear();
        let unit = match (c.to_ascii_uppercase(), in_time) {
            ('Y', _) => "y",
            ('M', false) => "mo",
            ('W', _) => "w",
            ('D', _) => "d",
            ('H', true) => "h",
            ('M', true) => "m",
            ('S', true) => "s",
            (other, _) => return Err(format!("invalid ISO-8601 unit '{other}' in '{input}'")),
        };
        add_unit(&mut acc, value, unit)?;
    }
    if !num.is_empty() {
        return Err(format!("trailing number in ISO-8601 duration '{input}'"));
    }

    let sign = if negative { -1 } else { 1 };
    Ok(DurationComponents {
        months: i32::try_from(sign * acc.0).map_err(|_| "months out of range".to_string())?,
        days: i32::try_from(sign * acc.1).map_err(|_| "days out of range".to_string())?,
        nanos: sign * acc.2,
    })
}

/// Apply a signed number of months to a UTC datetime, clamping the day to the
/// target month's length (chrono's `Months` already does this).
fn shift_months_dt(dt: DateTime<Utc>, months: i32) -> Option<DateTime<Utc>> {
    if months >= 0 {
        dt.checked_add_months(Months::new(months as u32))
    } else {
        dt.checked_sub_months(Months::new(months.unsigned_abs()))
    }
}

fn shift_days_dt(dt: DateTime<Utc>, days: i32) -> Option<DateTime<Utc>> {
    if days >= 0 {
        dt.checked_add_days(Days::new(days as u64))
    } else {
        dt.checked_sub_days(Days::new(days.unsigned_abs() as u64))
    }
}

/// Apply a duration to a timestamp (milliseconds since the Unix epoch),
/// `subtract`ing it if requested. Months and days are calendar-aware.
pub fn apply_to_timestamp_millis(
    millis: i64,
    dur: DurationComponents,
    subtract: bool,
) -> Option<i64> {
    let s: i64 = if subtract { -1 } else { 1 };
    let dt = Utc.timestamp_millis_opt(millis).single()?;
    let dt = shift_months_dt(dt, (s as i32).checked_mul(dur.months)?)?;
    let dt = shift_days_dt(dt, (s as i32).checked_mul(dur.days)?)?;
    let dt = dt.checked_add_signed(chrono::Duration::nanoseconds(s * dur.nanos))?;
    Some(dt.timestamp_millis())
}

/// Apply a duration to a `date` (Cassandra `u32`, days from epoch offset by
/// `2^31`). Sub-day nanosecond components are folded into whole days.
pub fn apply_to_date_days(date: u32, dur: DurationComponents, subtract: bool) -> Option<u32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let day_index = i64::from(date) - DATE_EPOCH_OFFSET;
    let base = if day_index >= 0 {
        epoch.checked_add_days(Days::new(day_index as u64))?
    } else {
        epoch.checked_sub_days(Days::new(day_index.unsigned_abs()))?
    };
    let s: i64 = if subtract { -1 } else { 1 };

    let months = (s as i32).checked_mul(dur.months)?;
    let shifted = if months >= 0 {
        base.checked_add_months(Months::new(months as u32))?
    } else {
        base.checked_sub_months(Months::new(months.unsigned_abs()))?
    };
    let extra_days = s.checked_mul(i64::from(dur.days))? + s * (dur.nanos / NANOS_PER_DAY);
    let result = if extra_days >= 0 {
        shifted.checked_add_days(Days::new(extra_days as u64))?
    } else {
        shifted.checked_sub_days(Days::new(extra_days.unsigned_abs()))?
    };

    let delta = result.signed_duration_since(epoch).num_days();
    u32::try_from(delta + DATE_EPOCH_OFFSET).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn compact_durations() {
        assert_eq!(
            parse_compact_duration("2d").unwrap(),
            DurationComponents {
                months: 0,
                days: 2,
                nanos: 0
            }
        );
        assert_eq!(
            parse_compact_duration("1mo").unwrap(),
            DurationComponents {
                months: 1,
                days: 0,
                nanos: 0
            }
        );
        assert_eq!(
            parse_compact_duration("1y").unwrap(),
            DurationComponents {
                months: 12,
                days: 0,
                nanos: 0
            }
        );
        assert_eq!(
            parse_compact_duration("1w").unwrap(),
            DurationComponents {
                months: 0,
                days: 7,
                nanos: 0
            }
        );
        // Compound form, and minutes-vs-month disambiguation by unit text.
        assert_eq!(
            parse_compact_duration("1mo3d").unwrap(),
            DurationComponents {
                months: 1,
                days: 3,
                nanos: 0
            }
        );
        assert_eq!(
            parse_compact_duration("89h4m48s").unwrap().nanos,
            89 * NANOS_PER_HOUR + 4 * NANOS_PER_MIN + 48 * NANOS_PER_SEC
        );
        assert_eq!(
            parse_compact_duration("3ms").unwrap().nanos,
            3 * NANOS_PER_MILLI
        );
        assert!(parse_compact_duration("2zz").is_err());

        assert!(is_compact_duration("2d"));
        assert!(is_compact_duration("89h4m48s"));
        assert!(!is_compact_duration("hello"));
        assert!(!is_compact_duration("2abc"));
        assert!(!is_iso_duration("P0000")); // digits but no unit
    }

    #[test]
    fn iso_basic_and_time() {
        assert_eq!(
            parse_iso_duration("P1Y2M3D").unwrap(),
            DurationComponents {
                months: 14,
                days: 3,
                nanos: 0
            }
        );
        assert_eq!(
            parse_iso_duration("P1W").unwrap(),
            DurationComponents {
                months: 0,
                days: 7,
                nanos: 0
            }
        );
        let d = parse_iso_duration("PT4H5M6S").unwrap();
        assert_eq!(d.months, 0);
        assert_eq!(d.days, 0);
        assert_eq!(
            d.nanos,
            4 * NANOS_PER_HOUR + 5 * NANOS_PER_MIN + 6 * NANOS_PER_SEC
        );
        assert!(parse_iso_duration("1Y").is_err()); // missing P
    }

    #[test]
    fn iso_month_vs_minute() {
        // M before T is months; M after T is minutes.
        let d = parse_iso_duration("P2MT3M").unwrap();
        assert_eq!(d.months, 2);
        assert_eq!(d.nanos, 3 * NANOS_PER_MIN);
    }

    #[test]
    fn timestamp_arithmetic_calendar_months() {
        // 2024-01-31 + 1 month clamps to 2024-02-29 (leap year).
        let jan31 = Utc
            .with_ymd_and_hms(2024, 1, 31, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let plus_month = apply_to_timestamp_millis(
            jan31,
            DurationComponents {
                months: 1,
                days: 0,
                nanos: 0,
            },
            false,
        )
        .unwrap();
        let dt = Utc.timestamp_millis_opt(plus_month).single().unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2024, 2, 29));
    }

    #[test]
    fn timestamp_subtract_days() {
        let base = Utc
            .with_ymd_and_hms(2017, 1, 3, 0, 0, 0)
            .unwrap()
            .timestamp_millis();
        let minus = apply_to_timestamp_millis(
            base,
            DurationComponents {
                months: 0,
                days: 2,
                nanos: 0,
            },
            true,
        )
        .unwrap();
        let dt = Utc.timestamp_millis_opt(minus).single().unwrap();
        assert_eq!((dt.year(), dt.month(), dt.day()), (2017, 1, 1));
    }

    #[test]
    fn date_subtract_days_roundtrips() {
        // 1970-01-03 == DATE_EPOCH_OFFSET + 2.
        let date = (DATE_EPOCH_OFFSET + 2) as u32;
        let minus = apply_to_date_days(
            date,
            DurationComponents {
                months: 0,
                days: 2,
                nanos: 0,
            },
            true,
        )
        .unwrap();
        assert_eq!(minus, DATE_EPOCH_OFFSET as u32); // back to 1970-01-01
    }
}
