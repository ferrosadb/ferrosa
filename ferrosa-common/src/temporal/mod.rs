//! Proleptic-Gregorian dates, times and timestamps, with no third-party
//! date library.
//!
//! Ferrosa used `chrono` for date parsing, formatting and calendar
//! arithmetic. RUSTSEC-2026-0543 declared every chrono release unmaintained,
//! so this module carries the small part of it the engine needs. It keeps
//! chrono 0.4.39's exact observable behaviour:
//!
//! * **Range.** Dates run from `-262143-01-01` to `262142-12-31`, chrono's
//!   `NaiveDate::MIN`/`MAX`. CQL's timestamp bounds and every client driver's
//!   decode limit were set against that range, so it is kept rather than
//!   narrowed to a newer library's `±9999`.
//! * **Parsing.** [`parse`] runs the same item-by-item algorithm as chrono's
//!   `parse_from_str` for the formats the engine uses, including its leniency
//!   (whitespace before numbers, 1-digit months and days, an optional
//!   fraction, a leap second written as `:60`) and its error kinds and
//!   messages.
//! * **Formatting.** `%Y` and RFC 3339 output write years outside `0..=9999`
//!   with a sign (`+10000`, `-0001`), as chrono does.
//!
//! The equivalence was checked differentially against chrono 0.4.39 by a
//! harness that lives outside the workspace (chrono may not appear in the
//! dependency graph, even as a dev-dependency); its vectors are the tests in
//! this module.

pub mod parse;

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// The earliest representable year (chrono's `NaiveDate::MIN`).
pub const MIN_YEAR: i32 = -262_143;
/// The latest representable year (chrono's `NaiveDate::MAX`).
pub const MAX_YEAR: i32 = 262_142;

const SECS_PER_DAY: i64 = 86_400;
const NANOS_PER_SEC: i64 = 1_000_000_000;
const NANOS_PER_DAY: i64 = SECS_PER_DAY * NANOS_PER_SEC;
/// Days from 0000-03-01 to 1970-01-01 in Howard Hinnant's era arithmetic.
const UNIX_EPOCH_SHIFT: i64 = 719_468;
const DAYS_PER_ERA: i64 = 146_097;

/// A calendar date inside chrono's representable range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Date {
    year: i32,
    month: u32,
    day: u32,
}

/// Whether `year` is a Gregorian leap year.
const fn is_leap_year(year: i64) -> bool {
    year.rem_euclid(4) == 0 && (year.rem_euclid(100) != 0 || year.rem_euclid(400) == 0)
}

/// The number of days in `month` (1-based) of `year`.
const fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

impl Date {
    /// The date `year-month-day`, or `None` when it does not exist or lies
    /// outside [`MIN_YEAR`]..=[`MAX_YEAR`].
    pub fn from_ymd(year: i64, month: u32, day: u32) -> Option<Date> {
        let in_range = (i64::from(MIN_YEAR)..=i64::from(MAX_YEAR)).contains(&year);
        if !in_range || !(1..=12).contains(&month) || day == 0 {
            return None;
        }
        if day > days_in_month(year, month) {
            return None;
        }
        let year = i32::try_from(year).ok()?;
        Some(Date { year, month, day })
    }

    /// The date `days` after 1970-01-01 (negative: before), or `None` outside
    /// the representable range.
    pub fn from_days_since_epoch(days: i64) -> Option<Date> {
        let z = days.checked_add(UNIX_EPOCH_SHIFT)?;
        let era = z.div_euclid(DAYS_PER_ERA);
        let doe = z.rem_euclid(DAYS_PER_ERA);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = doy - (153 * mp + 2) / 5 + 1;
        let month = if mp < 10 { mp + 3 } else { mp - 9 };
        let year = era.checked_mul(400)?.checked_add(yoe)? + i64::from(month <= 2);
        Date::from_ymd(year, month as u32, day as u32)
    }

    /// Days from 1970-01-01 to this date (negative before it).
    pub fn days_since_epoch(self) -> i64 {
        let year = i64::from(self.year) - i64::from(self.month <= 2);
        let era = year.div_euclid(400);
        let yoe = year.rem_euclid(400);
        let mp = i64::from((self.month + 9) % 12);
        let doy = (153 * mp + 2) / 5 + i64::from(self.day) - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * DAYS_PER_ERA + doe - UNIX_EPOCH_SHIFT
    }

    /// The year.
    pub fn year(self) -> i32 {
        self.year
    }

    /// The month, 1-based.
    pub fn month(self) -> u32 {
        self.month
    }

    /// The day of the month, 1-based.
    pub fn day(self) -> u32 {
        self.day
    }

    /// This date shifted by `months` calendar months, the day clamped to the
    /// target month's length (Jan 31 + 1 month = Feb 28/29), or `None` when
    /// the result leaves the representable range.
    pub fn checked_add_months(self, months: i64) -> Option<Date> {
        let index = i64::from(self.year)
            .checked_mul(12)?
            .checked_add(i64::from(self.month) - 1)?
            .checked_add(months)?;
        let year = index.div_euclid(12);
        let month = index.rem_euclid(12) as u32 + 1;
        let day = self.day.min(days_in_month(year, month));
        Date::from_ymd(year, month, day)
    }

    /// This date shifted by `days` days, or `None` outside the range.
    pub fn checked_add_days(self, days: i64) -> Option<Date> {
        Date::from_days_since_epoch(self.days_since_epoch().checked_add(days)?)
    }
}

/// Writes a year the way chrono's `%Y` and RFC 3339 do: four zero-padded
/// digits inside `0..=9999`, otherwise an explicit sign and at least four
/// digits.
fn write_year(f: &mut impl fmt::Write, year: i32) -> fmt::Result {
    if (0..=9999).contains(&year) {
        write!(f, "{year:04}")
    } else {
        write!(f, "{year:+05}")
    }
}

impl fmt::Display for Date {
    /// `%Y-%m-%d`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_year(f, self.year)?;
        write!(f, "-{:02}-{:02}", self.month, self.day)
    }
}

/// A time of day. `frac` exceeds a second only for a parsed leap second
/// (`23:59:60.5` is `secs = 86399, frac = 1.5e9`), as in chrono.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Time {
    secs: u32,
    frac: u32,
}

impl Time {
    /// `hour:minute:second.nano`, where a leap second is `second = 59` with
    /// `nano >= 1e9`. `None` when a field is out of range.
    pub fn from_hms_nano(hour: u32, minute: u32, second: u32, nano: u32) -> Option<Time> {
        let leap_ok = nano < 2_000_000_000 && (nano < 1_000_000_000 || second == 59);
        if hour >= 24 || minute >= 60 || second >= 60 || !leap_ok {
            return None;
        }
        Some(Time {
            secs: hour * 3600 + minute * 60 + second,
            frac: nano,
        })
    }

    /// Nanoseconds since midnight; a leap second counts past `:59`.
    pub fn nanos_since_midnight(self) -> i64 {
        i64::from(self.secs) * NANOS_PER_SEC + i64::from(self.frac)
    }
}

/// A date and time with no offset (UTC when it names an instant).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DateTime {
    date: Date,
    time: Time,
}

impl DateTime {
    /// The date at `time`.
    pub fn new(date: Date, time: Time) -> DateTime {
        DateTime { date, time }
    }

    /// The instant `secs` seconds and `nanos` (< 1e9) after the Unix epoch,
    /// or `None` outside the representable range.
    pub fn from_timestamp(secs: i64, nanos: u32) -> Option<DateTime> {
        if i64::from(nanos) >= NANOS_PER_SEC {
            return None;
        }
        let date = Date::from_days_since_epoch(secs.div_euclid(SECS_PER_DAY))?;
        let secs = secs.rem_euclid(SECS_PER_DAY) as u32;
        Some(DateTime {
            date,
            time: Time { secs, frac: nanos },
        })
    }

    /// The instant `millis` milliseconds after the Unix epoch.
    pub fn from_timestamp_millis(millis: i64) -> Option<DateTime> {
        let nanos = millis.rem_euclid(1000) as u32 * 1_000_000;
        DateTime::from_timestamp(millis.div_euclid(1000), nanos)
    }

    /// The date part.
    pub fn date(self) -> Date {
        self.date
    }

    /// The time-of-day part.
    pub fn time(self) -> Time {
        self.time
    }

    /// Whole seconds since the Unix epoch.
    pub fn timestamp(self) -> i64 {
        self.date.days_since_epoch() * SECS_PER_DAY + i64::from(self.time.secs)
    }

    /// Milliseconds since the Unix epoch (a leap second counts past `:59`,
    /// as chrono's `timestamp_millis` does).
    pub fn timestamp_millis(self) -> i64 {
        self.timestamp() * 1000 + i64::from(self.time.frac / 1_000_000)
    }

    /// Microseconds since the Unix epoch.
    pub fn timestamp_micros(self) -> i64 {
        self.timestamp() * 1_000_000 + i64::from(self.time.frac / 1000)
    }

    /// This instant moved by a whole number of calendar months, keeping the
    /// time of day; `None` outside the range.
    pub fn checked_add_months(self, months: i64) -> Option<DateTime> {
        let date = self.date.checked_add_months(months)?;
        Some(DateTime { date, ..self })
    }

    /// This instant moved by whole calendar days; `None` outside the range.
    pub fn checked_add_days(self, days: i64) -> Option<DateTime> {
        let date = self.date.checked_add_days(days)?;
        Some(DateTime { date, ..self })
    }

    /// This instant moved by `nanos` nanoseconds; `None` outside the range.
    /// Only defined on instants (no leap second), which is all callers hold.
    pub fn checked_add_nanos(self, nanos: i64) -> Option<DateTime> {
        assert!(
            i64::from(self.time.frac) < NANOS_PER_SEC,
            "nanosecond arithmetic on a leap second"
        );
        let day_nanos = i128::from(self.time.nanos_since_midnight()) + i128::from(nanos);
        let carry_days = i64::try_from(day_nanos.div_euclid(i128::from(NANOS_PER_DAY))).ok()?;
        let in_day = day_nanos.rem_euclid(i128::from(NANOS_PER_DAY)) as i64;
        let date = self.date.checked_add_days(carry_days)?;
        let time = Time {
            secs: (in_day / NANOS_PER_SEC) as u32,
            frac: (in_day % NANOS_PER_SEC) as u32,
        };
        Some(DateTime { date, time })
    }

    /// `%Y-%m-%d %H:%M:%S` (no fraction).
    pub fn format_ymd_hms(self) -> String {
        let mut out = String::with_capacity(19);
        self.write_ymd_hms(&mut out, ' ')
            .expect("writing to a String cannot fail");
        out
    }

    /// `%Y%m%d-%H%M%S`, the compact form used in run-directory names.
    pub fn format_compact(self) -> String {
        let mut out = String::with_capacity(15);
        let (hour, minute, second) = self.hms();
        write_year(&mut out, self.date.year).expect("writing to a String cannot fail");
        let (month, day) = (self.date.month, self.date.day);
        let rest = format!("{month:02}{day:02}-{hour:02}{minute:02}{second:02}");
        out.push_str(&rest);
        out
    }

    /// RFC 3339 in UTC exactly as chrono's `DateTime<Utc>::to_rfc3339`:
    /// `T` separator, a fraction of 0, 3, 6 or 9 digits (the fewest that are
    /// exact), and a `+00:00` offset.
    pub fn to_rfc3339(self) -> String {
        let mut out = String::with_capacity(32);
        self.write_ymd_hms(&mut out, 'T')
            .expect("writing to a String cannot fail");
        let nano = self.time.frac % 1_000_000_000;
        let fraction = match nano {
            0 => String::new(),
            n if n % 1_000_000 == 0 => format!(".{:03}", n / 1_000_000),
            n if n % 1000 == 0 => format!(".{:06}", n / 1000),
            n => format!(".{n:09}"),
        };
        out.push_str(&fraction);
        out.push_str("+00:00");
        out
    }

    /// Hour, minute and second; a leap second reads as `:59`.
    fn hms(self) -> (u32, u32, u32) {
        let secs = self.time.secs;
        (secs / 3600, secs / 60 % 60, secs % 60)
    }

    /// `%Y-%m-%d<sep>%H:%M:%S`. A leap second prints as `:60`, as chrono's
    /// `%S` and `write_rfc3339` both do.
    fn write_ymd_hms(self, out: &mut String, sep: char) -> fmt::Result {
        use fmt::Write;
        let (hour, minute, mut second) = self.hms();
        if self.time.frac >= 1_000_000_000 {
            second += 1;
        }
        write!(out, "{}{sep}{hour:02}:{minute:02}:{second:02}", self.date)
    }
}

/// Nanoseconds since the Unix epoch of a system clock reading (negative for a
/// clock set before 1970).
pub fn unix_nanos(at: SystemTime) -> i128 {
    match at.duration_since(UNIX_EPOCH) {
        Ok(after) => after.as_nanos() as i128,
        Err(before) => -(before.duration().as_nanos() as i128),
    }
}

/// The current instant, from the system clock.
///
/// # Panics
/// When the clock reads outside the representable range (years ±262142),
/// which no real clock does.
pub fn now() -> DateTime {
    let nanos = unix_nanos(SystemTime::now());
    let secs = i64::try_from(nanos.div_euclid(i128::from(NANOS_PER_SEC)))
        .expect("system clock within i64 seconds");
    let frac = nanos.rem_euclid(i128::from(NANOS_PER_SEC)) as u32;
    DateTime::from_timestamp(secs, frac).expect("system clock within the representable range")
}

#[cfg(test)]
mod tests;
