//! Text → date/time parsing with chrono 0.4.39's `parse_from_str` and
//! `parse_from_rfc3339` semantics, for the formats the engine accepts.
//!
//! A format is a slice of [`Item`]s, the same decomposition chrono's
//! `StrftimeItems` produces: `"%Y-%m-%d %H:%M:%S%.f"` is
//! [`formats::YMD_HMS_FRAC`]. Each item consumes input exactly as chrono's
//! corresponding item does:
//!
//! * numbers skip leading whitespace, then take 1..=2 digits (1..=4 for an
//!   unsigned year; a signed year takes any number);
//! * a space in the format skips any run of whitespace, including none;
//! * `%.f` is optional: a `.` then at least one digit, only the first nine
//!   significant, the rest consumed and ignored;
//! * `%z` is `±hh[:]mm` (a `:` or whitespace may separate them, and U+2212
//!   MINUS SIGN counts as `-`); `%#z` also takes `Z` and a bare `±hh`;
//! * any input left over is an error.

use std::fmt;

use super::{Date, DateTime, Time};

/// Why a parse failed. `Display` matches chrono's `ParseError` text, which
/// callers put in their own error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// A field was outside its range, or the result left the calendar range.
    OutOfRange,
    /// The fields name no valid date or time.
    Impossible,
    /// A required field was missing.
    NotEnough,
    /// The input held a character the format did not allow.
    Invalid,
    /// The input ended early.
    TooShort,
    /// Input remained after the format was complete.
    TooLong,
}

impl ParseError {
    /// chrono's message for this error kind. Callers that only need the text
    /// use this rather than `Display`, which keeps them free of a trait
    /// instance (the Lean extraction cannot elaborate opaque ones).
    pub fn message(self) -> &'static str {
        match self {
            ParseError::OutOfRange => "input is out of range",
            ParseError::Impossible => "no possible date and time matching input",
            ParseError::NotEnough => "input is not enough for unique date and time",
            ParseError::Invalid => "input contains invalid characters",
            ParseError::TooShort => "premature end of input",
            ParseError::TooLong => "trailing input",
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for ParseError {}

type Result<T> = std::result::Result<T, ParseError>;

/// One element of a format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Item {
    /// `%Y`.
    Year,
    /// `%m`.
    Month,
    /// `%d`.
    Day,
    /// `%H`.
    Hour,
    /// `%M`.
    Minute,
    /// `%S`.
    Second,
    /// `%.f`.
    Fraction,
    /// `%z`.
    Offset,
    /// `%#z`: also `Z`, and an offset without minutes.
    OffsetPermissive,
    /// Literal text, matched exactly.
    Literal(&'static str),
    /// Whitespace in the format: skips any whitespace in the input.
    Space,
}

/// The formats the engine parses, spelled as chrono strftime strings.
pub mod formats {
    use super::Item::{self, *};

    /// `%Y-%m-%d`.
    pub const YMD: &[Item] = &[Year, Literal("-"), Month, Literal("-"), Day];
    /// `%H:%M:%S`.
    pub const HMS: &[Item] = &[Hour, Literal(":"), Minute, Literal(":"), Second];
    /// `%H:%M:%S%.f`.
    pub const HMS_FRAC: &[Item] = &[Hour, Literal(":"), Minute, Literal(":"), Second, Fraction];
    /// `%Y-%m-%d %H:%M:%S`.
    pub const YMD_HMS: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Space,
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
    ];
    /// `%Y-%m-%d %H:%M:%S%.f`.
    pub const YMD_HMS_FRAC: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Space,
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
        Fraction,
    ];
    /// `%Y-%m-%dT%H:%M:%S`.
    pub const YMD_T_HMS: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Literal("T"),
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
    ];
    /// `%Y-%m-%dT%H:%M:%S%.f`.
    pub const YMD_T_HMS_FRAC: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Literal("T"),
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
        Fraction,
    ];
    /// `%Y-%m-%d %H:%M:%S%z`.
    pub const YMD_HMS_OFFSET: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Space,
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
        Offset,
    ];
    /// `%Y-%m-%d %H:%M:%S%.f%z`.
    pub const YMD_HMS_FRAC_OFFSET: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Space,
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
        Fraction,
        Offset,
    ];
    /// `%Y-%m-%d %H:%M:%S%.f%#z`.
    pub const YMD_HMS_FRAC_OFFSET_PERMISSIVE: &[Item] = &[
        Year,
        Literal("-"),
        Month,
        Literal("-"),
        Day,
        Space,
        Hour,
        Literal(":"),
        Minute,
        Literal(":"),
        Second,
        Fraction,
        OffsetPermissive,
    ];
}

/// The timestamp forms SQL text takes, in the order the parsers try them:
/// `%Y-%m-%d %H:%M:%S%.f`, `%Y-%m-%dT%H:%M:%S%.f`, `%Y-%m-%d %H:%M:%S`,
/// `%Y-%m-%dT%H:%M:%S`.
pub const SQL_TIMESTAMP: &[&[Item]] = &[
    formats::YMD_HMS_FRAC,
    formats::YMD_T_HMS_FRAC,
    formats::YMD_HMS,
    formats::YMD_T_HMS,
];

/// The fields a parse has read so far (chrono's `Parsed`, reduced to the
/// fields these formats can set).
#[derive(Debug, Default)]
struct Fields {
    year: Option<i64>,
    month: Option<u32>,
    day: Option<u32>,
    hour: Option<u32>,
    minute: Option<u32>,
    second: Option<u32>,
    nanosecond: Option<u32>,
    offset: Option<i32>,
}

/// Stores a field, rejecting a second, different value (chrono's
/// `set_if_consistent`).
fn set<T: PartialEq + Copy>(slot: &mut Option<T>, value: T) -> Result<()> {
    match *slot {
        Some(old) if old != value => Err(ParseError::Impossible),
        _ => {
            *slot = Some(value);
            Ok(())
        }
    }
}

/// Stores a numeric field after checking it lies in `range`.
fn set_in(slot: &mut Option<u32>, value: i64, range: std::ops::RangeInclusive<i64>) -> Result<()> {
    if !range.contains(&value) {
        return Err(ParseError::OutOfRange);
    }
    set(slot, value as u32)
}

impl Fields {
    fn set_numeric(&mut self, item: Item, value: i64) -> Result<()> {
        match item {
            Item::Year => {
                let year = i32::try_from(value).map_err(|_| ParseError::OutOfRange)?;
                set(&mut self.year, i64::from(year))
            }
            Item::Month => set_in(&mut self.month, value, 1..=12),
            Item::Day => set_in(&mut self.day, value, 1..=31),
            Item::Hour => set_in(&mut self.hour, value, 0..=23),
            Item::Minute => set_in(&mut self.minute, value, 0..=59),
            Item::Second => set_in(&mut self.second, value, 0..=60),
            _ => unreachable!("{item:?} is not numeric"),
        }
    }

    fn date(&self) -> Result<Date> {
        match (self.year, self.month, self.day) {
            (Some(year), Some(month), Some(day)) => {
                Date::from_ymd(year, month, day).ok_or(ParseError::OutOfRange)
            }
            _ => Err(ParseError::NotEnough),
        }
    }

    fn time(&self) -> Result<Time> {
        let hour = self.hour.ok_or(ParseError::NotEnough)?;
        let minute = self.minute.ok_or(ParseError::NotEnough)?;
        let (second, leap) = match self.second.unwrap_or(0) {
            60 => (59, 1_000_000_000),
            s => (s, 0),
        };
        let nano = match self.nanosecond {
            Some(_) if self.second.is_none() => return Err(ParseError::NotEnough),
            Some(n) => n,
            None => 0,
        };
        Time::from_hms_nano(hour, minute, second, leap + nano).ok_or(ParseError::OutOfRange)
    }

    fn datetime(&self) -> Result<DateTime> {
        let date = self.date();
        let time = self.time();
        Ok(DateTime::new(date?, time?))
    }

    /// The UTC instant the fields name (chrono's `to_datetime` followed by
    /// reading the instant).
    fn instant(&self) -> Result<DateTime> {
        let offset = self.offset.ok_or(ParseError::NotEnough)?;
        let local = self.datetime()?;
        if offset.unsigned_abs() >= 86_400 {
            return Err(ParseError::OutOfRange);
        }
        shift_by_offset(local, offset).ok_or(ParseError::Impossible)
    }
}

/// Converts a local date-time at `offset` seconds east of UTC to UTC, keeping
/// a leap second's fraction (chrono's `checked_sub_offset`).
fn shift_by_offset(local: DateTime, offset: i32) -> Option<DateTime> {
    let secs = i64::from(local.time.secs) - i64::from(offset);
    let date = local.date.checked_add_days(secs.div_euclid(86_400))?;
    let time = Time {
        secs: secs.rem_euclid(86_400) as u32,
        frac: local.time.frac,
    };
    Some(DateTime::new(date, time))
}

/// Reads `min..=max` ASCII digits (chrono's `scan::number`).
fn number(s: &str, min: usize, max: usize) -> Result<(&str, i64)> {
    let bytes = s.as_bytes();
    if bytes.len() < min {
        return Err(ParseError::TooShort);
    }
    let mut n = 0i64;
    for (i, &c) in bytes.iter().take(max).enumerate() {
        if !c.is_ascii_digit() {
            return if i < min {
                Err(ParseError::Invalid)
            } else {
                Ok((&s[i..], n))
            };
        }
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add(i64::from(c - b'0')))
            .ok_or(ParseError::OutOfRange)?;
    }
    Ok((&s[max.min(bytes.len())..], n))
}

/// Reads a numeric item's value (chrono's `Item::Numeric` arm).
fn numeric(s: &str, item: Item) -> Result<(&str, i64)> {
    let s = s.trim_start();
    if item != Item::Year {
        return number(s, 1, 2);
    }
    if let Some(rest) = s.strip_prefix('-') {
        let (rest, v) = number(rest, 1, usize::MAX)?;
        let v = 0i64.checked_sub(v).ok_or(ParseError::OutOfRange)?;
        return Ok((rest, v));
    }
    match s.strip_prefix('+') {
        Some(rest) => number(rest, 1, usize::MAX),
        None => number(s, 1, 4),
    }
}

/// Reads the digits after a `.` (chrono's `scan::nanosecond`).
fn nanosecond(s: &str) -> Result<(&str, u32)> {
    const SCALE: [i64; 10] = [
        0,
        100_000_000,
        10_000_000,
        1_000_000,
        100_000,
        10_000,
        1_000,
        100,
        10,
        1,
    ];
    let before = s.len();
    let (s, v) = number(s, 1, 9)?;
    let v = v * SCALE[before - s.len()];
    let s = s.trim_start_matches(|c: char| c.is_ascii_digit());
    Ok((s, v as u32))
}

/// Reads one character `c` (chrono's `scan::char`).
fn char(s: &str, c: u8) -> Result<&str> {
    match s.as_bytes().first() {
        Some(&b) if b == c => Ok(&s[1..]),
        Some(_) => Err(ParseError::Invalid),
        None => Err(ParseError::TooShort),
    }
}

/// The separator rule between offset hours and minutes.
#[derive(Clone, Copy)]
enum OffsetColon {
    /// `%z`: any run of `:` and whitespace, including none.
    ColonOrSpace,
    /// RFC 3339: exactly one `:`.
    Required,
}

/// Reads `±hh[sep]mm` as seconds east of UTC (chrono's
/// `scan::timezone_offset` with the minus-sign extension enabled).
fn offset(
    mut s: &str,
    colon: OffsetColon,
    zulu: bool,
    missing_minutes: bool,
) -> Result<(&str, i32)> {
    if zulu && matches!(s.as_bytes().first(), Some(b'Z' | b'z')) {
        return Ok((&s[1..], 0));
    }
    let negative = match s.chars().next() {
        Some('+') => false,
        Some('-' | '\u{2212}') => true,
        Some(_) => return Err(ParseError::Invalid),
        None => return Err(ParseError::TooShort),
    };
    s = &s[s.chars().next().map_or(0, char::len_utf8)..];
    let hours = match s.as_bytes() {
        [h1 @ b'0'..=b'9', h2 @ b'0'..=b'9', ..] => i32::from((h1 - b'0') * 10 + (h2 - b'0')),
        [_, _, ..] => return Err(ParseError::Invalid),
        _ => return Err(ParseError::TooShort),
    };
    s = &s[2..];
    s = match colon {
        OffsetColon::ColonOrSpace => s.trim_start_matches(|c: char| c == ':' || c.is_whitespace()),
        OffsetColon::Required => char(s, b':')?,
    };
    let minutes = match s.as_bytes() {
        [m1 @ b'0'..=b'5', m2 @ b'0'..=b'9', ..] => i32::from((m1 - b'0') * 10 + (m2 - b'0')),
        [b'6'..=b'9', b'0'..=b'9', ..] => return Err(ParseError::OutOfRange),
        [_, _, ..] => return Err(ParseError::Invalid),
        _ if missing_minutes => 0,
        _ => return Err(ParseError::TooShort),
    };
    s = match s.len() {
        len if len >= 2 => &s[2..],
        0 => s,
        _ => return Err(ParseError::TooShort),
    };
    let seconds = hours * 3600 + minutes * 60;
    Ok((s, if negative { -seconds } else { seconds }))
}

/// Applies one format item to the input (chrono's `parse_internal` loop body).
fn apply<'a>(fields: &mut Fields, s: &'a str, item: Item) -> Result<&'a str> {
    match item {
        Item::Literal(text) => {
            if s.len() < text.len() {
                return Err(ParseError::TooShort);
            }
            s.strip_prefix(text).ok_or(ParseError::Invalid)
        }
        Item::Space => Ok(s.trim_start()),
        Item::Fraction => match s.strip_prefix('.') {
            Some(rest) => {
                let (rest, nano) = nanosecond(rest)?;
                set(&mut fields.nanosecond, nano)?;
                Ok(rest)
            }
            None => Ok(s),
        },
        Item::Offset | Item::OffsetPermissive => {
            let permissive = item == Item::OffsetPermissive;
            let colon = OffsetColon::ColonOrSpace;
            let (rest, secs) = offset(s.trim_start(), colon, permissive, permissive)?;
            set(&mut fields.offset, secs)?;
            Ok(rest)
        }
        numeric_item => {
            let (rest, value) = numeric(s, numeric_item)?;
            fields.set_numeric(numeric_item, value)?;
            Ok(rest)
        }
    }
}

/// Runs a whole format over the input; leftover input is [`ParseError::TooLong`].
fn fields(s: &str, format: &[Item]) -> Result<Fields> {
    let mut fields = Fields::default();
    let mut rest = s;
    for &item in format {
        rest = apply(&mut fields, rest, item)?;
    }
    if !rest.is_empty() {
        return Err(ParseError::TooLong);
    }
    Ok(fields)
}

/// chrono's `NaiveDate::parse_from_str`.
pub fn date(s: &str, format: &[Item]) -> Result<Date> {
    fields(s, format)?.date()
}

/// chrono's `NaiveTime::parse_from_str`.
pub fn time(s: &str, format: &[Item]) -> Result<Time> {
    fields(s, format)?.time()
}

/// chrono's `NaiveDateTime::parse_from_str`: a date and time with no offset.
pub fn naive_datetime(s: &str, format: &[Item]) -> Result<DateTime> {
    fields(s, format)?.datetime()
}

/// The first of `candidates` that parses `s` as a date and time with no
/// offset (a chain of chrono `parse_from_str` calls joined by `or_else`).
pub fn first_naive_datetime(s: &str, candidates: &[&[Item]]) -> Option<DateTime> {
    candidates.iter().find_map(|f| naive_datetime(s, f).ok())
}

/// chrono's `DateTime::<FixedOffset>::parse_from_str`, returned as the UTC
/// instant it names.
pub fn datetime_with_offset(s: &str, format: &[Item]) -> Result<DateTime> {
    fields(s, format)?.instant()
}

/// chrono's `DateTime::parse_from_rfc3339`, returned as the UTC instant:
/// `YYYY-MM-DD`, then `T`, `t` or a space, `hh:mm:ss[.f+]`, then `Z`/`z` or
/// `±hh:mm` within ±23:59.
pub fn rfc3339(s: &str) -> Result<DateTime> {
    let mut f = Fields::default();
    let (s, year) = number(s, 4, 4)?;
    f.set_numeric(Item::Year, year)?;
    let (s, month) = number(char(s, b'-')?, 2, 2)?;
    f.set_numeric(Item::Month, month)?;
    let (s, day) = number(char(s, b'-')?, 2, 2)?;
    f.set_numeric(Item::Day, day)?;
    let s = match s.as_bytes().first() {
        Some(b't' | b'T' | b' ') => &s[1..],
        Some(_) => return Err(ParseError::Invalid),
        None => return Err(ParseError::TooShort),
    };
    let s = rfc3339_time(&mut f, s)?;
    let (s, secs) = offset(s, OffsetColon::Required, true, false)?;
    if secs.unsigned_abs() > (23 * 60 + 59) * 60 {
        return Err(ParseError::OutOfRange);
    }
    set(&mut f.offset, secs)?;
    if !s.is_empty() {
        return Err(ParseError::TooLong);
    }
    f.instant()
}

/// The `hh:mm:ss[.f+]` part of RFC 3339.
fn rfc3339_time<'a>(f: &mut Fields, s: &'a str) -> Result<&'a str> {
    let (s, hour) = number(s, 2, 2)?;
    f.set_numeric(Item::Hour, hour)?;
    let (s, minute) = number(char(s, b':')?, 2, 2)?;
    f.set_numeric(Item::Minute, minute)?;
    let (s, second) = number(char(s, b':')?, 2, 2)?;
    f.set_numeric(Item::Second, second)?;
    match s.strip_prefix('.') {
        Some(rest) => {
            let (rest, nano) = nanosecond(rest)?;
            set(&mut f.nanosecond, nano)?;
            Ok(rest)
        }
        None => Ok(s),
    }
}
