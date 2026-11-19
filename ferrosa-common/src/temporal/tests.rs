//! Golden vectors. Every expected value below was produced by chrono 0.4.39
//! for the same input (the differential harness that generated them, and
//! fuzzed two million further inputs with zero mismatches, lives outside the
//! workspace so chrono stays out of the dependency graph).

use super::parse::{self, formats};
use super::{Date, DateTime, MAX_YEAR, MIN_YEAR};

fn err(text: &str) -> Result<i64, String> {
    Err(text.to_string())
}

fn date_days(s: &str) -> Result<i64, String> {
    parse::date(s, formats::YMD)
        .map(Date::days_since_epoch)
        .map_err(|e| e.to_string())
}

#[test]
fn dates_parse_with_chronos_leniency_and_errors() {
    assert_eq!(date_days("2024-01-15"), Ok(19737));
    assert_eq!(date_days("2024-1-5"), Ok(19727));
    assert_eq!(date_days(" 2024-01-15"), Ok(19737));
    assert_eq!(date_days("2024-02-30"), err("input is out of range"));
    assert_eq!(date_days("2024-13-01"), err("input is out of range"));
    assert_eq!(date_days("2024-01-15 "), err("trailing input"));
    assert_eq!(date_days("24-01"), err("premature end of input"));
}

#[test]
fn years_beyond_four_digits_need_a_sign_and_stay_in_chronos_range() {
    assert_eq!(date_days("-262143-01-01"), Ok(-96_465_292));
    assert_eq!(date_days("+262142-12-31"), Ok(95_026_236));
    assert_eq!(date_days("+10000-01-01"), Ok(2_932_897));
    assert_eq!(date_days("-262144-12-31"), err("input is out of range"));
    assert_eq!(
        date_days("262142-12-31"),
        err("input contains invalid characters")
    );
}

fn time_nanos(s: &str) -> Result<i64, String> {
    parse::time(s, formats::HMS_FRAC)
        .map(|t| t.nanos_since_midnight())
        .map_err(|e| e.to_string())
}

#[test]
fn times_take_an_optional_fraction_and_a_leap_second() {
    assert_eq!(time_nanos("10:30:00"), Ok(37_800_000_000_000));
    assert_eq!(time_nanos("10:30:00.123456789"), Ok(37_800_123_456_789));
    assert_eq!(time_nanos("10:30:00.1234567891"), Ok(37_800_123_456_789));
    assert_eq!(time_nanos("23:59:60"), Ok(86_400_000_000_000));
    assert_eq!(time_nanos("23:59:60.5"), Ok(86_400_500_000_000));
    assert_eq!(time_nanos("10:30:00."), err("premature end of input"));
    assert_eq!(time_nanos("10:30"), err("premature end of input"));
    assert_eq!(time_nanos("24:00:00"), err("input is out of range"));
    assert_eq!(time_nanos("10:30:00Z"), err("trailing input"));
}

fn naive_micros(s: &str) -> Result<i64, String> {
    parse::naive_datetime(s, formats::YMD_HMS_FRAC)
        .map(DateTime::timestamp_micros)
        .map_err(|e| e.to_string())
}

#[test]
fn a_format_space_matches_any_run_of_whitespace_including_none() {
    assert_eq!(
        naive_micros("2024-01-15 10:30:00.5"),
        Ok(1_705_314_600_500_000)
    );
    assert_eq!(
        naive_micros("2024-01-15  10:30:00"),
        Ok(1_705_314_600_000_000)
    );
    assert_eq!(
        naive_micros("2024-01-1510:30:00"),
        Ok(1_705_314_600_000_000)
    );
    assert_eq!(
        naive_micros("2024-01-15T10:30:00"),
        err("input contains invalid characters")
    );
    assert_eq!(naive_micros("2024-01-15 10:30:00x"), err("trailing input"));
    assert_eq!(
        naive_micros("2024-01-15 23:59:60"),
        Ok(1_705_363_200_000_000)
    );
}

fn offset_micros(s: &str, permissive: bool) -> Result<i64, String> {
    let format = if permissive {
        formats::YMD_HMS_FRAC_OFFSET_PERMISSIVE
    } else {
        formats::YMD_HMS_FRAC_OFFSET
    };
    parse::datetime_with_offset(s, format)
        .map(DateTime::timestamp_micros)
        .map_err(|e| e.to_string())
}

#[test]
fn offsets_accept_colons_spaces_and_the_minus_sign_character() {
    for permissive in [false, true] {
        let at = |s| offset_micros(s, permissive);
        assert_eq!(at("2024-01-15 10:30:00+0530"), Ok(1_705_294_800_000_000));
        assert_eq!(at("2024-01-15 10:30:00 +05:30"), Ok(1_705_294_800_000_000));
        assert_eq!(
            at("2024-01-15 10:30:00\u{2212}0800"),
            Ok(1_705_343_400_000_000)
        );
        assert_eq!(
            at("2024-01-15 10:30:00+99:59"),
            err("input is out of range")
        );
        assert_eq!(
            at("2024-01-15 10:30:00+05:60"),
            err("input is out of range")
        );
        assert_eq!(
            at("-262143-01-01 00:00:00+0001"),
            err("no possible date and time matching input")
        );
    }
}

#[test]
fn only_the_permissive_offset_takes_zulu_and_bare_hours() {
    let strict = |s| offset_micros(s, false);
    let permissive = |s| offset_micros(s, true);
    assert_eq!(
        strict("2024-01-15 10:30:00+05"),
        err("premature end of input")
    );
    assert_eq!(
        permissive("2024-01-15 10:30:00+05"),
        Ok(1_705_296_600_000_000)
    );
    assert_eq!(
        strict("2024-01-15 10:30:00Z"),
        err("input contains invalid characters")
    );
    assert_eq!(
        permissive("2024-01-15 10:30:00Z"),
        Ok(1_705_314_600_000_000)
    );
}

fn rfc3339_millis(s: &str) -> Result<i64, String> {
    parse::rfc3339(s)
        .map(DateTime::timestamp_millis)
        .map_err(|e| e.to_string())
}

#[test]
fn rfc3339_parsing_matches_chrono() {
    assert_eq!(
        rfc3339_millis("2024-01-15T10:30:00Z"),
        Ok(1_705_314_600_000)
    );
    assert_eq!(
        rfc3339_millis("2024-01-15t10:30:00.123+05:30"),
        Ok(1_705_294_800_123)
    );
    assert_eq!(
        rfc3339_millis("2024-01-15 10:30:00-00:00"),
        Ok(1_705_314_600_000)
    );
    assert_eq!(
        rfc3339_millis("2024-01-15T23:59:60Z"),
        Ok(1_705_363_200_000)
    );
    let invalid = err("input contains invalid characters");
    assert_eq!(rfc3339_millis("2024-01-15T10:30:00+0530"), invalid);
    assert_eq!(rfc3339_millis("+2024-01-15T10:30:00Z"), invalid);
    assert_eq!(
        rfc3339_millis("2024-01-15T10:30:00+24:00"),
        err("input is out of range")
    );
    assert_eq!(
        rfc3339_millis("2024-01-15T10:30:00"),
        err("premature end of input")
    );
}

fn rendered(ms: i64) -> Option<(String, String, String)> {
    DateTime::from_timestamp_millis(ms)
        .map(|d| (d.to_rfc3339(), d.format_ymd_hms(), d.format_compact()))
}

fn strings(a: &str, b: &str, c: &str) -> Option<(String, String, String)> {
    Some((a.to_string(), b.to_string(), c.to_string()))
}

#[test]
fn instants_render_like_chrono_including_signed_years() {
    let cases = [
        (
            0,
            strings(
                "1970-01-01T00:00:00+00:00",
                "1970-01-01 00:00:00",
                "19700101-000000",
            ),
        ),
        (
            -1,
            strings(
                "1969-12-31T23:59:59.999+00:00",
                "1969-12-31 23:59:59",
                "19691231-235959",
            ),
        ),
        (
            1_700_000_000_120,
            strings(
                "2023-11-14T22:13:20.120+00:00",
                "2023-11-14 22:13:20",
                "20231114-221320",
            ),
        ),
        (
            253_402_300_800_000,
            strings(
                "+10000-01-01T00:00:00+00:00",
                "+10000-01-01 00:00:00",
                "+100000101-000000",
            ),
        ),
        (
            -62_167_219_200_001,
            strings(
                "-0001-12-31T23:59:59.999+00:00",
                "-0001-12-31 23:59:59",
                "-00011231-235959",
            ),
        ),
    ];
    for (ms, expected) in cases {
        assert_eq!(rendered(ms), expected, "{ms}");
    }
    let nanos = DateTime::from_timestamp(1_700_000_000, 123_456_789).unwrap();
    assert_eq!(nanos.to_rfc3339(), "2023-11-14T22:13:20.123456789+00:00");
    let micros = DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap();
    assert_eq!(micros.to_rfc3339(), "2023-11-14T22:13:20.123456+00:00");
}

#[test]
fn the_range_is_chronos_to_the_millisecond() {
    assert_eq!(
        rendered(8_210_266_876_799_999).map(|r| r.0),
        Some("+262142-12-31T23:59:59.999+00:00".to_string())
    );
    assert_eq!(rendered(8_210_266_876_800_000), None);
    assert_eq!(
        rendered(-8_334_601_228_800_000).map(|r| r.0),
        Some("-262143-01-01T00:00:00+00:00".to_string())
    );
    assert_eq!(rendered(-8_334_601_228_800_001), None);
    assert!(Date::from_ymd(i64::from(MIN_YEAR), 1, 1).is_some());
    assert!(Date::from_ymd(i64::from(MAX_YEAR), 12, 31).is_some());
    assert!(Date::from_ymd(i64::from(MIN_YEAR) - 1, 12, 31).is_none());
    assert!(Date::from_ymd(i64::from(MAX_YEAR) + 1, 1, 1).is_none());
}

#[test]
fn adding_months_clamps_to_the_month_end() {
    let shifted = |y, m, d, by| {
        Date::from_ymd(y, m, d)
            .unwrap()
            .checked_add_months(by)
            .map(|d| d.to_string())
    };
    assert_eq!(shifted(2024, 1, 31, 1).as_deref(), Some("2024-02-29"));
    assert_eq!(shifted(2023, 1, 31, 1).as_deref(), Some("2023-02-28"));
    assert_eq!(shifted(2024, 3, 31, -1).as_deref(), Some("2024-02-29"));
    assert_eq!(shifted(2024, 2, 29, 12).as_deref(), Some("2025-02-28"));
    assert_eq!(shifted(262_142, 12, 1, 1), None);
    assert_eq!(shifted(-262_143, 1, 1, -1), None);
}

#[test]
fn days_round_trip_across_the_whole_range() {
    let first = Date::from_ymd(i64::from(MIN_YEAR), 1, 1)
        .unwrap()
        .days_since_epoch();
    let last = Date::from_ymd(i64::from(MAX_YEAR), 12, 31)
        .unwrap()
        .days_since_epoch();
    let step = (last - first) / 100_003;
    let mut day = first;
    while day <= last {
        let date = Date::from_days_since_epoch(day).expect("in range");
        assert_eq!(date.days_since_epoch(), day);
        day += step;
    }
    assert_eq!(Date::from_days_since_epoch(first - 1), None);
    assert_eq!(Date::from_days_since_epoch(last + 1), None);
}
