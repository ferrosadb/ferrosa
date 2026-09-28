//! Module: the exact jsonb `Number` (D2, D2a, D14a, T11): an unscaled integer
//! plus a decimal scale, never an implicit `f64`.
//! Correctness: correct when a lexeme is digit-capped BEFORE any big-integer
//! is built (JB-D2), scale follows PG `numeric` (`1.10` stays scale 2), equality,
//! ordering and hashing agree and use the trailing-zero-free value form, and
//! text output preserves scale.
//! Last revised: 2026-09-28
//! Last changed: T-101 initial `Number`, lexeme scan and f64 shortest entry points.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use num_bigint::{BigInt, Sign};

use crate::error::JsonbError;
use crate::limits::{HardCeilings, HARD_MAX_DIGITS_AFTER_POINT, HARD_MAX_DIGITS_BEFORE_POINT};

/// The smallest Variant kind that holds a number (architecture C9 rules 3, 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberKind {
    Int8,
    Int16,
    Int32,
    Int64,
    Decimal4,
    Decimal8,
    Decimal16,
    /// `ferrosa.bigdecimal` (primitive 63).
    BigDecimal,
}

#[derive(Debug, Clone)]
enum Repr {
    /// Scale 0 and fits `i64`.
    I64(i64),
    /// Fits `i128` but is not the `I64` form.
    I128 { unscaled: i128, scale: u16 },
    /// Wider than `i128`.
    Big { unscaled: BigInt, scale: u16 },
}

/// An exact decimal: `unscaled x 10^-scale`, scale kept as written (D2a).
#[derive(Debug, Clone)]
pub struct Number(Repr);

impl Number {
    /// An integer number (scale 0).
    pub fn from_i64(v: i64) -> Number {
        Number(Repr::I64(v))
    }

    /// An unsigned integer; above `i64::MAX` it is `decimal16` scale 0 (T11).
    pub fn from_u64(v: u64) -> Number {
        match i64::try_from(v) {
            Ok(i) => Number(Repr::I64(i)),
            Err(_) => Number(Repr::I128 {
                unscaled: i128::from(v),
                scale: 0,
            }),
        }
    }

    /// Parse a JSON number lexeme. Digit caps are checked on the lexeme
    /// before any big-integer is built (D14a, JB-D2).
    pub fn parse_lexeme(lexeme: &str) -> Result<Number, JsonbError> {
        let scan = Scan::new(lexeme.as_bytes())?;
        let shape = scan.shape();
        HardCeilings::CURRENT.check_digits(shape.before, shape.after)?;
        let digits = scan.unscaled_digits(shape.trailing_zeros);
        let magnitude =
            BigInt::parse_bytes(&digits, 10).ok_or(JsonbError::InvalidNumber { offset: 0 })?;
        let unscaled = if scan.negative { -magnitude } else { magnitude };
        let scale =
            u16::try_from(shape.after).map_err(|_| JsonbError::DigitsAfterPointExceeded {
                digits: shape.after,
                max: HardCeilings::CURRENT.digits_after_point,
            })?;
        Ok(Number::from_bigint(unscaled, scale))
    }

    /// The exact shortest decimal of a finite `f64`; integral values get scale 1.
    pub fn from_f64_shortest(f: f64) -> Result<Number, JsonbError> {
        if !f.is_finite() {
            return Err(JsonbError::NonFiniteNumber);
        }
        let mut text = format!("{f}");
        if !text.contains('.') {
            text.push_str(".0");
        }
        Number::parse_lexeme(&text)
    }

    /// The `f64` for this number when it is exactly the shortest form of it.
    /// Numbers too wide for any finite `f64` return `None`.
    pub fn to_f64_if_shortest_round_trips(&self) -> Option<f64> {
        if let Repr::Big { unscaled, .. } = &self.0 {
            if unscaled.bits() > F64_MAX_BITS {
                return None;
            }
        }
        let f = self.to_string().parse::<f64>().ok()?;
        let back = Number::from_f64_shortest(f).ok()?;
        (back == *self).then_some(f)
    }

    /// Canonical smallest representation for `(unscaled, scale)`.
    fn from_bigint(unscaled: BigInt, scale: u16) -> Number {
        if scale == 0 {
            if let Ok(v) = i64::try_from(&unscaled) {
                return Number(Repr::I64(v));
            }
        }
        match i128::try_from(&unscaled) {
            Ok(v) => Number(Repr::I128 { unscaled: v, scale }),
            Err(_) => Number(Repr::Big { unscaled, scale }),
        }
    }

    /// The decimal scale (digits after the point).
    pub fn scale(&self) -> u16 {
        match &self.0 {
            Repr::I64(_) => 0,
            Repr::I128 { scale, .. } | Repr::Big { scale, .. } => *scale,
        }
    }

    /// The smallest Variant kind for the unscaled value at its scale.
    pub fn kind(&self) -> NumberKind {
        match &self.0 {
            Repr::I64(v) => int_kind(*v),
            Repr::I128 { unscaled, scale } => decimal_kind(unscaled.unsigned_abs(), *scale),
            Repr::Big { .. } => NumberKind::BigDecimal,
        }
    }

    /// The unscaled integer when it fits `i128` (every kind except a wide `Big`).
    pub(crate) fn unscaled_i128(&self) -> Option<i128> {
        match &self.0 {
            Repr::I64(v) => Some(i128::from(*v)),
            Repr::I128 { unscaled, .. } => Some(*unscaled),
            Repr::Big { .. } => None,
        }
    }

    pub(crate) fn to_bigint(&self) -> BigInt {
        match &self.0 {
            Repr::I64(v) => BigInt::from(*v),
            Repr::I128 { unscaled, .. } => BigInt::from(*unscaled),
            Repr::Big { unscaled, .. } => unscaled.clone(),
        }
    }

    /// (unscaled, scale) with trailing zeros removed down to scale 0; for
    /// comparison and hashing only (D2a).
    fn normalized(&self) -> (BigInt, u16) {
        let mut u = self.to_bigint();
        let mut s = self.scale();
        let ten = BigInt::from(10u8);
        while s > 0 {
            let (q, r) = num_integer_div_rem(&u, &ten);
            if r.sign() != Sign::NoSign {
                break;
            }
            u = q;
            s -= 1;
        }
        (u, s)
    }
}

/// More bits than any finite `f64` decimal expansion needs (~1100 digits).
const F64_MAX_BITS: u64 = 4_000;
/// Exponent digits saturate here so an absurd exponent cannot overflow.
const EXP_CLAMP: i64 = 1 << 40;
/// Integer-part digits at which a lexeme is refused unscanned (see `Scan::new`).
const INT_SCAN_CAP: usize = HARD_MAX_DIGITS_BEFORE_POINT + HARD_MAX_DIGITS_AFTER_POINT + 1;
/// Stand-in for a length that does not fit `i64`; far above every cap.
const LEN_CLAMP: i64 = i64::MAX / 4;

/// A validated lexeme, sliced but not yet converted to a number.
struct Scan<'a> {
    negative: bool,
    int: &'a [u8],
    frac: &'a [u8],
    exp: i64,
}

/// Digit counts derived from a `Scan` without building any big-integer.
struct Shape {
    before: usize,
    after: usize,
    trailing_zeros: usize,
}

fn take_digits(b: &[u8]) -> (&[u8], &[u8]) {
    let n = b.iter().take_while(|c| c.is_ascii_digit()).count();
    b.split_at(n)
}

fn saturating_usize(v: i64) -> usize {
    usize::try_from(v.max(0)).unwrap_or(usize::MAX)
}

impl<'a> Scan<'a> {
    /// Validate the JSON number grammar and slice out its parts.
    fn new(b: &'a [u8]) -> Result<Scan<'a>, JsonbError> {
        let bad = |rest: &[u8]| JsonbError::InvalidNumber {
            offset: b.len().saturating_sub(rest.len()),
        };
        let (negative, rest) = match b.split_first() {
            Some((b'-', r)) => (true, r),
            _ => (false, b),
        };
        if rest.first() == Some(&b'0') && rest.get(1).is_some_and(u8::is_ascii_digit) {
            return Err(bad(rest));
        }
        // The integer part has no leading zeros, so this many digits leave at
        // least 131073 before the point even at the largest allowed scale:
        // refuse without scanning the rest of an attacker-sized lexeme.
        let window = rest.get(..INT_SCAN_CAP).unwrap_or(rest);
        let (int, rest) = match take_digits(window) {
            (i, _) if i.len() == INT_SCAN_CAP => {
                return Err(JsonbError::DigitsBeforePointExceeded {
                    digits: INT_SCAN_CAP,
                    max: HardCeilings::CURRENT.digits_before_point,
                });
            }
            (i, _) => (i, rest.get(i.len()..).unwrap_or(&[])),
        };
        if int.is_empty() {
            return Err(bad(rest));
        }
        let (frac, rest) = match rest.split_first() {
            Some((b'.', r)) => {
                let (f, r) = take_digits(r);
                if f.is_empty() {
                    return Err(bad(r));
                }
                (f, r)
            }
            _ => (&[][..], rest),
        };
        let (exp, rest) = Scan::exponent(rest).ok_or_else(|| bad(rest))?;
        if !rest.is_empty() {
            return Err(bad(rest));
        }
        Ok(Scan {
            negative,
            int,
            frac,
            exp,
        })
    }

    /// Parse an optional `e[+-]digits`; `None` means malformed.
    fn exponent(rest: &[u8]) -> Option<(i64, &[u8])> {
        let Some((b'e' | b'E', r)) = rest.split_first() else {
            return Some((0, rest));
        };
        let (neg, r) = match r.split_first() {
            Some((b'-', r)) => (true, r),
            Some((b'+', r)) => (false, r),
            _ => (false, r),
        };
        let (digits, r) = take_digits(r);
        if digits.is_empty() {
            return None;
        }
        let mag = digits.iter().fold(0i64, |acc, d| {
            acc.saturating_mul(10)
                .saturating_add(i64::from(d - b'0'))
                .min(EXP_CLAMP)
        });
        Some((if neg { -mag } else { mag }, r))
    }

    fn leading_zeros(&self) -> usize {
        self.int
            .iter()
            .chain(self.frac)
            .take_while(|c| **c == b'0')
            .count()
    }

    /// Stored scale is `max(0, |F| - e)`; digits before the point follow from
    /// the significant digit count `n`. All in `i64`, no big-integer.
    fn shape(&self) -> Shape {
        let total = i64::try_from(self.int.len() + self.frac.len()).unwrap_or(LEN_CLAMP);
        let n = total - i64::try_from(self.leading_zeros()).unwrap_or(0);
        let f = i64::try_from(self.frac.len()).unwrap_or(LEN_CLAMP);
        let s0 = f - self.exp;
        Shape {
            before: if n == 0 { 0 } else { saturating_usize(n - s0) },
            after: saturating_usize(s0),
            trailing_zeros: saturating_usize(-s0),
        }
    }

    /// ASCII digits of the unscaled value: significant digits, then the zeros
    /// a positive exponent pushes left of the point. Called only after the
    /// digit caps passed, so the length is at most 147455.
    fn unscaled_digits(&self, trailing_zeros: usize) -> Vec<u8> {
        let lead = self.leading_zeros();
        let mut out: Vec<u8> = self
            .int
            .iter()
            .chain(self.frac)
            .skip(lead)
            .copied()
            .collect();
        if out.is_empty() {
            return vec![b'0'];
        }
        out.extend(std::iter::repeat_n(b'0', trailing_zeros));
        out
    }
}

const DEC4_LIMIT: u128 = 1_000_000_000;
const DEC8_LIMIT: u128 = 1_000_000_000_000_000_000;
const DEC16_LIMIT: u128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

fn int_kind(v: i64) -> NumberKind {
    if i8::try_from(v).is_ok() {
        NumberKind::Int8
    } else if i16::try_from(v).is_ok() {
        NumberKind::Int16
    } else if i32::try_from(v).is_ok() {
        NumberKind::Int32
    } else {
        NumberKind::Int64
    }
}

fn decimal_kind(mag: u128, scale: u16) -> NumberKind {
    if scale > 0 && mag < DEC4_LIMIT && scale <= 9 {
        NumberKind::Decimal4
    } else if scale > 0 && mag < DEC8_LIMIT && scale <= 18 {
        NumberKind::Decimal8
    } else if mag < DEC16_LIMIT && scale <= 38 {
        NumberKind::Decimal16
    } else {
        NumberKind::BigDecimal
    }
}

fn num_integer_div_rem(a: &BigInt, b: &BigInt) -> (BigInt, BigInt) {
    (a / b, a % b)
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.normalized() == other.normalized()
    }
}

impl Eq for Number {}

impl Hash for Number {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let (u, s) = self.normalized();
        u.hash(state);
        s.hash(state);
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        let (a, sa) = (self.to_bigint(), self.scale());
        let (b, sb) = (other.to_bigint(), other.scale());
        let pow = |d: u16| BigInt::from(10u8).pow(u32::from(d));
        match sa.cmp(&sb) {
            Ordering::Equal => a.cmp(&b),
            Ordering::Less => (a * pow(sb - sa)).cmp(&b),
            Ordering::Greater => a.cmp(&(b * pow(sa - sb))),
        }
    }
}

impl fmt::Display for Number {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let u = self.to_bigint();
        let scale = usize::from(self.scale());
        let digits = u.magnitude().to_string();
        if u.sign() == Sign::Minus {
            f.write_str("-")?;
        }
        if scale == 0 {
            return f.write_str(&digits);
        }
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let split = padded.len() - scale;
        let (int, frac) = padded.split_at(split);
        write!(f, "{int}.{frac}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::hash_map::DefaultHasher;

    fn num(s: &str) -> Number {
        Number::parse_lexeme(s).unwrap_or_else(|e| panic!("{s:?}: {e}"))
    }

    fn hash_of(n: &Number) -> u64 {
        let mut h = DefaultHasher::new();
        n.hash(&mut h);
        h.finish()
    }

    #[test]
    fn jsonb_number_boundaries_round_trip() {
        let k = |s: &str| num(s).kind();
        assert_eq!(num("9223372036854775807").kind(), NumberKind::Int64);
        assert_eq!(k("9223372036854775808"), NumberKind::Decimal16);
        assert_eq!(num("9223372036854775808").scale(), 0);
        assert_eq!(k("18446744073709551615"), NumberKind::Decimal16);
        assert_eq!(k("18446744073709551616"), NumberKind::Decimal16);
        assert_eq!(k(&"9".repeat(38)), NumberKind::Decimal16);
        assert_eq!(k(&"9".repeat(39)), NumberKind::BigDecimal);
        assert_eq!(k(&format!("0.{}1", "0".repeat(37))), NumberKind::Decimal16);
        assert_eq!(k(&format!("0.{}1", "0".repeat(38))), NumberKind::BigDecimal);
        assert_eq!(k("127"), NumberKind::Int8);
        assert_eq!(k("128"), NumberKind::Int16);
        assert_eq!(k("-2147483649"), NumberKind::Int64);
        assert_eq!(k("1.0"), NumberKind::Decimal4);
        for s in ["9223372036854775808", "18446744073709551616", "1.50"] {
            assert_eq!(num(&num(s).to_string()).to_string(), s);
        }
        assert!(matches!(
            Number::parse_lexeme("1e2147483648"),
            Err(JsonbError::DigitsBeforePointExceeded { .. })
        ));
    }

    #[test]
    fn jsonb_number_digit_caps_at_boundary() {
        assert!(Number::parse_lexeme(&"1".repeat(131_072)).is_ok());
        assert!(matches!(
            Number::parse_lexeme(&"1".repeat(131_073)),
            Err(JsonbError::DigitsBeforePointExceeded { .. })
        ));
        assert!(Number::parse_lexeme(&format!("0.{}", "1".repeat(16_383))).is_ok());
        assert!(matches!(
            Number::parse_lexeme(&format!("0.{}", "1".repeat(16_384))),
            Err(JsonbError::DigitsAfterPointExceeded { .. })
        ));
        assert!(matches!(
            Number::parse_lexeme("1e-16384"),
            Err(JsonbError::DigitsAfterPointExceeded { .. })
        ));
        let huge = "7".repeat(10 * 1024 * 1024);
        let start = std::time::Instant::now();
        assert!(Number::parse_lexeme(&huge).is_err());
        assert!(
            start.elapsed().as_millis() < 50,
            "took {:?}",
            start.elapsed()
        );
    }

    #[test]
    fn jsonb_number_scale_follows_pg_numeric() {
        // (lexeme, text postgres:16 prints for ::numeric)
        let golden = [
            ("1.10", "1.10"),
            ("1.5E2", "150"),
            ("1.5e1", "15"),
            ("1.50e1", "15.0"),
            ("-0", "0"),
            ("-0.0", "0.0"),
            ("0.00", "0.00"),
            ("0e5", "0"),
            ("1e-2", "0.01"),
            ("1E+2", "100"),
            ("12.5e-1", "1.25"),
            ("0.10e1", "1.0"),
            ("-1.0", "-1.0"),
            ("100", "100"),
            ("1e0", "1"),
            (
                "123456789012345678901234567890.123",
                "123456789012345678901234567890.123",
            ),
        ];
        for (lex, want) in golden {
            assert_eq!(num(lex).to_string(), want, "lexeme {lex}");
        }
        assert_eq!(num("1.0").scale(), 1);
    }

    #[test]
    fn jsonb_number_rejects_malformed_lexemes() {
        for bad in [
            "", "-", "01", "1.", ".5", "1e", "1e+", "+1", "1 ", "0x1", "--1", "1.5.2",
        ] {
            assert!(
                matches!(
                    Number::parse_lexeme(bad),
                    Err(JsonbError::InvalidNumber { .. })
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn jsonb_number_equal_by_value_not_scale() {
        let (a, b, c) = (num("1.0"), num("1"), num("1.00"));
        assert!(a == b && b == c);
        assert_eq!(hash_of(&a), hash_of(&c));
        assert_eq!(a.cmp(&num("1.5")), Ordering::Less);
        assert_ne!(num("10"), num("1"));
        assert_ne!(a.to_string(), c.to_string());
    }

    #[test]
    fn jsonb_number_f64_shortest_edges() {
        assert_eq!(
            Number::from_f64_shortest(3.0).map(|n| n.to_string()),
            Ok("3.0".into())
        );
        assert_eq!(
            Number::from_f64_shortest(0.1).map(|n| n.to_string()),
            Ok("0.1".into())
        );
        assert_eq!(
            Number::from_f64_shortest(f64::NAN),
            Err(JsonbError::NonFiniteNumber)
        );
        assert_eq!(
            Number::from_f64_shortest(f64::INFINITY),
            Err(JsonbError::NonFiniteNumber)
        );
    }

    proptest! {
        #[test]
        fn jsonb_number_f64_shortest_round_trip(bits in any::<u64>()) {
            let f = f64::from_bits(bits);
            prop_assume!(f.is_finite());
            let n = Number::from_f64_shortest(f).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(n.to_f64_if_shortest_round_trips(), Some(f));
            if f.fract() == 0.0 {
                prop_assert!(n.scale() >= 1);
            }
        }

        #[test]
        fn jsonb_number_eq_hash_agree(u in any::<i64>(), s1 in 0usize..6, s2 in 0usize..6) {
            let text = |s: usize| {
                let digits = format!("{:0>w$}", u.unsigned_abs(), w = s + 1);
                let (i, f) = digits.split_at(digits.len() - s);
                let sign = if u < 0 { "-" } else { "" };
                if s == 0 { format!("{sign}{i}") } else { format!("{sign}{i}.{f}") }
            };
            // the same value written at two scales
            let a = num(&format!("{}{}", text(0), if s1 > 0 { format!(".{}", "0".repeat(s1)) } else { String::new() }));
            let b = num(&format!("{}{}", text(0), if s2 > 0 { format!(".{}", "0".repeat(s2)) } else { String::new() }));
            prop_assert!(a == b);
            prop_assert_eq!(hash_of(&a), hash_of(&b));
            prop_assert_eq!(a.cmp(&b), Ordering::Equal);
        }

        #[test]
        fn jsonb_number_lexeme_round_trip_preserves_scale(
            int in "0|[1-9][0-9]{0,30}", frac in "[0-9]{0,30}", neg in any::<bool>()
        ) {
            let lex = format!("{}{}{}{}", if neg { "-" } else { "" }, int,
                if frac.is_empty() { "" } else { "." }, frac);
            let n = Number::parse_lexeme(&lex).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(usize::from(n.scale()), frac.len());
            let is_zero = int == "0" && frac.chars().all(|c| c == '0');
            let want = if neg && is_zero { lex.trim_start_matches('-').to_string() } else { lex.clone() };
            prop_assert_eq!(n.to_string(), want);
        }
    }
}
