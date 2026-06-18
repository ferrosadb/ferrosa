//! Core value / row / schema types for the relational engine.

use std::cmp::Ordering;
use std::net::IpAddr;

use num_bigint::BigInt;

/// A scalar column value. First-slice subset; widens toward the full Postgres
/// type system. `Eq`/`Hash` make values usable as join keys.
///
/// `Float` wraps [`ordered_float::OrderedFloat`] rather than a raw `f64` so the
/// enum keeps its `Eq`/`Hash`/`Ord` derives — `OrderedFloat` provides total
/// ordering (NaN-aware) and a stable hash, which the `hash_join` / `hash_aggregate`
/// keys depend on. SQL-level comparison ([`Value::sql_cmp`]) still uses the inner
/// `f64`'s `partial_cmp`, so NaN is UNKNOWN there.
///
/// `Uuid` wraps [`uuid::Uuid`] and `Bytea` wraps `Vec<u8>`; both inner types are
/// `Eq`/`Hash`/`Clone`, so the enum keeps its derives (and stays usable as a
/// join/group key).
///
/// The temporal / network / arbitrary-precision widenings all use derive-friendly
/// inner types so `Value` keeps `Eq`/`Hash`:
///
/// - `Timestamp(i64)` = **microseconds since the Unix epoch, UTC** (no offset).
/// - `Date(i32)` = **days since the Unix epoch** (1970-01-01).
/// - `Time(i64)` = **microseconds since midnight**.
/// - `Inet(std::net::IpAddr)` — already `Eq`/`Hash`/`Ord`.
/// - `Numeric { unscaled, scale }` — arbitrary precision, stored **normalized**
///   (trailing decimal zeros stripped, so equal numerics are structurally equal
///   ⇒ the `Eq`/`Hash` derives stay valid as group/join keys). Use
///   [`Value::numeric`] to construct one (it normalizes); do not build the variant
///   by hand or you risk two unequal-looking representations of the same number.
///
/// Out of scope (separate large efforts, see the storage-provider doc list):
/// collections (List/Set/Map/Tuple/UDT/Vector) and binary-format `numeric`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(ordered_float::OrderedFloat<f64>),
    Uuid(uuid::Uuid),
    Bytea(Vec<u8>),
    /// Microseconds since the Unix epoch (UTC).
    Timestamp(i64),
    /// Days since the Unix epoch (1970-01-01).
    Date(i32),
    /// Microseconds since midnight.
    Time(i64),
    /// An IPv4 or IPv6 address (Postgres `inet`).
    Inet(IpAddr),
    /// Arbitrary-precision decimal, stored normalized (see [`Value::numeric`]).
    /// The value is `unscaled * 10^(-scale)`.
    Numeric {
        unscaled: BigInt,
        scale: i32,
    },
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Construct a [`Value::Float`] from a raw `f64` (wraps it in `OrderedFloat`).
    pub fn float(f: f64) -> Value {
        Value::Float(ordered_float::OrderedFloat(f))
    }

    /// Construct a **normalized** [`Value::Numeric`] from a raw `(unscaled, scale)`
    /// pair. Normalization strips trailing decimal zeros — i.e. while `scale > 0`
    /// and `unscaled` is divisible by 10, divide out a factor of ten and decrement
    /// the scale. This makes `1.50` (unscaled=150, scale=2) and `1.5`
    /// (unscaled=15, scale=1) the SAME `Value`, so the `Eq`/`Hash` derives are a
    /// correct equality on the numeric value (vital for GROUP BY / DISTINCT /
    /// join keys). A zero value normalizes to `unscaled=0, scale=0`.
    pub fn numeric(unscaled: BigInt, scale: i32) -> Value {
        let (unscaled, scale) = normalize_numeric(unscaled, scale);
        Value::Numeric { unscaled, scale }
    }

    /// SQL three-valued comparison: `None` (UNKNOWN) when either side is NULL,
    /// the types are incomparable, or a float comparison involves NaN; callers
    /// treat UNKNOWN as "no match". Int and Float are cross-comparable via
    /// promotion of the int to `f64`.
    pub fn sql_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Text(a), Value::Text(b)) => Some(a.cmp(b)),
            (Value::Bool(a), Value::Bool(b)) => Some(a.cmp(b)),
            // Float vs Float: inner f64 partial_cmp; NaN ⇒ UNKNOWN.
            (Value::Float(a), Value::Float(b)) => a.0.partial_cmp(&b.0),
            // Cross numeric promotion: compare as f64; NaN ⇒ UNKNOWN.
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(&b.0),
            (Value::Float(a), Value::Int(b)) => a.0.partial_cmp(&(*b as f64)),
            // Uuid: natural Ord; Bytea: lexicographic byte compare. Cross-type
            // (and NULL) stays UNKNOWN, matching the other arms.
            (Value::Uuid(a), Value::Uuid(b)) => Some(a.cmp(b)),
            (Value::Bytea(a), Value::Bytea(b)) => Some(a.cmp(b)),
            // Temporal types compare by their integer representation (all are a
            // monotone count from a fixed origin, so integer order == time order).
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),
            (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (Value::Time(a), Value::Time(b)) => Some(a.cmp(b)),
            // Inet by `IpAddr`'s natural Ord (v4 < v6, then numeric).
            (Value::Inet(a), Value::Inet(b)) => Some(a.cmp(b)),
            // Numeric by VALUE, not by representation: align scales via BigInt
            // (multiply the larger-scale-deficit side by 10^delta) so e.g.
            // `1.5` and `1.50` compare equal and `2.0` > `1.99` regardless of
            // how each was normalized. Pure integer math — no float.
            (
                Value::Numeric {
                    unscaled: ua,
                    scale: sa,
                },
                Value::Numeric {
                    unscaled: ub,
                    scale: sb,
                },
            ) => Some(cmp_numeric(ua, *sa, ub, *sb)),
            _ => None,
        }
    }
}

/// Strip trailing decimal zeros from a `(unscaled, scale)` decimal: while the
/// scale is positive and the unscaled value is a multiple of ten, divide by ten
/// and drop the scale. Negative scales (value scaled UP by 10^|scale|) are left
/// as-is — they carry no trailing decimal zeros to strip. A zero value collapses
/// to `(0, 0)`.
fn normalize_numeric(mut unscaled: BigInt, mut scale: i32) -> (BigInt, i32) {
    use num_bigint::Sign;
    if unscaled.sign() == Sign::NoSign {
        return (BigInt::from(0), 0);
    }
    let ten = BigInt::from(10);
    while scale > 0 {
        let (q, r) = (&unscaled / &ten, &unscaled % &ten);
        if r.sign() != Sign::NoSign {
            break;
        }
        unscaled = q;
        scale -= 1;
    }
    (unscaled, scale)
}

/// Compare two decimals by value by aligning their scales. The decimal with the
/// SMALLER scale is multiplied up to the larger scale (`unscaled * 10^delta`),
/// then the two unscaled integers are compared directly. All integer math.
fn cmp_numeric(ua: &BigInt, sa: i32, ub: &BigInt, sb: i32) -> Ordering {
    if sa == sb {
        return ua.cmp(ub);
    }
    let max_scale = sa.max(sb);
    let scaled_a = scale_up(ua, max_scale - sa);
    let scaled_b = scale_up(ub, max_scale - sb);
    scaled_a.cmp(&scaled_b)
}

/// Multiply `n` by `10^power` (power >= 0). A non-positive power returns `n`.
fn scale_up(n: &BigInt, power: i32) -> BigInt {
    if power <= 0 {
        return n.clone();
    }
    n * BigInt::from(10).pow(power as u32)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
    Float,
    Uuid,
    Bytea,
    Timestamp,
    Date,
    Time,
    Inet,
    Numeric,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: ColumnType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: ColumnType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// The schema of a relation (an ordered list of columns).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelSchema {
    pub columns: Vec<Column>,
}

impl RelSchema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn width(&self) -> usize {
        self.columns.len()
    }
}

/// A row: positional values matching a [`RelSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row(pub Vec<Value>);

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Row(values)
    }

    pub fn get(&self, i: usize) -> &Value {
        &self.0[i]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_constructor_wraps_in_ordered_float() {
        assert_eq!(
            Value::float(1.5),
            Value::Float(ordered_float::OrderedFloat(1.5))
        );
    }

    #[test]
    fn sql_cmp_float_vs_float() {
        assert_eq!(
            Value::float(1.5).sql_cmp(&Value::float(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::float(2.5).sql_cmp(&Value::float(2.5)),
            Some(Ordering::Equal)
        );
        // NaN on either side is UNKNOWN.
        assert_eq!(Value::float(f64::NAN).sql_cmp(&Value::float(1.0)), None);
        assert_eq!(Value::float(1.0).sql_cmp(&Value::float(f64::NAN)), None);
    }

    #[test]
    fn sql_cmp_cross_int_and_float_promotes() {
        // Int vs Float
        assert_eq!(
            Value::Int(2).sql_cmp(&Value::float(2.5)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Int(3).sql_cmp(&Value::float(2.5)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Int(2).sql_cmp(&Value::float(2.0)),
            Some(Ordering::Equal)
        );
        // Float vs Int
        assert_eq!(
            Value::float(2.5).sql_cmp(&Value::Int(2)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::float(2.0).sql_cmp(&Value::Int(2)),
            Some(Ordering::Equal)
        );
        // NaN cross-compare is UNKNOWN.
        assert_eq!(Value::float(f64::NAN).sql_cmp(&Value::Int(1)), None);
        assert_eq!(Value::Int(1).sql_cmp(&Value::float(f64::NAN)), None);
    }

    #[test]
    fn sql_cmp_type_mismatch_and_null_are_unknown() {
        assert_eq!(Value::float(1.0).sql_cmp(&Value::Text("x".into())), None);
        assert_eq!(Value::float(1.0).sql_cmp(&Value::Null), None);
        assert_eq!(Value::Null.sql_cmp(&Value::float(1.0)), None);
    }

    #[test]
    fn sql_cmp_uuid_uses_natural_order() {
        let a = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
        let b = uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();
        assert_eq!(
            Value::Uuid(a).sql_cmp(&Value::Uuid(b)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Uuid(b).sql_cmp(&Value::Uuid(a)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Uuid(a).sql_cmp(&Value::Uuid(a)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn sql_cmp_bytea_is_lexicographic() {
        assert_eq!(
            Value::Bytea(vec![0x01, 0x02]).sql_cmp(&Value::Bytea(vec![0x01, 0x03])),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Bytea(vec![0x01, 0x02]).sql_cmp(&Value::Bytea(vec![0x01])),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::Bytea(vec![]).sql_cmp(&Value::Bytea(vec![])),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn sql_cmp_uuid_bytea_cross_and_null_are_unknown() {
        let u = uuid::Uuid::nil();
        assert_eq!(Value::Uuid(u).sql_cmp(&Value::Bytea(vec![0])), None);
        assert_eq!(Value::Bytea(vec![0]).sql_cmp(&Value::Uuid(u)), None);
        assert_eq!(Value::Uuid(u).sql_cmp(&Value::Int(1)), None);
        assert_eq!(Value::Uuid(u).sql_cmp(&Value::Null), None);
        assert_eq!(Value::Bytea(vec![0]).sql_cmp(&Value::Null), None);
    }

    #[test]
    fn uuid_and_bytea_are_eq_and_hashable() {
        // Both new variants must keep Value's Eq/Hash derives usable as keys.
        use std::collections::HashSet;
        let u = uuid::Uuid::nil();
        let mut set: HashSet<Value> = HashSet::new();
        set.insert(Value::Uuid(u));
        set.insert(Value::Uuid(u));
        set.insert(Value::Bytea(vec![1, 2, 3]));
        set.insert(Value::Bytea(vec![1, 2, 3]));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn column_type_has_uuid_and_bytea() {
        assert_eq!(ColumnType::Uuid, ColumnType::Uuid);
        assert_eq!(ColumnType::Bytea, ColumnType::Bytea);
        assert_ne!(ColumnType::Uuid, ColumnType::Bytea);
    }

    // ── Timestamp / Date / Time: integer-repr comparison ──────────────────

    #[test]
    fn sql_cmp_temporal_uses_integer_repr() {
        assert_eq!(
            Value::Timestamp(1).sql_cmp(&Value::Timestamp(2)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Timestamp(5).sql_cmp(&Value::Timestamp(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            Value::Date(-1).sql_cmp(&Value::Date(0)),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Time(86_400_000_000).sql_cmp(&Value::Time(0)),
            Some(Ordering::Greater)
        );
        // Cross-temporal-type comparisons are UNKNOWN.
        assert_eq!(Value::Timestamp(0).sql_cmp(&Value::Date(0)), None);
        assert_eq!(Value::Date(0).sql_cmp(&Value::Time(0)), None);
        assert_eq!(Value::Timestamp(0).sql_cmp(&Value::Null), None);
    }

    // ── Inet: IpAddr Ord ──────────────────────────────────────────────────

    #[test]
    fn sql_cmp_inet_uses_ipaddr_order() {
        use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
        let a = Value::Inet(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        let b = Value::Inet(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        assert_eq!(a.sql_cmp(&b), Some(Ordering::Less));
        assert_eq!(a.sql_cmp(&a), Some(Ordering::Equal));
        // v4 sorts before v6 in IpAddr's natural Ord.
        let v6 = Value::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(a.sql_cmp(&v6), Some(Ordering::Less));
        assert_eq!(a.sql_cmp(&Value::Null), None);
    }

    // ── Numeric: normalization + value comparison ─────────────────────────

    #[test]
    fn numeric_normalizes_trailing_zeros() {
        // 1.50 (150e-2) and 1.5 (15e-1) must be the SAME Value.
        let a = Value::numeric(BigInt::from(150), 2);
        let b = Value::numeric(BigInt::from(15), 1);
        assert_eq!(a, b);
        // Inspect the normalized form: 15 / scale 1.
        match a {
            Value::Numeric { unscaled, scale } => {
                assert_eq!(unscaled, BigInt::from(15));
                assert_eq!(scale, 1);
            }
            _ => panic!("expected Numeric"),
        }
        // Zero collapses to (0, 0) regardless of input scale.
        assert_eq!(
            Value::numeric(BigInt::from(0), 5),
            Value::numeric(BigInt::from(0), 0)
        );
        // An integer-valued decimal strips down to scale 0.
        assert_eq!(
            Value::numeric(BigInt::from(1200), 2),
            Value::numeric(BigInt::from(12), 0)
        );
        // Negative scale (value scaled up) is preserved.
        assert_eq!(
            Value::numeric(BigInt::from(12), -2),
            Value::Numeric {
                unscaled: BigInt::from(12),
                scale: -2
            }
        );
    }

    #[test]
    fn numeric_is_hashable_on_value() {
        use std::collections::HashSet;
        let mut set: HashSet<Value> = HashSet::new();
        set.insert(Value::numeric(BigInt::from(150), 2)); // 1.50
        set.insert(Value::numeric(BigInt::from(15), 1)); //  1.5  (same)
        set.insert(Value::numeric(BigInt::from(25), 1)); //  2.5
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn sql_cmp_numeric_compares_by_value_aligning_scales() {
        // 1.5 vs 1.50 ⇒ Equal even before normalization absorbs it.
        assert_eq!(
            Value::numeric(BigInt::from(15), 1).sql_cmp(&Value::numeric(BigInt::from(150), 2)),
            Some(Ordering::Equal)
        );
        // 2.0 vs 1.99 ⇒ Greater (align to scale 2: 200 vs 199).
        assert_eq!(
            Value::numeric(BigInt::from(2), 0).sql_cmp(&Value::numeric(BigInt::from(199), 2)),
            Some(Ordering::Greater)
        );
        // -1.5 vs 1.5 ⇒ Less.
        assert_eq!(
            Value::numeric(BigInt::from(-15), 1).sql_cmp(&Value::numeric(BigInt::from(15), 1)),
            Some(Ordering::Less)
        );
        // Negative scale: 1200 (12e2) vs 1199.
        assert_eq!(
            Value::numeric(BigInt::from(12), -2).sql_cmp(&Value::numeric(BigInt::from(1199), 0)),
            Some(Ordering::Greater)
        );
        // Cross-type / NULL is UNKNOWN.
        assert_eq!(
            Value::numeric(BigInt::from(1), 0).sql_cmp(&Value::Int(1)),
            None
        );
        assert_eq!(
            Value::numeric(BigInt::from(1), 0).sql_cmp(&Value::Null),
            None
        );
    }

    #[test]
    fn new_column_types_are_distinct() {
        assert_ne!(ColumnType::Timestamp, ColumnType::Date);
        assert_ne!(ColumnType::Date, ColumnType::Time);
        assert_ne!(ColumnType::Inet, ColumnType::Numeric);
        assert_eq!(ColumnType::Numeric, ColumnType::Numeric);
    }
}
