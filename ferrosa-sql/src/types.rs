//! Core value / row / schema types for the relational engine.

use std::cmp::Ordering;

/// A scalar column value. First-slice subset; widens toward the full Postgres
/// type system. `Eq`/`Hash` make values usable as join keys.
///
/// `Float` wraps [`ordered_float::OrderedFloat`] rather than a raw `f64` so the
/// enum keeps its `Eq`/`Hash`/`Ord` derives — `OrderedFloat` provides total
/// ordering (NaN-aware) and a stable hash, which the `hash_join` / `hash_aggregate`
/// keys depend on. SQL-level comparison ([`Value::sql_cmp`]) still uses the inner
/// `f64`'s `partial_cmp`, so NaN is UNKNOWN there.
/// `Uuid` wraps [`uuid::Uuid`] and `Bytea` wraps `Vec<u8>`; both inner types are
/// `Eq`/`Hash`/`Clone`, so the enum keeps its derives (and stays usable as a
/// join/group key). Future widenings with exact Postgres text representations —
/// `timestamp`, `numeric`, `inet` — are deferred (not implemented here).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(ordered_float::OrderedFloat<f64>),
    Uuid(uuid::Uuid),
    Bytea(Vec<u8>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Construct a [`Value::Float`] from a raw `f64` (wraps it in `OrderedFloat`).
    pub fn float(f: f64) -> Value {
        Value::Float(ordered_float::OrderedFloat(f))
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
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Int,
    Text,
    Bool,
    Float,
    Uuid,
    Bytea,
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
}
