//! Convert ferrosa CQL query results into Apache Arrow record batches.
//!
//! The Flight `DoGet` path runs a CQL `SELECT` via `ferrosa_cql`'s structured
//! result (`route_select_raw` → typed `column_types` + `Vec<Vec<Option<CqlValue>>>`)
//! and converts it here, **column by column**, into an Arrow [`RecordBatch`].
//!
//! Fail-loud: an unsupported CQL type or a value that does not match its
//! column's declared type returns an error rather than silently producing wrong
//! data. Every CQL type is covered — scalars, temporal, inet, varint, decimal
//! (as exact text), `list`/`set`, `map`, `tuple`, `udt`, and `vector`. The
//! reverse path ([`record_batch_to_rows`], for `DoPut`) covers the common
//! scalar Arrow types and fails loud (`UnsupportedArrow`) on the rest.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, IntervalMonthDayNanoArray, StringArray,
    Time64NanosecondArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use ferrosa_common::{CqlType, CqlValue};

/// Decoded Arrow batch: column names + row-major CQL values (the `DoPut` shape).
pub type DecodedRows = (Vec<String>, Vec<Vec<Option<CqlValue>>>);

/// Error converting a CQL result set to an Arrow record batch.
#[derive(Debug)]
pub enum ConvertError {
    /// The CQL type has no Arrow mapping yet (rich/rare types).
    Unsupported(CqlType),
    /// A cell's value did not match its column's declared CQL type.
    TypeMismatch {
        column: usize,
        expected: &'static str,
    },
    /// Arrow rejected the assembled batch (e.g. column length mismatch).
    Arrow(arrow::error::ArrowError),
    /// An Arrow `DataType` has no CQL mapping (reverse / `DoPut` path).
    UnsupportedArrow(DataType),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertError::Unsupported(t) => {
                write!(f, "Arrow conversion of CQL type {t:?} is not yet supported")
            }
            ConvertError::TypeMismatch { column, expected } => {
                write!(
                    f,
                    "column {column}: value did not match expected {expected}"
                )
            }
            ConvertError::Arrow(e) => write!(f, "arrow batch assembly failed: {e}"),
            ConvertError::UnsupportedArrow(dt) => {
                write!(f, "Arrow type {dt:?} has no CQL mapping (DoPut)")
            }
        }
    }
}

impl std::error::Error for ConvertError {}

/// Map a CQL type to its Arrow `DataType`, or `None` if unsupported.
pub fn cql_type_to_arrow(t: &CqlType) -> Option<DataType> {
    use CqlType::*;
    Some(match t {
        Int => DataType::Int32,
        Bigint | Counter => DataType::Int64,
        Smallint => DataType::Int16,
        Tinyint => DataType::Int8,
        Float => DataType::Float32,
        Double => DataType::Float64,
        Boolean => DataType::Boolean,
        Ascii | Varchar => DataType::Utf8,
        Blob => DataType::Binary,
        Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
        // Uuid/Timeuuid as canonical text in v1 (FixedSizeBinary(16) is a refinement).
        Uuid | Timeuuid => DataType::Utf8,
        // Text representations for types whose exact value is best preserved as a
        // string (IP literal; arbitrary-precision integer).
        Inet | Varint => DataType::Utf8,
        Date => DataType::Date32,
        Time => DataType::Time64(TimeUnit::Nanosecond),
        Duration => DataType::Interval(IntervalUnit::MonthDayNano),
        List(inner) | Set(inner) => {
            let item = cql_type_to_arrow(inner)?;
            DataType::List(Arc::new(Field::new("item", item, true)))
        }
        // Cassandra `vector<float, N>` -> fixed-width list of f32.
        Vector(_, dim) => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            *dim as i32,
        ),
        Map(k, v) => DataType::Map(
            map_entries_field(cql_type_to_arrow(k)?, cql_type_to_arrow(v)?),
            false,
        ),
        Tuple(elems) => {
            let mut fields = Vec::with_capacity(elems.len());
            for (i, t) in elems.iter().enumerate() {
                fields.push(Field::new(i.to_string(), cql_type_to_arrow(t)?, true));
            }
            DataType::Struct(Fields::from(fields))
        }
        Udt {
            fields: udt_fields, ..
        } => {
            let mut fields = Vec::with_capacity(udt_fields.len());
            for (name, t) in udt_fields {
                fields.push(Field::new(name, cql_type_to_arrow(t)?, true));
            }
            DataType::Struct(Fields::from(fields))
        }
        // Decimal carries a per-VALUE scale, which Arrow's column-level
        // Decimal128(precision, scale) cannot represent; emit the exact decimal
        // as text (lossless), matching how varint/inet are handled.
        Decimal => DataType::Utf8,
    })
}

/// Format a CQL decimal (`unscaled` x 10^-`scale`) as an exact decimal string.
/// `unscaled` is the signed integer rendering of the unscaled value.
fn format_decimal(scale: i32, unscaled: &str) -> String {
    if scale <= 0 {
        // value = unscaled x 10^(-scale): append |scale| trailing zeros.
        return format!("{unscaled}{}", "0".repeat(scale.unsigned_abs() as usize));
    }
    let scale = scale as usize;
    let (sign, digits) = unscaled
        .strip_prefix('-')
        .map_or(("", unscaled), |d| ("-", d));
    if digits.len() > scale {
        let point = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..point], &digits[point..])
    } else {
        format!("{sign}0.{}{}", "0".repeat(scale - digits.len()), digits)
    }
}

/// The `entries` struct field of an Arrow `Map` (keys non-null, values
/// nullable). Single source of truth so the column `DataType` from
/// [`cql_type_to_arrow`] and the array built in `build_map` are identical.
fn map_entries_field(key_dt: DataType, val_dt: DataType) -> Arc<Field> {
    let entries = DataType::Struct(Fields::from(vec![
        Field::new("keys", key_dt, false),
        Field::new("values", val_dt, true),
    ]));
    Arc::new(Field::new("entries", entries, false))
}

/// Convert a structured CQL result (column metadata + rows) into an Arrow
/// `RecordBatch`. All columns are nullable (CQL cells can be absent/NULL).
pub fn rows_to_record_batch(
    column_names: &[String],
    column_types: &[CqlType],
    rows: &[Vec<Option<CqlValue>>],
) -> Result<RecordBatch, ConvertError> {
    let mut fields = Vec::with_capacity(column_types.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_types.len());

    for (c, ct) in column_types.iter().enumerate() {
        let dt = cql_type_to_arrow(ct).ok_or_else(|| ConvertError::Unsupported(ct.clone()))?;
        arrays.push(build_array(ct, rows, c)?);
        let name = column_names
            .get(c)
            .cloned()
            .unwrap_or_else(|| format!("col{c}"));
        fields.push(Field::new(name, dt, true));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(ConvertError::Arrow)
}

/// Decode an Arrow `RecordBatch` into column names + row-major CQL values — the
/// inverse of [`rows_to_record_batch`], used by the Flight `DoPut` write path.
///
/// Covers the common scalar Arrow types; richer types fail loud
/// (`UnsupportedArrow`). `Utf8` decodes to `Text` — the target table's declared
/// column type disambiguates text/uuid/inet/varint when the row is written.
pub fn record_batch_to_rows(batch: &RecordBatch) -> Result<DecodedRows, ConvertError> {
    let schema = batch.schema();
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut cols: Vec<Vec<Option<CqlValue>>> = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        cols.push(decode_column(batch.column(c), schema.field(c).data_type())?);
    }

    // Transpose column-major -> row-major.
    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(cols.iter().map(|col| col[i].clone()).collect());
    }
    Ok((names, rows))
}

/// Decode one Arrow column into per-row `Option<CqlValue>` (NULL slot -> `None`).
fn decode_column(arr: &ArrayRef, dt: &DataType) -> Result<Vec<Option<CqlValue>>, ConvertError> {
    use arrow::array::Array;
    macro_rules! scalar {
        ($arrty:ty, $make:expr) => {{
            let a = arr
                .as_any()
                .downcast_ref::<$arrty>()
                .ok_or_else(|| ConvertError::UnsupportedArrow(dt.clone()))?;
            (0..a.len())
                .map(|i| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some($make(a.value(i)))
                    }
                })
                .collect()
        }};
    }
    Ok(match dt {
        DataType::Int32 => scalar!(Int32Array, CqlValue::Int),
        DataType::Int64 => scalar!(Int64Array, CqlValue::Bigint),
        DataType::Int16 => scalar!(Int16Array, CqlValue::Smallint),
        DataType::Int8 => scalar!(Int8Array, CqlValue::Tinyint),
        DataType::Boolean => scalar!(BooleanArray, CqlValue::Boolean),
        DataType::Float32 => scalar!(Float32Array, |v: f32| CqlValue::Float(v.to_bits())),
        DataType::Float64 => scalar!(Float64Array, |v: f64| CqlValue::Double(v.to_bits())),
        DataType::Utf8 => scalar!(StringArray, |v: &str| CqlValue::Text(v.to_string())),
        DataType::Binary => scalar!(BinaryArray, |v: &[u8]| CqlValue::Blob(v.to_vec())),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            scalar!(TimestampMillisecondArray, CqlValue::Timestamp)
        }
        other => return Err(ConvertError::UnsupportedArrow(other.clone())),
    })
}

/// Build one Arrow column array from the values at index `c` across `rows`.
fn build_array(
    ct: &CqlType,
    rows: &[Vec<Option<CqlValue>>],
    c: usize,
) -> Result<ArrayRef, ConvertError> {
    let values: Vec<Option<CqlValue>> = rows
        .iter()
        .map(|r| r.get(c).and_then(|cell| cell.as_ref()).cloned())
        .collect();
    build_values(ct, &values, c)
}

/// Build an Arrow array from a flat slice of typed values. Recurses for the
/// element type of `list`/`set` columns. `column` is the originating column
/// index, used only for `TypeMismatch` reporting.
fn build_values(
    ct: &CqlType,
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    // Primitive columns: collect Option<T> (NULL/absent -> None), erroring on a
    // value whose variant doesn't match the column's declared type.
    macro_rules! prim {
        ($arrty:ty, $variant:pat, $bind:ident => $val:expr, $exp:literal) => {{
            let mut out = Vec::with_capacity(values.len());
            for v in values {
                match v.as_ref() {
                    None | Some(CqlValue::Null) => out.push(None),
                    Some($variant) => out.push(Some($val)),
                    Some(_) => {
                        return Err(ConvertError::TypeMismatch {
                            column,
                            expected: $exp,
                        })
                    }
                }
            }
            Arc::new(<$arrty>::from(out)) as ArrayRef
        }};
    }

    // String-valued columns: map the matching variant(s) to an owned String.
    macro_rules! string_col {
        ($exp:literal, $($variant:pat => $s:expr),+ $(,)?) => {{
            let mut out: Vec<Option<String>> = Vec::with_capacity(values.len());
            for v in values {
                match v.as_ref() {
                    None | Some(CqlValue::Null) => out.push(None),
                    $(Some($variant) => out.push(Some($s)),)+
                    Some(_) => return Err(ConvertError::TypeMismatch { column, expected: $exp }),
                }
            }
            Arc::new(StringArray::from(out)) as ArrayRef
        }};
    }

    Ok(match ct {
        CqlType::Int => prim!(Int32Array, CqlValue::Int(v), v => *v, "int"),
        CqlType::Bigint => prim!(Int64Array, CqlValue::Bigint(v), v => *v, "bigint"),
        CqlType::Counter => prim!(Int64Array, CqlValue::Counter(v), v => *v, "counter"),
        CqlType::Smallint => prim!(Int16Array, CqlValue::Smallint(v), v => *v, "smallint"),
        CqlType::Tinyint => prim!(Int8Array, CqlValue::Tinyint(v), v => *v, "tinyint"),
        CqlType::Float => prim!(Float32Array, CqlValue::Float(b), b => f32::from_bits(*b), "float"),
        CqlType::Double => {
            prim!(Float64Array, CqlValue::Double(b), b => f64::from_bits(*b), "double")
        }
        CqlType::Boolean => prim!(BooleanArray, CqlValue::Boolean(v), v => *v, "boolean"),
        CqlType::Timestamp => {
            prim!(TimestampMillisecondArray, CqlValue::Timestamp(v), v => *v, "timestamp")
        }
        CqlType::Ascii | CqlType::Varchar => string_col!(
            "text",
            CqlValue::Ascii(s) => s.clone(),
            CqlValue::Text(s) => s.clone(),
        ),
        CqlType::Uuid | CqlType::Timeuuid => string_col!(
            "uuid",
            CqlValue::Uuid(u) => u.to_string(),
            CqlValue::Timeuuid(u) => u.to_string(),
        ),
        CqlType::Inet => string_col!("inet", CqlValue::Inet(ip) => ip.to_string()),
        CqlType::Varint => string_col!("varint", CqlValue::Varint(v) => v.to_string()),
        CqlType::Blob => {
            let owned: Vec<Option<Vec<u8>>> = {
                let mut out = Vec::with_capacity(values.len());
                for v in values {
                    match v.as_ref() {
                        None | Some(CqlValue::Null) => out.push(None),
                        Some(CqlValue::Blob(b)) => out.push(Some(b.clone())),
                        Some(_) => {
                            return Err(ConvertError::TypeMismatch {
                                column,
                                expected: "blob",
                            })
                        }
                    }
                }
                out
            };
            let refs: Vec<Option<&[u8]>> = owned.iter().map(|o| o.as_deref()).collect();
            Arc::new(BinaryArray::from(refs)) as ArrayRef
        }
        CqlType::Date => {
            // Cassandra `date` is days-since-epoch offset so that 2^31 == 1970-01-01;
            // Arrow Date32 is signed days since 1970-01-01.
            prim!(Date32Array, CqlValue::Date(d), d => (*d as i64 - (1i64 << 31)) as i32, "date")
        }
        CqlType::Time => prim!(Time64NanosecondArray, CqlValue::Time(t), t => *t, "time"),
        CqlType::Duration => {
            let mut out: Vec<Option<arrow::datatypes::IntervalMonthDayNano>> =
                Vec::with_capacity(values.len());
            for v in values {
                match v.as_ref() {
                    None | Some(CqlValue::Null) => out.push(None),
                    Some(CqlValue::Duration {
                        months,
                        days,
                        nanos,
                    }) => out.push(Some(arrow::datatypes::IntervalMonthDayNano::new(
                        *months, *days, *nanos,
                    ))),
                    Some(_) => {
                        return Err(ConvertError::TypeMismatch {
                            column,
                            expected: "duration",
                        })
                    }
                }
            }
            Arc::new(IntervalMonthDayNanoArray::from(out)) as ArrayRef
        }
        CqlType::List(inner) | CqlType::Set(inner) => build_list(inner, values, column)?,
        CqlType::Vector(_, dim) => build_vector(*dim, values, column)?,
        CqlType::Map(k, v) => build_map(k, v, values, column)?,
        CqlType::Tuple(elems) => build_tuple(elems, values, column)?,
        CqlType::Udt {
            fields: udt_fields, ..
        } => build_udt(udt_fields, values, column)?,
        CqlType::Decimal => string_col!(
            "decimal",
            CqlValue::Decimal { scale, unscaled } => format_decimal(*scale, &unscaled.to_string()),
        ),
    })
}

/// Assemble an Arrow `StructArray` from ordered `(name, type)` fields, the
/// per-field value columns (each `values.len()` long), and per-row validity.
fn assemble_struct(
    field_defs: &[(String, CqlType)],
    per_field: Vec<Vec<Option<CqlValue>>>,
    valid: Vec<bool>,
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    use arrow::array::StructArray;
    use arrow::buffer::NullBuffer;

    let mut fields = Vec::with_capacity(field_defs.len());
    let mut children: Vec<ArrayRef> = Vec::with_capacity(field_defs.len());
    for ((name, ft), col) in field_defs.iter().zip(per_field) {
        let arr = build_values(ft, &col, column)?;
        fields.push(Arc::new(Field::new(name, arr.data_type().clone(), true)));
        children.push(arr);
    }
    Ok(Arc::new(StructArray::new(
        Fields::from(fields),
        children,
        Some(NullBuffer::from(valid)),
    )) as ArrayRef)
}

/// `tuple<...>` -> Arrow `StructArray` with positional field names "0","1",...
/// Each element type recurses through `build_values`; a NULL tuple is a null row.
fn build_tuple(
    elem_types: &[CqlType],
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    let arity = elem_types.len();
    let mut per_field: Vec<Vec<Option<CqlValue>>> = (0..arity)
        .map(|_| Vec::with_capacity(values.len()))
        .collect();
    let mut valid = Vec::with_capacity(values.len());

    for v in values {
        match v.as_ref() {
            None | Some(CqlValue::Null) => {
                for col in per_field.iter_mut() {
                    col.push(None);
                }
                valid.push(false);
            }
            Some(CqlValue::Tuple(elems)) => {
                for (i, col) in per_field.iter_mut().enumerate() {
                    col.push(elems.get(i).cloned().flatten());
                }
                valid.push(true);
            }
            Some(_) => {
                return Err(ConvertError::TypeMismatch {
                    column,
                    expected: "tuple",
                })
            }
        }
    }

    let defs: Vec<(String, CqlType)> = elem_types
        .iter()
        .enumerate()
        .map(|(i, t)| (i.to_string(), t.clone()))
        .collect();
    assemble_struct(&defs, per_field, valid, column)
}

/// `udt` -> Arrow `StructArray` with the UDT's named fields (positional, matching
/// the type's field order). A NULL UDT is a null row.
fn build_udt(
    field_defs: &[(String, CqlType)],
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    let n = field_defs.len();
    let mut per_field: Vec<Vec<Option<CqlValue>>> =
        (0..n).map(|_| Vec::with_capacity(values.len())).collect();
    let mut valid = Vec::with_capacity(values.len());

    for v in values {
        match v.as_ref() {
            None | Some(CqlValue::Null) => {
                for col in per_field.iter_mut() {
                    col.push(None);
                }
                valid.push(false);
            }
            Some(CqlValue::Udt(entries)) => {
                for (i, col) in per_field.iter_mut().enumerate() {
                    col.push(entries.get(i).and_then(|(_, val)| val.clone()));
                }
                valid.push(true);
            }
            Some(_) => {
                return Err(ConvertError::TypeMismatch {
                    column,
                    expected: "udt",
                })
            }
        }
    }

    assemble_struct(field_defs, per_field, valid, column)
}

/// Build an Arrow `MapArray` for a `map<k, v>` column: flatten every row's
/// entries into parallel key/value child arrays with per-row offsets and
/// validity (a NULL row is a null map, distinct from an empty map). Keys and
/// values recurse through `build_values`.
fn build_map(
    kt: &CqlType,
    vt: &CqlType,
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    use arrow::array::{MapArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut keys: Vec<Option<CqlValue>> = Vec::new();
    let mut vals: Vec<Option<CqlValue>> = Vec::new();
    let mut valid: Vec<bool> = Vec::with_capacity(values.len());

    for v in values {
        match v.as_ref() {
            None | Some(CqlValue::Null) => valid.push(false),
            Some(CqlValue::Map(entries)) => {
                for (k, val) in entries {
                    keys.push(Some(k.clone()));
                    vals.push(Some(val.clone()));
                }
                valid.push(true);
            }
            Some(_) => {
                return Err(ConvertError::TypeMismatch {
                    column,
                    expected: "map",
                })
            }
        }
        offsets.push(keys.len() as i32);
    }

    let key_arr = build_values(kt, &keys, column)?;
    let val_arr = build_values(vt, &vals, column)?;
    let entries_field = map_entries_field(key_arr.data_type().clone(), val_arr.data_type().clone());
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("map_entries_field always builds a struct"),
    };
    let entries = StructArray::new(struct_fields, vec![key_arr, val_arr], None);
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::from(valid);
    Ok(Arc::new(MapArray::new(
        entries_field,
        offsets,
        entries,
        Some(nulls),
        false,
    )) as ArrayRef)
}

/// Build an Arrow `FixedSizeListArray` of `Float32` for a `vector<float, dim>`
/// column. Values are stored as f32 bit patterns. A NULL row still occupies
/// `dim` child slots (filled null) so the fixed stride holds; a non-NULL vector
/// with the wrong length is a data error (fail loud).
fn build_vector(
    dim: usize,
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    use arrow::array::FixedSizeListArray;
    use arrow::buffer::NullBuffer;

    let mut child: Vec<Option<f32>> = Vec::with_capacity(values.len() * dim);
    let mut valid: Vec<bool> = Vec::with_capacity(values.len());

    for v in values {
        match v.as_ref() {
            None | Some(CqlValue::Null) => {
                child.extend(std::iter::repeat_n(None, dim));
                valid.push(false);
            }
            Some(CqlValue::Vector(bits)) => {
                if bits.len() != dim {
                    return Err(ConvertError::TypeMismatch {
                        column,
                        expected: "vector<float, dim>",
                    });
                }
                child.extend(bits.iter().map(|b| Some(f32::from_bits(*b))));
                valid.push(true);
            }
            Some(_) => {
                return Err(ConvertError::TypeMismatch {
                    column,
                    expected: "vector",
                })
            }
        }
    }

    let child_arr = Arc::new(Float32Array::from(child)) as ArrayRef;
    let field = Arc::new(Field::new("item", DataType::Float32, true));
    let nulls = NullBuffer::from(valid);
    Ok(Arc::new(FixedSizeListArray::new(
        field,
        dim as i32,
        child_arr,
        Some(nulls),
    )) as ArrayRef)
}

/// Build an Arrow `ListArray` for a `list<inner>` / `set<inner>` column:
/// flatten every row's elements into one child array, tracking per-row offsets
/// and validity (a NULL row is a null list, distinct from an empty list).
fn build_list(
    inner: &CqlType,
    values: &[Option<CqlValue>],
    column: usize,
) -> Result<ArrayRef, ConvertError> {
    use arrow::array::ListArray;
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    let mut offsets: Vec<i32> = Vec::with_capacity(values.len() + 1);
    offsets.push(0);
    let mut child: Vec<Option<CqlValue>> = Vec::new();
    let mut valid: Vec<bool> = Vec::with_capacity(values.len());

    for v in values {
        match v.as_ref() {
            None | Some(CqlValue::Null) => valid.push(false),
            Some(CqlValue::List(items)) | Some(CqlValue::Set(items)) => {
                child.extend(items.iter().cloned().map(Some));
                valid.push(true);
            }
            Some(_) => {
                return Err(ConvertError::TypeMismatch {
                    column,
                    expected: "list/set",
                })
            }
        }
        offsets.push(child.len() as i32);
    }

    let child_arr = build_values(inner, &child, column)?;
    let field = Arc::new(Field::new("item", child_arr.data_type().clone(), true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let nulls = NullBuffer::from(valid);
    Ok(Arc::new(ListArray::new(field, offsets, child_arr, Some(nulls))) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, Date32Array, Int32Array, StringArray};

    #[test]
    fn converts_scalar_columns_with_nulls() {
        let names = vec!["id".to_string(), "name".to_string(), "active".to_string()];
        let types = vec![CqlType::Int, CqlType::Varchar, CqlType::Boolean];
        let rows = vec![
            vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Text("alice".into())),
                Some(CqlValue::Boolean(true)),
            ],
            // a row with a NULL name (absent cell handled the same way)
            vec![
                Some(CqlValue::Int(2)),
                Some(CqlValue::Null),
                Some(CqlValue::Boolean(false)),
            ],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "id");

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let nbatch = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(nbatch.value(0), "alice");
        assert!(nbatch.is_null(1), "NULL name -> Arrow null slot");

        let active = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active.value(0));
        assert!(!active.value(1));
    }

    #[test]
    fn converts_decimal_to_text() {
        let names = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let types = vec![CqlType::Decimal, CqlType::Decimal, CqlType::Decimal];
        let rows = vec![vec![
            // 12345 x 10^-2 = 123.45
            Some(CqlValue::Decimal {
                scale: 2,
                unscaled: num_bigint::BigInt::from(12345),
            }),
            // 5 x 10^-3 = 0.005 (zero-padded)
            Some(CqlValue::Decimal {
                scale: 3,
                unscaled: num_bigint::BigInt::from(5),
            }),
            // -7 x 10^2 = -700 (negative, non-positive scale)
            Some(CqlValue::Decimal {
                scale: -2,
                unscaled: num_bigint::BigInt::from(-7),
            }),
        ]];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let col = |i: usize| {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0)
                .to_string()
        };
        assert_eq!(col(0), "123.45");
        assert_eq!(col(1), "0.005");
        assert_eq!(col(2), "-700");
    }

    #[test]
    fn reverse_unsupported_arrow_fails_loud() {
        // record_batch_to_rows covers common scalars; a Date32 column (produced
        // by the forward path) has no reverse mapping -> UnsupportedArrow.
        let batch = rows_to_record_batch(
            &["d".to_string()],
            &[CqlType::Date],
            &[vec![Some(CqlValue::Date(2_147_483_648))]],
        )
        .unwrap();
        let err = record_batch_to_rows(&batch).unwrap_err();
        assert!(matches!(err, ConvertError::UnsupportedArrow(_)));
    }

    #[test]
    fn converts_list_column_with_null_and_empty() {
        use arrow::array::ListArray;
        let names = vec!["tags".to_string()];
        let types = vec![CqlType::List(Box::new(CqlType::Int))];
        let rows = vec![
            vec![Some(CqlValue::List(vec![
                CqlValue::Int(1),
                CqlValue::Int(2),
            ]))],
            vec![Some(CqlValue::Null)],         // null list
            vec![Some(CqlValue::List(vec![]))], // empty list (distinct from null)
            vec![Some(CqlValue::List(vec![CqlValue::Int(3)]))],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        assert_eq!(batch.num_rows(), 4);
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 2);
        assert!(list.is_null(1), "NULL list -> null slot");
        assert!(!list.is_null(2));
        assert_eq!(list.value(2).len(), 0, "empty list -> 0-length, not null");
        // Children are flattened across non-null lists: [1, 2, 3].
        let child = list.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(child.len(), 3);
        assert_eq!(child.value(0), 1);
        assert_eq!(child.value(2), 3);
    }

    #[test]
    fn converts_temporal_inet_varint_duration() {
        use std::net::IpAddr;
        let names = vec![
            "d".to_string(),
            "t".to_string(),
            "ip".to_string(),
            "vi".to_string(),
            "dur".to_string(),
        ];
        let types = vec![
            CqlType::Date,
            CqlType::Time,
            CqlType::Inet,
            CqlType::Varint,
            CqlType::Duration,
        ];
        let rows = vec![vec![
            Some(CqlValue::Date(2_147_483_648)), // 2^31 == 1970-01-01 -> Date32 0
            Some(CqlValue::Time(3_600_000_000_000)), // 01:00:00 in nanoseconds
            Some(CqlValue::Inet("127.0.0.1".parse::<IpAddr>().unwrap())),
            Some(CqlValue::Varint(num_bigint::BigInt::from(
                123_456_789_012_345_678i64,
            ))),
            Some(CqlValue::Duration {
                months: 1,
                days: 2,
                nanos: 3,
            }),
        ]];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        assert_eq!(batch.num_columns(), 5);

        let d = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(d.value(0), 0, "2^31 maps to the Arrow epoch (0 days)");

        let ip = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ip.value(0), "127.0.0.1");

        let vi = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(vi.value(0), "123456789012345678");
    }

    #[test]
    fn converts_vector_to_fixed_size_list() {
        use arrow::array::{FixedSizeListArray, Float32Array};
        let names = vec!["embedding".to_string()];
        let types = vec![CqlType::Vector(Box::new(CqlType::Float), 3)];
        let rows = vec![
            vec![Some(CqlValue::Vector(vec![
                1.0f32.to_bits(),
                2.0f32.to_bits(),
                3.0f32.to_bits(),
            ]))],
            vec![Some(CqlValue::Null)],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let fsl = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(fsl.value_length(), 3, "fixed dimension 3");
        assert!(!fsl.is_null(0));
        assert!(fsl.is_null(1), "NULL vector -> null row");
        let v0 = fsl.value(0);
        let f = v0.as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(f.value(0), 1.0);
        assert_eq!(f.value(2), 3.0);
    }

    #[test]
    fn converts_map_to_map_array() {
        use arrow::array::MapArray;
        let names = vec!["attrs".to_string()];
        let types = vec![CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::Int),
        )];
        let rows = vec![
            vec![Some(CqlValue::Map(vec![
                (CqlValue::Text("a".into()), CqlValue::Int(1)),
                (CqlValue::Text("b".into()), CqlValue::Int(2)),
            ]))],
            vec![Some(CqlValue::Null)],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let m = batch.column(0).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!m.is_null(0));
        assert!(m.is_null(1), "NULL map -> null row");
        assert_eq!(m.value_length(0), 2);
        let keys = m.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let vals = m.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(keys.value(0), "a");
        assert_eq!(vals.value(1), 2);
    }

    #[test]
    fn converts_tuple_to_struct() {
        use arrow::array::StructArray;
        let names = vec!["t".to_string()];
        let types = vec![CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])];
        let rows = vec![
            vec![Some(CqlValue::Tuple(vec![
                Some(CqlValue::Int(7)),
                Some(CqlValue::Text("x".into())),
            ]))],
            vec![Some(CqlValue::Null)],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(s.num_columns(), 2);
        assert!(!s.is_null(0));
        assert!(s.is_null(1), "NULL tuple -> null row");
        let f0 = s.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(f0.value(0), 7);
    }

    #[test]
    fn converts_udt_to_struct_with_named_fields() {
        use arrow::array::StructArray;
        let names = vec!["addr".to_string()];
        let types = vec![CqlType::Udt {
            keyspace: "ks".into(),
            name: "address".into(),
            fields: vec![
                ("city".into(), CqlType::Varchar),
                ("zip".into(), CqlType::Int),
            ],
        }];
        let rows = vec![vec![Some(CqlValue::Udt(vec![
            ("city".into(), Some(CqlValue::Text("NYC".into()))),
            ("zip".into(), Some(CqlValue::Int(10001))),
        ]))]];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(s.num_columns(), 2);
        match s.data_type() {
            DataType::Struct(f) => {
                assert_eq!(f[0].name(), "city");
                assert_eq!(f[1].name(), "zip");
            }
            other => panic!("expected struct, got {other:?}"),
        }
        let zip = s.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(zip.value(0), 10001);
    }

    #[test]
    fn record_batch_round_trips_scalars() {
        let names = vec!["id".to_string(), "name".to_string(), "active".to_string()];
        let types = vec![CqlType::Int, CqlType::Varchar, CqlType::Boolean];
        let rows = vec![
            vec![
                Some(CqlValue::Int(1)),
                Some(CqlValue::Text("alice".into())),
                Some(CqlValue::Boolean(true)),
            ],
            vec![Some(CqlValue::Int(2)), None, Some(CqlValue::Boolean(false))],
        ];

        let batch = rows_to_record_batch(&names, &types, &rows).unwrap();
        let (decoded_names, decoded_rows) = record_batch_to_rows(&batch).unwrap();
        assert_eq!(decoded_names, names);
        assert_eq!(decoded_rows, rows, "scalars round-trip CQL -> Arrow -> CQL");
    }

    #[test]
    fn type_mismatch_fails_loud() {
        // Column declared Int but a row carries Text — must error, not coerce.
        let names = vec!["id".to_string()];
        let types = vec![CqlType::Int];
        let rows = vec![vec![Some(CqlValue::Text("oops".into()))]];
        let err = rows_to_record_batch(&names, &types, &rows).unwrap_err();
        assert!(matches!(err, ConvertError::TypeMismatch { column: 0, .. }));
    }
}
