//! Module: shared oracle helpers for the jsonb conformance tests.
//! Correctness: correct when the value model, the metadata/value splitter and the
//! canonical-kind chooser are written from the Variant spec and the T-100..T-104
//! architecture rules, never by calling ferrosa-jsonb's own encoder or decoder to
//! derive an expectation. Ferrosa and parquet-variant are each read back into the
//! same `Model`, so equality of models is equality of meaning.
//! Last revised: 2026-09-28
//! Last changed: t_0b127826 initial oracle support.

#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};

use ferrosa_jsonb::{JsonbBuilder, JsonbError, Number, NumberKind, ValueKind, ValueRef};
use parquet_variant::{
    ShortString, Variant, VariantBuilder, VariantDecimal16, VariantDecimal4, VariantDecimal8,
};
use proptest::prelude::*;

/// The golden corpus that ferrosa-jsonb's own tests pin (T-102), reused verbatim.
#[path = "../../../ferrosa-jsonb/tests/common/mod.rs"]
pub mod golden;

pub use golden::Op;

/// The cell envelope byte (architecture 3.2): the only ferrosa-specific byte.
pub const ENVELOPE: u8 = 0xF1;
/// 10^38: unscaled magnitudes below it fit a Variant `decimal16`.
const POW10_38: u128 = 100_000_000_000_000_000_000_000_000_000_000_000_000;

/// An exact decimal: `unscaled x 10^-scale`. Integers have scale 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dec {
    pub unscaled: i128,
    pub scale: u8,
}

/// The value model both codecs are read back into. Objects are keyed by string,
/// so keys are unique and ordered by UTF-8 bytes (the Variant sort order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Model {
    Null,
    Bool(bool),
    Str(String),
    Num(Dec),
    Arr(Vec<Model>),
    Obj(BTreeMap<String, Model>),
}

// ---- lexemes ------------------------------------------------------------------

/// Independent reading of a JSON number lexeme: scale is the digits after the
/// point minus the exponent (never below zero), like PG `numeric`. `None` when the
/// value needs more than 38 digits or a scale above 38 (a bigdecimal in ferrosa).
pub fn lexeme_to_dec(lexeme: &str) -> Option<Dec> {
    let (neg, rest) = match lexeme.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, lexeme),
    };
    let (mantissa, exp) = match rest.find(['e', 'E']) {
        Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{int}{frac}");
    let mut sig = digits.trim_start_matches('0').to_string();
    let mut scale = i64::try_from(frac.len()).ok()? - exp;
    if sig.is_empty() {
        scale = scale.max(0);
    } else if scale < 0 {
        let pad = usize::try_from(-scale).ok()?;
        if sig.len().checked_add(pad)? > 38 {
            return None;
        }
        sig.push_str(&"0".repeat(pad));
        scale = 0;
    }
    if sig.len() > 38 || scale > 38 {
        return None;
    }
    let mag: i128 = if sig.is_empty() { 0 } else { sig.parse().ok()? };
    Some(Dec {
        unscaled: if neg { -mag } else { mag },
        scale: u8::try_from(scale).ok()?,
    })
}

/// A JSON lexeme for `d`.
pub fn dec_lexeme(d: &Dec) -> String {
    let mag = d.unscaled.unsigned_abs().to_string();
    let sign = if d.unscaled < 0 { "-" } else { "" };
    if d.scale == 0 {
        return format!("{sign}{mag}");
    }
    let scale = usize::from(d.scale);
    let padded = format!("{mag:0>width$}", width = scale + 1);
    let (int, frac) = padded.split_at(padded.len() - scale);
    format!("{sign}{int}.{frac}")
}

/// The unscaled value and scale of a ferrosa `Number`, read from its text form.
pub fn dec_of_number(n: &Number) -> Result<Dec, String> {
    let text = n.to_string();
    let (neg, body) = match text.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, text.as_str()),
    };
    let (int, frac) = body.split_once('.').unwrap_or((body, ""));
    let mag: i128 = format!("{int}{frac}")
        .parse()
        .map_err(|e| format!("number {text}: {e}"))?;
    let scale = u8::try_from(frac.len()).map_err(|e| format!("number {text}: {e}"))?;
    if scale != u8::try_from(n.scale()).map_err(|e| e.to_string())? {
        return Err(format!("number {text}: scale() disagrees with its text"));
    }
    Ok(Dec {
        unscaled: if neg { -mag } else { mag },
        scale,
    })
}

// ---- source model from golden ops -----------------------------------------------

enum Open {
    Obj(BTreeMap<String, Model>, Option<String>),
    Arr(Vec<Model>),
}

/// The value an op sequence denotes, with last-key-wins objects. The flag is false
/// when a number was too wide for a Variant decimal (a ferrosa bigdecimal), in
/// which case the case is outside the extension-free corpus.
pub fn model_from_ops(ops: &[Op]) -> (Model, bool) {
    fn put(stack: &mut [Open], root: &mut Option<Model>, v: Model) {
        match stack.last_mut() {
            Some(Open::Obj(map, key)) => {
                map.insert(key.take().expect("op stream: value without key"), v);
            }
            Some(Open::Arr(items)) => items.push(v),
            None => *root = Some(v),
        }
    }
    let (mut stack, mut root, mut qualifies) = (Vec::new(), None, true);
    for op in ops {
        match op {
            Op::ObjBegin => stack.push(Open::Obj(BTreeMap::new(), None)),
            Op::ArrBegin => stack.push(Open::Arr(Vec::new())),
            Op::Key(k) => match stack.last_mut() {
                Some(Open::Obj(_, key)) => *key = Some(k.clone()),
                _ => panic!("op stream: key outside an object"),
            },
            Op::ObjEnd => match stack.pop() {
                Some(Open::Obj(map, _)) => put(&mut stack, &mut root, Model::Obj(map)),
                _ => panic!("op stream: unbalanced object end"),
            },
            Op::ArrEnd => match stack.pop() {
                Some(Open::Arr(items)) => put(&mut stack, &mut root, Model::Arr(items)),
                _ => panic!("op stream: unbalanced array end"),
            },
            Op::Null => put(&mut stack, &mut root, Model::Null),
            Op::True => put(&mut stack, &mut root, Model::Bool(true)),
            Op::False => put(&mut stack, &mut root, Model::Bool(false)),
            Op::Str(s) => put(&mut stack, &mut root, Model::Str(s.clone())),
            Op::Num(l) => {
                let dec = lexeme_to_dec(l);
                qualifies &= dec.is_some();
                let dec = dec.unwrap_or(Dec {
                    unscaled: 0,
                    scale: 0,
                });
                put(&mut stack, &mut root, Model::Num(dec));
            }
        }
    }
    (root.expect("op stream: no root value"), qualifies)
}

/// Every object key used anywhere in `m`.
pub fn collect_keys(m: &Model, out: &mut BTreeSet<String>) {
    match m {
        Model::Obj(map) => {
            for (k, v) in map {
                out.insert(k.clone());
                collect_keys(v, out);
            }
        }
        Model::Arr(items) => items.iter().for_each(|v| collect_keys(v, out)),
        _ => {}
    }
}

// ---- ferrosa side ---------------------------------------------------------------

fn emit(b: &mut JsonbBuilder, m: &Model, reverse: bool) -> Result<(), JsonbError> {
    match m {
        Model::Null => b.null(),
        Model::Bool(v) => b.boolean(*v),
        Model::Str(s) => b.string(s),
        Model::Num(d) => b.number(Number::parse_lexeme(&dec_lexeme(d))?),
        Model::Arr(items) => {
            b.begin_array()?;
            for v in items {
                emit(b, v, reverse)?;
            }
            b.end_array()
        }
        Model::Obj(map) => {
            b.begin_object()?;
            let mut entries: Vec<_> = map.iter().collect();
            if reverse {
                entries.reverse();
            }
            for (k, v) in entries {
                b.key(k)?;
                emit(b, v, reverse)?;
            }
            b.end_object()
        }
    }
}

/// The ferrosa cell for `m`; `reverse` feeds object keys in descending order.
pub fn ferrosa_encode(m: &Model, reverse: bool) -> Vec<u8> {
    let mut b = JsonbBuilder::new(golden::limits());
    emit(&mut b, m, reverse).expect("ferrosa builder accepts the model");
    b.finish().expect("ferrosa finish").bytes
}

/// Read a validated ferrosa value back into the model.
pub fn ferrosa_to_model(v: ValueRef<'_>) -> Result<Model, String> {
    let e = |x: JsonbError| x.to_string();
    Ok(match v.kind().map_err(e)? {
        ValueKind::Null => Model::Null,
        ValueKind::Bool => Model::Bool(v.as_bool().map_err(e)?),
        ValueKind::String => Model::Str(v.as_str().map_err(e)?.to_string()),
        ValueKind::Number => Model::Num(dec_of_number(&v.as_number().map_err(e)?)?),
        ValueKind::Array => {
            let mut items = Vec::new();
            for item in v.as_array().map_err(e)?.iter() {
                items.push(ferrosa_to_model(item.map_err(e)?)?);
            }
            Model::Arr(items)
        }
        ValueKind::Object => {
            let mut map = BTreeMap::new();
            for entry in v.as_object().map_err(e)?.iter() {
                let (k, child) = entry.map_err(e)?;
                if map
                    .insert(k.to_string(), ferrosa_to_model(child)?)
                    .is_some()
                {
                    return Err(format!("duplicate key {k:?}"));
                }
            }
            Model::Obj(map)
        }
    })
}

/// True when any number under `v` is a ferrosa bigdecimal (primitive 63).
pub fn contains_bigdecimal(v: ValueRef<'_>) -> bool {
    match v.kind() {
        Ok(ValueKind::Number) => v
            .as_number()
            .is_ok_and(|n| n.kind() == NumberKind::BigDecimal),
        Ok(ValueKind::Array) => v
            .as_array()
            .is_ok_and(|a| a.iter().flatten().any(contains_bigdecimal)),
        Ok(ValueKind::Object) => v
            .as_object()
            .is_ok_and(|o| o.iter().flatten().any(|(_, c)| contains_bigdecimal(c))),
        _ => false,
    }
}

// ---- spec-derived cell splitting ------------------------------------------------

/// Little-endian unsigned integer.
pub fn le(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .rev()
        .fold(0, |a, b| (a << 8) | usize::from(*b))
}

/// Strip the envelope and split `metadata | value` exactly as the Variant spec
/// says: header bits 7-6 are `offset_size_minus_one`, then `dictionary_size`, then
/// `dictionary_size + 1` offsets of that width, then the key bytes.
pub fn split_cell(cell: &[u8]) -> (&[u8], &[u8]) {
    assert_eq!(cell[0], ENVELOPE, "cell must start with the envelope byte");
    let body = &cell[1..];
    let width = usize::from(body[0] >> 6) + 1;
    let count = le(&body[1..1 + width]);
    let last_at = 1 + width + count * width;
    let key_bytes = le(&body[last_at..last_at + width]);
    body.split_at(last_at + width + key_bytes)
}

// ---- upstream side --------------------------------------------------------------

/// Read a fully validated upstream variant back into the model.
pub fn variant_to_model(v: &Variant<'_, '_>) -> Result<Model, String> {
    let dec = |unscaled: i128, scale: u8| Model::Num(Dec { unscaled, scale });
    Ok(match v {
        Variant::Null => Model::Null,
        Variant::BooleanTrue => Model::Bool(true),
        Variant::BooleanFalse => Model::Bool(false),
        Variant::Int8(x) => dec(i128::from(*x), 0),
        Variant::Int16(x) => dec(i128::from(*x), 0),
        Variant::Int32(x) => dec(i128::from(*x), 0),
        Variant::Int64(x) => dec(i128::from(*x), 0),
        Variant::Decimal4(d) => dec(i128::from(d.integer()), d.scale()),
        Variant::Decimal8(d) => dec(i128::from(d.integer()), d.scale()),
        Variant::Decimal16(d) => dec(d.integer(), d.scale()),
        Variant::String(s) => Model::Str((*s).to_string()),
        Variant::ShortString(s) => Model::Str(s.as_str().to_string()),
        Variant::List(list) => {
            let mut items = Vec::new();
            for item in list.iter_try() {
                items.push(variant_to_model(&item.map_err(|e| e.to_string())?)?);
            }
            Model::Arr(items)
        }
        Variant::Object(obj) => {
            let mut map = BTreeMap::new();
            for entry in obj.iter_try() {
                let (k, child) = entry.map_err(|e| e.to_string())?;
                if map
                    .insert(k.to_string(), variant_to_model(&child)?)
                    .is_some()
                {
                    return Err(format!("upstream object repeats key {k:?}"));
                }
            }
            Model::Obj(map)
        }
        other => return Err(format!("unexpected upstream variant {other:?}")),
    })
}

/// The smallest Variant number for `d` (architecture C9): integers by width,
/// decimals by digits and scale; a scale-0 integer beyond i64 is a decimal16.
fn number_variant(d: &Dec) -> Variant<'static, 'static> {
    let mag = d.unscaled.unsigned_abs();
    if d.scale == 0 {
        if let Ok(x) = i8::try_from(d.unscaled) {
            return Variant::Int8(x);
        }
        if let Ok(x) = i16::try_from(d.unscaled) {
            return Variant::Int16(x);
        }
        if let Ok(x) = i32::try_from(d.unscaled) {
            return Variant::Int32(x);
        }
        if let Ok(x) = i64::try_from(d.unscaled) {
            return Variant::Int64(x);
        }
    } else if mag < 1_000_000_000 && d.scale <= 9 {
        let x = i32::try_from(d.unscaled).expect("below 10^9");
        return Variant::Decimal4(VariantDecimal4::try_new(x, d.scale).expect("decimal4"));
    } else if mag < 1_000_000_000_000_000_000 && d.scale <= 18 {
        let x = i64::try_from(d.unscaled).expect("below 10^18");
        return Variant::Decimal8(VariantDecimal8::try_new(x, d.scale).expect("decimal8"));
    }
    assert!(mag < POW10_38, "model numbers stay within 38 digits");
    Variant::Decimal16(VariantDecimal16::try_new(d.unscaled, d.scale).expect("decimal16"))
}

fn leaf_variant(m: &Model) -> Option<Variant<'_, '_>> {
    Some(match m {
        Model::Null => Variant::Null,
        Model::Bool(true) => Variant::BooleanTrue,
        Model::Bool(false) => Variant::BooleanFalse,
        Model::Str(s) => match ShortString::try_new(s) {
            Ok(short) => Variant::ShortString(short),
            Err(_) => Variant::String(s),
        },
        Model::Num(d) => number_variant(d),
        Model::Arr(_) | Model::Obj(_) => return None,
    })
}

/// Build `m` with parquet-variant's own builder, choosing every option the way a
/// canonical writer would: dictionary pre-registered in byte order, smallest number
/// kinds, short strings short. Returns `(metadata, value)`.
pub fn upstream_build(m: &Model) -> (Vec<u8>, Vec<u8>) {
    let mut keys = BTreeSet::new();
    collect_keys(m, &mut keys);
    let mut builder = VariantBuilder::new().with_field_names(keys.iter().map(String::as_str));
    match m {
        Model::Arr(items) => {
            let mut list = builder.new_list();
            for item in items {
                append_child(|v| list.append_value(v), item);
            }
            list.finish();
        }
        Model::Obj(map) => {
            let mut obj = builder.new_object();
            for (k, item) in map {
                append_child(|v| obj.insert(k, v), item);
            }
            obj.finish();
        }
        leaf => builder.append_value(leaf_variant(leaf).expect("leaf")),
    }
    builder.finish()
}

/// Hand `item` to `sink` as a variant: a leaf directly, a container through its
/// own upstream build (parquet-variant re-registers its keys in the parent).
fn append_child(sink: impl FnOnce(Variant<'_, '_>), item: &Model) {
    match leaf_variant(item) {
        Some(v) => sink(v),
        None => {
            let (md, val) = upstream_build(item);
            let v = Variant::try_new(&md, &val).expect("upstream builds a valid variant");
            sink(v);
        }
    }
}

/// The full cell (`envelope | metadata | value`) for an upstream build.
pub fn cell_of(metadata: &[u8], value: &[u8]) -> Vec<u8> {
    let mut cell = vec![ENVELOPE];
    cell.extend_from_slice(metadata);
    cell.extend_from_slice(value);
    cell
}

// ---- generators -----------------------------------------------------------------

fn dec_strategy() -> impl Strategy<Value = Dec> {
    let general =
        (0u32..=38, any::<u128>(), any::<bool>(), 0u8..=38).prop_map(|(digits, r, neg, scale)| {
            let mag = if digits == 0 {
                0
            } else {
                i128::try_from(r % 10u128.pow(digits)).expect("below 10^38")
            };
            Dec {
                unscaled: if neg { -mag } else { mag },
                scale,
            }
        });
    prop_oneof![
        any::<i64>().prop_map(|v| Dec {
            unscaled: i128::from(v),
            scale: 0
        }),
        (-2000i128..2000, 0u8..12).prop_map(|(unscaled, scale)| Dec { unscaled, scale }),
        general,
    ]
}

fn string_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        prop::collection::vec(any::<char>(), 0..24).prop_map(|c| c.into_iter().collect()),
        (58usize..70).prop_map(|n| "x".repeat(n)),
        (19usize..24).prop_map(|n| "\u{65e5}".repeat(n)),
    ]
}

fn key_strategy() -> impl Strategy<Value = String> {
    let alphabet = vec![
        'a',
        'b',
        'c',
        'Z',
        '_',
        '\u{e9}',
        '\u{65e5}',
        '\u{1f600}',
        '\u{0}',
    ];
    prop::collection::vec(prop::sample::select(alphabet), 0..5)
        .prop_map(|c| c.into_iter().collect())
}

/// Extension-free values, nesting up to 6 levels.
pub fn model_strategy() -> impl Strategy<Value = Model> {
    let leaf = prop_oneof![
        Just(Model::Null),
        any::<bool>().prop_map(Model::Bool),
        string_strategy().prop_map(Model::Str),
        dec_strategy().prop_map(Model::Num),
    ];
    leaf.prop_recursive(6, 96, 8, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..8).prop_map(Model::Arr),
            prop::collection::vec((key_strategy(), inner), 0..8)
                .prop_map(|kv| Model::Obj(kv.into_iter().collect())),
        ]
    })
}

/// A chain of `depth` containers (alternating array/object) around a scalar.
pub fn nested(depth: usize) -> Model {
    (0..depth).fold(Model::Bool(true), |inner, level| {
        if level % 2 == 0 {
            Model::Arr(vec![inner])
        } else {
            Model::Obj(BTreeMap::from([("k".to_string(), inner)]))
        }
    })
}
