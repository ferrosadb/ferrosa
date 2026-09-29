//! Module: validator and checked reader conformance (T-104, D4, D14b, FM-05,
//! FM-07, FM-17, JB-T1, JB-T10).
//! Correctness: correct when every canonical byte string validates and reads back
//! to the value that built it, every mutation of one is rejected with a typed
//! error or is itself canonical (never a panic), and hostile hand-built cells
//! are rejected without trusting any claimed count, offset or length.
//! Last revised: 2026-09-28
//! Last changed: T-104 initial suite.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::vec_init_then_push
)]

mod common;

use common::{corpus, encode, limits, Op};
use ferrosa_jsonb::{
    ArrayIter, JsonbBuilder, JsonbError, JsonbRef, Number, ObjectIter, ValueKind, ValueRef,
    HARD_MAX_ENCODED_BYTES,
};
use proptest::prelude::*;

// ---- oracle: rebuild a value from the reader, iteratively -------------------

enum Walk<'a> {
    Obj(ObjectIter<'a>),
    Arr(ArrayIter<'a>),
}

fn emit_scalar(b: &mut JsonbBuilder, v: ValueRef<'_>) -> Result<(), JsonbError> {
    match v.kind()? {
        ValueKind::Null => b.null(),
        ValueKind::Bool => b.boolean(v.as_bool()?),
        ValueKind::Number => b.number(v.as_number()?),
        ValueKind::String => b.string(v.as_str()?),
        ValueKind::Object | ValueKind::Array => Ok(()),
    }
}

fn open<'a>(b: &mut JsonbBuilder, v: ValueRef<'a>) -> Result<Option<Walk<'a>>, JsonbError> {
    match v.kind()? {
        ValueKind::Object => {
            b.begin_object()?;
            Ok(Some(Walk::Obj(v.as_object()?.iter())))
        }
        ValueKind::Array => {
            b.begin_array()?;
            Ok(Some(Walk::Arr(v.as_array()?.iter())))
        }
        ValueKind::Null | ValueKind::Bool | ValueKind::Number | ValueKind::String => {
            emit_scalar(b, v)?;
            Ok(None)
        }
    }
}

/// Re-encode a validated value through the canonical builder.
fn rebuild(r: &JsonbRef<'_>) -> Result<Vec<u8>, JsonbError> {
    let mut b = JsonbBuilder::new(limits());
    let mut stack: Vec<Walk<'_>> = Vec::new();
    stack.extend(open(&mut b, r.root())?);
    while let Some(top) = stack.last_mut() {
        let next = match top {
            Walk::Obj(it) => match it.next() {
                Some(item) => {
                    let (key, v) = item?;
                    b.key(key)?;
                    Some(v)
                }
                None => None,
            },
            Walk::Arr(it) => it.next().transpose()?,
        };
        match next {
            Some(v) => stack.extend(open(&mut b, v)?),
            None => {
                match stack.pop() {
                    Some(Walk::Obj(_)) => b.end_object()?,
                    Some(Walk::Arr(_)) => b.end_array()?,
                    None => {}
                };
            }
        }
    }
    Ok(b.finish()?.bytes)
}

/// Validate, then require the canonical round trip.
fn assert_round_trip(bytes: &[u8]) {
    let r = JsonbRef::validate(bytes).expect("canonical cell validates");
    assert_eq!(rebuild(&r).expect("rebuild"), bytes);
}

// ---- hand-built cells -------------------------------------------------------

fn width(v: usize) -> usize {
    match v {
        0..=0xFF => 1,
        0x100..=0xFFFF => 2,
        0x1_0000..=0xFF_FFFF => 3,
        _ => 4,
    }
}

fn le(v: usize, w: usize) -> Vec<u8> {
    v.to_le_bytes()[..w].to_vec()
}

fn meta(keys: &[&str]) -> Vec<u8> {
    let bytes: usize = keys.iter().map(|k| k.len()).sum();
    let w = width(keys.len().max(bytes));
    let mut out = vec![0x11 | (((w - 1) as u8) << 6)];
    out.extend(le(keys.len(), w));
    let mut off = 0;
    out.extend(le(off, w));
    for k in keys {
        off += k.len();
        out.extend(le(off, w));
    }
    for k in keys {
        out.extend(k.as_bytes());
    }
    out
}

fn cell(keys: &[&str], value: &[u8]) -> Vec<u8> {
    let mut out = vec![0xF1];
    out.extend(meta(keys));
    out.extend(value);
    out
}

fn offsets(children: &[Vec<u8>], w: usize) -> (Vec<u8>, Vec<u8>) {
    let mut table = le(0, w);
    let mut data = Vec::new();
    for c in children {
        data.extend(c);
        table.extend(le(data.len(), w));
    }
    (table, data)
}

fn arr(children: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = children.iter().map(Vec::len).sum();
    let w = width(total);
    let large = children.len() > 255;
    let mut out = vec![0x03 | (((w - 1) as u8) << 2) | (u8::from(large) << 4)];
    out.extend(le(children.len(), if large { 4 } else { 1 }));
    let (table, data) = offsets(children, w);
    out.extend(table);
    out.extend(data);
    out
}

fn obj(entries: &[(usize, Vec<u8>)]) -> Vec<u8> {
    let children: Vec<Vec<u8>> = entries.iter().map(|e| e.1.clone()).collect();
    let total: usize = children.iter().map(Vec::len).sum();
    let ow = width(total);
    let iw = width(entries.last().map_or(0, |e| e.0));
    let large = entries.len() > 255;
    let mut out =
        vec![0x02 | (((ow - 1) as u8) << 2) | (((iw - 1) as u8) << 4) | (u8::from(large) << 6)];
    out.extend(le(entries.len(), if large { 4 } else { 1 }));
    for e in entries {
        out.extend(le(e.0, iw));
    }
    let (table, data) = offsets(&children, ow);
    out.extend(table);
    out.extend(data);
    out
}

const NULL: &[u8] = &[0x00];
const INT8_ONE: &[u8] = &[0x0C, 0x01];

fn nested_arrays(depth: usize) -> Vec<u8> {
    let mut v = NULL.to_vec();
    for _ in 0..depth {
        v = arr(&[v]);
    }
    v
}

fn assert_invalid(bytes: &[u8]) {
    match JsonbRef::validate(bytes) {
        Err(JsonbError::InvalidEncoding { .. }) => {}
        other => panic!("expected InvalidEncoding for {bytes:02x?}, got {other:?}"),
    }
}

// ---- envelope, ceilings, tunables ------------------------------------------

#[test]
fn jsonb_hand_builders_agree_with_the_encoder() {
    let ours = cell(
        &["a", "b"],
        &obj(&[(0, INT8_ONE.to_vec()), (1, NULL.to_vec())]),
    );
    let mut b = JsonbBuilder::new(limits());
    b.begin_object().unwrap();
    b.key("b").unwrap();
    b.null().unwrap();
    b.key("a").unwrap();
    b.number(Number::from_i64(1)).unwrap();
    b.end_object().unwrap();
    assert_eq!(b.finish().unwrap().bytes, ours);
    assert_round_trip(&ours);
}

#[test]
fn jsonb_unknown_envelope_fails_loud() {
    for byte in [0xF2u8, 0x01, 0x00, 0xF0, 0xFF] {
        let mut c = cell(&[], NULL);
        c[0] = byte;
        assert_eq!(
            JsonbRef::validate(&c).unwrap_err(),
            JsonbError::UnknownEnvelope { byte }
        );
    }
    assert!(matches!(
        JsonbRef::validate(&[]),
        Err(JsonbError::InvalidEncoding { .. })
    ));
}

#[test]
fn jsonb_validate_ignores_tunable_limits() {
    // `validate` takes no `Limits`; a depth-900 cell stays readable although the
    // tunable depth could be lowered to 100 (D14b, FM-17).
    let c = cell(&[], &nested_arrays(900));
    let r = JsonbRef::validate(&c).expect("depth 900 is under the hard ceiling");
    assert_eq!(r.root().kind().unwrap(), ValueKind::Array);
}

#[test]
fn jsonb_validate_depth_1000_on_a_256k_stack() {
    let ok = cell(&[], &nested_arrays(1000));
    let too_deep = cell(&[], &nested_arrays(1001));
    let handle = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || {
            let r = JsonbRef::validate(&ok).expect("depth 1000 validates");
            let rebuilt = rebuild(&r).expect("iterative walk of depth 1000");
            assert_eq!(rebuilt, ok);
            assert!(matches!(
                JsonbRef::validate(&too_deep),
                Err(JsonbError::DepthExceeded { max: 1000, .. })
            ));
        })
        .unwrap();
    handle.join().expect("no stack overflow, no panic");
}

#[test]
fn jsonb_validate_refuses_above_the_size_ceiling() {
    let huge = vec![0u8; HARD_MAX_ENCODED_BYTES + 1];
    assert!(matches!(
        JsonbRef::validate(&huge),
        Err(JsonbError::EncodedTooLarge { .. })
    ));
}

// ---- golden corpus ----------------------------------------------------------

#[test]
fn jsonb_read_accepts_all_prior_golden_cells() {
    let cases = corpus();
    assert!(cases.len() >= 200);
    for (name, ops) in &cases {
        let bytes = encode(ops);
        let r = JsonbRef::validate(&bytes)
            .unwrap_or_else(|e| panic!("golden case {name} rejected: {e}"));
        let again = rebuild(&r).unwrap_or_else(|e| panic!("golden case {name} rebuild: {e}"));
        assert_eq!(again, bytes, "golden case {name} did not round-trip");
    }
}

fn mutate_and_check(name: &str, bytes: &[u8], stats: &mut (usize, usize)) {
    let check = |m: &[u8], stats: &mut (usize, usize)| match JsonbRef::validate(m) {
        Err(_) => stats.0 += 1,
        Ok(r) => {
            stats.1 += 1;
            assert_eq!(
                rebuild(&r).expect("rebuild of an accepted mutation"),
                m,
                "case {name}: accepted a non-canonical mutation"
            );
        }
    };
    let span = |n: usize| (0..n.min(300)).chain(n.saturating_sub(300)..n);
    for i in span(bytes.len()) {
        for mask in [0x01u8, 0x02, 0x20, 0x80, 0xFF] {
            let mut m = bytes.to_vec();
            m[i] ^= mask;
            check(&m, stats);
        }
    }
    for cut in span(bytes.len()) {
        check(&bytes[..cut], stats);
    }
    for extra in [0x00u8, 0xFF] {
        let mut m = bytes.to_vec();
        m.push(extra);
        check(&m, stats);
    }
}

#[test]
fn fuzz_jsonb_validate_rejects_noncanonical() {
    let mut stats = (0usize, 0usize);
    for (name, ops) in &corpus() {
        mutate_and_check(name, &encode(ops), &mut stats);
    }
    let (rejected, accepted) = stats;
    eprintln!("mutation corpus: {rejected} rejected, {accepted} accepted (all canonical)");
    assert!(rejected + accepted > 100_000, "corpus too small");
    assert!(rejected > accepted, "mutations should mostly be rejected");
}

// ---- hostile hand-built cells ----------------------------------------------

#[test]
fn jsonb_hostile_offsets_are_rejected() {
    let base = |off: &[u8]| {
        let mut v = vec![0x03, 0x03];
        v.extend(off);
        v.extend([0, 0, 0]);
        cell(&[], &v)
    };
    assert_round_trip(&base(&[0, 1, 2, 3]));
    assert_invalid(&base(&[0, 1, 2, 4])); // offset past the end
    assert_invalid(&base(&[0, 1, 2, 255])); // wild offset
    assert_invalid(&base(&[0, 2, 1, 3])); // non-monotone, circular-looking
    assert_invalid(&base(&[1, 1, 2, 3])); // first offset not 0
    assert_invalid(&base(&[0, 0, 3, 3])); // zero-length child
    assert_invalid(&base(&[0, 1, 2, 2])); // trailing byte left over
}

#[test]
fn jsonb_huge_claimed_counts_with_tiny_bodies_are_rejected() {
    let ff = [0xFFu8; 4];
    // large array, count 2^32 - 1, no table behind it
    let mut a = vec![0x13 | (3 << 2)];
    a.extend(ff);
    assert_invalid(&cell(&[], &a));
    // large object
    let mut o = vec![0x02 | (3 << 2) | (3 << 4) | (1 << 6)];
    o.extend(ff);
    o.extend([0; 8]);
    assert_invalid(&cell(&[], &o));
    // metadata claiming 2^32 - 1 keys with 4-byte offsets
    let mut m = vec![0xF1, 0x11 | (3 << 6)];
    m.extend(ff);
    m.extend([0; 16]);
    m.extend(NULL);
    assert_invalid(&m);
    // one-byte count 255 with a 5-byte body
    assert_invalid(&cell(&[], &[0x03, 0xFF, 0, 1, 0]));
}

#[test]
fn jsonb_dictionary_rules_are_enforced() {
    let one = obj(&[(0, NULL.to_vec())]);
    assert_round_trip(&cell(&["a"], &one));
    assert_invalid(&cell(&["a", "b"], &one)); // unused key (C3)
    assert_invalid(&cell(&["a"], &obj(&[(1, NULL.to_vec())]))); // id out of range
    let two = obj(&[(0, NULL.to_vec()), (1, NULL.to_vec())]);
    assert_round_trip(&cell(&["a", "b"], &two));
    assert_invalid(&cell(&["b", "a"], &two)); // dictionary not sorted
    assert_invalid(&cell(&["a", "a"], &two)); // duplicate key
    let swapped = obj(&[(1, NULL.to_vec()), (0, NULL.to_vec())]);
    assert_invalid(&cell(&["a", "b"], &swapped)); // ids not ascending
    let dup = obj(&[(0, NULL.to_vec()), (0, NULL.to_vec())]);
    assert_invalid(&cell(&["a", "b"], &dup)); // repeated id
    assert_invalid(&cell(&["\u{e9}", "a"], &two)); // bytewise order, not sorted
    let mut bad_key = cell(&["a"], &one);
    bad_key[5] = 0xFF; // key byte becomes invalid UTF-8
    assert_invalid(&bad_key);
}

#[test]
fn jsonb_non_canonical_shapes_are_rejected() {
    // metadata: unsorted flag clear, wrong version, reserved bit set, wide width
    for header in [0x01u8, 0x12, 0x31, 0x51] {
        let mut c = cell(&[], NULL);
        c[1] = header;
        assert_invalid(&c);
    }
    let mut wide = vec![0xF1, 0x11 | (1 << 6), 0, 0, 0, 0];
    wide.extend(NULL);
    assert_invalid(&wide); // 2-byte offsets where 1 byte suffices
                           // array with 2-byte offsets where 1 suffices
    assert_invalid(&cell(&[], &[0x07, 0x01, 0, 0, 1, 0, 0]));
    // array is_large set with count 1
    assert_invalid(&cell(&[], &[0x13, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0]));
    // reserved bits in an array header
    assert_invalid(&cell(&[], &[0xE3, 1, 0, 1, 0]));
    // trailing byte after the value
    assert_invalid(&cell(&[], &[0x00, 0x00]));
    // scalar wrong length: int8 with no payload, int16 with one byte
    assert_invalid(&cell(&[], &[0x0C]));
    assert_invalid(&cell(&[], &[0x10, 0x01]));
}

#[test]
fn jsonb_numbers_must_use_the_smallest_kind() {
    assert_round_trip(&cell(&[], &[0x0C, 0x7F]));
    assert_invalid(&cell(&[], &[0x10, 0x01, 0x00])); // 1 as int16
    assert_invalid(&cell(&[], &[0x14, 1, 0, 0, 0])); // 1 as int32
    assert_invalid(&cell(&[], &[0x18, 1, 0, 0, 0, 0, 0, 0, 0])); // 1 as int64
    assert_invalid(&cell(&[], &[0x20, 0x00, 1, 0, 0, 0])); // decimal4 scale 0
    assert_round_trip(&cell(&[], &[0x20, 0x01, 10, 0, 0, 0])); // 1.0
    assert_invalid(&cell(&[], &[0x24, 0x01, 10, 0, 0, 0, 0, 0, 0, 0])); // 1.0 as decimal8
    assert_invalid(&cell(&[], &[0x20, 0x0A, 1, 0, 0, 0])); // decimal4 scale 10
}

#[test]
fn jsonb_bigdecimal_layout_is_checked() {
    let big = |scale_zz: &[u8], payload: &[u8]| {
        let mut v = vec![63 << 2];
        v.extend(scale_zz);
        v.push(payload.len() as u8);
        v.extend(payload);
        cell(&[], &v)
    };
    // 2^127 does not fit i128: canonical bigdecimal at scale 0
    let mut p = vec![0x00, 0x80];
    p.extend([0u8; 15]);
    assert_round_trip(&big(&[0], &p));
    assert_invalid(&big(&[0], &[0x01])); // fits int8
    assert_invalid(&big(&[1], &p)); // negative scale
    assert_invalid(&big(&[0], &[])); // no payload
    let mut padded = vec![0x00];
    padded.extend(&p); // redundant leading 0x00
    assert_invalid(&big(&[0], &padded));
    assert_invalid(&big(&[0x80, 0x00], &p)); // non-minimal varint
    let mut lying = big(&[0], &p);
    lying[5] = 200; // claimed payload longer than the cell
    assert_invalid(&lying);
    // scale 16384 (zigzag 32768) is over the hard ceiling
    let mut over = vec![63 << 2, 0x80, 0x80, 0x02, p.len() as u8];
    over.extend(&p);
    assert!(JsonbRef::validate(&cell(&[], &over)).is_err());
    // about 147 600 digits: over the D14a caps
    let mut huge = vec![63 << 2, 0, 0xB4, 0xEF, 0x03, 0x01];
    huge.extend(vec![0u8; 61_300]);
    huge.truncate(6 + 61_300);
    assert!(matches!(
        JsonbRef::validate(&cell(&[], &huge)),
        Err(JsonbError::DigitsBeforePointExceeded { .. } | JsonbError::InvalidEncoding { .. })
    ));
}

#[test]
fn jsonb_strings_are_checked() {
    let short = |s: &[u8]| {
        let mut v = vec![((s.len() as u8) << 2) | 1];
        v.extend(s);
        cell(&[], &v)
    };
    assert_round_trip(&short(b"hi"));
    assert_invalid(&short(&[0xC3])); // truncated UTF-8
    assert_invalid(&short(&[0xFF, 0xFE])); // invalid UTF-8
    let long = |len_claim: u32, body: usize| {
        let mut v = vec![16 << 2];
        v.extend(len_claim.to_le_bytes());
        v.extend(vec![b'x'; body]);
        cell(&[], &v)
    };
    assert_round_trip(&long(64, 64));
    assert_invalid(&long(10, 10)); // short enough for a short string
    assert_invalid(&long(65, 64)); // claim longer than the body
    assert_invalid(&long(u32::MAX, 64)); // claim far longer than the body
    assert_invalid(&long(63, 64)); // trailing byte
}

#[test]
fn jsonb_unknown_and_excluded_primitives_are_rejected() {
    for id in [7u8, 11, 12, 13, 14, 15, 17, 18, 19, 20, 21, 30, 62] {
        let mut v = vec![id << 2];
        v.extend([0u8; 16]);
        assert_invalid(&cell(&[], &v));
    }
}

// ---- reader -----------------------------------------------------------------

fn doc() -> Vec<u8> {
    let ops = [
        Op::ObjBegin,
        Op::Key("name".into()),
        Op::Str("Ada".into()),
        Op::Key("age".into()),
        Op::Num("36".into()),
        Op::Key("tags".into()),
        Op::ArrBegin,
        Op::True,
        Op::Null,
        Op::Str("x".into()),
        Op::ArrEnd,
        Op::Key("price".into()),
        Op::Num("1.50".into()),
        Op::ObjEnd,
    ];
    encode(&ops)
}

#[test]
fn jsonb_reader_looks_up_fields_and_iterates_in_key_order() {
    let bytes = doc();
    let r = JsonbRef::validate(&bytes).unwrap();
    let o = r.root().as_object().unwrap();
    assert_eq!(o.len(), 4);
    let keys: Vec<&str> = o.iter().map(|i| i.unwrap().0).collect();
    assert_eq!(keys, ["age", "name", "price", "tags"]);
    let name = o.get("name").unwrap().unwrap();
    assert_eq!(name.as_str().unwrap(), "Ada");
    assert_eq!(
        o.get("age").unwrap().unwrap().as_number().unwrap(),
        Number::from_i64(36)
    );
    let price = o.get("price").unwrap().unwrap().as_number().unwrap();
    assert_eq!(price.scale(), 2);
    assert!(o.get("missing").unwrap().is_none());
    assert!(o.get("").unwrap().is_none());
    let tags = o.get("tags").unwrap().unwrap().as_array().unwrap();
    assert_eq!(tags.len(), 3);
    assert!(tags.get(0).unwrap().unwrap().as_bool().unwrap());
    assert_eq!(
        tags.get(1).unwrap().unwrap().kind().unwrap(),
        ValueKind::Null
    );
    assert!(tags.get(3).unwrap().is_none());
}

#[test]
fn jsonb_reader_reports_a_wrong_kind_instead_of_guessing() {
    let bytes = doc();
    let r = JsonbRef::validate(&bytes).unwrap();
    let root = r.root();
    assert!(matches!(root.as_str(), Err(JsonbError::WrongKind { .. })));
    assert!(matches!(root.as_array(), Err(JsonbError::WrongKind { .. })));
    assert!(matches!(root.as_bool(), Err(JsonbError::WrongKind { .. })));
    assert!(matches!(
        root.as_number(),
        Err(JsonbError::WrongKind { .. })
    ));
}

#[test]
fn jsonb_reader_finds_every_key_of_a_wide_object() {
    let mut ops = vec![Op::ObjBegin];
    for i in (0..1000).rev() {
        ops.push(Op::Key(format!("key{i:04}")));
        ops.push(Op::Num(i.to_string()));
    }
    ops.push(Op::ObjEnd);
    let bytes = encode(&ops);
    let r = JsonbRef::validate(&bytes).unwrap();
    let o = r.root().as_object().unwrap();
    for i in 0..1000i64 {
        let v = o.get(&format!("key{i:04}")).unwrap().unwrap();
        assert_eq!(v.as_number().unwrap(), Number::from_i64(i));
    }
    assert!(o.get("key1000").unwrap().is_none());
}

// ---- property ---------------------------------------------------------------

#[derive(Debug, Clone)]
enum Model {
    Null,
    Bool(bool),
    Str(String),
    Num(String),
    Arr(Vec<Model>),
    Obj(Vec<(String, Model)>),
}

fn model() -> impl Strategy<Value = Model> {
    let leaf = prop_oneof![
        Just(Model::Null),
        any::<bool>().prop_map(Model::Bool),
        "\\PC{0,80}".prop_map(Model::Str),
        "-?(0|[1-9][0-9]{0,40})(\\.[0-9]{1,45})?([eE][+-]?[0-9]{1,2})?".prop_map(Model::Num),
    ];
    leaf.prop_recursive(5, 96, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Model::Arr),
            prop::collection::btree_map("\\PC{0,12}", inner, 0..6)
                .prop_map(|m| Model::Obj(m.into_iter().collect())),
        ]
    })
}

fn ops_of(m: &Model, out: &mut Vec<Op>) {
    match m {
        Model::Null => out.push(Op::Null),
        Model::Bool(true) => out.push(Op::True),
        Model::Bool(false) => out.push(Op::False),
        Model::Str(s) => out.push(Op::Str(s.clone())),
        Model::Num(n) => out.push(Op::Num(n.clone())),
        Model::Arr(items) => {
            out.push(Op::ArrBegin);
            items.iter().for_each(|i| ops_of(i, out));
            out.push(Op::ArrEnd);
        }
        Model::Obj(entries) => {
            out.push(Op::ObjBegin);
            for (k, v) in entries {
                out.push(Op::Key(k.clone()));
                ops_of(v, out);
            }
            out.push(Op::ObjEnd);
        }
    }
}

fn same(m: &Model, v: ValueRef<'_>) -> bool {
    match m {
        Model::Null => v.kind() == Ok(ValueKind::Null),
        Model::Bool(b) => v.as_bool() == Ok(*b),
        Model::Str(s) => v.as_str() == Ok(s.as_str()),
        Model::Num(n) => {
            let want = Number::parse_lexeme(n).unwrap();
            v.as_number()
                .is_ok_and(|got| got == want && got.scale() == want.scale())
        }
        Model::Arr(items) => v.as_array().is_ok_and(|a| {
            a.len() == items.len()
                && items
                    .iter()
                    .enumerate()
                    .all(|(i, m)| matches!(a.get(i), Ok(Some(c)) if same(m, c)))
        }),
        Model::Obj(entries) => v.as_object().is_ok_and(|o| {
            o.len() == entries.len()
                && entries
                    .iter()
                    .all(|(k, m)| matches!(o.get(k), Ok(Some(c)) if same(m, c)))
        }),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(400))]

    #[test]
    fn jsonb_encode_validate_read_round_trips(m in model()) {
        let mut ops = Vec::new();
        ops_of(&m, &mut ops);
        let bytes = encode(&ops);
        let r = JsonbRef::validate(&bytes).expect("encoder output validates");
        prop_assert!(same(&m, r.root()));
        prop_assert_eq!(rebuild(&r).expect("rebuild"), bytes);
    }

    #[test]
    fn jsonb_random_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let mut c = vec![0xF1];
        c.extend(&bytes);
        if let Ok(r) = JsonbRef::validate(&c) {
            prop_assert_eq!(rebuild(&r).expect("rebuild"), c);
        }
    }
}
