//! Module: direction A of the jsonb conformance oracle (ours -> theirs).
//! Correctness: correct when every extension-free ferrosa-jsonb cell, with its
//! envelope stripped and its metadata split off by the Variant spec's own layout,
//! decodes under arrow-rs `parquet-variant` (full validation) to exactly the value
//! that was encoded: objects, arrays, strings, integers, decimals with scale,
//! booleans and null. Nothing here calls ferrosa's decoder to make an expectation.
//! Last revised: 2026-09-28
//! Last changed: t_0b127826 initial differential test.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use std::collections::{BTreeMap, BTreeSet};

use ferrosa_jsonb::JsonbRef;
use parquet_variant::{Variant, VariantMetadata};
use proptest::prelude::*;
use support::{
    collect_keys, contains_bigdecimal, ferrosa_encode, ferrosa_to_model, golden, model_from_ops,
    model_strategy, nested, split_cell, variant_to_model, Model,
};

/// Decode `cell` with parquet-variant and require it to mean `want`.
fn assert_upstream_reads(name: &str, cell: &[u8], want: &Model) {
    let (meta, value) = split_cell(cell);
    let md = VariantMetadata::try_new(meta)
        .unwrap_or_else(|e| panic!("{name}: upstream rejects our metadata: {e}"));
    assert!(md.is_sorted(), "{name}: sorted_strings bit must be set");
    let mut keys = BTreeSet::new();
    collect_keys(want, &mut keys);
    let dictionary: Vec<&str> = md.iter().collect();
    let expected: Vec<&str> = keys.iter().map(String::as_str).collect();
    assert_eq!(
        dictionary, expected,
        "{name}: dictionary is the sorted used keys"
    );
    let variant = Variant::try_new(meta, value)
        .unwrap_or_else(|e| panic!("{name}: upstream rejects our value: {e}"));
    let got = variant_to_model(&variant).unwrap_or_else(|e| panic!("{name}: {e}"));
    assert_eq!(&got, want, "{name}: upstream decodes a different value");
}

#[test]
fn golden_corpus_decodes_identically_in_upstream() {
    let (mut checked, mut skipped) = (0usize, Vec::new());
    for (name, ops) in golden::corpus() {
        let (model, qualifies) = model_from_ops(&ops);
        let cell = golden::encode(&ops);
        if !qualifies {
            // The only reason to skip is a bigdecimal (primitive 63): prove it.
            let checked_cell = JsonbRef::validate(&cell).expect("golden cell validates");
            assert!(
                contains_bigdecimal(checked_cell.root()),
                "{name}: skipped but holds no bigdecimal"
            );
            skipped.push(name);
            continue;
        }
        assert_upstream_reads(&name, &cell, &model);
        checked += 1;
    }
    eprintln!(
        "direction A golden: {checked} checked, {} skipped (bigdecimal)",
        skipped.len()
    );
    assert!(
        checked >= 200,
        "corpus shrank: only {checked} cases checked"
    );
}

#[test]
fn ferrosa_reader_and_upstream_agree_on_golden_values() {
    for (name, ops) in golden::corpus() {
        let (model, qualifies) = model_from_ops(&ops);
        if !qualifies {
            continue;
        }
        let cell = golden::encode(&ops);
        let reader = JsonbRef::validate(&cell).expect("validates");
        assert_eq!(
            ferrosa_to_model(reader.root()).as_ref(),
            Ok(&model),
            "{name}"
        );
    }
}

#[test]
fn wide_dictionaries_and_containers_decode_in_upstream() {
    let big_object: BTreeMap<String, Model> = (0..70_000)
        .map(|i| (format!("k{i:05}"), Model::Bool(i % 2 == 0)))
        .collect();
    let mut cases = vec![
        (
            "70000 keys (3-byte field ids, large object)".to_string(),
            Model::Obj(big_object),
        ),
        (
            "70000 nulls (large array, 3-byte offsets)".to_string(),
            Model::Arr(vec![Model::Null; 70_000]),
        ),
    ];
    // Single-key dictionaries whose byte length straddles each offset width.
    for len in [254usize, 255, 256, 257, 65_534, 65_535, 65_536, 65_537] {
        cases.push((
            format!("one key of {len} bytes"),
            Model::Obj(BTreeMap::from([("q".repeat(len), Model::Null)])),
        ));
    }
    for (name, model) in cases {
        for reverse in [false, true] {
            assert_upstream_reads(&name, &ferrosa_encode(&model, reverse), &model);
        }
    }
}

#[test]
fn nesting_to_upstream_limit_decodes() {
    // parquet-variant's MAX_NESTING_DEPTH is 128; the corpus stops there (D5a).
    for depth in [1usize, 2, 64, 127, 128] {
        let model = nested(depth);
        assert_upstream_reads(
            &format!("depth {depth}"),
            &ferrosa_encode(&model, false),
            &model,
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(768))]

    #[test]
    fn generated_values_decode_identically_in_upstream(model in model_strategy(), reverse in any::<bool>()) {
        let cell = ferrosa_encode(&model, reverse);
        let (meta, value) = split_cell(&cell);
        let md = VariantMetadata::try_new(meta).map_err(|e| TestCaseError::fail(format!("metadata: {e}")))?;
        prop_assert!(md.is_sorted());
        let variant = Variant::try_new(meta, value).map_err(|e| TestCaseError::fail(format!("value: {e}")))?;
        let got = variant_to_model(&variant).map_err(TestCaseError::fail)?;
        prop_assert_eq!(got, model);
    }
}
