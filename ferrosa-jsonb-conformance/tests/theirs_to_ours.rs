//! Module: direction B of the jsonb conformance oracle (theirs -> ours).
//! Correctness: correct when values built by arrow-rs `parquet-variant`'s own
//! builder, wrapped in our one-byte envelope, are accepted by `JsonbRef::validate`
//! and read back as the same value, and are byte-identical to what ferrosa-jsonb
//! writes for that value (both writers are canonical when driven canonically).
//! The single systematic difference is upstream's empty-dictionary header, which
//! clears `sorted_strings`; ferrosa rejects it (documented in the README).
//! Last revised: 2026-09-28
//! Last changed: t_0b127826 initial differential test.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use ferrosa_jsonb::{EncodingFault, JsonbError, JsonbRef};
use parquet_variant::Variant;
use proptest::prelude::*;
use support::{
    cell_of, collect_keys, ferrosa_encode, ferrosa_to_model, golden, model_from_ops,
    model_strategy, nested, upstream_build, variant_to_model, Model,
};

/// Header byte upstream writes for an empty dictionary: version 1, unsorted.
const UPSTREAM_EMPTY_METADATA: [u8; 3] = [0x01, 0x00, 0x00];

/// Check one model end to end; returns true when the model has no object keys.
fn check(name: &str, model: &Model) -> bool {
    let (metadata, value) = upstream_build(model);
    // Upstream must agree with the model it was built from.
    let variant = Variant::try_new(&metadata, &value)
        .unwrap_or_else(|e| panic!("{name}: upstream rejects its own output: {e}"));
    assert_eq!(
        variant_to_model(&variant).as_ref(),
        Ok(model),
        "{name}: upstream round trip"
    );

    let mut keys = std::collections::BTreeSet::new();
    collect_keys(model, &mut keys);
    let keyless = keys.is_empty();
    let mut meta = metadata.clone();
    if keyless {
        assert_eq!(
            meta, UPSTREAM_EMPTY_METADATA,
            "{name}: upstream empty dictionary"
        );
        let err = JsonbRef::validate(&cell_of(&meta, &value)).err();
        assert_eq!(
            err,
            Some(JsonbError::InvalidEncoding {
                reason: EncodingFault::BadMetadataHeader
            }),
            "{name}: unsorted-flag empty dictionary must be refused with the typed fault"
        );
        // The sorted bit is the only difference; setting it must make it valid.
        meta[0] |= 0x10;
    }
    let cell = cell_of(&meta, &value);
    let reader = JsonbRef::validate(&cell)
        .unwrap_or_else(|e| panic!("{name}: ferrosa refuses an upstream-built cell: {e}"));
    assert_eq!(
        ferrosa_to_model(reader.root()).as_ref(),
        Ok(model),
        "{name}: ferrosa reads a different value"
    );
    assert_eq!(
        cell,
        ferrosa_encode(model, false),
        "{name}: not byte-identical to ferrosa's encoding"
    );
    keyless
}

#[test]
fn upstream_built_golden_corpus_validates_and_reads_in_ferrosa() {
    let (mut checked, mut keyless) = (0usize, 0usize);
    for (name, ops) in golden::corpus() {
        let (model, qualifies) = model_from_ops(&ops);
        if !qualifies {
            continue;
        }
        keyless += usize::from(check(&name, &model));
        checked += 1;
    }
    eprintln!("direction B golden: {checked} checked, {keyless} keyless (sorted bit patched)");
    assert!(
        checked >= 200,
        "corpus shrank: only {checked} cases checked"
    );
}

#[test]
fn upstream_built_nesting_to_128_validates_and_reads_in_ferrosa() {
    for depth in [1usize, 2, 64, 127, 128] {
        check(&format!("depth {depth}"), &nested(depth));
    }
}

#[test]
fn upstream_rejects_depth_129_where_ferrosa_accepts_it() {
    // Divergence, not a defect: parquet-variant caps nesting at 128 (an
    // implementation limit); ferrosa's ceiling is HARD_MAX_DEPTH. The corpus stays
    // within 128, so this only pins that the two limits are different.
    let model = nested(129);
    let cell = ferrosa_encode(&model, false);
    let reader = JsonbRef::validate(&cell).expect("ferrosa accepts depth 129");
    assert_eq!(ferrosa_to_model(reader.root()).as_ref(), Ok(&model));
    let (meta, value) = support::split_cell(&cell);
    assert!(
        Variant::try_new(meta, value).is_err(),
        "upstream accepted depth 129"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(384))]

    #[test]
    fn generated_upstream_values_validate_and_read_in_ferrosa(model in model_strategy()) {
        check("generated", &model);
    }
}
