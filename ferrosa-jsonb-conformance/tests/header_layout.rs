//! Module: independent pin of the Variant metadata header bit layout.
//! Correctness: correct when ferrosa-jsonb writes and reads `offset_size_minus_one`
//! at bits 7-6 of the metadata header (bit 5 reserved, bit 4 `sorted_strings`, bits
//! 3-0 version), judged by parquet-variant's behaviour and by the header byte its
//! own builder writes, never by ferrosa's own byte expectations. T-104 found T-102
//! writing the field at bit 5 while its hand-written byte tests agreed with the
//! misreading; this file is the regression net for that bug (t_0b127826).
//! Last revised: 2026-09-28
//! Last changed: t_0b127826 initial pin.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod support;

use std::collections::BTreeMap;

use ferrosa_jsonb::{EncodingFault, JsonbError, JsonbRef};
use parquet_variant::{Variant, VariantBuilder, VariantMetadata};
use support::{cell_of, ferrosa_encode, split_cell, Model};

fn fault(reason: EncodingFault) -> Option<JsonbError> {
    Some(JsonbError::InvalidEncoding { reason })
}

fn one_key(len: usize) -> Model {
    Model::Obj(BTreeMap::from([("q".repeat(len), Model::Null)]))
}

/// The metadata upstream's own builder writes for a dictionary of one key.
fn upstream_metadata(key: &str) -> Vec<u8> {
    let mut b = VariantBuilder::new().with_field_names([key]);
    b.new_object().finish();
    b.finish().0
}

#[test]
fn encoder_header_matches_upstream_builder_at_every_offset_width() {
    // Key bytes 10 / 300 / 70_000 need offset widths 1 / 2 / 3.
    for (len, width) in [(10usize, 1u8), (300, 2), (70_000, 3)] {
        let cell = ferrosa_encode(&one_key(len), false);
        let (meta, value) = split_cell(&cell);
        let header = meta[0];
        assert_eq!(
            header >> 6,
            width - 1,
            "len {len}: offset_size_minus_one lives at bits 7-6"
        );
        assert_eq!(
            header & 0x20,
            0,
            "len {len}: bit 5 is reserved and must be clear"
        );
        assert_eq!(header & 0x10, 0x10, "len {len}: sorted_strings bit");
        assert_eq!(header & 0x0F, 1, "len {len}: version 1");
        let key = "q".repeat(len);
        assert_eq!(
            meta,
            upstream_metadata(&key),
            "len {len}: bytes differ from upstream's builder"
        );
        let variant = Variant::try_new(meta, value).expect("upstream reads the cell");
        assert!(matches!(variant, Variant::Object(_)));
    }
}

/// `[header, count = 0 (w bytes), offset[0] = 0 (w bytes)]`, an empty dictionary.
fn empty_dictionary(width: usize, extra_bits: u8) -> Vec<u8> {
    let header = 0x11 | extra_bits | (u8::try_from(width - 1).expect("width") << 6);
    let mut meta = vec![header];
    meta.extend(std::iter::repeat_n(0u8, 2 * width));
    meta
}

#[test]
fn validator_reads_offset_width_from_bits_7_6() {
    for width in 1..=4usize {
        let meta = empty_dictionary(width, 0);
        // Upstream accepts every width for an empty dictionary.
        let md = VariantMetadata::try_new(&meta).expect("upstream accepts the metadata");
        assert_eq!(md.len(), 0);
        let cell = cell_of(&meta, &[0x00]);
        let verdict = JsonbRef::validate(&cell).err();
        if width == 1 {
            assert_eq!(verdict, None, "canonical 1-byte metadata must validate");
        } else {
            // Only misreading the width can turn this into anything but C4.
            assert_eq!(
                verdict,
                fault(EncodingFault::NonCanonical("C4")),
                "width {width}"
            );
        }
    }
}

#[test]
fn validator_treats_bit_5_as_reserved_not_as_a_width() {
    // Upstream ignores bit 5 (parses width 1); ferrosa refuses it as non-canonical.
    let meta = empty_dictionary(1, 0x20);
    assert!(
        VariantMetadata::try_new(&meta).is_ok(),
        "upstream ignores bit 5"
    );
    let verdict = JsonbRef::validate(&cell_of(&meta, &[0x00])).err();
    assert_eq!(verdict, fault(EncodingFault::NonCanonical("C2")));
}

#[test]
fn value_headers_match_upstream_builder_layout() {
    // Container headers carry widths in bits 3-2 (offsets) and 5-4 (object ids) and
    // is_large in bit 6 (objects) or bit 4 (arrays); compare with upstream's bytes.
    let cases: [(&str, Model); 4] = [
        (
            "wide array",
            Model::Arr(vec![Model::Str("y".repeat(300)); 3]),
        ),
        ("large array", Model::Arr(vec![Model::Null; 300])),
        (
            "object with 2-byte ids and offsets",
            Model::Obj(
                (0..300)
                    .map(|i| (format!("k{i:03}"), Model::Str("z".repeat(3))))
                    .collect(),
            ),
        ),
        (
            "object with 3-byte offsets",
            Model::Obj(BTreeMap::from([(
                "a".to_string(),
                Model::Str("w".repeat(70_000)),
            )])),
        ),
    ];
    for (name, model) in cases {
        let ours = ferrosa_encode(&model, false);
        let (_, our_value) = split_cell(&ours);
        let (_, their_value) = support::upstream_build(&model);
        assert_eq!(
            our_value,
            their_value.as_slice(),
            "{name}: value bytes differ from upstream's builder"
        );
    }
}
