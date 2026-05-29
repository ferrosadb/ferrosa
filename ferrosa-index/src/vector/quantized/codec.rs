//! Low-bit quantized vector codecs.
//!
//! Q2 and Q1 are coarse routing representations: they are useful for measuring
//! candidate pruning cost, not for final nearest-neighbor proof without rerank.

use std::collections::HashSet;
use std::fmt;

use crate::vector::l2_distance;
use crate::IndexError;

/// Supported scalar quantization widths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuantizationBits {
    /// Two-bit scalar quantization, four codes per byte.
    Q2,
    /// One-bit scalar quantization, eight codes per byte. Experimental until
    /// recall gates show acceptable quality for a target corpus.
    Q1,
}

impl QuantizationBits {
    /// Number of bits used by each packed code.
    pub const fn width(self) -> u8 {
        match self {
            QuantizationBits::Q2 => 2,
            QuantizationBits::Q1 => 1,
        }
    }

    /// Number of distinct representable scalar codes.
    pub const fn levels(self) -> u8 {
        1 << self.width()
    }

    /// Human-readable gate label used in benchmark output.
    pub const fn gate_label(self) -> &'static str {
        match self {
            QuantizationBits::Q2 => "Q2 gated",
            QuantizationBits::Q1 => "Q1 experimental",
        }
    }

    /// Whether this tier is explicitly experimental.
    pub const fn is_experimental(self) -> bool {
        matches!(self, QuantizationBits::Q1)
    }
}

impl fmt::Display for QuantizationBits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QuantizationBits::Q2 => f.write_str("Q2"),
            QuantizationBits::Q1 => f.write_str("Q1"),
        }
    }
}

/// Packed scalar-quantized vector with per-vector affine metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantizedVector {
    bits: QuantizationBits,
    dimension: usize,
    min: f32,
    scale: f32,
    codes: Vec<u8>,
}

impl QuantizedVector {
    /// Encode `vector` with min/max affine quantization and bit-packed codes.
    pub fn encode(vector: &[f32], bits: QuantizationBits) -> Result<Self, IndexError> {
        if vector.is_empty() {
            return Err(IndexError::Format("cannot quantize an empty vector".into()));
        }
        if let Some(value) = vector.iter().find(|value| !value.is_finite()) {
            return Err(IndexError::Format(format!(
                "cannot quantize non-finite vector value {value}"
            )));
        }

        let (min, max) = vector
            .iter()
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(min, max), value| {
                (min.min(*value), max.max(*value))
            });
        let span = max - min;
        let scale = if span == 0.0 {
            0.0
        } else {
            span / f32::from(bits.levels() - 1)
        };
        let max_code = bits.levels() - 1;
        let raw_codes = vector.iter().map(|value| {
            if scale == 0.0 {
                0
            } else {
                ((*value - min) / scale)
                    .round()
                    .clamp(0.0, f32::from(max_code)) as u8
            }
        });
        let codes = pack_codes(bits, raw_codes);

        Ok(Self {
            bits,
            dimension: vector.len(),
            min,
            scale,
            codes,
        })
    }

    /// Decode packed codes back to approximate `f32` values.
    pub fn decode(&self) -> Vec<f32> {
        unpack_codes(self.bits, &self.codes, self.dimension)
            .into_iter()
            .map(|code| self.min + f32::from(code) * self.scale)
            .collect()
    }

    /// Worst-case per-component absolute error for this affine codebook.
    pub fn max_abs_error_bound(&self) -> f32 {
        self.scale / 2.0
    }

    /// Number of packed bytes used by the quantized payload.
    pub fn packed_len(&self) -> usize {
        self.codes.len()
    }

    /// Quantization tier for this vector.
    pub fn bits(&self) -> QuantizationBits {
        self.bits
    }
}

/// Recall characterization for benchmark/reporting gates.
#[derive(Debug, Clone, PartialEq)]
pub struct RecallCharacterization {
    pub bits: QuantizationBits,
    pub experimental: bool,
    pub recall_at_k: f32,
    pub recall_impact: f32,
    pub exact_top_k: Vec<usize>,
    pub quantized_top_k: Vec<usize>,
}

impl RecallCharacterization {
    /// Stable single-line benchmark output for `--nocapture` characterization.
    pub fn to_benchmark_line(&self) -> String {
        format!(
            "quantized_codec_low_bits {} recall@{}={:.3} recall_impact={:.3}",
            self.bits.gate_label(),
            self.exact_top_k.len(),
            self.recall_at_k,
            self.recall_impact
        )
    }
}

/// Compare exact top-k with quantized/dequantized top-k for a small corpus.
pub fn characterize_recall_impact(
    bits: QuantizationBits,
    corpus: &[Vec<f32>],
    query: &[f32],
    k: usize,
) -> Result<RecallCharacterization, IndexError> {
    if corpus.is_empty() {
        return Err(IndexError::Format(
            "cannot characterize recall for an empty corpus".into(),
        ));
    }
    if k == 0 {
        return Err(IndexError::Format(
            "recall characterization requires k > 0".into(),
        ));
    }
    if let Some(vector) = corpus.iter().find(|vector| vector.len() != query.len()) {
        return Err(IndexError::DimensionMismatch {
            expected: query.len(),
            got: vector.len(),
        });
    }

    let exact_top_k = top_k_by_distance(corpus.iter().map(|vector| l2_distance(vector, query)), k);
    let quantized_distances = corpus
        .iter()
        .map(|vector| {
            QuantizedVector::encode(vector, bits)
                .map(|encoded| l2_distance(&encoded.decode(), query))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let quantized_top_k = top_k_by_distance(quantized_distances, k);
    let exact_set = exact_top_k.iter().copied().collect::<HashSet<_>>();
    let hits = quantized_top_k
        .iter()
        .filter(|index| exact_set.contains(index))
        .count();
    let recall_at_k = hits as f32 / exact_top_k.len() as f32;

    Ok(RecallCharacterization {
        bits,
        experimental: bits.is_experimental(),
        recall_at_k,
        recall_impact: 1.0 - recall_at_k,
        exact_top_k,
        quantized_top_k,
    })
}

fn pack_codes(bits: QuantizationBits, codes: impl IntoIterator<Item = u8>) -> Vec<u8> {
    let width = bits.width();
    let mask = bits.levels() - 1;
    let mut packed = Vec::new();
    let mut byte = 0_u8;
    let mut used_bits = 0_u8;

    for code in codes {
        byte |= (code & mask) << used_bits;
        used_bits += width;
        if used_bits == 8 {
            packed.push(byte);
            byte = 0;
            used_bits = 0;
        }
    }
    if used_bits > 0 {
        packed.push(byte);
    }
    packed
}

fn unpack_codes(bits: QuantizationBits, packed: &[u8], dimension: usize) -> Vec<u8> {
    let width = bits.width();
    let mask = bits.levels() - 1;
    let mut codes = Vec::with_capacity(dimension);

    for byte in packed {
        let mut consumed = 0_u8;
        while consumed < 8 && codes.len() < dimension {
            codes.push((byte >> consumed) & mask);
            consumed += width;
        }
    }
    codes
}

fn top_k_by_distance(distances: impl IntoIterator<Item = f32>, k: usize) -> Vec<usize> {
    let mut scored = distances.into_iter().enumerate().collect::<Vec<_>>();
    scored.sort_by(
        |(left_index, left_distance), (right_index, right_distance)| {
            left_distance
                .total_cmp(right_distance)
                .then_with(|| left_index.cmp(right_index))
        },
    );
    scored.into_iter().take(k).map(|(index, _)| index).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip_bound(bits: QuantizationBits, vector: &[f32]) {
        let encoded = QuantizedVector::encode(vector, bits).expect("encode low-bit vector");
        let decoded = encoded.decode();
        assert_eq!(decoded.len(), vector.len());
        let max_error = vector
            .iter()
            .zip(decoded.iter())
            .map(|(expected, actual)| (expected - actual).abs())
            .fold(0.0_f32, f32::max);
        assert!(
            max_error <= encoded.max_abs_error_bound() + 1.0e-6,
            "{bits:?} max_error={max_error} bound={}",
            encoded.max_abs_error_bound()
        );
    }

    #[test]
    fn quantized_codec_low_bits_q2_round_trips_with_declared_error_bound() {
        assert_round_trip_bound(
            QuantizationBits::Q2,
            &[-1.0, -0.75, -0.20, 0.0, 0.25, 0.5, 0.9, 1.0, 1.4],
        );
    }

    #[test]
    fn quantized_codec_low_bits_q1_round_trips_with_declared_error_bound() {
        assert_round_trip_bound(
            QuantizationBits::Q1,
            &[-1.0, -0.25, 0.0, 0.25, 0.75, 1.0, 1.25, 2.0, 2.5],
        );
    }

    #[test]
    fn quantized_codec_low_bits_pack_density_matches_bit_width() {
        let q2 = QuantizedVector::encode(&[0.0; 9], QuantizationBits::Q2).unwrap();
        let q1 = QuantizedVector::encode(&[0.0; 9], QuantizationBits::Q1).unwrap();
        assert_eq!(q2.packed_len(), 3, "Q2 packs four dimensions per byte");
        assert_eq!(q1.packed_len(), 2, "Q1 packs eight dimensions per byte");
    }

    #[test]
    fn quantized_codec_low_bits_characterization_labels_q1_experimental_and_reports_recall_impact()
    {
        let corpus = [
            vec![1.0, 0.0, 0.0],
            vec![0.9, 0.1, 0.0],
            vec![0.0, 1.0, 0.0],
            vec![0.0, 0.9, 0.1],
            vec![0.0, 0.0, 1.0],
        ];
        let report =
            characterize_recall_impact(QuantizationBits::Q1, &corpus, &[1.0, 0.05, 0.0], 2)
                .expect("recall characterization");
        println!("{}", report.to_benchmark_line());
        assert!(report.experimental, "Q1 must stay explicitly experimental");
        assert!(report.to_benchmark_line().contains("Q1 experimental"));
        assert!(report.to_benchmark_line().contains("recall@2="));
        assert!(report.to_benchmark_line().contains("recall_impact="));
    }
}
