//! Module: Scalar quantization codecs for quantized vector index tiers.
//! Correctness: Correct when fixed-range Q8/Q4 encodings round-trip within one half-step, distance estimates stay within documented tolerances, and malformed packed bytes fail loudly.
//! Last revised: 2026-05-29
//! Last changed: Implemented Q8/Q4 scalar codec encode/decode and distance estimates.

use crate::{DistanceMetric, IndexError};

/// Scalar quantization precision for a single vector tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarPrecision {
    /// Unsigned 8-bit scalar quantization; one byte per dimension.
    Q8,
    /// Unsigned 4-bit scalar quantization; two dimensions packed per byte.
    Q4,
}

impl ScalarPrecision {
    fn levels(self) -> u8 {
        match self {
            Self::Q8 => u8::MAX,
            Self::Q4 => 0x0F,
        }
    }
}

/// Fixed-range scalar quantizer used by Q8/Q4 vector tiers.
///
/// Values are linearly mapped from `[min, max]` into unsigned integer levels.
/// Decoding maps each level back to `min + level * step`; therefore the maximum
/// absolute reconstruction error for in-range finite values is half a quantized
/// step. Out-of-range finite input is clamped so ingest can tolerate a small
/// calibration drift while keeping deterministic fail-loud behavior for invalid
/// dimensions, ranges, non-finite values, or malformed packed bytes.
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarQuantizer {
    precision: ScalarPrecision,
    min: f32,
    max: f32,
    dimensions: usize,
}

impl ScalarQuantizer {
    /// Create a Q8 scalar codec for `dimensions` values in `[min, max]`.
    pub fn q8(min: f32, max: f32, dimensions: usize) -> Result<Self, IndexError> {
        Self::new(ScalarPrecision::Q8, min, max, dimensions)
    }

    /// Create a Q4 scalar codec for `dimensions` values in `[min, max]`.
    pub fn q4(min: f32, max: f32, dimensions: usize) -> Result<Self, IndexError> {
        Self::new(ScalarPrecision::Q4, min, max, dimensions)
    }

    /// Create a fixed-range scalar codec at the requested precision.
    pub fn new(
        precision: ScalarPrecision,
        min: f32,
        max: f32,
        dimensions: usize,
    ) -> Result<Self, IndexError> {
        if dimensions == 0 {
            return Err(IndexError::Format(
                "scalar quantizer dimensions must be nonzero".to_string(),
            ));
        }
        if !min.is_finite() || !max.is_finite() || min >= max {
            return Err(IndexError::Format(format!(
                "invalid scalar quantizer range: min={min}, max={max}"
            )));
        }
        Ok(Self {
            precision,
            min,
            max,
            dimensions,
        })
    }

    /// Number of vector dimensions expected by this codec.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// Maximum absolute reconstruction error for an in-range scalar.
    pub fn max_abs_error(&self) -> f32 {
        self.step() / 2.0
    }

    /// Encoded payload length in bytes for this codec.
    pub fn encoded_len(&self) -> usize {
        match self.precision {
            ScalarPrecision::Q8 => self.dimensions,
            ScalarPrecision::Q4 => self.dimensions.div_ceil(2),
        }
    }

    /// Encode one vector into this codec's compact byte representation.
    pub fn encode(&self, vector: &[f32]) -> Result<Vec<u8>, IndexError> {
        self.check_vector_dimensions(vector.len())?;
        let mut levels = Vec::with_capacity(vector.len());
        for value in vector {
            if !value.is_finite() {
                return Err(IndexError::Format(
                    "cannot quantize non-finite vector component".to_string(),
                ));
            }
            levels.push(self.quantize_level(*value));
        }

        match self.precision {
            ScalarPrecision::Q8 => Ok(levels),
            ScalarPrecision::Q4 => Ok(pack_q4_levels(&levels)),
        }
    }

    /// Decode one compact byte payload into approximate `f32` values.
    pub fn decode(&self, bytes: &[u8]) -> Result<Vec<f32>, IndexError> {
        self.decode_levels(bytes).map(|levels| {
            levels
                .into_iter()
                .map(|level| self.dequantize_level(level))
                .collect()
        })
    }

    /// Estimate distance between two encoded vectors by decoding the quantized
    /// payloads and applying the same metric implementation used by f32 vectors.
    pub fn distance_estimate(
        &self,
        encoded_a: &[u8],
        encoded_b: &[u8],
        metric: DistanceMetric,
    ) -> Result<f32, IndexError> {
        let decoded_a = self.decode(encoded_a)?;
        let decoded_b = self.decode(encoded_b)?;
        Ok(crate::vector::distance(&metric, &decoded_a, &decoded_b))
    }

    fn check_vector_dimensions(&self, got: usize) -> Result<(), IndexError> {
        if got != self.dimensions {
            return Err(IndexError::DimensionMismatch {
                expected: self.dimensions,
                got,
            });
        }
        Ok(())
    }

    fn step(&self) -> f32 {
        (self.max - self.min) / f32::from(self.precision.levels())
    }

    fn quantize_level(&self, value: f32) -> u8 {
        let clamped = value.clamp(self.min, self.max);
        let scaled = ((clamped - self.min) / self.step()).round();
        scaled.clamp(0.0, f32::from(self.precision.levels())) as u8
    }

    fn dequantize_level(&self, level: u8) -> f32 {
        self.min + f32::from(level) * self.step()
    }

    fn decode_levels(&self, bytes: &[u8]) -> Result<Vec<u8>, IndexError> {
        match self.precision {
            ScalarPrecision::Q8 => {
                if bytes.len() != self.dimensions {
                    return Err(IndexError::Format(format!(
                        "malformed q8 payload: expected {} bytes for {} dimensions, got {}",
                        self.dimensions,
                        self.dimensions,
                        bytes.len()
                    )));
                }
                Ok(bytes.to_vec())
            }
            ScalarPrecision::Q4 => unpack_q4_levels(bytes, self.dimensions),
        }
    }
}

fn pack_q4_levels(levels: &[u8]) -> Vec<u8> {
    let mut packed = Vec::with_capacity(levels.len().div_ceil(2));
    for pair in levels.chunks(2) {
        let low = pair[0] & 0x0F;
        let high = pair.get(1).copied().unwrap_or(0) & 0x0F;
        packed.push(low | (high << 4));
    }
    packed
}

fn unpack_q4_levels(bytes: &[u8], dimensions: usize) -> Result<Vec<u8>, IndexError> {
    let expected_len = dimensions.div_ceil(2);
    if bytes.len() != expected_len {
        return Err(IndexError::Format(format!(
            "malformed q4 payload: expected {expected_len} bytes for {dimensions} dimensions, got {}",
            bytes.len()
        )));
    }
    if dimensions % 2 == 1 && bytes.last().is_some_and(|last| last & 0xF0 != 0) {
        return Err(IndexError::Format(
            "nonzero q4 padding nibble for odd dimensions".to_string(),
        ));
    }

    let mut levels = Vec::with_capacity(dimensions);
    for byte in bytes {
        levels.push(byte & 0x0F);
        if levels.len() < dimensions {
            levels.push(byte >> 4);
        }
    }
    Ok(levels)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_each_dimension_within(decoded: &[f32], expected: &[f32], max_abs_error: f32) {
        assert_eq!(decoded.len(), expected.len());
        for (idx, (actual, expected)) in decoded.iter().zip(expected.iter()).enumerate() {
            let abs_error = (actual - expected).abs();
            assert!(
                abs_error <= max_abs_error,
                "dimension {idx}: decoded {actual} expected {expected}, abs_error {abs_error} > {max_abs_error}"
            );
        }
    }

    #[test]
    fn quantized_codec_q8_q4_q8_known_vector_round_trips_with_half_step_bound() {
        let vector = [-1.0, -0.5, 0.0, 0.5, 1.0];
        let codec = ScalarQuantizer::q8(-1.0, 1.0, vector.len()).expect("valid q8 codec");

        let encoded = codec.encode(&vector).expect("q8 encodes known vector");
        assert_eq!(encoded.len(), vector.len());

        let decoded = codec.decode(&encoded).expect("q8 decodes known vector");
        assert_each_dimension_within(&decoded, &vector, codec.max_abs_error());
        assert!(codec.max_abs_error() <= 1.0 / 255.0);
    }

    #[test]
    fn quantized_codec_q8_q4_q4_known_vector_round_trips_with_half_step_bound() {
        let vector = [-1.0, -0.5, 0.0, 0.5, 1.0];
        let codec = ScalarQuantizer::q4(-1.0, 1.0, vector.len()).expect("valid q4 codec");

        let encoded = codec.encode(&vector).expect("q4 encodes known vector");
        assert_eq!(encoded.len(), vector.len().div_ceil(2));

        let decoded = codec.decode(&encoded).expect("q4 decodes known vector");
        assert_each_dimension_within(&decoded, &vector, codec.max_abs_error());
        assert!(codec.max_abs_error() <= 1.0 / 15.0);
    }

    #[test]
    fn quantized_codec_q8_q4_q8_l2_distance_estimate_tracks_f32_baseline() {
        let a = [-1.0, -0.25, 0.25, 0.75];
        let b = [-0.75, 0.0, 0.5, 1.0];
        let codec = ScalarQuantizer::q8(-1.0, 1.0, a.len()).expect("valid q8 codec");
        let encoded_a = codec.encode(&a).expect("q8 encodes a");
        let encoded_b = codec.encode(&b).expect("q8 encodes b");

        let estimate = codec
            .distance_estimate(&encoded_a, &encoded_b, DistanceMetric::L2)
            .expect("q8 estimates l2 distance");
        let baseline = crate::vector::l2_distance(&a, &b);

        assert!(
            (estimate - baseline).abs() <= 0.02,
            "estimate {estimate}, baseline {baseline}"
        );
    }

    #[test]
    fn quantized_codec_q8_q4_q4_l2_distance_estimate_tracks_f32_baseline_with_documented_tolerance()
    {
        let a = [-1.0, -0.25, 0.25, 0.75];
        let b = [-0.75, 0.0, 0.5, 1.0];
        let codec = ScalarQuantizer::q4(-1.0, 1.0, a.len()).expect("valid q4 codec");
        let encoded_a = codec.encode(&a).expect("q4 encodes a");
        let encoded_b = codec.encode(&b).expect("q4 encodes b");

        let estimate = codec
            .distance_estimate(&encoded_a, &encoded_b, DistanceMetric::L2)
            .expect("q4 estimates l2 distance");
        let baseline = crate::vector::l2_distance(&a, &b);

        assert!(
            (estimate - baseline).abs() <= 0.2,
            "estimate {estimate}, baseline {baseline}"
        );
    }

    #[test]
    fn quantized_codec_q8_q4_q4_rejects_short_packed_bytes() {
        let codec = ScalarQuantizer::q4(-1.0, 1.0, 5).expect("valid q4 codec");

        let err = codec
            .decode(&[0x10, 0x32])
            .expect_err("short q4 payload must fail loudly");

        assert!(err.to_string().contains("malformed q4 payload"));
    }

    #[test]
    fn quantized_codec_q8_q4_q4_rejects_nonzero_padding_nibble_for_odd_dimensions() {
        let codec = ScalarQuantizer::q4(-1.0, 1.0, 5).expect("valid q4 codec");
        let encoded = codec
            .encode(&[-1.0, -0.5, 0.0, 0.5, 1.0])
            .expect("q4 encodes");
        let mut malformed = encoded.clone();
        *malformed.last_mut().expect("last byte") |= 0xF0;

        let err = codec
            .decode(&malformed)
            .expect_err("nonzero padding nibble must fail loudly");

        assert!(err.to_string().contains("nonzero q4 padding nibble"));
    }
}
