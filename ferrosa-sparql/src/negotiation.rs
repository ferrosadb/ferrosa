//! Query-form-aware HTTP negotiation, including the SPARQL 1.2 JSON media version.
//! Correctness: q=0 exclusions and explicit versions win over wildcard ranges.
//! Last revised: 2026-10-27
//! Last changed: Negotiate only formats that preserve the result's RDF terms.
use crate::{engine::SparqlResult, results::ResultFormat};

/// Select a supported representation using RFC 9110 range specificity and quality.
/// Candidate order supplies the server preference when the client is indifferent.
pub(crate) fn negotiate(accept: &str, result: &SparqlResult) -> Option<ResultFormat> {
    let graph = matches!(result, SparqlResult::Graph(_));
    let rdf12 = matches!(result,SparqlResult::Select(rows) if rows.requires_json12());
    let candidates: &[ResultFormat] = if graph {
        &[ResultFormat::NTriples, ResultFormat::Turtle]
    } else if rdf12 {
        &[ResultFormat::Json12]
    } else {
        &[
            ResultFormat::Json,
            ResultFormat::Json12,
            ResultFormat::Json11,
            ResultFormat::Xml,
        ]
    };
    let accept = if accept.trim().is_empty() {
        "*/*"
    } else {
        accept
    };
    let mut best = None;
    let mut best_quality = 0.0;
    for &candidate in candidates {
        let mut preference: Option<(usize, f64)> = None;
        for range in accept.split(',') {
            if let Some((specificity, quality)) = match_range(range, candidate) {
                if preference.is_none_or(|(old, _)| specificity > old) {
                    preference = Some((specificity, quality));
                }
            }
        }
        if let Some((_, quality)) = preference {
            if quality > best_quality {
                best = Some(candidate);
                best_quality = quality;
            }
        }
    }
    best
}
fn match_range(range: &str, format: ResultFormat) -> Option<(usize, f64)> {
    let mut parts = range.split(';');
    let media = parts.next()?.trim();
    let actual = format.content_type().split(';').next()?;
    let specificity = media_specificity(media, actual)?;
    let (quality, version) = range_parameters(parts)?;
    if let Some(version) = version {
        let candidate_version = match format {
            ResultFormat::Json12 => "1.2",
            ResultFormat::Json11 => "1.1",
            _ => return None,
        };
        if version != candidate_version {
            return None;
        }
    }
    Some((specificity * 10 + usize::from(version.is_some()), quality))
}

fn media_specificity(media: &str, actual: &str) -> Option<usize> {
    if media.eq_ignore_ascii_case(actual) {
        Some(2)
    } else if media == "*/*" {
        Some(0)
    } else if media.strip_suffix("/*").is_some_and(|kind| {
        actual
            .split_once('/')
            .is_some_and(|(actual, _)| actual.eq_ignore_ascii_case(kind))
    }) {
        Some(1)
    } else {
        None
    }
}
fn range_parameters<'a>(parts: impl Iterator<Item = &'a str>) -> Option<(f64, Option<&'a str>)> {
    let mut quality = 1.0;
    let mut version = None;
    for parameter in parts {
        let (key, value) = parameter.trim().split_once('=')?;
        let value = value.trim().trim_matches('"');
        if key.trim().eq_ignore_ascii_case("q") {
            quality = value.parse::<f64>().ok()?;
            if !quality.is_finite() || !(0.0..=1.0).contains(&quality) {
                return None;
            }
        } else if key.trim().eq_ignore_ascii_case("version") {
            if version.replace(value).is_some() {
                return None;
            }
        } else {
            return None;
        }
    }
    Some((quality, version))
}
