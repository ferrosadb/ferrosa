//! Validated `ALTER TABLE ... WITH <option> = <value>` patches.
//!
//! `ALTER TABLE ... WITH` changes only the options it names; every other option
//! keeps its current value. A patch is parsed and validated in full before
//! anything is applied, so an unknown option or a bad value refuses the whole
//! statement and changes nothing.
//!
//! The accepted options are the ones `CREATE TABLE` already stores:
//! `compaction`, `compression`, `comment`, `default_time_to_live`,
//! `gc_grace_seconds` and `bloom_filter_fp_chance`. Like Cassandra, a map
//! option (`compaction`, `compression`) replaces the whole map.

use std::collections::{HashMap, HashSet};

use crate::error::CqlError;
use ferrosa_schema::TableParams;

/// Cassandra's ceiling for `default_time_to_live` (20 years, in seconds).
const MAX_DEFAULT_TIME_TO_LIVE: i32 = 630_720_000;

/// One validated option assignment.
#[derive(Debug, Clone)]
enum TableOption {
    Compaction(HashMap<String, String>),
    Compression(HashMap<String, String>),
    Comment(String),
    DefaultTimeToLive(i32),
    GcGraceSeconds(i32),
    BloomFilterFpChance(f64),
}

/// A validated set of table option assignments from `ALTER TABLE ... WITH`.
#[derive(Debug, Clone, Default)]
pub struct TableOptionPatch {
    options: Vec<TableOption>,
}

impl TableOptionPatch {
    /// Parse and validate every `(name, value)` pair the parser collected.
    ///
    /// Returns `CqlError::Invalid` naming the option for an unknown option, a
    /// value that does not parse or is out of range, or an option given twice.
    pub fn parse(options: &[(String, String)]) -> Result<Self, CqlError> {
        let mut seen = HashSet::new();
        let mut parsed = Vec::with_capacity(options.len());
        for (name, value) in options {
            let name = name.to_lowercase();
            if !seen.insert(name.clone()) {
                return Err(CqlError::Invalid(format!(
                    "table option '{name}' is given more than once"
                )));
            }
            parsed.push(parse_option(&name, value)?);
        }
        Ok(Self { options: parsed })
    }

    /// True when the statement named no options.
    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }

    /// Return `current` with this patch's options applied over it.
    pub fn apply_to(&self, current: &TableParams) -> TableParams {
        let mut params = current.clone();
        for option in &self.options {
            match option {
                TableOption::Compaction(map) => params.compaction = map.clone(),
                TableOption::Compression(map) => params.compression = map.clone(),
                TableOption::Comment(text) => params.comment = text.clone(),
                TableOption::DefaultTimeToLive(ttl) => params.default_time_to_live = *ttl,
                TableOption::GcGraceSeconds(gc) => params.gc_grace_seconds = *gc,
                TableOption::BloomFilterFpChance(fp) => params.bloom_filter_fp_chance = *fp,
            }
        }
        params
    }
}

fn parse_option(name: &str, value: &str) -> Result<TableOption, CqlError> {
    match name {
        "compaction" => {
            let map = parse_map(name, value)?;
            if !map.contains_key("class") {
                return Err(invalid(name, value, "a compaction map needs a 'class'"));
            }
            Ok(TableOption::Compaction(map))
        }
        "compression" => parse_map(name, value).map(TableOption::Compression),
        "comment" => Ok(TableOption::Comment(value.to_string())),
        "default_time_to_live" => {
            let ttl = parse_non_negative_int(name, value)?;
            if ttl > MAX_DEFAULT_TIME_TO_LIVE {
                return Err(invalid(
                    name,
                    value,
                    &format!("the maximum is {MAX_DEFAULT_TIME_TO_LIVE} seconds"),
                ));
            }
            Ok(TableOption::DefaultTimeToLive(ttl))
        }
        "gc_grace_seconds" => parse_non_negative_int(name, value).map(TableOption::GcGraceSeconds),
        "bloom_filter_fp_chance" => {
            let fp: f64 = value
                .parse()
                .map_err(|_| invalid(name, value, "expected a number"))?;
            if !(fp > 0.0 && fp <= 1.0) {
                return Err(invalid(name, value, "must be greater than 0 and at most 1"));
            }
            Ok(TableOption::BloomFilterFpChance(fp))
        }
        _ => Err(CqlError::Invalid(format!(
            "unknown or unsupported table option '{name}' in ALTER TABLE ... WITH; \
             supported: compaction, compression, comment, default_time_to_live, \
             gc_grace_seconds, bloom_filter_fp_chance"
        ))),
    }
}

fn invalid(name: &str, value: &str, why: &str) -> CqlError {
    CqlError::Invalid(format!(
        "invalid value '{value}' for table option '{name}': {why}"
    ))
}

fn parse_non_negative_int(name: &str, value: &str) -> Result<i32, CqlError> {
    let n: i32 = value
        .parse()
        .map_err(|_| invalid(name, value, "expected a whole number of seconds"))?;
    if n < 0 {
        return Err(invalid(name, value, "must not be negative"));
    }
    Ok(n)
}

/// Parse the parser's stringified map literal (`{'k': 'v', ...}`), refusing
/// anything that is not one.
fn parse_map(name: &str, value: &str) -> Result<HashMap<String, String>, CqlError> {
    let trimmed = value.trim();
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| invalid(name, value, "expected a map literal {'key': 'value', ...}"))?;
    let mut map = HashMap::new();
    if inner.trim().is_empty() {
        return Ok(map);
    }
    for pair in inner.split(',') {
        let (k, v) = pair
            .split_once(':')
            .ok_or_else(|| invalid(name, value, "expected 'key': 'value' entries"))?;
        let k = k.trim().trim_matches('\'').trim_matches('"');
        let v = v.trim().trim_matches('\'').trim_matches('"');
        if k.is_empty() {
            return Err(invalid(name, value, "map keys must not be empty"));
        }
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_patch_changes_only_the_options_it_names() {
        let current = TableParams {
            comment: "keep".into(),
            default_time_to_live: 77,
            ..TableParams::default()
        };
        let patch = TableOptionPatch::parse(&opts(&[("GC_GRACE_SECONDS", "4500")])).unwrap();
        let merged = patch.apply_to(&current);
        assert_eq!(merged.gc_grace_seconds, 4500);
        assert_eq!(merged.comment, "keep");
        assert_eq!(merged.default_time_to_live, 77);
    }

    #[test]
    fn an_empty_patch_is_empty() {
        assert!(TableOptionPatch::parse(&[]).unwrap().is_empty());
    }

    #[test]
    fn a_malformed_map_entry_is_refused() {
        let err = TableOptionPatch::parse(&opts(&[("compression", "{'class'}")])).unwrap_err();
        assert!(matches!(err, CqlError::Invalid(msg) if msg.contains("compression")));
    }
}
