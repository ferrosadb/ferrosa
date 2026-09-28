//! Module: tunable ingest `Limits` and compiled read `HardCeilings`.
//! Correctness: correct when a tunable is never zero, never above its ceiling and
//! never above the write-path maximum, and reads consult only `HardCeilings`.
//! Last revised: 2026-09-28
//! Last changed: T-100 initial implementation (D14, D14a, D14b, D14d, D6b).

use crate::error::{JsonbError, LimitsError};

/// Fixed read-path nesting ceiling (D14, D14b).
pub const HARD_MAX_DEPTH: u32 = 1000;
/// Fixed ceiling on digits before the decimal point (D14a).
pub const HARD_MAX_DIGITS_BEFORE_POINT: usize = 131_072;
/// Fixed ceiling on digits after the decimal point (D14a).
pub const HARD_MAX_DIGITS_AFTER_POINT: usize = 16_383;
/// Fixed absolute ceiling on an encoded value: 256 MiB (D14d).
pub const HARD_MAX_ENCODED_BYTES: usize = 256 * 1024 * 1024;

const DEFAULT_MAX_BYTES: u64 = 10 * 1024 * 1024;
const DEFAULT_MAX_DEPTH: u64 = 1000;
const DEFAULT_MAX_KEY_LIST: u64 = 1000;
const DEFAULT_MAX_INDEX_TERMS: u64 = 10_000;
const DEFAULT_MAX_PATH_LEN: u64 = 4 * 1024;
const DEFAULT_PATH_STEP_BUDGET: u64 = 1_000_000;

// Ceilings for tunables that D14b does not fix. Chosen in T-100; see roadmap.
const CEILING_KEY_LIST: u64 = 1_000_000;
const CEILING_INDEX_TERMS: u64 = 10_000_000;
const CEILING_PATH_LEN: u64 = 1024 * 1024;
const CEILING_PATH_STEP_BUDGET: u64 = 1_000_000_000;

/// Compiled read ceilings (D14b); not configurable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardCeilings {
    pub depth: u32,
    pub digits_before_point: usize,
    pub digits_after_point: usize,
    pub encoded_bytes: usize,
}

impl HardCeilings {
    /// The compiled values.
    pub const CURRENT: HardCeilings = HardCeilings {
        depth: HARD_MAX_DEPTH,
        digits_before_point: HARD_MAX_DIGITS_BEFORE_POINT,
        digits_after_point: HARD_MAX_DIGITS_AFTER_POINT,
        encoded_bytes: HARD_MAX_ENCODED_BYTES,
    };

    /// Refuse a nesting depth above the ceiling.
    pub fn check_depth(&self, depth: u32) -> Result<(), JsonbError> {
        if depth > self.depth {
            return Err(JsonbError::DepthExceeded {
                depth,
                max: self.depth,
            });
        }
        Ok(())
    }

    /// Refuse an encoded length above the ceiling.
    pub fn check_encoded_len(&self, len: usize) -> Result<(), JsonbError> {
        if len > self.encoded_bytes {
            return Err(JsonbError::EncodedTooLarge {
                len,
                max: self.encoded_bytes,
            });
        }
        Ok(())
    }

    /// Refuse a number whose digit counts exceed the D14a ceilings.
    pub fn check_digits(&self, before: usize, after: usize) -> Result<(), JsonbError> {
        if before > self.digits_before_point {
            return Err(JsonbError::DigitsBeforePointExceeded {
                digits: before,
                max: self.digits_before_point,
            });
        }
        if after > self.digits_after_point {
            return Err(JsonbError::DigitsAfterPointExceeded {
                digits: after,
                max: self.digits_after_point,
            });
        }
        Ok(())
    }
}

/// What to do with a duplicate object key on input (D6b).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DuplicateKeyPolicy {
    /// The last occurrence wins; the dedupe is counted by the caller.
    #[default]
    LastWins,
    /// A duplicate is a typed error.
    Error,
}

/// Tunable ingest limits (D14, D14b): writes and query arguments only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_input_bytes: usize,
    pub max_encoded_bytes: usize,
    pub max_depth: u32,
    pub max_key_list: usize,
    pub max_index_terms_per_doc: usize,
    pub max_path_len: usize,
    pub path_step_budget: u64,
    pub duplicate_keys: DuplicateKeyPolicy,
}

/// TOML-supplied values; `None` means unset. The binary maps its TOML into this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LimitsConfig {
    pub max_input_bytes: Option<u64>,
    pub max_encoded_bytes: Option<u64>,
    pub max_nesting_depth: Option<u64>,
    pub max_key_list_length: Option<u64>,
    pub max_index_terms_per_doc: Option<u64>,
    pub max_path_len: Option<u64>,
    pub path_step_budget: Option<u64>,
    pub duplicate_keys: Option<DuplicateKeyPolicy>,
}

struct Spec {
    name: &'static str,
    env: Option<&'static str>,
    default: u64,
    ceiling: u64,
}

const SPEC_INPUT: Spec = Spec {
    name: "max_input_bytes",
    env: Some("FERROSA_JSONB_MAX_INPUT_BYTES"),
    default: DEFAULT_MAX_BYTES,
    ceiling: HARD_MAX_ENCODED_BYTES as u64,
};
const SPEC_ENCODED: Spec = Spec {
    name: "max_encoded_bytes",
    env: Some("FERROSA_JSONB_MAX_ENCODED_BYTES"),
    default: DEFAULT_MAX_BYTES,
    ceiling: HARD_MAX_ENCODED_BYTES as u64,
};
const SPEC_DEPTH: Spec = Spec {
    name: "max_nesting_depth",
    env: Some("FERROSA_JSONB_MAX_NESTING_DEPTH"),
    default: DEFAULT_MAX_DEPTH,
    ceiling: HARD_MAX_DEPTH as u64,
};
const SPEC_KEY_LIST: Spec = Spec {
    name: "max_key_list_length",
    env: Some("FERROSA_JSONB_MAX_KEY_LIST_LENGTH"),
    default: DEFAULT_MAX_KEY_LIST,
    ceiling: CEILING_KEY_LIST,
};
const SPEC_TERMS: Spec = Spec {
    name: "max_index_terms_per_doc",
    env: Some("FERROSA_JSONB_MAX_INDEX_TERMS_PER_DOC"),
    default: DEFAULT_MAX_INDEX_TERMS,
    ceiling: CEILING_INDEX_TERMS,
};
const SPEC_PATH_LEN: Spec = Spec {
    name: "max_path_len",
    env: None,
    default: DEFAULT_MAX_PATH_LEN,
    ceiling: CEILING_PATH_LEN,
};
const SPEC_STEPS: Spec = Spec {
    name: "path_step_budget",
    env: None,
    default: DEFAULT_PATH_STEP_BUDGET,
    ceiling: CEILING_PATH_STEP_BUDGET,
};

/// TOML wins over env, env wins over the default; then zero and ceiling checks.
fn resolve(
    spec: &Spec,
    toml: Option<u64>,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<u64, LimitsError> {
    let value = match (toml, spec.env) {
        (Some(v), _) => v,
        (None, Some(var)) => match env(var) {
            Some(raw) => raw
                .trim()
                .parse::<u64>()
                .map_err(|_| LimitsError::InvalidEnv { var, raw })?,
            None => spec.default,
        },
        (None, None) => spec.default,
    };
    if value == 0 {
        return Err(LimitsError::Zero { name: spec.name });
    }
    if value > spec.ceiling {
        return Err(LimitsError::AboveCeiling {
            name: spec.name,
            value,
            ceiling: spec.ceiling,
        });
    }
    Ok(value)
}

fn above_ceiling(spec: &Spec, value: u64) -> LimitsError {
    LimitsError::AboveCeiling {
        name: spec.name,
        value,
        ceiling: spec.ceiling,
    }
}

fn to_usize(spec: &Spec, value: u64) -> Result<usize, LimitsError> {
    usize::try_from(value).map_err(|_| above_ceiling(spec, value))
}

fn check_write_path(name: &'static str, value: u64, max: u64) -> Result<(), LimitsError> {
    if value > max {
        return Err(LimitsError::AboveWritePath {
            name,
            value,
            write_path_max: max,
        });
    }
    Ok(())
}

impl Limits {
    /// Resolve limits from TOML, then the process environment, then defaults.
    ///
    /// `write_path_max` is the commit-log segment size; the binary passes it
    /// because this crate has no ferrosa dependency (D14d).
    pub fn from_config(cfg: &LimitsConfig, write_path_max: u64) -> Result<Limits, LimitsError> {
        // Non-unicode values are passed through lossily so they fail to parse
        // as InvalidEnv rather than being treated as unset.
        let env = |k: &str| std::env::var_os(k).map(|v| v.to_string_lossy().into_owned());
        Limits::from_config_with_env(cfg, &env, write_path_max)
    }

    /// As [`Limits::from_config`] with an injected environment lookup (tests).
    pub fn from_config_with_env(
        cfg: &LimitsConfig,
        env: &dyn Fn(&str) -> Option<String>,
        write_path_max: u64,
    ) -> Result<Limits, LimitsError> {
        if write_path_max == 0 {
            return Err(LimitsError::Zero {
                name: "write_path_max",
            });
        }
        let input = resolve(&SPEC_INPUT, cfg.max_input_bytes, env)?;
        let encoded = resolve(&SPEC_ENCODED, cfg.max_encoded_bytes, env)?;
        check_write_path(SPEC_INPUT.name, input, write_path_max)?;
        check_write_path(SPEC_ENCODED.name, encoded, write_path_max)?;
        let depth = resolve(&SPEC_DEPTH, cfg.max_nesting_depth, env)?;
        let keys = resolve(&SPEC_KEY_LIST, cfg.max_key_list_length, env)?;
        let terms = resolve(&SPEC_TERMS, cfg.max_index_terms_per_doc, env)?;
        let path_len = resolve(&SPEC_PATH_LEN, cfg.max_path_len, env)?;
        let steps = resolve(&SPEC_STEPS, cfg.path_step_budget, env)?;
        Ok(Limits {
            max_input_bytes: to_usize(&SPEC_INPUT, input)?,
            max_encoded_bytes: to_usize(&SPEC_ENCODED, encoded)?,
            max_depth: u32::try_from(depth).map_err(|_| above_ceiling(&SPEC_DEPTH, depth))?,
            max_key_list: to_usize(&SPEC_KEY_LIST, keys)?,
            max_index_terms_per_doc: to_usize(&SPEC_TERMS, terms)?,
            max_path_len: to_usize(&SPEC_PATH_LEN, path_len)?,
            path_step_budget: steps,
            duplicate_keys: cfg.duplicate_keys.unwrap_or_default(),
        })
    }

    /// Refuse input text longer than `max_input_bytes`. Callers check as bytes
    /// arrive, so an oversized document is refused before it is buffered.
    pub fn check_input_len(&self, len: usize) -> Result<(), JsonbError> {
        if len > self.max_input_bytes {
            return Err(JsonbError::InputTooLarge {
                len,
                max: self.max_input_bytes,
            });
        }
        Ok(())
    }

    /// Refuse an encoded length above `max_encoded_bytes`.
    pub fn check_encoded_len(&self, len: usize) -> Result<(), JsonbError> {
        if len > self.max_encoded_bytes {
            return Err(JsonbError::EncodedTooLarge {
                len,
                max: self.max_encoded_bytes,
            });
        }
        Ok(())
    }

    /// Refuse a nesting depth above `max_depth`.
    pub fn check_depth(&self, depth: u32) -> Result<(), JsonbError> {
        if depth > self.max_depth {
            return Err(JsonbError::DepthExceeded {
                depth,
                max: self.max_depth,
            });
        }
        Ok(())
    }

    /// Refuse a key list longer than `max_key_list`.
    pub fn check_key_list(&self, len: usize) -> Result<(), JsonbError> {
        if len > self.max_key_list {
            return Err(JsonbError::KeyListTooLong {
                len,
                max: self.max_key_list,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEGMENT: u64 = 32 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn load(cfg: &LimitsConfig) -> Result<Limits, LimitsError> {
        Limits::from_config_with_env(cfg, &no_env, SEGMENT)
    }

    #[test]
    fn jsonb_limits_tunable_above_hard_ceiling_refuses_startup() {
        let big = 257 * MIB;
        // Use a write-path max above the ceiling so the ceiling check is what fires.
        let load_big = |cfg: &LimitsConfig| Limits::from_config_with_env(cfg, &no_env, 512 * MIB);
        let cases: [(&str, LimitsConfig); 7] = [
            (
                "max_input_bytes",
                LimitsConfig {
                    max_input_bytes: Some(big),
                    ..Default::default()
                },
            ),
            (
                "max_encoded_bytes",
                LimitsConfig {
                    max_encoded_bytes: Some(big),
                    ..Default::default()
                },
            ),
            (
                "max_nesting_depth",
                LimitsConfig {
                    max_nesting_depth: Some(1001),
                    ..Default::default()
                },
            ),
            (
                "max_key_list_length",
                LimitsConfig {
                    max_key_list_length: Some(CEILING_KEY_LIST + 1),
                    ..Default::default()
                },
            ),
            (
                "max_index_terms_per_doc",
                LimitsConfig {
                    max_index_terms_per_doc: Some(CEILING_INDEX_TERMS + 1),
                    ..Default::default()
                },
            ),
            (
                "max_path_len",
                LimitsConfig {
                    max_path_len: Some(CEILING_PATH_LEN + 1),
                    ..Default::default()
                },
            ),
            (
                "path_step_budget",
                LimitsConfig {
                    path_step_budget: Some(CEILING_PATH_STEP_BUDGET + 1),
                    ..Default::default()
                },
            ),
        ];
        for (name, cfg) in cases {
            let err = load_big(&cfg);
            assert!(
                matches!(&err, Err(LimitsError::AboveCeiling { name: n, .. }) if *n == name),
                "{name}: {err:?}"
            );
            let msg = err.map(|_| ()).unwrap_err().to_string();
            assert!(msg.contains(name), "message must name the tunable: {msg}");
        }
        // Exactly at the ceiling is accepted.
        let at = LimitsConfig {
            max_encoded_bytes: Some(256 * MIB),
            ..Default::default()
        };
        assert!(load_big(&at).is_ok());
    }

    #[test]
    fn jsonb_limits_zero_refuses_startup() {
        let zero = |cfg: LimitsConfig, name: &str| {
            let err = load(&cfg);
            assert!(
                matches!(&err, Err(LimitsError::Zero { name: n }) if *n == name),
                "{name}: {err:?}"
            );
        };
        zero(
            LimitsConfig {
                max_input_bytes: Some(0),
                ..Default::default()
            },
            "max_input_bytes",
        );
        zero(
            LimitsConfig {
                max_encoded_bytes: Some(0),
                ..Default::default()
            },
            "max_encoded_bytes",
        );
        zero(
            LimitsConfig {
                max_nesting_depth: Some(0),
                ..Default::default()
            },
            "max_nesting_depth",
        );
        zero(
            LimitsConfig {
                max_key_list_length: Some(0),
                ..Default::default()
            },
            "max_key_list_length",
        );
        zero(
            LimitsConfig {
                max_index_terms_per_doc: Some(0),
                ..Default::default()
            },
            "max_index_terms_per_doc",
        );
        zero(
            LimitsConfig {
                max_path_len: Some(0),
                ..Default::default()
            },
            "max_path_len",
        );
        zero(
            LimitsConfig {
                path_step_budget: Some(0),
                ..Default::default()
            },
            "path_step_budget",
        );
        // A zero from the environment is refused too.
        let env = |k: &str| (k == "FERROSA_JSONB_MAX_NESTING_DEPTH").then(|| "0".to_string());
        let err = Limits::from_config_with_env(&LimitsConfig::default(), &env, SEGMENT);
        assert_eq!(
            err,
            Err(LimitsError::Zero {
                name: "max_nesting_depth"
            })
        );
    }

    #[test]
    fn jsonb_limits_above_commitlog_segment_refuses_startup() {
        let cfg = LimitsConfig {
            max_encoded_bytes: Some(64 * MIB),
            ..Default::default()
        };
        let err = load(&cfg);
        assert_eq!(
            err,
            Err(LimitsError::AboveWritePath {
                name: "max_encoded_bytes",
                value: 64 * MIB,
                write_path_max: SEGMENT,
            })
        );
        let msg = err.map(|_| ()).unwrap_err().to_string();
        assert!(
            msg.contains(&(64 * MIB).to_string()) && msg.contains(&SEGMENT.to_string()),
            "{msg}"
        );
        let input = LimitsConfig {
            max_input_bytes: Some(33 * MIB),
            ..Default::default()
        };
        assert!(matches!(
            load(&input),
            Err(LimitsError::AboveWritePath {
                name: "max_input_bytes",
                ..
            })
        ));
        // A default above a small write-path maximum is refused as well.
        let small = Limits::from_config_with_env(&LimitsConfig::default(), &no_env, MIB);
        assert!(matches!(small, Err(LimitsError::AboveWritePath { .. })));
        // Equal to the write-path maximum is accepted.
        let eq = LimitsConfig {
            max_encoded_bytes: Some(SEGMENT),
            ..Default::default()
        };
        assert!(load(&eq).is_ok());
    }

    #[test]
    fn jsonb_limits_toml_wins_over_env() {
        let env = |k: &str| match k {
            "FERROSA_JSONB_MAX_NESTING_DEPTH" => Some("10".to_string()),
            "FERROSA_JSONB_MAX_KEY_LIST_LENGTH" => Some("77".to_string()),
            _ => None,
        };
        let cfg = LimitsConfig {
            max_nesting_depth: Some(50),
            ..Default::default()
        };
        let limits = Limits::from_config_with_env(&cfg, &env, SEGMENT);
        let limits = limits.unwrap();
        assert_eq!(limits.max_depth, 50, "TOML wins over env");
        assert_eq!(limits.max_key_list, 77, "env applies when TOML is unset");
    }

    #[test]
    fn jsonb_limits_defaults_match_d14() {
        let l = load(&LimitsConfig::default()).unwrap();
        assert_eq!(l.max_input_bytes, 10 * 1024 * 1024);
        assert_eq!(l.max_encoded_bytes, 10 * 1024 * 1024);
        assert_eq!(l.max_depth, 1000);
        assert_eq!(l.max_key_list, 1000);
        assert_eq!(l.max_index_terms_per_doc, 10_000);
        assert_eq!(l.max_path_len, 4096);
        assert_eq!(l.path_step_budget, 1_000_000);
        assert_eq!(l.duplicate_keys, DuplicateKeyPolicy::LastWins);
    }

    #[test]
    fn jsonb_limits_unparseable_env_is_an_error() {
        let env = |k: &str| (k == "FERROSA_JSONB_MAX_INPUT_BYTES").then(|| "ten".to_string());
        let err = Limits::from_config_with_env(&LimitsConfig::default(), &env, SEGMENT);
        assert!(
            matches!(err, Err(LimitsError::InvalidEnv { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn jsonb_limits_duplicate_policy_from_config() {
        let cfg = LimitsConfig {
            duplicate_keys: Some(DuplicateKeyPolicy::Error),
            ..Default::default()
        };
        assert_eq!(
            load(&cfg).unwrap().duplicate_keys,
            DuplicateKeyPolicy::Error
        );
    }

    #[test]
    fn jsonb_hard_ceilings_are_the_decided_values() {
        let c = HardCeilings::CURRENT;
        assert_eq!(
            (
                c.depth,
                c.digits_before_point,
                c.digits_after_point,
                c.encoded_bytes
            ),
            (1000, 131_072, 16_383, 256 * 1024 * 1024)
        );
    }

    #[test]
    fn jsonb_hard_ceilings_check_at_and_over_the_boundary() {
        let c = HardCeilings::CURRENT;
        assert!(c.check_depth(1000).is_ok());
        assert_eq!(
            c.check_depth(1001),
            Err(JsonbError::DepthExceeded {
                depth: 1001,
                max: 1000
            })
        );
        assert!(c.check_encoded_len(HARD_MAX_ENCODED_BYTES).is_ok());
        assert!(c.check_encoded_len(HARD_MAX_ENCODED_BYTES + 1).is_err());
        assert!(c.check_digits(131_072, 16_383).is_ok());
        assert!(matches!(
            c.check_digits(131_073, 0),
            Err(JsonbError::DigitsBeforePointExceeded { .. })
        ));
        assert!(matches!(
            c.check_digits(0, 16_384),
            Err(JsonbError::DigitsAfterPointExceeded { .. })
        ));
    }

    #[test]
    fn jsonb_limits_ingest_checks_use_tunables() {
        let cfg = LimitsConfig {
            max_input_bytes: Some(8),
            max_encoded_bytes: Some(9),
            max_nesting_depth: Some(3),
            max_key_list_length: Some(2),
            ..Default::default()
        };
        let l = load(&cfg).unwrap();
        assert!(l.check_input_len(8).is_ok() && l.check_input_len(9).is_err());
        assert!(l.check_encoded_len(9).is_ok() && l.check_encoded_len(10).is_err());
        assert!(l.check_depth(3).is_ok() && l.check_depth(4).is_err());
        assert!(l.check_key_list(2).is_ok() && l.check_key_list(3).is_err());
    }
}
