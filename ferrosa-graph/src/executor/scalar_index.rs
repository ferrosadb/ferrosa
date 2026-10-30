//! Exact byte probes covering the existing graph scalar equality semantics.
use ferrosa_index::IndexKey;

use crate::parser::{Expr, Literal};

pub(super) struct Probe {
    pub(super) key: IndexKey,
    pub(super) aliases: Aliases,
}

pub(super) enum Aliases {
    None,
    Hex(Option<IndexKey>),
    Boolean(u16),
    Float {
        next: f64,
        remaining: u8,
        single_precision: bool,
    },
}

impl Aliases {
    pub(super) fn is_empty(&self) -> bool {
        matches!(self, Self::None | Self::Float { remaining: 0, .. })
    }
}

impl Iterator for Aliases {
    type Item = IndexKey;

    fn next(&mut self) -> Option<IndexKey> {
        match self {
            Self::None => None,
            Self::Hex(key) => key.take(),
            Self::Boolean(next) if *next <= 255 => {
                let key = IndexKey(vec![*next as u8]);
                *next += 1;
                Some(key)
            }
            Self::Boolean(_) => None,
            Self::Float {
                next,
                remaining,
                single_precision,
            } if *remaining > 0 => {
                let key = float_key(*next, *single_precision);
                *next = adjacent(*next, *single_precision, true);
                *remaining -= 1;
                Some(key)
            }
            Self::Float { .. } => None,
        }
    }
}

fn one(bytes: Vec<u8>) -> Probe {
    Probe {
        key: IndexKey(bytes),
        aliases: Aliases::None,
    }
}

pub(super) fn probe(value: &Expr, column_type: &str) -> Option<Probe> {
    match value {
        Expr::Literal(Literal::Integer(value)) if column_type == "int" => {
            Some(one(i32::try_from(*value).ok()?.to_be_bytes().to_vec()))
        }
        Expr::Literal(Literal::Integer(value)) if matches!(column_type, "bigint" | "counter") => {
            Some(one(value.to_be_bytes().to_vec()))
        }
        Expr::Literal(Literal::String(value)) if column_type == "uuid" => {
            if let Ok(uuid) = uuid::Uuid::parse_str(value) {
                Some(one(uuid.as_bytes().to_vec()))
            } else {
                // The decoder hex-encodes malformed UUID lengths.
                let bytes = decode_hex(value)?;
                (bytes.len() != 16).then(|| one(bytes))
            }
        }
        Expr::Literal(Literal::String(value))
            if matches!(column_type, "text" | "varchar" | "ascii" | "blob") =>
        {
            let alias = decode_hex(value).filter(|bytes| std::str::from_utf8(bytes).is_err());
            Some(Probe {
                key: IndexKey(value.as_bytes().to_vec()),
                aliases: alias.map_or(Aliases::None, |bytes| Aliases::Hex(Some(IndexKey(bytes)))),
            })
        }
        Expr::Literal(Literal::Bool(false)) if column_type == "boolean" => Some(one(vec![0])),
        Expr::Literal(Literal::Bool(true)) if column_type == "boolean" => Some(Probe {
            key: IndexKey(vec![1]),
            aliases: Aliases::Boolean(2),
        }),
        Expr::Literal(Literal::Float(value)) if matches!(column_type, "float" | "double") => {
            float_probe(*value, column_type == "float")
        }
        Expr::Literal(Literal::Integer(value)) if matches!(column_type, "float" | "double") => {
            float_probe(*value as f64, column_type == "float")
        }
        _ => None,
    }
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    let decoded = hex::decode(value).ok()?;
    // hex::encode emits lower case; a differently cased query cannot match it.
    value
        .bytes()
        .all(|byte| !byte.is_ascii_uppercase())
        .then_some(decoded)
}

fn float_key(value: f64, single_precision: bool) -> IndexKey {
    IndexKey(if single_precision {
        (value as f32).to_be_bytes().to_vec()
    } else {
        value.to_be_bytes().to_vec()
    })
}

fn adjacent(value: f64, single_precision: bool, up: bool) -> f64 {
    if single_precision {
        let value = value as f32;
        (if up {
            value.next_up()
        } else {
            value.next_down()
        }) as f64
    } else if up {
        value.next_up()
    } else {
        value.next_down()
    }
}

fn float_probe(value: f64, single_precision: bool) -> Option<Probe> {
    if !value.is_finite() {
        return None;
    }
    let center = if single_precision {
        (value as f32) as f64
    } else {
        value
    };
    if !center.is_finite() {
        return None;
    }
    let matches = |candidate: f64| (candidate - value).abs() < f64::EPSILON;
    let mut low = center;
    let mut high = center;
    let mut count = 1;
    if matches(center) {
        // Preserve the evaluator's epsilon comparison. Near zero it may match
        // unboundedly many encodings; retain the range path rather than miss rows.
        while matches(adjacent(low, single_precision, false)) {
            low = adjacent(low, single_precision, false);
            count += 1;
            if count > 8 {
                return None;
            }
        }
        while matches(adjacent(high, single_precision, true)) {
            high = adjacent(high, single_precision, true);
            count += 1;
            if count > 8 {
                return None;
            }
        }
    }
    Some(Probe {
        key: float_key(low, single_precision),
        aliases: Aliases::Float {
            next: adjacent(low, single_precision, true),
            remaining: count - 1,
            single_precision,
        },
    })
}
