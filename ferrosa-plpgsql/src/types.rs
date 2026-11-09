//! The PostgreSQL type names a PL/pgSQL declaration or signature may use, and their OIDs.

/// `bool`.
pub const OID_BOOL: u32 = 16;
/// `text`.
pub const OID_TEXT: u32 = 25;

/// The OID of a type name as written in a declaration (`text`, `int`, `bigint`, `json`, …),
/// or `None` when this subset does not support it.
pub fn type_oid(name: &str) -> Option<u32> {
    let lower = name.trim().to_ascii_lowercase();
    let base = lower
        .strip_prefix("pg_catalog.")
        .unwrap_or(lower.as_str())
        .split('(')
        .next()
        .unwrap_or("")
        .trim();
    Some(match base {
        "bool" | "boolean" => OID_BOOL,
        "bytea" => 17,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "int4" | "int" | "integer" => 23,
        "text" => OID_TEXT,
        "json" => 114,
        "float4" | "real" => 700,
        "float8" | "double precision" => 701,
        "varchar" | "character varying" => 1043,
        "date" => 1082,
        "time" => 1083,
        "timestamp" | "timestamp without time zone" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        "numeric" | "decimal" => 1700,
        "uuid" => 2950,
        "jsonb" => 3802,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_names_resolve_to_their_oids() {
        assert_eq!(type_oid("text"), Some(OID_TEXT));
        assert_eq!(type_oid("JSON"), Some(114));
        assert_eq!(type_oid("int"), Some(23));
        assert_eq!(type_oid("bigint"), Some(20));
        assert_eq!(type_oid("varchar(20)"), Some(1043));
        assert_eq!(type_oid("pg_catalog.bool"), Some(OID_BOOL));
    }

    #[test]
    fn an_unsupported_type_is_none_not_a_guess() {
        assert_eq!(type_oid("record"), None);
        assert_eq!(type_oid("oban_job_state"), None);
    }
}
