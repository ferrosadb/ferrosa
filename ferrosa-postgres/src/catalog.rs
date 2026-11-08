//! `pg_catalog` virtual-table projections for the Postgres front-end.
//!
//! Postgres drivers (psql `\d`, JDBC/ODBC, ORMs) introspect the database by
//! querying `pg_catalog.pg_namespace` / `pg_class` / `pg_attribute` / `pg_type` /
//! `pg_index` / `pg_constraint`.
//! This module projects those catalog shapes from the *live* `ferrosa-schema`
//! metadata into [`ferrosa_sql::InMemoryTable`]s, so the bespoke relational
//! engine's scan operators (decision D3) can read them with no special-casing.
//!
//! Why here and not in `ferrosa-sql`: the catalog *shapes* (column names,
//! relkind codes) and Postgres **type OIDs** are Postgres-specific. Keeping
//! them in `ferrosa-postgres` leaves `ferrosa-sql` a pure relational engine.
//!
//! Namespace model (D5/D8): a ferrosa keyspace is a Postgres schema; tables
//! live in keyspaces; columns have types. We additionally expose the two
//! reserved Postgres schemas `pg_catalog` and `information_schema` so drivers
//! that resolve them by name do not fault.
//!
//! ## OID scheme
//!
//! Real Postgres uses an `oid` (unsigned 32-bit) type. The first-slice
//! [`ferrosa_sql::Value`] has no unsigned variant, so OIDs are represented as
//! `Value::Int(i64)` here — every OID we mint fits in `u32`, so the widening to
//! `i64` is lossless. OIDs are assigned **deterministically** by a stable
//! FNV-1a hash of a kind-prefixed natural key, folded into the user-OID range
//! `[16384, u32::MAX]` (Postgres reserves `< 16384` for built-ins, so type
//! OIDs like 23/25 never collide with a synthetic namespace/relation OID). The
//! scheme is pure (no counters, no insertion-order dependence), so the same
//! schema always projects the same OIDs.

use ferrosa_common::timeuuid::is_reserved_column_name;

use crate::pg_types::{pg_type_of_column, PgType, PgTypeError};
use ferrosa_schema::{ColumnMetadata, Schema, SchemaSnapshot, TableMetadata};
use ferrosa_sql::{Column, ColumnType, InMemoryTable, RelSchema, Row, Value};

/// First OID Postgres hands out to user objects. Everything below is reserved
/// for built-in catalog entries (the fixed type OIDs in [`crate::pg_types`] live here).
const FIRST_USER_OID: u32 = 16_384;

/// Deterministic synthetic OID for a named object of a given kind.
///
/// FNV-1a over `"{kind}:{name}"`, folded into `[FIRST_USER_OID, u32::MAX]`. The
/// `kind` prefix keeps a namespace and a same-named relation from colliding.
pub(crate) fn synthetic_oid(kind: &str, name: &str) -> u32 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = FNV_OFFSET;
    for byte in kind
        .bytes()
        .chain(std::iter::once(b':'))
        .chain(name.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    let span = u64::from(u32::MAX - FIRST_USER_OID);
    FIRST_USER_OID + (hash % span) as u32
}

/// OID of the namespace (Postgres schema) for a keyspace name.
pub(crate) fn namespace_oid(keyspace: &str) -> u32 {
    synthetic_oid("namespace", keyspace)
}

/// OID of the relation (table) `keyspace.table`.
pub(crate) fn relation_oid(keyspace: &str, table: &str) -> u32 {
    synthetic_oid("relation", &format!("{keyspace}.{table}"))
}

/// OID of the index named `index_name` in `keyspace`.
///
/// A distinct `kind` prefix keeps an index OID from colliding with the table it
/// indexes (or any other relation), matching Postgres, where indexes and tables
/// share one relation-OID space but never one physical object.
fn index_oid(keyspace: &str, index_name: &str) -> u32 {
    synthetic_oid("index", &format!("{keyspace}.{index_name}"))
}

/// OID of the primary-key constraint on `keyspace.table`.
fn primary_key_constraint_oid(keyspace: &str, table: &str) -> u32 {
    synthetic_oid("constraint", &format!("{keyspace}.{table}"))
}

/// The name of the index that backs `table`'s primary key.
///
/// PostgreSQL names it `<table>_pkey` by default, and ferrosa's
/// `ALTER TABLE ... ADD PRIMARY KEY` builds its secondary index under that very
/// name (`ddl.rs::execute_add_primary_key`), so the two agree by construction.
fn primary_key_index_name(table: &str) -> String {
    format!("{table}_pkey")
}

/// `(column name, Postgres attnum)` for every column of `meta`, in declared order.
///
/// Postgres numbers a table's own columns 1..n and its SYSTEM columns with NEGATIVE
/// attnums (`ctid` is -1). A reserved `_sys_` column is exactly that: it must not
/// consume a positive ordinal, or every ordinary column would be off by one to a
/// client reading the catalog. This is the ONE place the number is computed, shared
/// by `pg_attribute` (which lists every column) and the index/constraint projections
/// (which resolve a key column to it), so the catalog cannot disagree with itself.
fn column_attnums(meta: &TableMetadata) -> Vec<(&str, i64)> {
    let mut ordinal = 0i64;
    let mut system = 0i64;
    meta.columns
        .keys()
        .map(|name| {
            let attnum = if is_reserved_column_name(name) {
                system -= 1;
                system
            } else {
                ordinal += 1;
                ordinal
            };
            (name.as_str(), attnum)
        })
        .collect()
}

/// The Postgres `attnum` of column `name` in `meta`, or `None` when `meta` has no
/// such column.
fn attnum_of(meta: &TableMetadata, name: &str) -> Option<i64> {
    column_attnums(meta)
        .into_iter()
        .find(|(column, _)| *column == name)
        .map(|(_, attnum)| attnum)
}

/// The attnums of `columns` in `meta`, in `columns` order; `None` when a name is
/// not a column of `meta`.
///
/// A key naming an absent column cannot be projected to a correct attnum, so the
/// caller OMITS the row rather than guessing a number a client could not detect.
fn attnums_of(meta: &TableMetadata, columns: &[String]) -> Option<Vec<i64>> {
    columns.iter().map(|name| attnum_of(meta, name)).collect()
}

/// An `int2vector`/`int2[]` value of `attnums`, in the carrier this engine has.
///
/// `ferrosa_sql::Value` has no integer-array variant, so the attnums ride in its one
/// array value, `TextArray`, as decimal strings — the values are exact and ordered;
/// only the OID differs (see the module OID note, and [`crate::pg_types`] on the
/// text-array OID 1009).
fn attnum_vector(attnums: &[i64]) -> Value {
    Value::TextArray(attnums.iter().map(|n| Some(n.to_string())).collect())
}

/// OID column value — see the module-level OID-scheme note on the `Int` choice.
fn oid_val(oid: u32) -> Value {
    // Carried as Value::Int in [0, 2^32); the column type is ColumnType::Oid.
    Value::Int(i64::from(oid))
}

/// Keyspace names from the snapshot, sorted for deterministic row order.
fn sorted_keyspaces(snapshot: &SchemaSnapshot) -> Vec<String> {
    let mut names: Vec<String> = snapshot.keyspaces.keys().cloned().collect();
    names.sort();
    names
}

/// `(keyspace, table)` pairs from the snapshot, sorted for deterministic order.
///
/// Tables with ferrosa's reserved `_sys_` prefix (the enum metadata table,
/// `pg_enum::META_TABLE`) are ferrosa's own storage, not relations a client created, so
/// they are not projected.
fn sorted_tables(snapshot: &SchemaSnapshot) -> Vec<(String, String)> {
    let mut keys: Vec<(String, String)> = snapshot
        .tables
        .keys()
        .filter(|(_, table)| !is_reserved_column_name(table))
        .cloned() // the projection's sort key list; owned so it can be sorted
        .collect();
    keys.sort();
    keys
}

/// Reserved Postgres schemas always present alongside the user keyspaces.
const RESERVED_NAMESPACES: [&str; 2] = ["pg_catalog", "information_schema"];

/// `pg_catalog.pg_namespace` — one row per keyspace plus the reserved schemas.
///
/// Columns: `oid` (synthetic namespace OID, as `Int`), `nspname` (schema name).
pub fn pg_namespace(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Oid),
        Column::new("nspname", ColumnType::Text),
    ]);

    let mut rows: Vec<Row> =
        Vec::with_capacity(RESERVED_NAMESPACES.len() + snapshot.keyspaces.len());
    for ns in RESERVED_NAMESPACES {
        rows.push(Row::new(vec![
            oid_val(namespace_oid(ns)),
            Value::Text(ns.to_string().into()),
        ]));
    }
    for ks in sorted_keyspaces(&snapshot) {
        rows.push(Row::new(vec![
            oid_val(namespace_oid(&ks)),
            Value::Text(ks.into()),
        ]));
    }

    InMemoryTable::new(rel_schema, rows)
}

/// `pg_catalog.pg_class` — one row per table, plus one per primary-key index.
///
/// Columns: `oid` (relation/index OID), `relname` (table or index name),
/// `relnamespace` (owning keyspace's namespace OID), `relkind` (`'r'` ordinary
/// table, `'i'` index).
///
/// A table with a primary key gets an `'i'` row for the index that backs it,
/// named `primary_key_index_name` (`<table>_pkey`) — the row a client joins to
/// through `pg_index.indexrelid` to name the key. A table whose only key is the
/// synthesized `_sys_ck_` gets none: there is no primary-key index to point at.
pub fn pg_class(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Oid),
        Column::new("relname", ColumnType::Text),
        Column::new("relnamespace", ColumnType::Oid),
        Column::new("relkind", ColumnType::Text),
    ]);

    let tables = sorted_tables(&snapshot);
    let mut rows: Vec<Row> = Vec::with_capacity(tables.len());
    for (ks, table) in tables {
        // The index name is derived from the table name BEFORE the table name is
        // moved into its row, so neither is cloned.
        let index_name = snapshot
            .tables
            .get(&(ks.clone(), table.clone()))
            .is_some_and(|meta| !crate::pg_key::of(meta).is_empty())
            .then(|| primary_key_index_name(&table));
        rows.push(Row::new(vec![
            oid_val(relation_oid(&ks, &table)),
            Value::Text(table.into()),
            oid_val(namespace_oid(&ks)),
            Value::Text("r".to_string().into()),
        ]));
        if let Some(index_name) = index_name {
            rows.push(Row::new(vec![
                oid_val(index_oid(&ks, &index_name)),
                Value::Text(index_name.into()),
                oid_val(namespace_oid(&ks)),
                Value::Text("i".to_string().into()),
            ]));
        }
    }

    InMemoryTable::new(rel_schema, rows)
}

/// `pg_catalog.pg_attribute` — one row per column per table.
///
/// Columns: `attrelid` (owning relation OID), `attname` (column name),
/// `atttypid` (Postgres type OID via [`crate::pg_types`]), `attnum` (1-based
/// ordinal in the table's column order).
///
/// # Errors
///
/// A column whose stored CQL type does not resolve is a [`PgTypeError`]; the
/// projection never advertises an unresolvable type as `text`.
pub fn pg_attribute(schema: &Schema) -> Result<InMemoryTable, PgTypeError> {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("attrelid", ColumnType::Oid),
        Column::new("attname", ColumnType::Text),
        Column::new("atttypid", ColumnType::Oid),
        Column::new("attnum", ColumnType::Int),
        Column::new("attisdropped", ColumnType::Bool),
    ]);

    let mut rows: Vec<Row> = Vec::new();
    for (ks, table) in sorted_tables(&snapshot) {
        let Some(meta) = snapshot.tables.get(&(ks.clone(), table.clone())) else {
            continue;
        };
        let relid = relation_oid(&ks, &table);
        // IndexMap preserves the table's declared column order; attnum comes from
        // `column_attnums`, the one place the number is decided (1-based for a table
        // column, negative for a reserved `_sys_` SYSTEM column — see that function).
        for (col, (_, attnum)) in meta.columns.values().zip(column_attnums(meta)) {
            let mut pg = pg_type_of_column(&col.column_type, &ks, schema)?;
            // An enum-typed column is stored as text but is typed by its enum.
            if let Some((type_ks, type_name)) = crate::pg_enum::column_enum(meta, &col.name) {
                pg.oid = crate::pg_enum::enum_oid(type_ks, type_name);
            }
            rows.push(attribute_row(relid, col, attnum, pg));
        }
    }

    Ok(InMemoryTable::new(rel_schema, rows))
}

/// Build a single `pg_attribute` row for `col` at 0-based `idx`.
fn attribute_row(relid: u32, col: &ColumnMetadata, attnum: i64, pg: PgType) -> Row {
    Row::new(vec![
        oid_val(relid),
        Value::Text(col.name.clone().into()),
        oid_val(pg.oid),
        Value::Int(attnum),
        // A dropped column leaves the schema entirely; none is ever reported.
        Value::Bool(false),
    ])
}

/// `pg_catalog.pg_type` — every built-in type the front end carries, plus any
/// other type a column uses.
///
/// Columns: `oid`, `typname`, `typnamespace` (pg_catalog), `typlen`, `typtype`
/// (`b` base), `typsend`/`typreceive`/`typoutput`/`typinput` (PostgreSQL's own
/// I/O function names, which drivers such as Postgrex match to pick a codec),
/// `typelem` (an array's element type, else 0), and `typrelid`/`typbasetype`
/// (always 0: no composite or domain types exist).
///
/// # Errors
///
/// As [`pg_attribute`]: an unresolvable column type is a [`PgTypeError`], and so
/// is a type with no known I/O function names (never guessed).
pub fn pg_type(schema: &Schema) -> Result<InMemoryTable, PgTypeError> {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Oid),
        Column::new("typname", ColumnType::Text),
        Column::new("typnamespace", ColumnType::Oid),
        Column::new("typlen", ColumnType::Int),
        Column::new("typtype", ColumnType::Text),
        Column::new("typsend", ColumnType::Text),
        Column::new("typreceive", ColumnType::Text),
        Column::new("typoutput", ColumnType::Text),
        Column::new("typinput", ColumnType::Text),
        Column::new("typelem", ColumnType::Oid),
        Column::new("typrelid", ColumnType::Oid),
        Column::new("typbasetype", ColumnType::Oid),
    ]);

    let mut types: Vec<(u32, &'static str, i16)> = crate::pg_types::ALL_PG_TYPES
        .iter()
        .chain(crate::pg_types::ARRAY_PG_TYPES.iter())
        .map(|p| (p.oid, p.typname, p.typlen))
        .collect();
    for ((ks, name), table) in &snapshot.tables {
        // ferrosa's own `_sys_` tables (the enum metadata table) are not projected.
        if is_reserved_column_name(name) {
            continue;
        }
        for col in table.columns.values() {
            let p = pg_type_of_column(&col.column_type, ks, schema)?;
            types.push((p.oid, p.typname, p.typlen));
        }
    }
    types.sort_unstable_by_key(|t| t.0);
    types.dedup_by_key(|t| t.0);

    let mut rows: Vec<Row> = Vec::with_capacity(types.len());
    for (oid, typname, typlen) in types {
        let io = io_functions(typname)
            .ok_or_else(|| PgTypeError::UnknownPgTypeName(typname.to_string()))?;
        let mut cells = Vec::with_capacity(12);
        cells.push(oid_val(oid));
        cells.push(Value::Text(typname.into()));
        cells.push(oid_val(PG_CATALOG_NAMESPACE_OID));
        cells.push(Value::Int(i64::from(typlen)));
        cells.push(Value::Text("b".into()));
        for f in io {
            cells.push(Value::Text(f.into()));
        }
        cells.push(oid_val(array_element_oid(typname).unwrap_or(0)));
        cells.push(oid_val(0));
        cells.push(oid_val(0));
        rows.push(Row::new(cells));
    }
    // User enum types and their array types (`pg_enum::enum_type_rows`), in the same
    // columns; an enum has typlen 4 and its array -1, as in PostgreSQL.
    for row in crate::pg_enum::enum_type_rows(&snapshot) {
        let typlen = if row.typtype == "e" { 4 } else { -1 };
        rows.push(Row::new(vec![
            oid_val(row.oid),
            Value::Text(row.typname.into()),
            oid_val(row.typnamespace),
            Value::Int(typlen),
            Value::Text(row.typtype.into()),
            Value::Text(row.typsend.into()),
            Value::Text(row.typreceive.into()),
            Value::Text(row.typoutput.into()),
            Value::Text(row.typinput.into()),
            oid_val(row.typelem),
            oid_val(row.typrelid),
            oid_val(row.typbasetype),
        ]));
    }

    Ok(InMemoryTable::new(rel_schema, rows))
}

/// `oid` (OID 26).
const PG_OID_OID: u32 = 26;
/// `pg_catalog`'s namespace OID in PostgreSQL.
const PG_CATALOG_NAMESPACE_OID: u32 = 11;

/// PostgreSQL's `typsend`, `typreceive`, `typoutput`, `typinput` for a type.
fn io_functions(typname: &str) -> Option<[String; 4]> {
    if typname.starts_with('_') {
        return Some(["array_send", "array_recv", "array_out", "array_in"].map(String::from));
    }
    // Types whose functions are `<name>send`, `<name>recv`, `<name>out`, `<name>in`.
    let plain = matches!(
        typname,
        "int2"
            | "int4"
            | "int8"
            | "text"
            | "bool"
            | "float4"
            | "float8"
            | "bytea"
            | "oid"
            | "varchar"
            | "bpchar"
            | "name"
            | "char"
    );
    // Types whose functions are `<name>_send`, `<name>_recv`, ...
    let underscored = matches!(
        typname,
        "uuid"
            | "timestamp"
            | "timestamptz"
            | "date"
            | "time"
            | "inet"
            | "numeric"
            | "jsonb"
            | "json"
            | "jsonpath"
            | "interval"
    );
    let sep = match (plain, underscored) {
        (true, _) => "",
        (_, true) => "_",
        _ => return None,
    };
    Some([
        format!("{typname}{sep}send"),
        format!("{typname}{sep}recv"),
        format!("{typname}{sep}out"),
        format!("{typname}{sep}in"),
    ])
}

/// The element type OID of a built-in array type.
fn array_element_oid(typname: &str) -> Option<u32> {
    let elem = typname.strip_prefix('_')?;
    if elem == "oid" {
        return Some(PG_OID_OID);
    }
    crate::pg_types::ALL_PG_TYPES
        .iter()
        .find(|p| p.typname == elem)
        .map(|p| p.oid)
}

/// `pg_catalog.pg_index` — one row per index.
///
/// Columns: `indexrelid` (the index OID), `indrelid` (the indexed table OID),
/// `indisprimary` (a real bool), and `indkey` (the key columns' attnums, in key
/// order).
///
/// Only a table's **primary-key** index is projected — the one psql's describe
/// joins on (`indisprimary`) and the only one this front end builds
/// (`primary_key_index_name`). The key comes from [`crate::pg_key::of`], which
/// returns the **declared** key and nothing for a table whose only key is the
/// synthesized `_sys_ck_`; such a table therefore gets no row, and a client cannot
/// be shown a primary key the user never declared.
///
/// A table with no key also gets no row. A table created through CQL derives a key
/// from its storage key (`pg_key::of`), and is reported like any declared key.
pub fn pg_index(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("indexrelid", ColumnType::Oid),
        Column::new("indrelid", ColumnType::Oid),
        Column::new("indisprimary", ColumnType::Bool),
        Column::new("indkey", ColumnType::TextArray),
    ]);

    let mut rows: Vec<Row> = Vec::new();
    for (ks, table) in sorted_tables(&snapshot) {
        let Some(meta) = snapshot.tables.get(&(ks.clone(), table.clone())) else {
            continue;
        };
        let Some(attnums) = primary_key_attnums(&ks, &table, meta) else {
            continue;
        };
        let index_name = primary_key_index_name(&table);
        rows.push(Row::new(vec![
            oid_val(index_oid(&ks, &index_name)),
            oid_val(relation_oid(&ks, &table)),
            Value::Bool(true),
            attnum_vector(&attnums),
        ]));
    }

    InMemoryTable::new(rel_schema, rows)
}

/// `pg_catalog.pg_constraint` — one row per primary key.
///
/// Columns: `oid` (constraint OID), `conname` (`<table>_pkey`), `contype` (`'p'`),
/// `conrelid` (the table's OID), and `conkey` (the key columns' attnums, in key
/// order).
///
/// As with [`pg_index`], the key is [`crate::pg_key::of`], so a table whose only key
/// is the synthesized `_sys_ck_` gets no row: it has no primary key to constrain.
pub fn pg_constraint(schema: &Schema) -> InMemoryTable {
    let snapshot = schema.snapshot();
    let rel_schema = RelSchema::new(vec![
        Column::new("oid", ColumnType::Oid),
        Column::new("conname", ColumnType::Text),
        Column::new("contype", ColumnType::Text),
        Column::new("conrelid", ColumnType::Oid),
        Column::new("conkey", ColumnType::TextArray),
    ]);

    let mut rows: Vec<Row> = Vec::new();
    for (ks, table) in sorted_tables(&snapshot) {
        let Some(meta) = snapshot.tables.get(&(ks.clone(), table.clone())) else {
            continue;
        };
        let Some(attnums) = primary_key_attnums(&ks, &table, meta) else {
            continue;
        };
        rows.push(Row::new(vec![
            oid_val(primary_key_constraint_oid(&ks, &table)),
            Value::Text(primary_key_index_name(&table).into()),
            Value::Text("p".to_string().into()),
            oid_val(relation_oid(&ks, &table)),
            attnum_vector(&attnums),
        ]));
    }

    InMemoryTable::new(rel_schema, rows)
}

/// The attnums of `keyspace.table`'s primary key, or `None` when it has no key that
/// `pg_index`/`pg_constraint` should report.
///
/// `None` in two cases, both "report nothing rather than a wrong row":
///
/// - [`crate::pg_key::of`] is empty — the table has no key, or its only key is the
///   synthesized `_sys_ck_`, which is ferrosa's own and not a Postgres key;
/// - the key names a column `meta` no longer has, so no correct attnum exists.
///
/// The second is logged, not silently dropped: it is unreachable while `DROP COLUMN`
/// refuses key columns, and if that ever changes the log is the only trace.
fn primary_key_attnums(keyspace: &str, table: &str, meta: &TableMetadata) -> Option<Vec<i64>> {
    let key = crate::pg_key::of(meta);
    if key.is_empty() {
        return None;
    }
    match attnums_of(meta, &key) {
        Some(attnums) => Some(attnums),
        None => {
            tracing::warn!(
                keyspace = %keyspace,
                table = %table,
                "primary key names a column absent from the table; omitting the catalog row"
            );
            None
        }
    }
}

/// The `pg_catalog` relations this front end serves (projected or streamed).
pub(crate) const CATALOG_RELATIONS: [&str; 9] = [
    "pg_namespace",
    "pg_class",
    "pg_attribute",
    "pg_type",
    "pg_index",
    "pg_constraint",
    "pg_range",
    "pg_description",
    "pg_enum",
];

/// Parse a statement off the wire and apply PostgreSQL's implicit search path:
/// `pg_catalog` is searched before every other schema, so an unqualified
/// `pg_type` IS `pg_catalog.pg_type` (Postgrex's type bootstrap relies on it).
/// A CTE of the same name still shadows the catalog relation, as in PostgreSQL.
pub(crate) fn parse_pg(sql: &str) -> Result<ferrosa_sql::Statement, ferrosa_sql::ParseError> {
    let mut stmt = ferrosa_sql::parse_statement(sql)?;
    if let ferrosa_sql::Statement::Select(select) = &mut stmt {
        rewrite_obj_description(select)?;
        qualify_catalog_names(select, &[]);
        refuse_unknown_casts(select)?;
    }
    Ok(stmt)
}

/// `obj_description(oid, 'catalog')` is a lookup in `pg_description`; rewrite it
/// to that scalar subquery so the engine needs no catalog function. Ecto checks
/// Oban's migration version with it. A catalog other than the ones a comment
/// can be on here is refused by name.
fn rewrite_obj_description(
    select: &mut ferrosa_sql::SelectStmt,
) -> Result<(), ferrosa_sql::ParseError> {
    walk_scalars(select, &mut |e| rewrite_obj_description_in(e))
}

fn rewrite_obj_description_in(
    e: &mut ferrosa_sql::ScalarExpr,
) -> Result<(), ferrosa_sql::ParseError> {
    use ferrosa_sql::ScalarExpr;
    e.try_for_each_child_mut(&mut rewrite_obj_description_in, &mut |q| {
        rewrite_obj_description(q)
    })?;
    let ScalarExpr::Func { name, args } = e else {
        return Ok(());
    };
    if name.strip_prefix("pg_catalog.").unwrap_or(name) != "obj_description" {
        return Ok(());
    }
    let classoid = match args.get(1) {
        None => None,
        Some(ScalarExpr::Literal(Value::Text(c))) => Some(match c.as_str() {
            "pg_class" => 1259,
            "pg_type" => 1247,
            "pg_namespace" => 2615,
            _ => {
                return Err(ferrosa_sql::ParseError::UnsupportedSelectExpr(
                    "obj_description for that catalog",
                ))
            }
        }),
        Some(_) => {
            return Err(ferrosa_sql::ParseError::UnsupportedSelectExpr(
                "obj_description with a non-literal catalog name",
            ))
        }
    };
    if args.is_empty() || args.len() > 2 {
        return Err(ferrosa_sql::ParseError::UnsupportedSelectExpr(
            "obj_description takes one or two arguments",
        ));
    }
    let object = args.swap_remove(0);
    *e = ScalarExpr::Subquery(Box::new(description_lookup(object, classoid)));
    Ok(())
}

/// `(SELECT d.description FROM pg_catalog.pg_description AS d WHERE d.objoid = <object>
/// [AND d.classoid = <classoid>] AND d.objsubid = 0)`.
fn description_lookup(
    object: ferrosa_sql::ScalarExpr,
    classoid: Option<i64>,
) -> ferrosa_sql::SelectStmt {
    use ferrosa_sql::{BinOp, CmpOp, ScalarExpr};
    let col = |name: &str| {
        ScalarExpr::Column(ferrosa_sql::ast::ColumnRef {
            qualifier: Some("d".into()),
            name: name.into(),
        })
    };
    let eq = |l: ScalarExpr, r: ScalarExpr| ScalarExpr::Binary {
        op: BinOp::Cmp(CmpOp::Eq),
        left: Box::new(l),
        right: Box::new(r),
    };
    let and = |l: ScalarExpr, r: ScalarExpr| ScalarExpr::Binary {
        op: BinOp::And,
        left: Box::new(l),
        right: Box::new(r),
    };
    let mut cond = eq(col("objoid"), object);
    if let Some(c) = classoid {
        cond = and(
            cond,
            eq(col("classoid"), ScalarExpr::Literal(Value::Int(c))),
        );
    }
    cond = and(
        cond,
        eq(col("objsubid"), ScalarExpr::Literal(Value::Int(0))),
    );
    ferrosa_sql::SelectStmt {
        ctes: Vec::new(),
        distinct: false,
        projection: ferrosa_sql::Projection::Items(vec![ferrosa_sql::SelectItem::Column {
            col: ferrosa_sql::ast::ColumnRef {
                qualifier: Some("d".into()),
                name: "description".into(),
            },
            alias: None,
        }]),
        from: ferrosa_sql::ast::TableRef {
            schema: Some("pg_catalog".into()),
            table: "pg_description".into(),
            alias: Some("d".into()),
        },
        join: None,
        joins: Vec::new(),
        filter: Some(ferrosa_sql::Expr::Scalar(Box::new(cond))),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_param: None,
        offset_param: None,
    }
}

/// Refuse, at Parse time and by the name as written, a cast to a type nothing
/// here implements, as PostgreSQL refuses an unknown type when the statement is
/// prepared rather than when it first runs.
fn refuse_unknown_casts(
    select: &mut ferrosa_sql::SelectStmt,
) -> Result<(), ferrosa_sql::ParseError> {
    let mut check =
        |e: &mut ferrosa_sql::ScalarExpr| -> Result<(), ferrosa_sql::ParseError> { cast_check(e) };
    walk_scalars(select, &mut check)
}

fn cast_check(e: &mut ferrosa_sql::ScalarExpr) -> Result<(), ferrosa_sql::ParseError> {
    if let ferrosa_sql::ScalarExpr::Cast { ty, .. } = e {
        if !ferrosa_sql::cast_type_supported(ty) {
            let mut name = match &ty.schema {
                Some(schema) => format!("{schema}.{}", ty.name),
                None => ty.name.clone(),
            };
            if ty.array {
                name.push_str("[]");
            }
            return Err(ferrosa_sql::ParseError::UnsupportedCast(name));
        }
    }
    e.try_for_each_child_mut(&mut cast_check, &mut |q| {
        walk_scalars(q, &mut |e| cast_check(e))
    })
}

/// Apply `f` to every top-level scalar expression of `select` (select items,
/// join conditions, general conditions, CTE bodies); `f` recurses itself.
fn walk_scalars<E>(
    select: &mut ferrosa_sql::SelectStmt,
    f: &mut dyn FnMut(&mut ferrosa_sql::ScalarExpr) -> Result<(), E>,
) -> Result<(), E> {
    for cte in &mut select.ctes {
        walk_scalars(&mut cte.query, f)?;
    }
    for join in &mut select.joins {
        f(&mut join.on)?;
    }
    if let ferrosa_sql::Projection::Items(items) = &mut select.projection {
        for item in items {
            if let ferrosa_sql::SelectItem::Expr { expr, .. } = item {
                f(expr)?;
            }
        }
    }
    for cond in select.filter.iter_mut().chain(select.having.iter_mut()) {
        walk_condition(cond, f)?;
    }
    Ok(())
}

fn walk_condition<E>(
    expr: &mut ferrosa_sql::Expr,
    f: &mut dyn FnMut(&mut ferrosa_sql::ScalarExpr) -> Result<(), E>,
) -> Result<(), E> {
    use ferrosa_sql::Expr;
    match expr {
        Expr::And(a, b) | Expr::Or(a, b) => {
            walk_condition(a, f)?;
            walk_condition(b, f)
        }
        Expr::Not(a) => walk_condition(a, f),
        Expr::Scalar(e) => f(e),
        Expr::Compare { .. } => Ok(()),
    }
}

fn qualify_catalog_names(select: &mut ferrosa_sql::SelectStmt, outer_ctes: &[String]) {
    let mut ctes: Vec<String> = outer_ctes.to_vec();
    for cte in &mut select.ctes {
        qualify_catalog_names(&mut cte.query, &ctes);
        ctes.push(cte.name.clone());
    }
    let qualify = |t: &mut ferrosa_sql::ast::TableRef| {
        if t.schema.is_none()
            && !ctes.contains(&t.table)
            && CATALOG_RELATIONS.contains(&t.table.as_str())
        {
            t.schema = Some("pg_catalog".to_string());
        }
    };
    qualify(&mut select.from);
    if let Some(join) = &mut select.join {
        qualify(&mut join.table);
    }
    for join in &mut select.joins {
        qualify(&mut join.table);
        qualify_in_scalar(&mut join.on, &ctes);
    }
    if let ferrosa_sql::Projection::Items(items) = &mut select.projection {
        for item in items {
            if let ferrosa_sql::SelectItem::Expr { expr, .. } = item {
                qualify_in_scalar(expr, &ctes);
            }
        }
    }
    for cond in select.filter.iter_mut().chain(select.having.iter_mut()) {
        qualify_in_condition(cond, &ctes);
    }
}

fn qualify_in_condition(expr: &mut ferrosa_sql::Expr, ctes: &[String]) {
    use ferrosa_sql::Expr;
    match expr {
        Expr::And(a, b) | Expr::Or(a, b) => {
            qualify_in_condition(a, ctes);
            qualify_in_condition(b, ctes);
        }
        Expr::Not(a) => qualify_in_condition(a, ctes),
        Expr::Scalar(e) => qualify_in_scalar(e, ctes),
        Expr::Compare { .. } => {}
    }
}

fn qualify_in_scalar(expr: &mut ferrosa_sql::ScalarExpr, ctes: &[String]) {
    let walked: Result<(), std::convert::Infallible> = expr.try_for_each_child_mut(
        &mut |child| {
            qualify_in_scalar(child, ctes);
            Ok(())
        },
        &mut |query| {
            qualify_catalog_names(query, ctes);
            Ok(())
        },
    );
    let Ok(()) = walked;
}

/// All catalog tables, keyed by their `pg_catalog` relation name, so a future
/// query path can resolve `pg_catalog.<name>` to a [`ferrosa_sql::TableProvider`].
///
/// # Errors
///
/// Propagates a [`PgTypeError`] from [`pg_attribute`] / [`pg_type`].
pub fn catalog_tables(schema: &Schema) -> Result<Vec<(String, InMemoryTable)>, PgTypeError> {
    Ok(vec![
        ("pg_namespace".to_string(), pg_namespace(schema)),
        ("pg_class".to_string(), pg_class(schema)),
        ("pg_attribute".to_string(), pg_attribute(schema)?),
        ("pg_type".to_string(), pg_type(schema)?),
        ("pg_index".to_string(), pg_index(schema)),
        ("pg_constraint".to_string(), pg_constraint(schema)),
    ])
}

/// Resolve a relation name — as PostgreSQL's `regclass` input — to the OID this
/// front end projects for it in `pg_catalog.pg_class`.
///
/// `name` may be unqualified (`t`) or schema-qualified (`ks.t`). An unqualified
/// name resolves in `default_schema`, the **same** rule the query path uses for a
/// `FROM` relation (`table_ref.schema.as_deref().unwrap_or(default_schema)`), so
/// the two agree by construction rather than by a second, divergent rule.
/// Double-quoted parts are taken literally (and may contain a `.`); an unquoted
/// part is folded to lower case, as PostgreSQL folds an unquoted identifier.
///
/// A base relation resolves to [`relation_oid`]. A table's primary-key index
/// (`<table>_pkey`) is a relation too — it has its own `pg_class` row with
/// `relkind = 'i'` — so it resolves to [`index_oid`]. Returns `None` when nothing
/// by that name exists; the caller turns that into a `42P01` (undefined_table),
/// never a zero OID that would silently match nothing.
/// `'name'::regnamespace`: the namespace OID of the keyspace (or reserved schema) `name`,
/// or `None` when no such schema exists (PostgreSQL's `3F000`). The integration point for the
/// expression engine's `regnamespace` cast (census statement 9's `'public'::regnamespace::oid`).
// Called by the W1 cast engine once it merges; until then only the tests call it.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn resolve_regnamespace(schema: &Schema, name: &str) -> Option<u32> {
    let name = name.trim_matches('"');
    let known =
        RESERVED_NAMESPACES.contains(&name) || schema.snapshot().keyspaces.contains_key(name);
    known.then(|| namespace_oid(name))
}

pub(crate) fn resolve_regclass(schema: &Schema, default_schema: &str, name: &str) -> Option<u32> {
    let (keyspace, table) = match parse_relation_name(name)?.as_slice() {
        [table] => (default_schema.to_string(), table.clone()),
        [ks, table] => (ks.clone(), table.clone()),
        _ => return None,
    };
    let snapshot = schema.snapshot();
    if snapshot
        .tables
        .contains_key(&(keyspace.clone(), table.clone()))
    {
        return Some(relation_oid(&keyspace, &table));
    }
    let is_index = snapshot.tables.iter().any(|((ks, tbl), meta)| {
        *ks == keyspace
            && table == primary_key_index_name(tbl)
            && !crate::pg_key::of(meta).is_empty()
    });
    is_index.then(|| index_oid(&keyspace, &table))
}

/// Split a `regclass` name into its parts: `ks.tbl`, `"ks"."tbl"`, or `tbl`.
///
/// An unquoted part is trimmed and lowercased; a quoted part is literal, with
/// `""` an escaped quote. `None` for an empty part or trailing junk, so a
/// malformed name is unreachable rather than silently truncated.
fn parse_relation_name(name: &str) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    let mut chars = name.chars().peekable();
    loop {
        let mut part = String::new();
        if chars.peek() == Some(&'"') {
            chars.next(); // opening quote
            let mut closed = false;
            while let Some(c) = chars.next() {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        part.push('"');
                    } else {
                        closed = true;
                        break;
                    }
                } else {
                    part.push(c);
                }
            }
            if !closed {
                return None;
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == '.' {
                    break;
                }
                part.push(c);
                chars.next();
            }
            part = part.trim().to_ascii_lowercase();
        }
        if part.is_empty() {
            return None;
        }
        parts.push(part);
        match chars.next() {
            None => return Some(parts),
            Some('.') => {}
            Some(_) => return None,
        }
    }
}

/// Shared schema fixtures for catalog and `pg_types` tests.
#[cfg(test)]
pub(crate) mod test_support {
    use ferrosa_schema::{
        AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
        RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
    };

    pub(crate) fn test_config() -> SchemaConfig {
        SchemaConfig {
            hasher: PasswordHasher::Bcrypt { cost: 4 },
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(TestAuditSink::new()),
            secrets: Box::new(EnvSecretsProvider),
            mode: DeploymentMode::Development,
        }
    }

    pub(crate) fn empty_schema() -> Schema {
        Schema::new(test_config()).expect("schema bootstraps")
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{empty_schema, test_config};
    use super::*;
    use ferrosa_schema::{
        AuthContext, ClusteringOrder, ColumnKind, ColumnMetadata, KeyspaceMetadata,
        ReplicationParams, Schema, TableMetadata, TableParams,
    };
    use ferrosa_sql::{TableProvider, Value};
    use indexmap::IndexMap;
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    fn superuser() -> AuthContext {
        AuthContext {
            role: "cassandra".to_string(),
            is_superuser: true,
            must_change_password: false,
        }
    }

    fn column(name: &str, kind: ColumnKind, ty: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.to_string(),
            kind,
            position: 0,
            column_type: ty.to_string(),
            clustering_order: ClusteringOrder::None,
            mask: None,
        }
    }

    /// Build a schema with keyspace `ks` and table `tbl(id int PK, name text)`
    /// through the public DDL API (so the projection reads the same metadata a
    /// real CREATE TABLE would produce).
    fn schema_with_ks_tbl() -> Schema {
        schema_with_ks_tbl_extra(&[])
    }

    /// As [`schema_with_ks_tbl`], plus one regular column per `(name, cql_type)`.
    fn schema_with_ks_tbl_extra(extra: &[(&str, &str)]) -> Schema {
        let schema = Schema::new(test_config()).expect("schema bootstraps");
        let auth = superuser();

        schema
            .create_keyspace(
                KeyspaceMetadata {
                    name: "ks".to_string(),
                    durable_writes: true,
                    replication: ReplicationParams {
                        strategy: "SimpleStrategy".to_string(),
                        options: {
                            let mut o = HashMap::new();
                            o.insert("replication_factor".to_string(), "1".to_string());
                            o
                        },
                    },
                },
                &auth,
            )
            .expect("create keyspace ks");

        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            column("id", ColumnKind::PartitionKey, "int"),
        );
        columns.insert(
            "name".to_string(),
            column("name", ColumnKind::Regular, "text"),
        );
        for (name, ty) in extra {
            columns.insert(name.to_string(), column(name, ColumnKind::Regular, ty));
        }

        schema
            .create_table(
                TableMetadata {
                    keyspace: "ks".to_string(),
                    name: "tbl".to_string(),
                    id: Uuid::new_v4(),
                    columns,
                    partition_key: vec!["id".to_string()],
                    clustering_key: vec![],
                    params: TableParams::default(),
                    flags: HashSet::new(),
                    extensions: HashMap::new(),
                    is_system: false,
                },
                &auth,
            )
            .expect("create table tbl");

        schema
    }

    /// Keyspace `ks` metadata for the catalog fixtures.
    fn keyspace_ks() -> KeyspaceMetadata {
        KeyspaceMetadata {
            name: "ks".to_string(),
            durable_writes: true,
            replication: ReplicationParams {
                strategy: "SimpleStrategy".to_string(),
                options: HashMap::from([("replication_factor".to_string(), "1".to_string())]),
            },
        }
    }

    /// A schema holding keyspace `ks` and table `meta`, created through the public
    /// DDL API so the projection reads the same metadata a real DDL would produce.
    fn schema_with_table(meta: TableMetadata) -> Schema {
        let schema = empty_schema();
        let auth = superuser();
        schema
            .create_keyspace(keyspace_ks(), &auth)
            .expect("create keyspace ks");
        schema.create_table(meta, &auth).expect("create table");
        schema
    }

    /// Keyspace `ks` with one table planned from `sql` by the PostgreSQL DDL front
    /// end — the exact metadata a real `CREATE TABLE` produces (synthesized or
    /// declared key included).
    fn schema_with_planned_table(sql: &str) -> Schema {
        let ferrosa_sql::Statement::CreateTable(stmt) =
            ferrosa_sql::parse_statement(sql).expect("must parse")
        else {
            panic!("expected a CREATE TABLE: {sql}");
        };
        schema_with_table(crate::ddl::plan_create_table(&stmt, "ks").expect("must plan"))
    }

    /// Collect a provider's rows into a Vec for assertions.
    /// `'public'::regnamespace` resolves to the same OID `pg_namespace` reports; an unknown
    /// schema resolves to nothing (the caller's `3F000`).
    #[test]
    fn regnamespace_resolves_to_the_pg_namespace_oid() {
        let schema = schema_with_ks_tbl();
        let ns = pg_namespace(&schema);
        let ks_row = rows_of(&ns)
            .into_iter()
            .find(|r| r.0[1] == Value::Text("ks".to_string().into()))
            .expect("ks row");
        assert_eq!(
            resolve_regnamespace(&schema, "ks").map(|o| Value::Int(i64::from(o))),
            Some(ks_row.0[0].clone())
        );
        assert!(resolve_regnamespace(&schema, "pg_catalog").is_some());
        assert_eq!(resolve_regnamespace(&schema, "nope"), None);
    }

    fn rows_of(table: &InMemoryTable) -> Vec<Row> {
        table.scan().collect()
    }

    /// `atttypid` of column `name` in `ks.tbl` as projected by `pg_attribute`.
    fn atttypid(schema: &Schema, name: &str) -> Value {
        let table = pg_attribute(schema).expect("pg_attribute projects");
        rows_of(&table)
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == name))
            .map(|r| r.get(2).clone())
            .expect("attribute present")
    }

    #[test]
    fn pg_attribute_types_come_from_pg_types_and_float_double_agree() {
        let types = [
            ("f", "float"),
            ("d", "double"),
            ("s", "smallint"),
            ("c", "counter"),
            ("m", "frozen<map<text, text>>"),
        ];
        let schema = schema_with_ks_tbl_extra(&types);
        // float and double both advertise float8 (t_cd417149).
        assert_eq!(atttypid(&schema, "f"), Value::Int(701));
        assert_eq!(atttypid(&schema, "d"), Value::Int(701));
        assert_eq!(atttypid(&schema, "s"), Value::Int(23));
        assert_eq!(atttypid(&schema, "c"), Value::Int(20));
        // Composites are the named text-rendered arm, not a fallback.
        assert_eq!(atttypid(&schema, "m"), Value::Int(25));
    }

    #[test]
    fn unresolvable_column_type_fails_the_projection_loudly() {
        let schema = schema_with_ks_tbl_extra(&[("bad", "no_such_udt")]);
        for err in [
            pg_attribute(&schema).expect_err("pg_attribute refuses"),
            pg_type(&schema).expect_err("pg_type refuses"),
            catalog_tables(&schema).expect_err("catalog_tables refuses"),
        ] {
            assert!(err.to_string().contains("no_such_udt"), "{err}");
        }
    }

    #[test]
    fn pg_namespace_contains_keyspace_and_reserved_schemas() {
        let schema = schema_with_ks_tbl();
        let table = pg_namespace(&schema);
        let rows = rows_of(&table);

        let names: Vec<&str> = rows
            .iter()
            .filter_map(|r| match r.get(1) {
                Value::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();

        assert!(names.contains(&"ks"), "namespaces: {names:?}");
        assert!(names.contains(&"pg_catalog"));
        assert!(names.contains(&"information_schema"));

        // The ks row carries its synthetic namespace OID.
        let ks_oid = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "ks"))
            .map(|r| r.get(0).clone())
            .expect("ks namespace row present");
        assert_eq!(ks_oid, oid_val(namespace_oid("ks")));
    }

    #[test]
    fn pg_class_lists_table_with_namespace_oid_and_relkind() {
        let schema = schema_with_ks_tbl();
        let table = pg_class(&schema);
        let rows = rows_of(&table);

        let tbl_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "tbl"))
            .expect("tbl row present in pg_class");

        // relnamespace == ks's namespace oid
        assert_eq!(tbl_row.get(2).clone(), oid_val(namespace_oid("ks")));
        // relkind == 'r'
        assert_eq!(tbl_row.get(3).clone(), Value::Text("r".to_string().into()));
        // oid == relation oid
        assert_eq!(tbl_row.get(0).clone(), oid_val(relation_oid("ks", "tbl")));
    }

    /// A reserved `_sys_` column is a SYSTEM column to a Postgres client: it takes a NEGATIVE
    /// attnum, exactly as `ctid` does, and must not consume a positive ordinal — otherwise
    /// every ordinary column reads one too high to a client that introspects this catalog.
    /// It stays LISTED, which is the whole discovery story for a column `SELECT *` hides.
    #[test]
    fn pg_attribute_numbers_a_reserved_sys_column_negatively() {
        let schema = schema_with_ks_tbl_extra(&[("_sys_ck_", "uuid")]);
        let rows = rows_of(&pg_attribute(&schema).expect("pg_attribute projects"));

        let attnum = |name: &str| -> i64 {
            rows.iter()
                .find(|r| matches!(r.get(1), Value::Text(s) if s == name))
                .map(|r| match r.get(3) {
                    Value::Int(n) => *n,
                    other => panic!("{name}: attnum is not an int: {other:?}"),
                })
                .unwrap_or_else(|| panic!("{name} must be listed in pg_attribute"))
        };

        assert_eq!(
            attnum("_sys_ck_"),
            -1,
            "a reserved column is a system column and takes a negative attnum"
        );
        // The user's own columns are unaffected: still 1-based, still contiguous.
        assert_eq!(attnum("id"), 1);
        assert_eq!(attnum("name"), 2);
    }

    #[test]
    fn pg_attribute_lists_columns_with_ordinals_and_type_oids() {
        let schema = schema_with_ks_tbl();
        let table = pg_attribute(&schema).expect("pg_attribute projects");
        let rows = rows_of(&table);

        let relid = relation_oid("ks", "tbl");

        let id_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "id"))
            .expect("id attribute present");
        assert_eq!(id_row.get(0).clone(), oid_val(relid)); // attrelid
        assert_eq!(id_row.get(2).clone(), Value::Int(23)); // atttypid int4
        assert_eq!(id_row.get(3).clone(), Value::Int(1)); // attnum 1-based

        let name_row = rows
            .iter()
            .find(|r| matches!(r.get(1), Value::Text(s) if s == "name"))
            .expect("name attribute present");
        assert_eq!(name_row.get(0).clone(), oid_val(relid));
        assert_eq!(name_row.get(2).clone(), Value::Int(25)); // atttypid text
        assert_eq!(name_row.get(3).clone(), Value::Int(2)); // attnum 2
    }

    #[test]
    fn pg_type_maps_used_types() {
        let schema = schema_with_ks_tbl();
        let table = pg_type(&schema).expect("pg_type projects");
        let rows = rows_of(&table);

        let pairs: Vec<(i64, &str)> = rows
            .iter()
            .filter_map(|r| match (r.get(0), r.get(1)) {
                (Value::Int(oid), Value::Text(name)) => Some((*oid, name.as_str())),
                _ => None,
            })
            .collect();

        assert!(pairs.contains(&(23, "int4")), "pg_type rows: {pairs:?}");
        assert!(pairs.contains(&(25, "text")), "pg_type rows: {pairs:?}");
    }

    /// Postgrex's type bootstrap reads every type's I/O functions to pick its
    /// codec, so `pg_type` lists every built-in type the front end carries (used
    /// by a column or not), with PostgreSQL's own function names, `typelem` for
    /// arrays, and zero `typrelid`/`typbasetype` (no composites or domains).
    #[test]
    fn pg_type_lists_every_builtin_type_with_its_io_functions() {
        let schema = schema_with_ks_tbl();
        let table = pg_type(&schema).expect("pg_type projects");
        let col = |name: &str| {
            table
                .schema()
                .index_of(name)
                .unwrap_or_else(|| panic!("{name}"))
        };
        let rows = rows_of(&table);
        let row = |oid: i64| {
            rows.iter()
                .find(|r| r.get(col("oid")) == &Value::Int(oid))
                .unwrap_or_else(|| panic!("no pg_type row {oid}"))
                .clone()
        };
        let text = |r: &Row, c: &str| match r.get(col(c)) {
            Value::Text(t) => t.as_str().to_string(),
            other => panic!("{c}: {other:?}"),
        };
        // int8 is used by no column of this schema, and is still listed.
        let int8 = row(20);
        assert_eq!(text(&int8, "typsend"), "int8send");
        assert_eq!(text(&int8, "typreceive"), "int8recv");
        assert_eq!(text(&int8, "typoutput"), "int8out");
        assert_eq!(text(&int8, "typinput"), "int8in");
        assert_eq!(int8.get(col("typelem")), &Value::Int(0));
        assert_eq!(text(&row(2950), "typsend"), "uuid_send");
        assert_eq!(text(&row(3802), "typreceive"), "jsonb_recv");
        assert_eq!(text(&row(1184), "typsend"), "timestamptz_send");
        let int4_array = row(1007);
        assert_eq!(text(&int4_array, "typsend"), "array_send");
        assert_eq!(int4_array.get(col("typelem")), &Value::Int(23));
        // The `_oid` array names an `oid` element type, so `oid` has a row too.
        assert_eq!(row(1028).get(col("typelem")), &Value::Int(26));
        assert_eq!(text(&row(26), "typsend"), "oidsend");
        for r in &rows {
            assert_eq!(r.get(col("typrelid")), &Value::Int(0));
            assert_eq!(r.get(col("typbasetype")), &Value::Int(0));
        }
        let mut oids: Vec<&Value> = rows.iter().map(|r| r.get(col("oid"))).collect();
        let before = oids.len();
        oids.dedup();
        assert_eq!(before, oids.len(), "one row per type");
    }

    /// Every OID column of the catalog is typed `oid` (26), not `int4`: synthetic
    /// relation OIDs reach 2^32 - 1, which an int4 cannot carry.
    #[test]
    fn catalog_oid_columns_are_typed_oid() {
        let schema = schema_with_ks_tbl();
        let tables = catalog_tables(&schema).expect("catalog projects");
        let oid_columns = [
            ("pg_namespace", "oid"),
            ("pg_class", "oid"),
            ("pg_class", "relnamespace"),
            ("pg_attribute", "attrelid"),
            ("pg_attribute", "atttypid"),
            ("pg_type", "oid"),
            ("pg_type", "typnamespace"),
            ("pg_type", "typelem"),
            ("pg_type", "typrelid"),
            ("pg_type", "typbasetype"),
            ("pg_index", "indexrelid"),
            ("pg_index", "indrelid"),
            ("pg_constraint", "oid"),
            ("pg_constraint", "conrelid"),
        ];
        for (table, column) in oid_columns {
            let (_, t) = tables.iter().find(|(n, _)| n == table).expect(table);
            let i = t
                .schema()
                .index_of(column)
                .unwrap_or_else(|| panic!("{table}.{column}"));
            assert_eq!(
                t.schema().columns[i].ty,
                ColumnType::Oid,
                "{table}.{column}"
            );
        }
    }

    #[test]
    fn obj_description_becomes_a_pg_description_lookup() {
        let sql = "SELECT pg_catalog.obj_description(pg_class.oid, 'pg_class') FROM pg_class \
                   LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace \
                   WHERE pg_class.relname = 'oban_jobs' AND pg_namespace.nspname = 'public'";
        let ferrosa_sql::Statement::Select(select) = parse_pg(sql).expect("parses") else {
            panic!("a select");
        };
        assert_eq!(select.from.schema.as_deref(), Some("pg_catalog"));
        let tables: Vec<&str> = select.tables().iter().map(|t| t.table.as_str()).collect();
        assert!(tables.contains(&"pg_description"), "{tables:?}");
        assert!(parse_pg("SELECT obj_description(1, 'pg_proc')").is_err());
    }

    #[test]
    fn pg_attribute_reports_no_dropped_columns() {
        let schema = schema_with_ks_tbl();
        let table = pg_attribute(&schema).expect("pg_attribute projects");
        let col = table
            .schema()
            .index_of("attisdropped")
            .expect("attisdropped column");
        assert!(rows_of(&table)
            .iter()
            .all(|r| r.get(col) == &Value::Bool(false)));
    }

    /// PG-T161a-06: a jsonb column projects as OID 3802 in `pg_attribute` and
    /// puts a `jsonb` row in `pg_type` (D11).
    #[test]
    fn pg_type_and_pg_attribute_report_jsonb_3802() {
        let schema = schema_with_ks_tbl_extra(&[("doc", "jsonb")]);
        assert_eq!(atttypid(&schema, "doc"), Value::Int(3802));
        let rows = rows_of(&pg_type(&schema).expect("pg_type projects"));
        assert!(
            rows.iter()
                .any(|r| r.get(0) == &Value::Int(3802) && r.get(1) == &Value::Text("jsonb".into())),
            "pg_type has a jsonb row: {rows:?}"
        );
    }

    /// A declared composite key must appear in all three projections psql's
    /// describe-table joins — `pg_index` (the index, with its real `indisprimary`
    /// and attnum key), `pg_class` (the index named `<table>_pkey`) and
    /// `pg_constraint` (`contype='p'` with the same key) — carrying the same attnums
    /// `pg_attribute` gives the columns.
    #[test]
    fn a_declared_primary_key_is_reported_in_pg_index_class_and_constraint() {
        let schema = schema_with_planned_table(
            "CREATE TABLE orders (oid int, line int, qty int, PRIMARY KEY (oid, line))",
        );

        let relid = oid_val(relation_oid("ks", "orders"));
        let indexrelid = oid_val(index_oid("ks", "orders_pkey"));
        let key = Value::TextArray(vec![Some("1".to_string()), Some("2".to_string())]);

        // pg_index: one primary-key row, indkey = the key columns' attnums.
        let index_rows = rows_of(&pg_index(&schema));
        assert_eq!(index_rows.len(), 1, "one index row: {index_rows:?}");
        let row = &index_rows[0];
        assert_eq!(row.get(0), &indexrelid, "indexrelid");
        assert_eq!(row.get(1), &relid, "indrelid");
        assert_eq!(row.get(2), &Value::Bool(true), "indisprimary");
        assert_eq!(row.get(3), &key, "indkey is the key attnums, in key order");

        // pg_class: an 'i' row named what psql expects, joined by indexrelid.
        let class_rows = rows_of(&pg_class(&schema));
        let idx_row = class_rows
            .iter()
            .find(|r| r.get(0) == &indexrelid)
            .expect("the pk index has a pg_class row");
        assert_eq!(
            idx_row.get(1),
            &Value::Text("orders_pkey".to_string().into())
        );
        assert_eq!(
            idx_row.get(3),
            &Value::Text("i".to_string().into()),
            "relkind = i"
        );

        // pg_constraint: contype 'p' with the same key attnums.
        let con_rows = rows_of(&pg_constraint(&schema));
        assert_eq!(con_rows.len(), 1, "one constraint row: {con_rows:?}");
        let con = &con_rows[0];
        assert_eq!(
            con.get(1),
            &Value::Text("orders_pkey".to_string().into()),
            "conname"
        );
        assert_eq!(con.get(2), &Value::Text("p".to_string().into()), "contype");
        assert_eq!(con.get(3), &relid, "conrelid");
        assert_eq!(con.get(4), &key, "conkey");
    }

    /// psql must never claim a primary key the user never declared. A table created
    /// with no `PRIMARY KEY` keys on the synthesized `_sys_ck_`, which is ferrosa's
    /// own system column: `pg_key::of` returns nothing, so there is no `pg_index`,
    /// no `pg_constraint` and no `'i'` row in `pg_class`. This is pgbench's
    /// `pgbench_accounts` shape.
    ///
    /// The last third is the negative control: the SAME projection DOES report a key
    /// when one was declared, so the absences above are a decision about the key, not
    /// an empty (or unwired) projection.
    #[test]
    fn a_synthesized_key_reports_no_primary_key() {
        let schema = schema_with_planned_table(
            "CREATE TABLE pgbench_accounts (aid int, bid int, abalance int, filler char(84))",
        );

        assert!(
            rows_of(&pg_index(&schema)).is_empty(),
            "a synthesized key is not a Postgres primary key: no pg_index row"
        );
        assert!(
            rows_of(&pg_constraint(&schema)).is_empty(),
            "and no pg_constraint row"
        );
        // The table itself IS listed, so the absences are not an empty catalog.
        let class_rows = rows_of(&pg_class(&schema));
        assert!(
            class_rows.iter().any(|r| r.get(1)
                == &Value::Text("pgbench_accounts".to_string().into())
                && r.get(3) == &Value::Text("r".to_string().into())),
            "the table is listed as an ordinary relation"
        );
        assert!(
            !class_rows
                .iter()
                .any(|r| r.get(3) == &Value::Text("i".to_string().into())),
            "but no index row is invented for the synthesized key"
        );

        // Negative control: a declared key on the same shape IS reported.
        let declared = schema_with_planned_table("CREATE TABLE t (aid int PRIMARY KEY, bid int)");
        assert_eq!(rows_of(&pg_index(&declared)).len(), 1);
        assert_eq!(rows_of(&pg_constraint(&declared)).len(), 1);
    }

    /// The `ALTER TABLE ... ADD PRIMARY KEY` shape: a table created PK-less keys on
    /// the synthesized `_sys_ck_` (attnum -1), then declares a real key. The declared
    /// column's attnum is its POSITIVE ordinal among real columns, and that — not the
    /// synthetic key, and not the column's index in the key list — is what
    /// `pg_index.indkey` must carry.
    #[test]
    fn a_declared_key_on_a_pk_less_table_uses_the_real_column_attnum() {
        let ferrosa_sql::Statement::CreateTable(stmt) =
            ferrosa_sql::parse_statement("CREATE TABLE t (aid int, bid int)").expect("must parse")
        else {
            panic!("expected a CREATE TABLE");
        };
        let mut meta = crate::ddl::plan_create_table(&stmt, "ks").expect("must plan");
        meta.extensions.insert(
            crate::pg_key::PRIMARY_KEY_EXTENSION.to_string(),
            crate::pg_key::encode(&["bid".to_string()]),
        );
        let schema = schema_with_table(meta);

        let attnum = |name: &str| -> i64 {
            rows_of(&pg_attribute(&schema).expect("pg_attribute projects"))
                .into_iter()
                .find(|r| matches!(r.get(1), Value::Text(s) if s == name))
                .and_then(|r| match r.get(3) {
                    Value::Int(n) => Some(*n),
                    _ => None,
                })
                .unwrap_or_else(|| panic!("{name} missing from pg_attribute"))
        };
        assert_eq!(
            attnum(ferrosa_common::timeuuid::SYNTHETIC_KEY_COLUMN),
            -1,
            "the synthesized key is a system column"
        );
        assert_eq!(attnum("bid"), 2, "declared column keeps its real ordinal");

        let rows = rows_of(&pg_index(&schema));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get(3),
            &Value::TextArray(vec![Some("2".to_string())]),
            "indkey names the declared column's real attnum, not the synthetic key"
        );
    }

    #[test]
    fn catalog_tables_exposes_all_six_relations() {
        let schema = schema_with_ks_tbl();
        let tables = catalog_tables(&schema).expect("catalog projects");
        let names: Vec<&str> = tables.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "pg_namespace",
                "pg_class",
                "pg_attribute",
                "pg_type",
                "pg_index",
                "pg_constraint",
            ]
        );
    }

    #[test]
    fn projections_are_deterministic() {
        // Same schema content projects identical OIDs on every build (pure hash,
        // no counters / insertion-order dependence).
        let a = pg_class(&schema_with_ks_tbl());
        let b = pg_class(&schema_with_ks_tbl());
        assert_eq!(rows_of(&a), rows_of(&b));
    }
}
