//! Read-set / write-set extraction for Accord transactions.
//!
//! Given a sequence of CQL statements accumulated in a transaction block,
//! this module extracts the sets of keys each statement reads or writes.
//! The key sets are keyed by `(keyspace, table)` so that cross-table
//! transactions are supported. Keys are serialized [`Term`] values
//! extracted from WHERE clauses (reads, updates, deletes) and INSERT
//! value lists (writes).

use std::collections::{HashMap, HashSet};

use crate::ast::{
    BatchStatement, ComparisonOp, DeleteStatement, InsertStatement, SelectStatement, Statement,
    Term, UpdateStatement, WhereClause,
};

/// Identifier for a table within a keyspace.
/// The first element is the keyspace name, the second is the table name.
type TableKey = (String, String);

/// A serialized partition key value.
///
/// We serialize [`Term`] values into a canonical byte representation so they
/// can be stored in a `HashSet`. This is intentionally opaque — the bytes are
/// only used for equality comparison and hashing within a single transaction.
type KeyBytes = Vec<u8>;

/// Read-set and write-set extracted from one or more transaction statements.
#[derive(Debug, Clone, Default)]
pub struct TransactionKeySet {
    /// Keys that will be read, grouped by (keyspace, table).
    pub read_set: HashMap<TableKey, HashSet<KeyBytes>>,
    /// Keys that will be written, grouped by (keyspace, table).
    pub write_set: HashMap<TableKey, HashSet<KeyBytes>>,
}

impl TransactionKeySet {
    /// Create an empty key set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge another key set into this one (set union).
    pub fn union(&mut self, other: &TransactionKeySet) {
        for (table, keys) in &other.read_set {
            self.read_set
                .entry(table.clone())
                .or_default()
                .extend(keys.iter().cloned());
        }
        for (table, keys) in &other.write_set {
            self.write_set
                .entry(table.clone())
                .or_default()
                .extend(keys.iter().cloned());
        }
    }

    /// Add a key to the read set for the given table.
    fn add_read(&mut self, table: TableKey, key: KeyBytes) {
        self.read_set.entry(table).or_default().insert(key);
    }

    /// Add a key to the write set for the given table.
    fn add_write(&mut self, table: TableKey, key: KeyBytes) {
        self.write_set.entry(table).or_default().insert(key);
    }

    /// Returns true if both read and write sets are empty.
    pub fn is_empty(&self) -> bool {
        self.read_set.values().all(|s| s.is_empty())
            && self.write_set.values().all(|s| s.is_empty())
    }
}

/// Serialize a [`Term`] into a canonical byte representation for hashing.
///
/// The encoding is deterministic: identical `Term` values produce identical
/// byte sequences. Each variant is prefixed with a single tag byte to
/// prevent collisions between types.
fn serialize_term(term: &Term) -> KeyBytes {
    let mut buf = Vec::new();
    serialize_term_into(term, &mut buf);
    buf
}

fn serialize_term_into(term: &Term, buf: &mut Vec<u8>) {
    match term {
        Term::StringLiteral(s) => {
            buf.push(0x01);
            buf.extend_from_slice(&(s.len() as u32).to_be_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        Term::IntegerLiteral(n) => {
            buf.push(0x02);
            buf.extend_from_slice(&n.to_be_bytes());
        }
        Term::FloatLiteral(f) => {
            buf.push(0x03);
            buf.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Term::UuidLiteral(u) => {
            buf.push(0x04);
            buf.extend_from_slice(u.as_bytes());
        }
        Term::BlobLiteral(b) => {
            buf.push(0x05);
            buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
            buf.extend_from_slice(b);
        }
        Term::BoolLiteral(b) => {
            buf.push(0x06);
            buf.push(u8::from(*b));
        }
        Term::Null => {
            buf.push(0x07);
        }
        Term::BindMarker(name) => {
            buf.push(0x08);
            match name {
                Some(n) => {
                    buf.push(0x01);
                    buf.extend_from_slice(&(n.len() as u32).to_be_bytes());
                    buf.extend_from_slice(n.as_bytes());
                }
                None => {
                    buf.push(0x00);
                }
            }
        }
        Term::InList(terms) | Term::ListLiteral(terms) | Term::SetLiteral(terms) => {
            let tag = match term {
                Term::InList(_) => 0x09,
                Term::ListLiteral(_) => 0x0A,
                _ => 0x0B,
            };
            buf.push(tag);
            buf.extend_from_slice(&(terms.len() as u32).to_be_bytes());
            for t in terms {
                serialize_term_into(t, buf);
            }
        }
        Term::MapLiteral(pairs) => {
            buf.push(0x0C);
            buf.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
            for (k, v) in pairs {
                serialize_term_into(k, buf);
                serialize_term_into(v, buf);
            }
        }
        Term::TupleLiteral(terms) => {
            buf.push(0x0D);
            buf.extend_from_slice(&(terms.len() as u32).to_be_bytes());
            for t in terms {
                serialize_term_into(t, buf);
            }
        }
        Term::FunctionCall {
            keyspace,
            name,
            args,
        } => {
            buf.push(0x0E);
            if let Some(ks) = keyspace {
                buf.push(0x01);
                buf.extend_from_slice(&(ks.len() as u32).to_be_bytes());
                buf.extend_from_slice(ks.as_bytes());
            } else {
                buf.push(0x00);
            }
            buf.extend_from_slice(&(name.len() as u32).to_be_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(args.len() as u32).to_be_bytes());
            for a in args {
                serialize_term_into(a, buf);
            }
        }
    }
}

/// Build a composite key from multiple WHERE clause equality values.
///
/// When a statement has multiple equality predicates (e.g. composite
/// partition key), we combine the column names and values into a single
/// deterministic byte sequence so the composite key is treated as one unit.
fn build_composite_key(clauses: &[(String, KeyBytes)]) -> KeyBytes {
    if clauses.len() == 1 {
        return clauses[0].1.clone();
    }
    let mut sorted = clauses.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    let mut buf = Vec::new();
    buf.push(0xFF); // composite marker
    buf.extend_from_slice(&(sorted.len() as u32).to_be_bytes());
    for (col, key) in &sorted {
        buf.extend_from_slice(&(col.len() as u32).to_be_bytes());
        buf.extend_from_slice(col.as_bytes());
        buf.extend_from_slice(&(key.len() as u32).to_be_bytes());
        buf.extend_from_slice(key);
    }
    buf
}

/// Extract equality key values from WHERE clauses.
///
/// Returns a composite key built from all `column = value` predicates.
/// Returns `None` if there are no equality predicates (e.g. range-only scans).
fn extract_where_keys(where_clauses: &[WhereClause]) -> Option<KeyBytes> {
    let eq_clauses: Vec<(String, KeyBytes)> = where_clauses
        .iter()
        .filter(|wc| wc.op == ComparisonOp::Eq && !wc.token_fn)
        .map(|wc| (wc.column.clone(), serialize_term(&wc.value)))
        .collect();

    if eq_clauses.is_empty() {
        return None;
    }
    Some(build_composite_key(&eq_clauses))
}

/// Resolve the table key, using a default keyspace when the statement
/// doesn't specify one.
fn table_key(keyspace: &Option<String>, table: &str, default_ks: &str) -> TableKey {
    let ks = keyspace.as_deref().unwrap_or(default_ks).to_string();
    (ks, table.to_string())
}

/// Extract read-set and write-set from a single statement.
///
/// The `default_keyspace` is used when the statement does not explicitly
/// qualify the table with a keyspace name.
pub fn extract_keys(stmt: &Statement, default_keyspace: &str) -> TransactionKeySet {
    let mut ks = TransactionKeySet::new();

    match stmt {
        Statement::Select(sel) => {
            extract_select(&mut ks, sel, default_keyspace);
        }
        Statement::Insert(ins) => {
            extract_insert(&mut ks, ins, default_keyspace);
        }
        Statement::Update(upd) => {
            extract_update(&mut ks, upd, default_keyspace);
        }
        Statement::Delete(del) => {
            extract_delete(&mut ks, del, default_keyspace);
        }
        Statement::Batch(batch) => {
            extract_batch(&mut ks, batch, default_keyspace);
        }
        // DDL and other statements don't participate in key extraction.
        _ => {}
    }

    ks
}

fn extract_select(ks: &mut TransactionKeySet, sel: &SelectStatement, default_keyspace: &str) {
    let tk = table_key(&sel.keyspace, &sel.table, default_keyspace);
    if let Some(key) = extract_where_keys(&sel.where_clauses) {
        ks.add_read(tk, key);
    }
}

fn extract_insert(ks: &mut TransactionKeySet, ins: &InsertStatement, default_keyspace: &str) {
    let tk = table_key(&ins.keyspace, &ins.table, default_keyspace);

    // For INSERT, we build a composite key from all (column, value) pairs.
    // At parse time we don't know which columns form the partition key,
    // so we include all explicitly provided columns — the transaction
    // coordinator will narrow this to partition key columns at execution time.
    // However, for key-set extraction we serialize the full column-value pairs
    // as the key identity.
    let pairs: Vec<(String, KeyBytes)> = ins
        .columns
        .iter()
        .zip(ins.values.iter())
        .map(|(col, val)| (col.clone(), serialize_term(val)))
        .collect();

    if !pairs.is_empty() {
        let key = build_composite_key(&pairs);
        ks.add_write(tk, key);
    }
}

fn extract_update(ks: &mut TransactionKeySet, upd: &UpdateStatement, default_keyspace: &str) {
    let tk = table_key(&upd.keyspace, &upd.table, default_keyspace);
    if let Some(key) = extract_where_keys(&upd.where_clauses) {
        ks.add_write(tk, key);
    }
}

fn extract_delete(ks: &mut TransactionKeySet, del: &DeleteStatement, default_keyspace: &str) {
    let tk = table_key(&del.keyspace, &del.table, default_keyspace);
    if let Some(key) = extract_where_keys(&del.where_clauses) {
        ks.add_write(tk, key);
    }
}

fn extract_batch(ks: &mut TransactionKeySet, batch: &BatchStatement, default_keyspace: &str) {
    for stmt in &batch.statements {
        let child = extract_keys(stmt, default_keyspace);
        ks.union(&child);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    /// Helper: create a SELECT with WHERE id = 42
    fn make_select(keyspace: Option<&str>, table: &str, col: &str, val: Term) -> Statement {
        Statement::Select(SelectStatement {
            keyspace: keyspace.map(String::from),
            table: table.to_string(),
            columns: vec![SelectColumn::Star],
            where_clauses: vec![WhereClause {
                column: col.to_string(),
                op: ComparisonOp::Eq,
                value: val,
                token_fn: false,
            }],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
            ann_of: None,
        })
    }

    /// Helper: create an INSERT into (col) VALUES (val)
    fn make_insert(
        keyspace: Option<&str>,
        table: &str,
        cols: Vec<&str>,
        vals: Vec<Term>,
    ) -> Statement {
        Statement::Insert(InsertStatement {
            keyspace: keyspace.map(String::from),
            table: table.to_string(),
            columns: cols.into_iter().map(String::from).collect(),
            values: vals,
            if_not_exists: false,
            using_timestamp: None,
            using_ttl: None,
        })
    }

    /// Helper: create an UPDATE with WHERE clause
    fn make_update(keyspace: Option<&str>, table: &str, col: &str, val: Term) -> Statement {
        Statement::Update(UpdateStatement {
            keyspace: keyspace.map(String::from),
            table: table.to_string(),
            assignments: vec![Assignment::Simple {
                column: "data".to_string(),
                value: Term::StringLiteral("value".to_string()),
            }],
            where_clauses: vec![WhereClause {
                column: col.to_string(),
                op: ComparisonOp::Eq,
                value: val,
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![],
            using_timestamp: None,
            using_ttl: None,
        })
    }

    /// Helper: create a DELETE with WHERE clause
    fn make_delete(keyspace: Option<&str>, table: &str, col: &str, val: Term) -> Statement {
        Statement::Delete(DeleteStatement {
            keyspace: keyspace.map(String::from),
            table: table.to_string(),
            columns: vec![],
            where_clauses: vec![WhereClause {
                column: col.to_string(),
                op: ComparisonOp::Eq,
                value: val,
                token_fn: false,
            }],
            if_exists: false,
            if_conditions: vec![],
            using_timestamp: None,
        })
    }

    #[test]
    fn readset_writeset_extraction() {
        // SELECT reads key; INSERT writes key
        let sel = make_select(None, "users", "id", Term::IntegerLiteral(42));
        let ins = make_insert(
            None,
            "users",
            vec!["id", "name"],
            vec![
                Term::IntegerLiteral(7),
                Term::StringLiteral("alice".to_string()),
            ],
        );

        let sel_keys = extract_keys(&sel, "ks");
        let ins_keys = extract_keys(&ins, "ks");

        // SELECT -> read set has one entry for (ks, users)
        assert_eq!(sel_keys.read_set.len(), 1);
        let read_keys = &sel_keys.read_set[&("ks".to_string(), "users".to_string())];
        assert_eq!(read_keys.len(), 1);
        assert!(sel_keys.write_set.is_empty());

        // INSERT -> write set has one entry for (ks, users)
        assert_eq!(ins_keys.write_set.len(), 1);
        let write_keys = &ins_keys.write_set[&("ks".to_string(), "users".to_string())];
        assert_eq!(write_keys.len(), 1);
        assert!(ins_keys.read_set.is_empty());
    }

    #[test]
    fn readset_writeset_cross_table() {
        // Statements targeting different tables in different keyspaces
        let sel = make_select(Some("ks1"), "users", "id", Term::IntegerLiteral(1));
        let ins = make_insert(
            Some("ks2"),
            "orders",
            vec!["order_id"],
            vec![Term::IntegerLiteral(100)],
        );
        let upd = make_update(Some("ks1"), "accounts", "acc_id", Term::IntegerLiteral(5));

        let mut combined = TransactionKeySet::new();
        combined.union(&extract_keys(&sel, "default"));
        combined.union(&extract_keys(&ins, "default"));
        combined.union(&extract_keys(&upd, "default"));

        // Read set: ks1.users
        assert!(combined
            .read_set
            .contains_key(&("ks1".to_string(), "users".to_string())));
        assert_eq!(
            combined.read_set[&("ks1".to_string(), "users".to_string())].len(),
            1
        );

        // Write set: ks2.orders and ks1.accounts
        assert!(combined
            .write_set
            .contains_key(&("ks2".to_string(), "orders".to_string())));
        assert!(combined
            .write_set
            .contains_key(&("ks1".to_string(), "accounts".to_string())));

        // No cross-contamination: ks1.users not in write set
        assert!(!combined
            .write_set
            .contains_key(&("ks1".to_string(), "users".to_string())));
    }

    #[test]
    fn readset_writeset_overlapping() {
        // Same key appears in both read and write sets
        let key_val = Term::IntegerLiteral(42);
        let sel = make_select(None, "users", "id", key_val.clone());
        let upd = make_update(None, "users", "id", key_val);

        let mut combined = TransactionKeySet::new();
        combined.union(&extract_keys(&sel, "ks"));
        combined.union(&extract_keys(&upd, "ks"));

        let table = ("ks".to_string(), "users".to_string());

        // Key is in BOTH read and write sets
        assert!(combined.read_set.contains_key(&table));
        assert!(combined.write_set.contains_key(&table));
        assert_eq!(combined.read_set[&table].len(), 1);
        assert_eq!(combined.write_set[&table].len(), 1);

        // The serialized key bytes should be equal since both use id = 42
        let read_key = combined.read_set[&table].iter().next().unwrap();
        let write_key = combined.write_set[&table].iter().next().unwrap();
        assert_eq!(read_key, write_key);
    }

    #[test]
    fn readset_writeset_batch_in_txn() {
        // BATCH containing INSERT + DELETE decomposes into individual key sets
        let batch = Statement::Batch(BatchStatement {
            batch_type: BatchType::Logged,
            statements: vec![
                make_insert(
                    None,
                    "users",
                    vec!["id", "name"],
                    vec![
                        Term::IntegerLiteral(1),
                        Term::StringLiteral("bob".to_string()),
                    ],
                ),
                make_delete(None, "users", "id", Term::IntegerLiteral(2)),
                make_insert(
                    Some("ks2"),
                    "orders",
                    vec!["order_id"],
                    vec![Term::IntegerLiteral(99)],
                ),
            ],
            using_timestamp: None,
        });

        let keys = extract_keys(&batch, "ks");

        // Write set should have entries for ks.users and ks2.orders
        let users_table = ("ks".to_string(), "users".to_string());
        let orders_table = ("ks2".to_string(), "orders".to_string());

        assert!(keys.write_set.contains_key(&users_table));
        assert!(keys.write_set.contains_key(&orders_table));

        // ks.users should have 2 distinct keys (INSERT id=1 and DELETE id=2)
        assert_eq!(keys.write_set[&users_table].len(), 2);

        // ks2.orders should have 1 key
        assert_eq!(keys.write_set[&orders_table].len(), 1);

        // No read set entries (INSERT and DELETE are writes)
        assert!(keys.read_set.is_empty());
    }

    #[test]
    fn empty_where_produces_no_keys() {
        // SELECT without WHERE (full scan) produces empty read set
        let sel = Statement::Select(SelectStatement {
            keyspace: None,
            table: "users".to_string(),
            columns: vec![SelectColumn::Star],
            where_clauses: vec![],
            order_by: vec![],
            limit: None,
            allow_filtering: false,
            ann_of: None,
        });

        let keys = extract_keys(&sel, "ks");
        assert!(keys.is_empty());
    }

    #[test]
    fn serialize_term_deterministic() {
        // Same term always produces same bytes
        let t1 = Term::IntegerLiteral(42);
        let t2 = Term::IntegerLiteral(42);
        assert_eq!(serialize_term(&t1), serialize_term(&t2));

        // Different terms produce different bytes
        let t3 = Term::IntegerLiteral(43);
        assert_ne!(serialize_term(&t1), serialize_term(&t3));

        // Different types with same-looking value produce different bytes
        let t4 = Term::StringLiteral("42".to_string());
        assert_ne!(serialize_term(&t1), serialize_term(&t4));
    }

    #[test]
    fn union_merges_correctly() {
        let mut a = TransactionKeySet::new();
        a.add_read(("ks".to_string(), "t1".to_string()), vec![1, 2, 3]);
        a.add_write(("ks".to_string(), "t1".to_string()), vec![4, 5, 6]);

        let mut b = TransactionKeySet::new();
        b.add_read(("ks".to_string(), "t1".to_string()), vec![7, 8, 9]);
        b.add_read(("ks".to_string(), "t2".to_string()), vec![10, 11]);

        a.union(&b);

        // t1 reads: {[1,2,3], [7,8,9]}
        assert_eq!(a.read_set[&("ks".to_string(), "t1".to_string())].len(), 2);
        // t2 reads: {[10,11]}
        assert_eq!(a.read_set[&("ks".to_string(), "t2".to_string())].len(), 1);
        // t1 writes unchanged
        assert_eq!(a.write_set[&("ks".to_string(), "t1".to_string())].len(), 1);
    }

    #[test]
    fn default_keyspace_used_when_not_specified() {
        let sel = make_select(None, "users", "id", Term::IntegerLiteral(1));
        let keys = extract_keys(&sel, "my_ks");

        assert!(keys
            .read_set
            .contains_key(&("my_ks".to_string(), "users".to_string())));
    }

    #[test]
    fn explicit_keyspace_overrides_default() {
        let sel = make_select(Some("explicit_ks"), "users", "id", Term::IntegerLiteral(1));
        let keys = extract_keys(&sel, "default_ks");

        assert!(keys
            .read_set
            .contains_key(&("explicit_ks".to_string(), "users".to_string())));
        assert!(!keys
            .read_set
            .contains_key(&("default_ks".to_string(), "users".to_string())));
    }
}
