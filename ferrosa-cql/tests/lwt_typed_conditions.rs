//! LWT `IF` conditions compare the value the client sent against the stored
//! value AS THE COLUMN'S DECLARED TYPE, over the real CQL front end.
//!
//! Each test boots a standalone node, connects the crate's own CQL client over
//! TCP, seeds one row, and then drives conditional UPDATE/DELETE statements both
//! unprepared (a literal in the query text) and prepared (a value bound to `?`
//! in an EXECUTE frame). Every case asserts the EFFECT, not just the reply:
//!
//! - a condition that holds must report `[applied]=true` AND the row read back
//!   must carry the new value;
//! - a condition that does not hold must report `[applied]=false` AND the row
//!   read back must still carry the original value.
//!
//! The defect these hold down: the IF clause's operand reached the evaluator as
//! an untyped parser term and was converted by a type-blind helper. Integer
//! literals were narrowed to a 32-bit `int` (so any timestamp, or any bigint
//! past `i32`, never matched), strings stayed `text` (so `date`, `time`, `inet`
//! and `ascii` columns never matched), floats stayed `double` (so `decimal` and
//! `float` columns never matched), and blob-carried bound values (`date`,
//! `time`, `varint`, `decimal`) were dropped to "no value". `=` then never held
//! and `!=` ALWAYS held, so a guarded write fired exactly when it must not, and
//! `[applied]=false` was indistinguishable from losing a race.

use std::net::SocketAddr;

use ferrosa_cql::client::{CqlClient, QueryResult};
use ferrosa_cql::server::{CqlServer, ServerConfig};
use ferrosa_cql::types::{encode_value, CqlValue};
use num_bigint::BigInt;

/// CQL consistency ONE.
const CL_ONE: u16 = 0x0001;

/// A standalone node and a connected client. The temp dir lives as long as the
/// node so its data directory is not removed under it.
struct Node {
    client: CqlClient,
    _dir: tempfile::TempDir,
}

async fn start_node() -> Node {
    let dir = tempfile::TempDir::new().expect("temp data dir");
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    let config = ServerConfig {
        bind_addr: "127.0.0.1:0".parse().expect("bind addr"),
        max_connections: 10,
        auth_disabled: true,
        ..ServerConfig::default()
    };
    let addr: SocketAddr = CqlServer::new(config, state)
        .start_background()
        .await
        .expect("start the CQL server");
    let client = CqlClient::connect(addr).await.expect("connect the client");
    Node { client, _dir: dir }
}

impl Node {
    async fn run(&mut self, cql: &str) -> QueryResult {
        self.client
            .query(cql)
            .await
            .unwrap_or_else(|e| panic!("`{cql}` failed: {e:?}"))
    }

    /// Create `lwt_ty.t (k int PRIMARY KEY, c <ty>, v text)` and seed row k=1
    /// with `c = <seed>` and `v = 'orig'`.
    async fn seeded_table(&mut self, ty: &str, seed: &str) {
        self.run(
            "CREATE KEYSPACE IF NOT EXISTS lwt_ty WITH REPLICATION = \
             {'class': 'SimpleStrategy', 'replication_factor': '1'}",
        )
        .await;
        self.run(&format!(
            "CREATE TABLE lwt_ty.t (k int PRIMARY KEY, c {ty}, v text)"
        ))
        .await;
        self.run(&format!(
            "INSERT INTO lwt_ty.t (k, c, v) VALUES (1, {seed}, 'orig')"
        ))
        .await;
        assert_eq!(self.read_v().await.as_deref(), Some("orig"), "seed landed");
    }

    /// As [`Node::seeded_table`], but bind the seed value: a value with no
    /// CQL literal form that this front end accepts (a varint past `i64`).
    async fn seeded_table_bound(&mut self, ty: &str, seed: &CqlValue) {
        self.seeded_table(ty, "null").await;
        let cql = "UPDATE lwt_ty.t SET c = ? WHERE k = 1";
        let stmt = self
            .client
            .prepare(cql)
            .await
            .unwrap_or_else(|e| panic!("PREPARE `{cql}`: {e:?}"));
        self.client
            .execute(&stmt, &[Some(encode_value(seed))], CL_ONE)
            .await
            .unwrap_or_else(|e| panic!("EXECUTE `{cql}` with {seed:?}: {e:?}"));
    }

    /// The value of `v` in row k=1, read back through the same front end.
    async fn read_v(&mut self) -> Option<String> {
        let res = self.run("SELECT v FROM lwt_ty.t WHERE k = 1").await;
        let row = res.rows.first()?;
        let cell = row.columns.first().cloned().flatten()?;
        Some(String::from_utf8(cell).expect("v is utf8 text"))
    }

    async fn reset_v(&mut self) {
        self.run("UPDATE lwt_ty.t SET v = 'orig' WHERE k = 1").await;
        assert_eq!(self.read_v().await.as_deref(), Some("orig"), "reset landed");
    }
}

/// The `[applied]` flag of an LWT reply.
fn applied(res: &QueryResult, what: &str) -> bool {
    assert_eq!(
        res.column_names.first().map(String::as_str),
        Some("[applied]"),
        "{what}: an LWT must reply with the [applied] column, got {:?}",
        res.column_names
    );
    let cell = res.rows[0].columns[0]
        .as_deref()
        .unwrap_or_else(|| panic!("{what}: [applied] must not be null"));
    assert_eq!(cell.len(), 1, "{what}: [applied] is a boolean");
    cell[0] != 0
}

/// Assert the value effect of one conditional write.
async fn assert_effect(node: &mut Node, what: &str, res: &QueryResult, should_apply: bool) {
    let was_applied = applied(res, what);
    let v = node.read_v().await;
    if should_apply {
        assert!(
            was_applied,
            "{what}: condition holds, must be [applied]=true"
        );
        assert_eq!(v.as_deref(), Some("new"), "{what}: applied write must land");
    } else {
        assert!(
            !was_applied,
            "{what}: condition fails, must be [applied]=false"
        );
        assert_eq!(
            v.as_deref(),
            Some("orig"),
            "{what}: refused write must not land"
        );
    }
}

/// Drive `UPDATE ... IF c <cond>` unprepared, once per case.
async fn unprepared_cases(node: &mut Node, cases: &[(&str, bool)]) {
    for (cond, should_apply) in cases {
        node.reset_v().await;
        let cql = format!("UPDATE lwt_ty.t SET v = 'new' WHERE k = 1 IF c {cond}");
        let res = node.run(&cql).await;
        assert_effect(node, &cql, &res, *should_apply).await;
    }
}

/// Drive `UPDATE ... IF c <op> ?` prepared, binding `value`, once per case.
async fn prepared_cases(node: &mut Node, cases: &[(&str, CqlValue, bool)]) {
    for (op, value, should_apply) in cases {
        node.reset_v().await;
        let cql = format!("UPDATE lwt_ty.t SET v = ? WHERE k = ? IF c {op} ?");
        let stmt = node
            .client
            .prepare(&cql)
            .await
            .unwrap_or_else(|e| panic!("PREPARE `{cql}`: {e:?}"));
        let values = vec![
            Some(encode_value(&CqlValue::Text("new".into()))),
            Some(encode_value(&CqlValue::Int(1))),
            Some(encode_value(value)),
        ];
        let res = node
            .client
            .execute(&stmt, &values, CL_ONE)
            .await
            .unwrap_or_else(|e| panic!("EXECUTE `{cql}` with {value:?}: {e:?}"));
        let what = format!("{cql} bound {value:?}");
        assert_effect(node, &what, &res, *should_apply).await;
    }
}

// ---------------------------------------------------------------------------
// timestamp
// ---------------------------------------------------------------------------

/// 2023-11-14T22:13:20.123Z: past `i32`, so a 32-bit narrowing cannot hold it.
const TS: i64 = 1_700_000_000_123;

#[tokio::test]
async fn timestamp_condition_unprepared_compares_as_timestamp() {
    let mut node = start_node().await;
    node.seeded_table("timestamp", &TS.to_string()).await;
    unprepared_cases(
        &mut node,
        &[
            ("= 1700000000124", false),
            ("!= 1700000000123", false),
            ("< 1700000000123", false),
            ("= 1700000000123", true),
            ("<= 1700000000123", true),
            ("= '2023-11-14T22:13:20.123Z'", true),
            ("> '2023-11-14T22:13:20.122Z'", true),
            ("IN (1, 1700000000123)", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn timestamp_condition_prepared_compares_as_timestamp() {
    let mut node = start_node().await;
    node.seeded_table("timestamp", &TS.to_string()).await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Timestamp(TS + 1), false),
            ("!=", CqlValue::Timestamp(TS), false),
            (">", CqlValue::Timestamp(TS), false),
            ("=", CqlValue::Timestamp(TS), true),
            ("<=", CqlValue::Timestamp(TS), true),
            ("<", CqlValue::Timestamp(TS + 1), true),
        ],
    )
    .await;
}

/// The conditional DELETE path evaluates the same IF clause.
#[tokio::test]
async fn timestamp_condition_on_delete_compares_as_timestamp() {
    let mut node = start_node().await;
    node.seeded_table("timestamp", &TS.to_string()).await;

    let miss = "DELETE FROM lwt_ty.t WHERE k = 1 IF c = 1700000000124";
    let res = node.run(miss).await;
    assert!(!applied(&res, miss), "{miss}: must be [applied]=false");
    assert_eq!(
        node.read_v().await.as_deref(),
        Some("orig"),
        "{miss}: row kept"
    );

    let hit = "DELETE FROM lwt_ty.t WHERE k = 1 IF c = 1700000000123";
    let res = node.run(hit).await;
    assert!(applied(&res, hit), "{hit}: must be [applied]=true");
    assert_eq!(node.read_v().await, None, "{hit}: row must be deleted");
}

// ---------------------------------------------------------------------------
// bigint / varint
// ---------------------------------------------------------------------------

/// 5_000_000_000 narrowed to `i32` is 705_032_704.
const BIG: i64 = 5_000_000_000;

#[tokio::test]
async fn bigint_condition_unprepared_compares_past_i32() {
    let mut node = start_node().await;
    node.seeded_table("bigint", &BIG.to_string()).await;
    unprepared_cases(
        &mut node,
        &[
            ("= 705032704", false),
            ("!= 5000000000", false),
            ("= 5000000000", true),
            ("> 4999999999", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn bigint_condition_prepared_compares_past_i32() {
    let mut node = start_node().await;
    node.seeded_table("bigint", &BIG.to_string()).await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Bigint(705_032_704), false),
            ("!=", CqlValue::Bigint(BIG), false),
            ("=", CqlValue::Bigint(BIG), true),
            (">=", CqlValue::Bigint(BIG), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn varint_condition_unprepared_compares_past_i32() {
    let mut node = start_node().await;
    node.seeded_table("varint", &BIG.to_string()).await;
    unprepared_cases(
        &mut node,
        &[
            ("= 705032704", false),
            ("!= 5000000000", false),
            ("= 5000000000", true),
            ("< 5000000001", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn varint_condition_prepared_compares_by_value() {
    let huge: BigInt = "123456789012345678901234567890".parse().expect("bigint");
    let mut node = start_node().await;
    node.seeded_table_bound("varint", &CqlValue::Varint(huge.clone()))
        .await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Varint(huge.clone() + 1), false),
            ("!=", CqlValue::Varint(huge.clone()), false),
            ("=", CqlValue::Varint(huge.clone()), true),
            ("<", CqlValue::Varint(huge + 1), true),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// decimal / double / float
// ---------------------------------------------------------------------------

#[tokio::test]
async fn decimal_condition_unprepared_compares_by_value() {
    let mut node = start_node().await;
    node.seeded_table("decimal", "1.50").await;
    unprepared_cases(
        &mut node,
        &[
            ("= 1.25", false),
            ("!= 1.5", false),
            ("= 1.5", true),
            ("> 1.25", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn decimal_condition_prepared_compares_by_value() {
    let mut node = start_node().await;
    node.seeded_table("decimal", "1.50").await;
    let dec = |unscaled: i64, scale: i32| CqlValue::Decimal {
        scale,
        unscaled: BigInt::from(unscaled),
    };
    prepared_cases(
        &mut node,
        &[
            ("=", dec(125, 2), false),
            ("!=", dec(15, 1), false),
            // 1.5 at a different scale is the same number.
            ("=", dec(15, 1), true),
            ("=", dec(150, 2), true),
            ("<", dec(2, 0), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn double_condition_unprepared_accepts_an_integer_literal() {
    let mut node = start_node().await;
    node.seeded_table("double", "2.0").await;
    unprepared_cases(
        &mut node,
        &[
            ("= 2.5", false),
            ("!= 2", false),
            ("= 2", true),
            ("< 2.5", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn float_condition_unprepared_compares_as_float() {
    let mut node = start_node().await;
    node.seeded_table("float", "1.5").await;
    unprepared_cases(
        &mut node,
        &[
            ("= 1.25", false),
            ("!= 1.5", false),
            ("= 1.5", true),
            ("> 1.25", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn float_condition_prepared_compares_as_float() {
    let mut node = start_node().await;
    node.seeded_table("float", "1.5").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Float(1.25f32.to_bits()), false),
            ("=", CqlValue::Float(1.5f32.to_bits()), true),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// date / time
// ---------------------------------------------------------------------------

/// 2024-01-15 in CQL `date` encoding (days since epoch + 2^31).
const DATE_2024_01_15: u32 = 19_737 + (1u32 << 31);
/// 10:30:00 as nanoseconds since midnight.
const TIME_10_30: i64 = 37_800 * 1_000_000_000;

#[tokio::test]
async fn date_condition_unprepared_compares_as_date() {
    let mut node = start_node().await;
    node.seeded_table("date", "'2024-01-15'").await;
    unprepared_cases(
        &mut node,
        &[
            ("= '2024-01-16'", false),
            ("!= '2024-01-15'", false),
            ("= '2024-01-15'", true),
            ("< '2024-01-16'", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn date_condition_prepared_compares_as_date() {
    let mut node = start_node().await;
    node.seeded_table("date", "'2024-01-15'").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Date(DATE_2024_01_15 + 1), false),
            ("!=", CqlValue::Date(DATE_2024_01_15), false),
            ("=", CqlValue::Date(DATE_2024_01_15), true),
            ("<", CqlValue::Date(DATE_2024_01_15 + 1), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn time_condition_unprepared_compares_as_time() {
    let mut node = start_node().await;
    node.seeded_table("time", "'10:30:00'").await;
    unprepared_cases(
        &mut node,
        &[
            ("> '10:30:00'", false),
            ("!= '10:30:00'", false),
            ("= '10:30:00'", true),
            (">= '10:30:00'", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn time_condition_prepared_compares_as_time() {
    let mut node = start_node().await;
    node.seeded_table("time", "'10:30:00'").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Time(TIME_10_30 + 1), false),
            ("!=", CqlValue::Time(TIME_10_30), false),
            ("=", CqlValue::Time(TIME_10_30), true),
            ("<", CqlValue::Time(TIME_10_30 + 1), true),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// uuid / timeuuid
// ---------------------------------------------------------------------------

const UUID_A: &str = "6ba7b810-9dad-41d1-80b4-00c04fd430c8";
const UUID_B: &str = "6ba7b811-9dad-41d1-80b4-00c04fd430c8";
/// Version-1 uuids whose BYTE order is the reverse of their TIME order:
/// `T_EARLY` has time_low 0xffffffff and time_mid 0; `T_LATE` has time_low 0
/// and time_mid 1, so it is 1 tick later yet sorts first bytewise.
const T_EARLY: &str = "ffffffff-0000-1000-8000-000000000000";
const T_LATE: &str = "00000000-0001-1000-8000-000000000000";

fn uuid(s: &str) -> uuid::Uuid {
    s.parse().expect("uuid literal")
}

/// Guard: uuid equality matched before the fix; kept as the uuid mirror.
#[tokio::test]
async fn uuid_condition_unprepared_compares_as_uuid() {
    let mut node = start_node().await;
    node.seeded_table("uuid", UUID_A).await;
    unprepared_cases(
        &mut node,
        &[
            (&format!("= {UUID_B}"), false),
            (&format!("!= {UUID_A}"), false),
            (&format!("= {UUID_A}"), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn timeuuid_condition_unprepared_compares_as_timeuuid() {
    let mut node = start_node().await;
    node.seeded_table("timeuuid", T_EARLY).await;
    unprepared_cases(
        &mut node,
        &[
            (&format!("= {T_LATE}"), false),
            (&format!("!= {T_EARLY}"), false),
            (&format!("> {T_LATE}"), false),
            (&format!("= {T_EARLY}"), true),
            // Ordered by the embedded time, not by bytes.
            (&format!("< {T_LATE}"), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn timeuuid_condition_prepared_compares_as_timeuuid() {
    let mut node = start_node().await;
    node.seeded_table("timeuuid", T_EARLY).await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Timeuuid(uuid(T_LATE)), false),
            ("!=", CqlValue::Timeuuid(uuid(T_EARLY)), false),
            ("=", CqlValue::Timeuuid(uuid(T_EARLY)), true),
            ("<", CqlValue::Timeuuid(uuid(T_LATE)), true),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// boolean / text / ascii / blob / inet
// ---------------------------------------------------------------------------

/// Guard: boolean equality matched before the fix; kept as a mirror.
#[tokio::test]
async fn boolean_condition_prepared_compares_as_boolean() {
    let mut node = start_node().await;
    node.seeded_table("boolean", "true").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Boolean(false), false),
            ("!=", CqlValue::Boolean(true), false),
            ("=", CqlValue::Boolean(true), true),
        ],
    )
    .await;
}

/// Guard: text equality matched before the fix; kept as a mirror.
#[tokio::test]
async fn text_condition_prepared_compares_as_text() {
    let mut node = start_node().await;
    node.seeded_table("text", "'abc'").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Text("abd".into()), false),
            ("=", CqlValue::Text("abc".into()), true),
            ("<", CqlValue::Text("abd".into()), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn ascii_condition_unprepared_compares_as_ascii() {
    let mut node = start_node().await;
    node.seeded_table("ascii", "'abc'").await;
    unprepared_cases(
        &mut node,
        &[
            ("= 'abd'", false),
            ("!= 'abc'", false),
            ("= 'abc'", true),
            ("< 'abd'", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn blob_condition_unprepared_compares_as_blob() {
    let mut node = start_node().await;
    node.seeded_table("blob", "0xcafe").await;
    unprepared_cases(
        &mut node,
        &[
            ("= 0xbeef", false),
            ("!= 0xcafe", false),
            ("= 0xcafe", true),
            ("> 0xca00", true),
        ],
    )
    .await;
}

#[tokio::test]
async fn blob_condition_prepared_compares_as_blob() {
    let mut node = start_node().await;
    node.seeded_table("blob", "0xcafe").await;
    prepared_cases(
        &mut node,
        &[
            ("=", CqlValue::Blob(vec![0xbe, 0xef].into()), false),
            ("!=", CqlValue::Blob(vec![0xca, 0xfe].into()), false),
            ("=", CqlValue::Blob(vec![0xca, 0xfe].into()), true),
        ],
    )
    .await;
}

#[tokio::test]
async fn inet_condition_unprepared_compares_as_inet() {
    let mut node = start_node().await;
    node.seeded_table("inet", "'10.0.0.2'").await;
    unprepared_cases(
        &mut node,
        &[
            ("= '10.0.0.3'", false),
            ("!= '10.0.0.2'", false),
            ("= '10.0.0.2'", true),
            // Ordered by address bytes, not by the dotted string.
            ("> '9.0.0.1'", true),
        ],
    )
    .await;
}

// ---------------------------------------------------------------------------
// refusals
// ---------------------------------------------------------------------------

/// An operand that cannot be the column's type is an invalid request, never a
/// silent `[applied]=false`.
#[tokio::test]
async fn a_condition_operand_of_the_wrong_type_is_refused() {
    let mut node = start_node().await;
    node.seeded_table("timestamp", &TS.to_string()).await;
    let cql = "UPDATE lwt_ty.t SET v = 'new' WHERE k = 1 IF c = true";
    let err = node
        .client
        .query(cql)
        .await
        .expect_err("a boolean operand for a timestamp column must be refused");
    let text = format!("{err:?}");
    assert!(
        text.contains("type mismatch"),
        "{cql}: refusal must name the type mismatch, got {text}"
    );
    assert_eq!(
        node.read_v().await.as_deref(),
        Some("orig"),
        "row unchanged"
    );
}

/// A condition on a column the table does not have is an invalid request.
#[tokio::test]
async fn a_condition_on_an_undefined_column_is_refused() {
    let mut node = start_node().await;
    node.seeded_table("int", "7").await;
    let cql = "UPDATE lwt_ty.t SET v = 'new' WHERE k = 1 IF nope = 7";
    let err = node
        .client
        .query(cql)
        .await
        .expect_err("a condition on an undefined column must be refused");
    let text = format!("{err:?}");
    assert!(
        text.contains("nope"),
        "{cql}: refusal must name the column, got {text}"
    );
    assert_eq!(
        node.read_v().await.as_deref(),
        Some("orig"),
        "row unchanged"
    );
}
