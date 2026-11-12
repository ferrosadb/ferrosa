//! Non-ASCII text must survive the CQL lexer byte for byte.
//!
//! 2026-11-09: the Forge task board (which writes inline-literal CQL) read back
//! "~1,300–1,550" as "~1,300â\u{80}\u{93}1,550". The lexer's slow path for a
//! string literal containing a `''` escape, and its quoted-identifier reader,
//! pushed each UTF-8 BYTE as a `char`, so every multi-byte code point became
//! two or three Latin-1 code points. A literal without `''` took the fast path
//! and was unaffected, which is why the corruption looked intermittent: it hit
//! exactly the text that also contained an apostrophe.
//!
//! These tests drive the real TCP server and read the value back, so they hold
//! at the far end of the pipeline rather than at the lexer.

// Uses `ferrosa_cql::test_util`, which only exists with the `test-util` feature
// (same gate as `protocol_conformance.rs`).
#![cfg(feature = "test-util")]

use ferrosa_cql::client::CqlClient;
use ferrosa_cql::server::{CqlServer, ServerConfig};

async fn client() -> (CqlClient, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let state = ferrosa_cql::test_util::standalone_for_test(dir.path());
    let server = CqlServer::new(
        ServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            max_connections: 10,
            auth_disabled: true,
            ..ServerConfig::default()
        },
        state,
    );
    let addr = server.start_background().await.unwrap();
    let mut client = CqlClient::connect(addr).await.unwrap();
    client
        .query(
            "CREATE KEYSPACE ks WITH replication = \
             {'class': 'SimpleStrategy', 'replication_factor': 1}",
        )
        .await
        .unwrap();
    (client, dir)
}

/// The text column `v` of row `id`, decoded as UTF-8.
async fn read_v(client: &mut CqlClient, id: i32) -> String {
    let result = client
        .query(&format!("SELECT v FROM ks.t WHERE id = {id}"))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1, "row {id} must exist");
    let bytes = result.rows[0].columns[0].clone().expect("v is not null");
    String::from_utf8(bytes).expect("a text cell is UTF-8")
}

/// The board's own text: an apostrophe (so the literal carries `''`), an en
/// dash, an em dash, and code points from two- and three-byte UTF-8 ranges.
const WITH_ESCAPE: &str = "node's ~1,300–1,550 — café 日本";

#[tokio::test]
async fn a_string_literal_with_an_escaped_quote_keeps_its_non_ascii_text() {
    let (mut client, _dir) = client().await;
    client
        .query("CREATE TABLE ks.t (id int PRIMARY KEY, v text)")
        .await
        .unwrap();
    // GIVEN a literal with '' and multi-byte characters, and a control
    // literal with the same characters and no escape (the lexer's fast path)
    let escaped = WITH_ESCAPE.replace('\'', "''");
    client
        .query(&format!("INSERT INTO ks.t (id, v) VALUES (1, '{escaped}')"))
        .await
        .unwrap();
    client
        .query("INSERT INTO ks.t (id, v) VALUES (2, 'nodes ~1,300–1,550 — café 日本')")
        .await
        .unwrap();

    // THEN both read back exactly as written
    assert_eq!(
        read_v(&mut client, 2).await,
        "nodes ~1,300–1,550 — café 日本"
    );
    assert_eq!(read_v(&mut client, 1).await, WITH_ESCAPE);
}

#[tokio::test]
async fn a_quoted_identifier_keeps_its_non_ascii_name() {
    let (mut client, _dir) = client().await;
    client
        .query("CREATE TABLE ks.p (id int PRIMARY KEY, \"prénom\" text)")
        .await
        .unwrap();
    client
        .query("INSERT INTO ks.p (id, \"prénom\") VALUES (1, 'x')")
        .await
        .unwrap();

    let result = client
        .query("SELECT \"prénom\" FROM ks.p WHERE id = 1")
        .await
        .unwrap();

    assert_eq!(result.column_names, vec!["prénom".to_string()]);
}
