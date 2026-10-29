//! Protocol/JSON negotiation against W3C drafts dated 2026-07-23 and 2026-08-13.
//! Correctness: real HTTP requests either honor their options or fail before updates.
//! Last revised: 2026-10-27
//! Last changed: Pin cardinality, dataset refusal, version and query-form contracts.
use ferrosa_cluster::write_path::WritePath;
use ferrosa_schema::{
    AuthMethod, DeploymentMode, EnvSecretsProvider, PasswordHasher, PasswordPolicy,
    RateLimitConfig, Schema, SchemaConfig, TestAuditSink,
};
use ferrosa_sparql::{
    engine::{SparqlConfig, SparqlEngine},
    http::{start_sparql_http, AppState, SparqlHttpConfig},
};
use ferrosa_storage::{StorageEngine, StorageEngineConfig};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn test_schema() -> Schema {
    Schema::new(SchemaConfig {
        hasher: PasswordHasher::default(),
        password_policy: PasswordPolicy::permissive(),
        auth_method: AuthMethod::Password,
        rate_limit: RateLimitConfig::default(),
        audit_sink: Box::new(TestAuditSink::new()),
        secrets: Box::new(EnvSecretsProvider),
        mode: DeploymentMode::Development,
    })
    .unwrap()
}
struct Server {
    addr: std::net::SocketAddr,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
    _dir: tempfile::TempDir,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server() -> Server {
    let dir = tempfile::tempdir().unwrap();
    let storage =
        Arc::new(StorageEngine::new(StorageEngineConfig::test_config(dir.path()), None).unwrap());
    let engine = Arc::new(SparqlEngine::new(
        storage.clone(),
        Arc::new(WritePath::direct(storage)),
        SparqlConfig::default(),
    ));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let state = AppState {
        engine,
        schema: Arc::new(test_schema()),
        auth_disabled: true,
    };
    let task = tokio::spawn(async move {
        start_sparql_http(
            &SparqlHttpConfig {
                bind_addr: addr,
                ..Default::default()
            },
            state,
        )
        .await
    });
    for _ in 0..200 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Server {
                addr,
                task,
                _dir: dir,
            };
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("HTTP server did not bind")
}
struct Reply {
    status: u16,
    headers: String,
    body: String,
}
async fn request(
    s: &Server,
    method: &str,
    path: &str,
    content_type: &str,
    accept: &str,
    body: &str,
) -> Reply {
    let mut tcp = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let req=format!("{method} {path} HTTP/1.1\r\nHost: {}\r\nContent-Type: {content_type}\r\nAccept: {accept}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",s.addr,body.len());
    tcp.write_all(req.as_bytes()).await.unwrap();
    // Fixtures are bounded; the response cap also catches an unexpected full scan.
    let mut bytes = Vec::new();
    tcp.take(1024 * 1024).read_to_end(&mut bytes).await.unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    Reply {
        status: head.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: head.to_ascii_lowercase(),
        body: body.to_owned(),
    }
}
const JSON: &str = "application/sparql-results+json";
const FORM: &str = "application/x-www-form-urlencoded";
const QUERY: &str = "application/sparql-query";
fn form(params: &[(&str, &str)]) -> String {
    serde_urlencoded::to_string(params).unwrap()
}
#[tokio::test]
async fn explicit_json12_negotiates_select_and_ask() {
    let s = server().await;
    for query in ["SELECT (1 AS ?v) WHERE {}", "ASK {}"] {
        let r = request(
            &s,
            "POST",
            "/sparql",
            QUERY,
            "application/sparql-results+json; version=1.2",
            query,
        )
        .await;
        assert_eq!(r.status, 200, "{}", r.body);
        assert!(r.headers.contains("version=1.2"), "{}", r.headers);
        let data: serde_json::Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!(data["head"]["version"], "1.2");
    }
}
#[tokio::test]
async fn query_form_and_quality_control_response_media() {
    let s = server().await;
    let q = "CONSTRUCT { <urn:s> <urn:p> \"x\\ny\" } WHERE {}";
    let r = request(&s, "POST", "/sparql", QUERY, "*/*", q).await;
    assert_eq!(r.status, 200);
    assert!(
        r.headers.contains("content-type: application/n-triples"),
        "{}",
        r.headers
    );
    assert!(r.body.contains("\\n"));
    for (q, a, status, media) in [
        ("SELECT (1 AS ?v) WHERE {}", "text/turtle", 406, ""),
        (q, JSON, 406, ""),
        (
            "ASK {}",
            "application/sparql-results+xml;q=0.2, application/sparql-results+json;q=0.9",
            200,
            JSON,
        ),
        (
            "ASK {}",
            "application/sparql-results+json;q=0, */*;q=0.5",
            200,
            "application/sparql-results+xml",
        ),
        (
            "ASK {}",
            "application/sparql-results+json;version=99",
            406,
            "",
        ),
        (
            "SELECT (TRIPLE(<urn:s>,<urn:p>,1) AS ?t) WHERE {}",
            "application/sparql-results+json;version=1.1",
            406,
            "",
        ),
    ] {
        let r = request(&s, "POST", "/sparql", QUERY, a, q).await;
        assert_eq!(r.status, status, "accept={a}, body={}", r.body);
        if status == 200 {
            assert!(r.headers.contains(media), "{}", r.headers)
        }
    }
}
#[tokio::test]
async fn duplicate_and_mixed_operations_are_rejected_before_update() {
    let s = server().await;
    let ins = "INSERT DATA { <urn:s> <urn:p> 1 }";
    for params in [
        vec![("query", "ASK {}"), ("query", "ASK {}")],
        vec![("update", ins), ("update", ins)],
        vec![("query", "ASK {}"), ("update", ins)],
        vec![("query", "ASK {}"), ("version", "1.2"), ("version", "1.2")],
    ] {
        let r = request(&s, "POST", "/sparql", FORM, JSON, &form(&params)).await;
        assert_eq!(r.status, 400, "{}", r.body);
    }
    let path = format!(
        "/sparql?{}",
        form(&[("query", "ASK {}"), ("query", "ASK {}")])
    );
    assert_eq!(request(&s, "GET", &path, "", JSON, "").await.status, 400);
    let r = request(&s, "POST", "/sparql", QUERY, JSON, "ASK { ?s ?p ?o }").await;
    let data: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(data["boolean"], false);
}
#[tokio::test]
async fn dataset_options_are_never_silently_ignored() {
    let s = server().await;
    for key in ["default-graph-uri", "named-graph-uri"] {
        let body = form(&[("query", "ASK {}"), (key, "urn:g")]);
        assert_eq!(
            request(&s, "POST", "/sparql", FORM, JSON, &body)
                .await
                .status,
            400
        );
        let path = format!("/sparql?{body}");
        assert_eq!(request(&s, "GET", &path, "", JSON, "").await.status, 400);
        let path = format!("/sparql?{}", form(&[(key, "urn:g")]));
        assert_eq!(
            request(&s, "POST", &path, QUERY, JSON, "ASK {}")
                .await
                .status,
            400
        );
    }
    for path in ["/sparql", "/sparql/update"] {
        for key in ["using-graph-uri", "using-named-graph-uri"] {
            let ins = "INSERT DATA { <urn:s> <urn:p> 1 }";
            let body = form(&[("update", ins), (key, "urn:g")]);
            assert_eq!(
                request(&s, "POST", path, FORM, JSON, &body).await.status,
                400
            );
            let path = format!("{path}?{}", form(&[(key, "urn:g")]));
            assert_eq!(
                request(&s, "POST", &path, "application/sparql-update", JSON, ins)
                    .await
                    .status,
                400
            );
        }
    }
    let r = request(&s, "POST", "/sparql", QUERY, JSON, "ASK { ?s ?p ?o }").await;
    let data: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(data["boolean"], false);
}
#[tokio::test]
async fn versions_are_validated_in_each_transport_without_forcing_result_version() {
    let s = server().await;
    for v in ["1.2", "99"] {
        let expected = if v == "1.2" { 200 } else { 400 };
        let body = form(&[("query", "ASK {}"), ("version", v)]);
        assert_eq!(
            request(&s, "POST", "/sparql", FORM, JSON, &body)
                .await
                .status,
            expected
        );
        let path = format!("/sparql?{body}");
        assert_eq!(
            request(&s, "GET", &path, "", JSON, "").await.status,
            expected
        );
        let ct = format!("{QUERY};version=\"{v}\"");
        let r = request(&s, "POST", "/sparql", &ct, JSON, "ASK {}").await;
        assert_eq!(r.status, expected);
        if expected == 200 {
            let data: serde_json::Value = serde_json::from_str(&r.body).unwrap();
            assert!(data["head"].get("version").is_none())
        }
    }
}
#[tokio::test]
async fn relative_iris_use_the_service_uri_unless_base_is_explicit() {
    let s = server().await;
    for (query, expected) in [
        (
            "SELECT (<item> AS ?v) WHERE {}",
            format!("http://{}/item", s.addr),
        ),
        (
            "BASE <https://example.org/base/> SELECT (<item> AS ?v) WHERE {}",
            "https://example.org/base/item".into(),
        ),
    ] {
        let r = request(&s, "POST", "/sparql", QUERY, JSON, query).await;
        assert_eq!(r.status, 200, "{}", r.body);
        let data: serde_json::Value = serde_json::from_str(&r.body).unwrap();
        assert_eq!(data["results"]["bindings"][0]["v"]["value"], expected);
    }
}

#[tokio::test]
async fn update_versions_and_service_base_work_for_both_post_routes() {
    let s = server().await;
    for (i, path) in ["/sparql", "/sparql/update"].iter().enumerate() {
        let update = format!("INSERT DATA {{ <row{i}> <property> 1 }}");
        let body = form(&[("update", &update), ("version", "1.2")]);
        assert_eq!(
            request(&s, "POST", path, FORM, JSON, &body).await.status,
            200
        );
        let invalid = form(&[("update", &update), ("version", "99")]);
        assert_eq!(
            request(&s, "POST", path, FORM, JSON, &invalid).await.status,
            400
        );
        assert_eq!(
            request(
                &s,
                "POST",
                path,
                "application/sparql-update;version=99",
                JSON,
                &update
            )
            .await
            .status,
            400
        );
    }
    let query = format!(
        "ASK {{ <http://{}/row0> <http://{}/property> 1 }}",
        s.addr, s.addr
    );
    let r = request(&s, "POST", "/sparql", QUERY, JSON, &query).await;
    assert_eq!(r.status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&r.body).unwrap()["boolean"],
        true
    );
}

#[tokio::test]
async fn media_types_and_wildcard_types_are_case_insensitive() {
    let s = server().await;
    let r = request(
        &s,
        "POST",
        "/sparql",
        "Application/Sparql-Update; Charset=UTF-8",
        JSON,
        "INSERT DATA { <urn:s> <urn:p> 1 }",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let body = form(&[("query", "ASK { <urn:s> <urn:p> 1 }")]);
    let r = request(
        &s,
        "POST",
        "/sparql",
        "Application/X-Www-Form-Urlencoded",
        "APPLICATION/*",
        &body,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&r.body).unwrap()["boolean"],
        true
    );
}
