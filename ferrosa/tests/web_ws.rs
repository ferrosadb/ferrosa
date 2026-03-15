//! Integration tests for the WebSocket virtual table subscription handler.
//!
//! Since the `ferrosa` crate is a binary (no lib.rs), these tests build a
//! minimal axum router directly from the underlying crate dependencies,
//! wiring just the WebSocket route path needed for testing.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use ferrosa_schema::{
    RowPredicate, SubscriptionMode, VirtualColumnDef, VirtualRow, VirtualTable,
    VirtualTableRegistry,
};
use tokio_tungstenite::tungstenite;

// ---------------------------------------------------------------------------
// Stub virtual table
// ---------------------------------------------------------------------------

struct StubConnectionsTable;

impl VirtualTable for StubConnectionsTable {
    fn name(&self) -> &str {
        "connections"
    }

    fn keyspace(&self) -> &str {
        "system_observability"
    }

    fn columns(&self) -> &[VirtualColumnDef] {
        &[]
    }

    fn primary_key_columns(&self) -> &[usize] {
        &[]
    }

    fn read(&self, _predicate: Option<&RowPredicate>) -> Vec<VirtualRow> {
        vec![VirtualRow { cells: vec![] }]
    }

    fn subscription_mode(&self) -> SubscriptionMode {
        SubscriptionMode::Pollable
    }
}

// ---------------------------------------------------------------------------
// Minimal WebSocket handler (mirrors ferrosa/src/web/ws.rs logic)
//
// Integration tests need a working server with the same JSON protocol.
// We replicate the core handler inline since the ferrosa crate is binary-only.
// ---------------------------------------------------------------------------

const OUTBOUND_CHANNEL_CAPACITY: usize = 64;
const DEFAULT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);

#[derive(Clone)]
struct TestState {
    registry: Arc<VirtualTableRegistry>,
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<TestState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.registry))
}

#[derive(Deserialize)]
struct ClientMessage {
    #[serde(rename = "type")]
    msg_type: String,
    table: Option<String>,
}

fn virtual_table_to_json(registry: &VirtualTableRegistry, table_name: &str) -> Value {
    let table = match registry.get("system_observability", table_name) {
        Some(t) => t,
        None => return json!([]),
    };
    let rows = table.read(None);
    let json_rows: Vec<Value> = rows
        .iter()
        .map(|_row| Value::Object(serde_json::Map::new()))
        .collect();
    json!(json_rows)
}

async fn handle_socket(socket: WebSocket, registry: Arc<VirtualTableRegistry>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<String>(OUTBOUND_CHANNEL_CAPACITY);

    let sender_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let mut subscriptions: std::collections::HashMap<String, tokio::task::JoinHandle<()>> =
        std::collections::HashMap::new();

    while let Some(Ok(msg)) = ws_receiver.next().await {
        match msg {
            Message::Text(text) => {
                let parsed: ClientMessage = match serde_json::from_str(&text) {
                    Ok(m) => m,
                    Err(_) => {
                        let err = json!({"type": "error", "message": "invalid JSON"}).to_string();
                        let _ = tx.try_send(err);
                        continue;
                    }
                };

                match parsed.msg_type.as_str() {
                    "subscribe" => {
                        let Some(table_name) = parsed.table else {
                            let err = json!({"type": "error", "message": "missing table field"})
                                .to_string();
                            let _ = tx.try_send(err);
                            continue;
                        };

                        let table = match registry.get("system_observability", &table_name) {
                            Some(t) => t,
                            None => {
                                let err = json!({
                                    "type": "error",
                                    "message": format!("unknown table: {table_name}")
                                })
                                .to_string();
                                let _ = tx.try_send(err);
                                continue;
                            }
                        };

                        let mode = table.subscription_mode();

                        if let Some(handle) = subscriptions.remove(&table_name) {
                            handle.abort();
                        }

                        match mode {
                            SubscriptionMode::None => {
                                let rows = virtual_table_to_json(&registry, &table_name);
                                let data =
                                    json!({"type": "data", "table": &table_name, "rows": rows})
                                        .to_string();
                                let _ = tx.try_send(data);
                            }
                            SubscriptionMode::Pollable | SubscriptionMode::DemandDriven { .. } => {
                                let interval = match mode {
                                    SubscriptionMode::DemandDriven { default_interval } => {
                                        default_interval
                                    }
                                    _ => DEFAULT_POLL_INTERVAL,
                                };

                                let poll_tx = tx.clone();
                                let poll_registry = registry.clone();
                                let poll_table_name = table_name.clone();

                                let handle = tokio::spawn(async move {
                                    let mut ticker = tokio::time::interval(interval);
                                    loop {
                                        ticker.tick().await;
                                        let rows =
                                            virtual_table_to_json(&poll_registry, &poll_table_name);
                                        let data = json!({
                                            "type": "data",
                                            "table": &poll_table_name,
                                            "rows": rows,
                                        })
                                        .to_string();
                                        let _ = poll_tx.try_send(data);
                                    }
                                });

                                subscriptions.insert(table_name, handle);
                            }
                        }
                    }
                    "unsubscribe" => {
                        if let Some(table_name) = parsed.table {
                            if let Some(handle) = subscriptions.remove(&table_name) {
                                handle.abort();
                            }
                        }
                    }
                    other => {
                        let err = json!({
                            "type": "error",
                            "message": format!("unknown message type: {other}")
                        })
                        .to_string();
                        let _ = tx.try_send(err);
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    for (_, handle) in subscriptions {
        handle.abort();
    }
    drop(tx);
    let _ = sender_task.await;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Start a test server with the given registry and return a WebSocket connection.
async fn connect_ws(
    registry: Arc<VirtualTableRegistry>,
) -> (
    futures::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tungstenite::Message,
    >,
    futures::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
) {
    let state = TestState { registry };
    let router = Router::new()
        .route("/api/ws", get(ws_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });

    let url = format!("ws://127.0.0.1:{}/api/ws", addr.port());
    let (ws_stream, _) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("WebSocket connect");
    let (sender, receiver) = ws_stream.split();
    (sender, receiver)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ws_subscribe_receives_data() {
    let registry = Arc::new(VirtualTableRegistry::new());
    registry.register(Arc::new(StubConnectionsTable));

    let (mut sender, mut receiver) = connect_ws(registry).await;

    // Subscribe to "connections".
    let subscribe_msg = json!({"type": "subscribe", "table": "connections"}).to_string();
    sender
        .send(tungstenite::Message::Text(subscribe_msg.into()))
        .await
        .expect("send subscribe");

    // Wait for a data message (timeout 5s).
    let data_msg = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
        .await
        .expect("timeout waiting for data message")
        .expect("stream ended")
        .expect("ws error");

    let text = match data_msg {
        tungstenite::Message::Text(t) => t.to_string(),
        other => panic!("expected Text message, got {other:?}"),
    };
    let parsed: Value = serde_json::from_str(&text).expect("parse JSON");
    assert_eq!(parsed["type"], "data");
    assert_eq!(parsed["table"], "connections");
    assert!(parsed["rows"].is_array());

    // Unsubscribe and close.
    let unsub_msg = json!({"type": "unsubscribe", "table": "connections"}).to_string();
    sender
        .send(tungstenite::Message::Text(unsub_msg.into()))
        .await
        .expect("send unsubscribe");
    sender
        .send(tungstenite::Message::Close(None))
        .await
        .expect("send close");
}

#[tokio::test]
async fn ws_unknown_table_returns_error() {
    let registry = Arc::new(VirtualTableRegistry::new());
    // No tables registered — any subscribe should fail.

    let (mut sender, mut receiver) = connect_ws(registry).await;

    // Subscribe to "nonexistent".
    let subscribe_msg = json!({"type": "subscribe", "table": "nonexistent"}).to_string();
    sender
        .send(tungstenite::Message::Text(subscribe_msg.into()))
        .await
        .expect("send subscribe");

    // Wait for error message.
    let err_msg = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next())
        .await
        .expect("timeout waiting for error message")
        .expect("stream ended")
        .expect("ws error");

    let text = match err_msg {
        tungstenite::Message::Text(t) => t.to_string(),
        other => panic!("expected Text message, got {other:?}"),
    };
    let parsed: Value = serde_json::from_str(&text).expect("parse JSON");
    assert_eq!(parsed["type"], "error");
    let message = parsed["message"].as_str().expect("message field");
    assert!(
        message.contains("nonexistent"),
        "error message should mention 'nonexistent', got: {message}"
    );

    sender
        .send(tungstenite::Message::Close(None))
        .await
        .expect("send close");
}
