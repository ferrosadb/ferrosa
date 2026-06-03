//! WebSocket handler for streaming virtual table data to subscribers.
//!
//! ## JSON Protocol
//!
//! ```text
//! Client → Server:  {"type": "subscribe", "table": "connections"}
//! Client → Server:  {"type": "unsubscribe", "table": "connections"}
//! Server → Client:  {"type": "data", "table": "connections", "rows": [...]}
//! Server → Client:  {"type": "error", "message": "unknown table: foo"}
//! ```
//!
//! Each subscription spawns a poll task that periodically reads the virtual
//! table and pushes updated rows to the client. The poll interval depends on
//! the table's [`SubscriptionMode`]:
//!
//! - `Pollable` — [`DEFAULT_POLL_INTERVAL`] (2 s)
//! - `DemandDriven { default_interval }` — uses the table's hint
//! - `None` — sends one snapshot and does not poll

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use ferrosa_schema::{SubscriptionMode, VirtualTableRegistry};

use super::api::virtual_table_to_json;
use super::WebAppState;

/// Bounded channel capacity for outbound WebSocket messages.
///
/// When the client cannot keep up, messages are dropped with a warning rather
/// than blocking the poll task.
const OUTBOUND_CHANNEL_CAPACITY: usize = 64;

/// Default poll interval for `Pollable` virtual tables.
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Accept a WebSocket upgrade and hand off to [`handle_socket`].
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<WebAppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state.registry))
}

/// Message received from the WebSocket client.
#[derive(Deserialize)]
struct ClientMessage {
    #[serde(rename = "type")]
    msg_type: String,
    table: Option<String>,
}

/// Tracks an active subscription: cancellation token + background task.
struct WsSubscription {
    cancel: CancellationToken,
    #[allow(dead_code)]
    task: JoinHandle<()>,
}

/// Main WebSocket loop.
///
/// 1. Splits the socket into sender / receiver halves.
/// 2. Creates a bounded `mpsc` channel for outbound messages.
/// 3. Spawns a sender task that forwards channel messages to the WebSocket.
/// 4. Tracks active subscriptions in a `HashMap<String, WsSubscription>`.
/// 5. Loops on inbound messages and dispatches subscribe / unsubscribe.
/// 6. On exit, cancels all subscription tasks and awaits the sender task.
async fn handle_socket(socket: WebSocket, registry: Arc<VirtualTableRegistry>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<String>(OUTBOUND_CHANNEL_CAPACITY);

    // Sender task: forward channel messages to WebSocket.
    let sender_task =
        ferrosa_common::task_pool::TaskPool::current("websocket-send").spawn(async move {
            while let Some(msg) = rx.recv().await {
                if ws_sender.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        });

    let mut subscriptions: HashMap<String, WsSubscription> = HashMap::new();

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

                        // Look up the table in the registry.
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

                        // Cancel any existing subscription for this table.
                        if let Some(sub) = subscriptions.remove(&table_name) {
                            sub.cancel.cancel();
                        }

                        match mode {
                            SubscriptionMode::None => {
                                // One-shot snapshot, no poll task.
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

                                let cancel = CancellationToken::new();
                                let task_cancel = cancel.clone();

                                let task = ferrosa_common::task_pool::TaskPool::current(
                                    "websocket-subscription",
                                )
                                .spawn(async move {
                                    let mut ticker = tokio::time::interval(interval);
                                    loop {
                                        tokio::select! {
                                            _ = ticker.tick() => {
                                                let rows = virtual_table_to_json(
                                                    &poll_registry,
                                                    &poll_table_name,
                                                );
                                                let data = json!({
                                                    "type": "data",
                                                    "table": &poll_table_name,
                                                    "rows": rows,
                                                })
                                                .to_string();
                                                if poll_tx.try_send(data).is_err() {
                                                    tracing::warn!(
                                                        table = %poll_table_name,
                                                        "outbound channel full, dropping message"
                                                    );
                                                }
                                            }
                                            _ = task_cancel.cancelled() => {
                                                tracing::debug!(
                                                    table = %poll_table_name,
                                                    "subscription poll task shutting down"
                                                );
                                                break;
                                            }
                                        }
                                    }
                                });

                                subscriptions.insert(table_name, WsSubscription { cancel, task });
                            }
                        }
                    }
                    "unsubscribe" => {
                        if let Some(table_name) = parsed.table {
                            if let Some(sub) = subscriptions.remove(&table_name) {
                                sub.cancel.cancel();
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
            // Ignore ping/pong/binary.
            _ => {}
        }
    }

    // Cleanup: cancel all subscription tasks.
    for (_, sub) in subscriptions {
        sub.cancel.cancel();
    }
    drop(tx);
    let _ = sender_task.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // ClientMessage deserialization
    // -----------------------------------------------------------------------

    #[test]
    fn parse_subscribe_message() {
        let raw = r#"{"type": "subscribe", "table": "connections"}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("valid subscribe");
        assert_eq!(msg.msg_type, "subscribe");
        assert_eq!(msg.table.as_deref(), Some("connections"));
    }

    #[test]
    fn parse_unsubscribe_message() {
        let raw = r#"{"type": "unsubscribe", "table": "connections"}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("valid unsubscribe");
        assert_eq!(msg.msg_type, "unsubscribe");
        assert_eq!(msg.table.as_deref(), Some("connections"));
    }

    #[test]
    fn parse_subscribe_missing_table_field() {
        let raw = r#"{"type": "subscribe"}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("table is optional in struct");
        assert_eq!(msg.msg_type, "subscribe");
        assert!(msg.table.is_none(), "table should be None when omitted");
    }

    #[test]
    fn parse_unknown_message_type() {
        let raw = r#"{"type": "ping", "table": "connections"}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("unknown type still parses");
        assert_eq!(msg.msg_type, "ping");
    }

    #[test]
    fn parse_invalid_json_fails() {
        let raw = "not valid json";
        let result = serde_json::from_str::<ClientMessage>(raw);
        assert!(result.is_err(), "invalid JSON must fail to parse");
    }

    #[test]
    fn parse_empty_object_fails() {
        // `type` field is required (via `msg_type`).
        let raw = "{}";
        let result = serde_json::from_str::<ClientMessage>(raw);
        assert!(
            result.is_err(),
            "empty object missing required 'type' field must fail"
        );
    }

    #[test]
    fn parse_null_table_field() {
        let raw = r#"{"type": "subscribe", "table": null}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("null table is valid");
        assert_eq!(msg.msg_type, "subscribe");
        assert!(msg.table.is_none(), "null table should deserialize as None");
    }

    // -----------------------------------------------------------------------
    // Protocol message formatting
    // -----------------------------------------------------------------------

    #[test]
    fn error_message_format_invalid_json() {
        let err = json!({"type": "error", "message": "invalid JSON"});
        let serialized = err.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&serialized).expect("round-trip must work");
        assert_eq!(parsed["type"], "error");
        assert_eq!(parsed["message"], "invalid JSON");
    }

    #[test]
    fn error_message_format_missing_table() {
        let err = json!({"type": "error", "message": "missing table field"});
        let serialized = err.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["type"], "error");
        assert_eq!(parsed["message"], "missing table field");
    }

    #[test]
    fn error_message_format_unknown_table() {
        let table_name = "nonexistent";
        let err = json!({
            "type": "error",
            "message": format!("unknown table: {table_name}")
        });
        let serialized = err.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["type"], "error");
        assert!(parsed["message"].as_str().unwrap().contains("nonexistent"));
    }

    #[test]
    fn error_message_format_unknown_type() {
        let msg_type = "foo";
        let err = json!({
            "type": "error",
            "message": format!("unknown message type: {msg_type}")
        });
        let serialized = err.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["type"], "error");
        assert!(parsed["message"].as_str().unwrap().contains("foo"));
    }

    #[test]
    fn data_message_format() {
        let table_name = "connections";
        let rows = json!([{"host": "127.0.0.1"}]);
        let data = json!({"type": "data", "table": table_name, "rows": rows});
        let serialized = data.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["type"], "data");
        assert_eq!(parsed["table"], "connections");
        assert!(parsed["rows"].is_array());
        assert_eq!(parsed["rows"][0]["host"], "127.0.0.1");
    }

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    #[test]
    fn outbound_channel_capacity_is_nonzero() {
        let capacity = OUTBOUND_CHANNEL_CAPACITY;
        assert!(capacity > 0, "channel capacity must be positive");
    }

    #[test]
    fn default_poll_interval_is_reasonable() {
        let interval = DEFAULT_POLL_INTERVAL;
        assert!(
            interval >= Duration::from_millis(100),
            "poll interval must be at least 100ms to avoid busy-looping"
        );
        assert!(
            interval <= Duration::from_secs(60),
            "poll interval must be at most 60s to remain responsive"
        );
    }

    // -----------------------------------------------------------------------
    // ClientMessage — additional deserialization edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_subscribe_with_extra_fields_ignored() {
        let raw = r#"{"type": "subscribe", "table": "connections", "extra": 42}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("extra fields should be ignored");
        assert_eq!(msg.msg_type, "subscribe");
        assert_eq!(msg.table.as_deref(), Some("connections"));
    }

    #[test]
    fn parse_empty_string_table_field() {
        let raw = r#"{"type": "subscribe", "table": ""}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("empty string is valid");
        assert_eq!(msg.table.as_deref(), Some(""));
    }

    #[test]
    fn parse_subscribe_with_integer_type_fails() {
        let raw = r#"{"type": 123}"#;
        let result = serde_json::from_str::<ClientMessage>(raw);
        assert!(result.is_err(), "integer type field must fail to parse");
    }

    #[test]
    fn parse_subscribe_preserves_table_case() {
        let raw = r#"{"type": "subscribe", "table": "ActiveQueries"}"#;
        let msg: ClientMessage = serde_json::from_str(raw).expect("case-sensitive");
        assert_eq!(msg.table.as_deref(), Some("ActiveQueries"));
    }

    // -----------------------------------------------------------------------
    // Protocol message formatting — round-trip consistency
    // -----------------------------------------------------------------------

    #[test]
    fn data_message_with_empty_rows() {
        let table_name = "connections";
        let rows: Vec<serde_json::Value> = vec![];
        let data = json!({"type": "data", "table": table_name, "rows": rows});
        let serialized = data.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["type"], "data");
        assert_eq!(parsed["table"], "connections");
        assert!(parsed["rows"].as_array().unwrap().is_empty());
    }

    #[test]
    fn data_message_with_multiple_rows() {
        let rows = json!([
            {"host": "10.0.0.1", "port": 9042},
            {"host": "10.0.0.2", "port": 9043},
        ]);
        let data = json!({"type": "data", "table": "connections", "rows": rows});
        let serialized = data.to_string();
        let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(parsed["rows"].as_array().unwrap().len(), 2);
        assert_eq!(parsed["rows"][0]["host"], "10.0.0.1");
        assert_eq!(parsed["rows"][1]["port"], 9043);
    }

    #[test]
    fn error_messages_always_have_type_and_message_fields() {
        let test_cases = vec![
            "invalid JSON",
            "missing table field",
            "unknown table: foo",
            "unknown message type: bar",
        ];

        for msg_text in test_cases {
            let err = json!({"type": "error", "message": msg_text});
            let serialized = err.to_string();
            let parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap();
            assert_eq!(
                parsed["type"], "error",
                "error message should have type=error for: {msg_text}"
            );
            assert_eq!(
                parsed["message"], msg_text,
                "error message text mismatch for: {msg_text}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // WsSubscription cancellation token
    // -----------------------------------------------------------------------

    #[test]
    fn cancellation_token_starts_uncancelled() {
        let token = tokio_util::sync::CancellationToken::new();
        assert!(
            !token.is_cancelled(),
            "new CancellationToken should not be cancelled"
        );
    }

    #[test]
    fn cancellation_token_clone_propagates() {
        let token = tokio_util::sync::CancellationToken::new();
        let child = token.clone();
        token.cancel();
        assert!(
            child.is_cancelled(),
            "cloned token should be cancelled when original is cancelled"
        );
    }

    // -----------------------------------------------------------------------
    // SubscriptionMode — interval selection logic
    // -----------------------------------------------------------------------

    #[test]
    fn pollable_mode_uses_default_interval() {
        let mode = ferrosa_schema::SubscriptionMode::Pollable;
        let interval = match mode {
            ferrosa_schema::SubscriptionMode::DemandDriven { default_interval } => default_interval,
            _ => DEFAULT_POLL_INTERVAL,
        };
        assert_eq!(interval, DEFAULT_POLL_INTERVAL);
    }

    #[test]
    fn demand_driven_mode_uses_custom_interval() {
        let custom = Duration::from_millis(500);
        let mode = ferrosa_schema::SubscriptionMode::DemandDriven {
            default_interval: custom,
        };
        let interval = match mode {
            ferrosa_schema::SubscriptionMode::DemandDriven { default_interval } => default_interval,
            _ => DEFAULT_POLL_INTERVAL,
        };
        assert_eq!(interval, custom);
    }

    #[test]
    fn none_mode_does_not_match_pollable_branch() {
        let mode = ferrosa_schema::SubscriptionMode::None;
        let is_polling = matches!(
            mode,
            ferrosa_schema::SubscriptionMode::Pollable
                | ferrosa_schema::SubscriptionMode::DemandDriven { .. }
        );
        assert!(
            !is_polling,
            "SubscriptionMode::None should not match the polling branch"
        );
    }

    // -----------------------------------------------------------------------
    // Channel capacity — bounded channel does not panic
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn outbound_channel_respects_capacity() {
        let (tx, _rx) = mpsc::channel::<String>(OUTBOUND_CHANNEL_CAPACITY);
        // Fill the channel up to capacity.
        for i in 0..OUTBOUND_CHANNEL_CAPACITY {
            let result = tx.try_send(format!("msg-{i}"));
            assert!(
                result.is_ok(),
                "should be able to send up to capacity, failed at {i}"
            );
        }
        // One more should fail (channel full).
        let overflow = tx.try_send("overflow".to_string());
        assert!(
            overflow.is_err(),
            "channel should be full after sending capacity messages"
        );
    }
}
