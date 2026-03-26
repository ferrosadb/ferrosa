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
    let sender_task = tokio::spawn(async move {
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

                                let task = tokio::spawn(async move {
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
