//! `LISTEN` / `NOTIFY` / `pg_notify` (decision D13).
//!
//! A notification raised inside a transaction is queued in the session ([`TxnNotes`], cleared
//! by every path that ends the transaction) and published to listening sessions only when the
//! transaction commits. The [`NotifyHub`] holds a publish lock across a notifying transaction's
//! commit **and** its publication — PostgreSQL's `NotifyQueueLock` — so the order sessions see
//! is commit order, a rolled-back transaction publishes nothing, and nothing is published twice
//! (`specs/lean4/Ferrosa/Sql/Notify.lean`).
//!
//! Each listener has a bounded queue. Before a notifying transaction commits, every queue it
//! would write to is checked for room under the publish lock; if one is full the COMMIT is
//! refused with `54000` before anything is applied — PostgreSQL's "too many notifications in
//! the NOTIFY queue" — rather than committing and dropping notifications.
//!
//! Scope: one node. Every session of this node's PostgreSQL listener shares the hub. A
//! cluster node refuses `LISTEN` (see `ferrosa-postgres/README.md`), so no session can wait for
//! a notification that another node would have to deliver.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::messages::BackendMessage;
use crate::query::error_response;

/// A listener's queue depth. A full queue refuses the notifying COMMIT (`54000`).
pub const LISTENER_QUEUE_DEPTH: usize = 8192;
/// Distinct notifications one transaction may queue (`54000` beyond).
pub const MAX_NOTES_PER_TXN: usize = 65_536;
/// PostgreSQL's payload limit: shorter than 8000 bytes.
pub const MAX_PAYLOAD_BYTES: usize = 7999;

/// One committed notification, shared by every listener it is delivered to.
#[derive(Debug, PartialEq, Eq)]
pub struct Notification {
    /// The notifying session's process id.
    pub pid: i32,
    /// Channel.
    pub channel: Arc<str>,
    /// Payload.
    pub payload: Arc<str>,
}

impl Notification {
    /// The wire message.
    pub fn to_message(&self) -> BackendMessage {
        BackendMessage::NotificationResponse {
            process_id: self.pid,
            channel: Arc::clone(&self.channel),
            payload: Arc::clone(&self.payload),
        }
    }
}

type Sender = mpsc::Sender<Arc<Notification>>;
/// Channel → the listening sessions (pid, queue).
type ListenerMap = HashMap<Arc<str>, Vec<(i32, Sender)>>;

/// The node's notification hub, shared by every session of the listener.
pub struct NotifyHub {
    /// Channel → listening sessions (by session pid).
    listeners: Mutex<ListenerMap>,
    /// Held across a notifying commit and its publication, and across a LISTEN taking effect.
    publish: Arc<tokio::sync::Mutex<()>>,
    next_pid: AtomicI32,
}

impl Default for NotifyHub {
    fn default() -> Self {
        Self {
            listeners: Mutex::new(HashMap::new()),
            publish: Arc::new(tokio::sync::Mutex::new(())),
            next_pid: AtomicI32::new(1),
        }
    }
}

/// Validate a NOTIFY channel and payload as PostgreSQL does.
///
/// # Errors
///
/// `22023` for an empty channel or a payload of 8000 bytes or more.
pub fn validate(channel: &str, payload: &str) -> Result<(), BackendMessage> {
    if channel.is_empty() {
        return Err(error_response("22023", "channel name cannot be empty"));
    }
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(error_response("22023", "payload string too long"));
    }
    Ok(())
}

/// A transaction's queued notifications and LISTEN/UNLISTEN actions, applied only at commit.
#[derive(Debug, Default)]
pub struct TxnNotes {
    notes: Vec<(Arc<str>, Arc<str>)>,
    seen: HashSet<(Arc<str>, Arc<str>)>,
    listen: Vec<ListenAction>,
}

/// A LISTEN or UNLISTEN waiting for its transaction to commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenAction {
    /// `LISTEN channel`.
    Listen(Arc<str>),
    /// `UNLISTEN channel`, or `UNLISTEN *` (`None`).
    Unlisten(Option<Arc<str>>),
}

impl TxnNotes {
    /// Queue a notification; a repeated `(channel, payload)` is queued once.
    ///
    /// # Errors
    ///
    /// `22023` for an invalid channel or payload; `54000` past [`MAX_NOTES_PER_TXN`].
    pub fn notify(&mut self, channel: &str, payload: &str) -> Result<(), BackendMessage> {
        validate(channel, payload)?;
        let key: (Arc<str>, Arc<str>) = (Arc::from(channel), Arc::from(payload));
        if self.seen.contains(&key) {
            return Ok(());
        }
        if self.notes.len() >= MAX_NOTES_PER_TXN {
            return Err(error_response(
                "54000",
                "too many notifications queued in one transaction",
            ));
        }
        self.seen.insert((Arc::clone(&key.0), Arc::clone(&key.1)));
        self.notes.push(key);
        Ok(())
    }

    /// Queue a LISTEN/UNLISTEN.
    pub fn listen(&mut self, action: ListenAction) {
        self.listen.push(action);
    }

    /// No notifications to publish (LISTEN actions may still be queued).
    pub fn has_notifications(&self) -> bool {
        !self.notes.is_empty()
    }

    /// No notifications and no LISTEN actions.
    pub fn is_empty(&self) -> bool {
        self.notes.is_empty() && self.listen.is_empty()
    }
}

/// A session's subscription: its channels and its bounded queue.
pub struct Listener {
    pid: i32,
    hub: Arc<NotifyHub>,
    channels: HashSet<Arc<str>>,
    tx: Sender,
    rx: mpsc::Receiver<Arc<Notification>>,
}

impl Listener {
    /// The next delivered notification, when one is queued (never waits).
    pub fn try_next(&mut self) -> Option<Arc<Notification>> {
        self.rx.try_recv().ok()
    }

    /// Wait for the next delivered notification.
    pub async fn next(&mut self) -> Option<Arc<Notification>> {
        self.rx.recv().await
    }

    /// Channels currently listened to.
    pub fn channels(&self) -> &HashSet<Arc<str>> {
        &self.channels
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        let mut map = match self.hub.listeners.lock() {
            Ok(map) => map,
            Err(poisoned) => {
                tracing::error!(
                    "NOTIFY hub listener map was poisoned; recovering it to unregister a session"
                );
                poisoned.into_inner()
            }
        };
        for channel in &self.channels {
            remove_listener(&mut map, channel, self.pid);
        }
    }
}

fn remove_listener(map: &mut ListenerMap, channel: &str, pid: i32) {
    if let Some(list) = map.get_mut(channel) {
        list.retain(|(p, _)| *p != pid);
        if list.is_empty() {
            map.remove(channel);
        }
    }
}

/// The publish lock, held across a notifying commit and its publication.
pub struct PublishGuard {
    hub: Arc<NotifyHub>,
    _lock: tokio::sync::OwnedMutexGuard<()>,
}

impl NotifyHub {
    /// A fresh process id for a session (used as the NotificationResponse pid).
    pub fn allocate_pid(&self) -> i32 {
        self.next_pid.fetch_add(1, Ordering::Relaxed)
    }

    /// Take the publish lock.
    pub async fn lock(self: &Arc<Self>) -> PublishGuard {
        PublishGuard {
            hub: Arc::clone(self),
            _lock: Arc::clone(&self.publish).lock_owned().await,
        }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, ListenerMap> {
        match self.listeners.lock() {
            Ok(map) => map,
            Err(poisoned) => {
                tracing::error!(
                    "NOTIFY hub listener map was poisoned; continuing with its contents"
                );
                poisoned.into_inner()
            }
        }
    }
}

impl PublishGuard {
    /// Check that every listener `notes` would reach has room for all of them.
    ///
    /// # Errors
    ///
    /// `54000` naming the channel whose listener queue is full.
    pub fn reserve(&self, notes: &TxnNotes) -> Result<(), BackendMessage> {
        let mut per_channel: HashMap<&str, usize> = HashMap::new();
        for (channel, _) in &notes.notes {
            *per_channel.entry(channel).or_default() += 1;
        }
        let map = self.hub.map();
        for (channel, count) in per_channel {
            let full = map
                .get(channel)
                .into_iter()
                .flatten()
                .any(|(_, tx)| !tx.is_closed() && tx.capacity() < count);
            if full {
                tracing::warn!(channel, "NOTIFY refused: a listener's queue is full");
                return Err(error_response(
                    "54000",
                    &format!(
                        "too many notifications in the NOTIFY queue of a listener on \"{channel}\""
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Publish a committed transaction's notifications, in the order they were queued.
    pub fn publish(&self, pid: i32, notes: TxnNotes) {
        let map = self.hub.map();
        for (channel, payload) in notes.notes {
            let Some(listeners) = map.get(&channel) else {
                continue;
            };
            let note = Arc::new(Notification {
                pid,
                channel,
                payload,
            });
            for (_, tx) in listeners {
                match tx.try_send(Arc::clone(&note)) {
                    Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                    // `reserve` checked the room under this same lock, and only publishers
                    // take room; reaching this is a bug, so it is loud.
                    Err(mpsc::error::TrySendError::Full(_)) => tracing::error!(
                        channel = %note.channel,
                        "NOTIFY listener queue full after reservation: notification lost"
                    ),
                }
            }
        }
    }

    /// Apply LISTEN/UNLISTEN actions for a session, creating its listener on first LISTEN.
    pub fn apply_listens(
        &self,
        listener: &mut Option<Listener>,
        pid: i32,
        actions: Vec<ListenAction>,
    ) {
        let mut map = self.hub.map();
        for action in actions {
            match action {
                ListenAction::Listen(channel) => {
                    let l = listener.get_or_insert_with(|| new_listener(&self.hub, pid));
                    if l.channels.insert(Arc::clone(&channel)) {
                        map.entry(channel).or_default().push((pid, l.tx.clone()));
                        // mpsc sender handle: one per subscribed channel
                    }
                }
                ListenAction::Unlisten(target) => {
                    let Some(l) = listener.as_mut() else { continue };
                    let removed: Vec<Arc<str>> = match target {
                        Some(channel) => l.channels.take(&channel).into_iter().collect(),
                        None => l.channels.drain().collect(),
                    };
                    for channel in removed {
                        remove_listener(&mut map, &channel, pid);
                    }
                }
            }
        }
    }
}

fn new_listener(hub: &Arc<NotifyHub>, pid: i32) -> Listener {
    let (tx, rx) = mpsc::channel(LISTENER_QUEUE_DEPTH);
    Listener {
        pid,
        hub: Arc::clone(hub),
        channels: HashSet::new(),
        tx,
        rx,
    }
}

impl TxnNotes {
    /// Split into the notifications (to publish) and the LISTEN actions (to apply).
    pub fn into_parts(self) -> (TxnNotes, Vec<ListenAction>) {
        let TxnNotes {
            notes,
            seen,
            listen,
        } = self;
        (
            TxnNotes {
                notes,
                seen,
                listen: Vec::new(),
            },
            listen,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notes(pairs: &[(&str, &str)]) -> TxnNotes {
        let mut n = TxnNotes::default();
        for (c, p) in pairs {
            n.notify(c, p).expect("valid");
        }
        n
    }

    async fn subscribed(hub: &Arc<NotifyHub>, channel: &str) -> (Option<Listener>, i32) {
        let pid = hub.allocate_pid();
        let mut listener = None;
        hub.lock().await.apply_listens(
            &mut listener,
            pid,
            vec![ListenAction::Listen(Arc::from(channel))],
        );
        (listener, pid)
    }

    #[tokio::test]
    async fn publish_delivers_in_queue_order_and_collapses_duplicates() {
        let hub = Arc::new(NotifyHub::default());
        let (mut l, _) = subscribed(&hub, "c").await;
        let batch = notes(&[("c", "1"), ("c", "2"), ("c", "1"), ("other", "x")]);
        let guard = hub.lock().await;
        guard.reserve(&batch).expect("room");
        guard.publish(7, batch);
        drop(guard);
        let l = l.as_mut().expect("listener");
        let got: Vec<_> = std::iter::from_fn(|| l.try_next())
            .map(|n| n.payload.to_string())
            .collect();
        assert_eq!(got, vec!["1", "2"]);
    }

    #[tokio::test]
    async fn a_full_listener_queue_refuses_before_publishing() {
        let hub = Arc::new(NotifyHub::default());
        let (_l, _) = subscribed(&hub, "c").await;
        let mut big = TxnNotes::default();
        for i in 0..=LISTENER_QUEUE_DEPTH {
            big.notify("c", &i.to_string()).expect("valid");
        }
        let error = hub.lock().await.reserve(&big).expect_err("full");
        assert!(matches!(error, BackendMessage::ErrorResponse { .. }));
    }

    #[tokio::test]
    async fn dropping_a_listener_unregisters_it() {
        let hub = Arc::new(NotifyHub::default());
        let (l, _) = subscribed(&hub, "c").await;
        assert_eq!(hub.map().get("c").map(Vec::len), Some(1));
        drop(l);
        assert!(hub.map().get("c").is_none());
    }

    #[test]
    fn invalid_channel_and_payload_are_refused() {
        assert!(validate("", "x").is_err());
        assert!(validate("c", &"x".repeat(8000)).is_err());
        assert!(validate("c", &"x".repeat(7999)).is_ok());
    }
}
