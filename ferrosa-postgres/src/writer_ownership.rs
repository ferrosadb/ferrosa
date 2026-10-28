//! Local UUID/partition-stripe ownership. FIFO predecessors are explicit wait
//! dependencies. Metadata is never locked across await, evaluation or storage I/O.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

pub(crate) const STRIPES: usize = 1024;
type Key = (uuid::Uuid, usize);
type WriterId = u64;

struct State {
    max_identities: usize,
    max_references: usize,
    references: usize,
    next_id: WriterId,
    // One outgoing edge per live transaction: at most one acquisition in flight.
    transactions: HashMap<WriterId, Option<WriterId>>,
    resources: HashMap<Key, VecDeque<Request>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            max_identities: 16_384,
            max_references: 262_144,
            references: 0,
            next_id: 0,
            transactions: HashMap::new(),
            resources: HashMap::new(),
        }
    }
}

struct Request {
    writer: WriterId,
    // None means granted, including a reservation not yet observed by its task.
    ready: Option<oneshot::Sender<()>>,
}

#[derive(Default, Clone)]
pub(crate) struct WriterLocks(Arc<Mutex<State>>);

impl WriterLocks {
    #[cfg(test)]
    pub(crate) fn with_limits_for_test(ids: usize, refs: usize) -> Self {
        Self(Arc::new(Mutex::new(State {
            max_identities: ids,
            max_references: refs,
            ..State::default()
        })))
    }
    #[cfg(test)]
    pub(crate) fn has_waiters_for_test(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .transactions
            .values()
            .any(Option::is_some)
    }
    #[cfg(test)]
    pub(crate) fn is_owned_for_test(&self, id: uuid::Uuid, slot: usize) -> bool {
        self.0.lock().unwrap().resources.contains_key(&(id, slot))
    }
}

impl State {
    fn closes_cycle(&self, requester: WriterId, mut predecessor: WriterId) -> bool {
        // Iterative and bounded by admitted identities. The existing graph is
        // acyclic; deleting a queue entry only shortens a dependency path.
        for _ in 0..self.transactions.len() {
            if predecessor == requester {
                return true;
            }
            match self.transactions[&predecessor] {
                Some(next) => predecessor = next,
                None => return false,
            }
        }
        unreachable!("writer dependency graph was already cyclic")
    }

    fn remove(&mut self, key: Key, writer: WriterId) {
        let queue = self.resources.get_mut(&key).expect("registered resource");
        let index = queue
            .iter()
            .position(|r| r.writer == writer)
            .expect("registered request");
        queue.remove(index);
        self.references -= 1;
        if index == 0 {
            if let Some(next) = queue.front_mut() {
                self.transactions.insert(next.writer, None);
                // Failed delivery is fine: the pending RAII guard still owns
                // this reservation and will release it, even without polling.
                if let Some(ready) = next.ready.take() {
                    let _ = ready.send(());
                }
            }
        } else if let Some(next) = queue.get(index) {
            self.transactions
                .insert(next.writer, Some(queue[index - 1].writer));
        }
        if queue.is_empty() {
            self.resources.remove(&key);
        }
    }
}

pub(crate) fn stripe(key: &[u8]) -> usize {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hash);
    hash.finish() as usize % STRIPES
}

/// Covers both a queued request and a granted reservation. Moving it into the
/// lease is synchronous with observing the handoff; cancellation at either side
/// therefore cannot orphan ownership.
struct RequestGuard {
    registry: WriterLocks,
    writer: WriterId,
    key: Key,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut state = self.registry.0.lock().expect("writer registry poisoned");
        state.remove(self.key, self.writer);
        state.transactions.insert(self.writer, None);
    }
}

/// Stable identity and ownership move with the spilling write-set through
/// durable apply and publication. No row payloads are retained here.
#[derive(Default)]
pub(crate) struct WriterLease {
    identity: Option<(WriterLocks, WriterId)>,
    pub(crate) lifetime: Option<Arc<super::TransactionLifetime>>,
    guards: BTreeMap<Key, RequestGuard>,
}
impl Drop for WriterLease {
    fn drop(&mut self) {
        self.guards.clear();
        if let Some((registry, writer)) = &self.identity {
            registry
                .0
                .lock()
                .expect("writer registry poisoned")
                .transactions
                .remove(writer);
        }
    }
}

impl WriterLease {
    pub(crate) fn owns(&self, id: uuid::Uuid, slot: usize) -> bool {
        self.guards.contains_key(&(id, slot))
    }

    pub(crate) async fn acquire(
        &mut self,
        registry: &WriterLocks,
        id: uuid::Uuid,
        slot: usize,
    ) -> Result<(), crate::messages::BackendMessage> {
        assert!(slot < STRIPES);
        if self
            .lifetime
            .as_ref()
            .is_some_and(|lifetime| lifetime.is_expired())
        {
            return Err(crate::query::error_response(
                "40001",
                "PostgreSQL transaction snapshot expired",
            ));
        }
        if self.owns(id, slot) {
            return Ok(());
        }
        let key = (id, slot);
        let (ready, receive) = oneshot::channel();
        let writer;
        {
            let mut state = registry.0.lock().expect("writer registry poisoned");
            if state.references >= state.max_references
                || (self.identity.is_none() && state.transactions.len() >= state.max_identities)
            {
                return Err(crate::query::error_response(
                    "53200",
                    "PostgreSQL writer ownership admission exhausted",
                ));
            }
            writer = if let Some((bound, writer)) = &self.identity {
                assert!(
                    Arc::ptr_eq(&bound.0, &registry.0),
                    "writer moved between managers"
                );
                *writer
            } else {
                state.next_id = state.next_id.checked_add(1).ok_or_else(|| {
                    crate::query::error_response(
                        "53200",
                        "PostgreSQL writer identity space exhausted",
                    )
                })?;
                let writer = state.next_id;
                state.transactions.insert(writer, None);
                self.identity = Some((registry.clone(), writer));
                writer
            };
            let predecessor = state
                .resources
                .get(&key)
                .and_then(|q| q.back())
                .map(|r| r.writer);
            if predecessor.is_some_and(|p| state.closes_cycle(writer, p)) {
                return Err(crate::query::error_response(
                    "40P01",
                    "deadlock detected in PostgreSQL writer ownership",
                ));
            }
            state.transactions.insert(writer, predecessor);
            let ready = if predecessor.is_none() {
                let _ = ready.send(());
                None
            } else {
                Some(ready)
            };
            state
                .resources
                .entry(key)
                .or_default()
                .push_back(Request { writer, ready });
            state.references += 1;
        }
        let guard = RequestGuard {
            registry: registry.clone(),
            writer,
            key,
        };
        if let Some(lifetime) = &self.lifetime {
            tokio::select! {
                biased;
                () = lifetime.until_expired() => {
                    return Err(crate::query::error_response("40001", "PostgreSQL transaction snapshot expired"));
                }
                result = receive => { result.expect("writer handoff sender lives with its request"); }
            }
        } else {
            receive
                .await
                .expect("writer handoff sender lives with its request");
        }
        self.guards.insert(key, guard);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{poll, FutureExt};
    use std::task::Poll;

    fn deadlock(result: Poll<Result<(), crate::messages::BackendMessage>>) {
        assert!(
            matches!(result, Poll::Ready(Err(crate::messages::BackendMessage::ErrorResponse { ref fields })) if fields.iter().any(|f| *f == (b'C', "40P01".into()))),
            "request closing a cycle must immediately fail 40P01: {result:?}"
        );
    }

    #[tokio::test]
    async fn three_owner_cycle_rejects_requester() {
        let registry = WriterLocks::default();
        let table = uuid::Uuid::new_v4();
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        a.acquire(&registry, table, 0).await.unwrap();
        b.acquire(&registry, table, 1).await.unwrap();
        c.acquire(&registry, table, 2).await.unwrap();
        let mut ab = Box::pin(a.acquire(&registry, table, 1));
        let mut bc = Box::pin(b.acquire(&registry, table, 2));
        assert!(poll!(&mut ab).is_pending());
        assert!(poll!(&mut bc).is_pending());
        deadlock(poll!(c.acquire(&registry, table, 0).boxed()));
        drop(c);
        bc.await.unwrap();
        drop(b);
        ab.await.unwrap();
    }

    #[tokio::test]
    async fn fifo_predecessor_cycle_rejects_requester() {
        let registry = WriterLocks::default();
        let table = uuid::Uuid::new_v4();
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        a.acquire(&registry, table, 0).await.unwrap();
        c.acquire(&registry, table, 1).await.unwrap();
        let mut ba = Box::pin(b.acquire(&registry, table, 0));
        let mut cb = Box::pin(c.acquire(&registry, table, 0));
        assert!(poll!(&mut ba).is_pending());
        assert!(poll!(&mut cb).is_pending());
        deadlock(poll!(a.acquire(&registry, table, 1).boxed()));
        drop(a);
        ba.await.unwrap();
        drop(b);
        cb.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_grant_and_queue_entry_do_not_leak_ownership() {
        let registry = WriterLocks::default();
        let table = uuid::Uuid::new_v4();
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        a.acquire(&registry, table, 0).await.unwrap();
        let mut ba = Box::pin(b.acquire(&registry, table, 0));
        let mut ca = Box::pin(c.acquire(&registry, table, 0));
        assert!(poll!(&mut ba).is_pending());
        assert!(poll!(&mut ca).is_pending());
        drop(a); // grant B, then cancel before it can observe the grant
        drop(ba);
        ca.await.unwrap();
        assert!(!b.owns(table, 0));
        assert!(c.owns(table, 0));
        let mut bc = Box::pin(b.acquire(&registry, table, 0));
        assert!(poll!(&mut bc).is_pending());
        drop(bc); // cancel while still queued
        drop(c);
        b.acquire(&registry, table, 0).await.unwrap();
    }
    fn limited(ids: usize, refs: usize) -> WriterLocks {
        WriterLocks::with_limits_for_test(ids, refs)
    }

    fn exhausted(result: Poll<Result<(), crate::messages::BackendMessage>>) {
        assert!(
            matches!(result, Poll::Ready(Err(crate::messages::BackendMessage::ErrorResponse { ref fields })) if fields.iter().any(|f| *f == (b'C', "53200".into()))),
            "resource exhaustion must fail loudly with 53200: {result:?}"
        );
    }

    #[tokio::test]
    async fn admission_limits_count_identities_and_held_plus_pending_references() {
        let table = uuid::Uuid::new_v4();
        let registry = limited(2, 8);
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        a.acquire(&registry, table, 0).await.unwrap();
        b.acquire(&registry, table, 1).await.unwrap();
        exhausted(poll!(c.acquire(&registry, table, 2).boxed()));
        drop(a);
        c.acquire(&registry, table, 2).await.unwrap();
        drop(b);
        drop(c);
        assert!(registry.0.lock().unwrap().transactions.is_empty());
        assert!(registry.0.lock().unwrap().resources.is_empty());
    }

    #[tokio::test]
    async fn reference_admission_counts_held_and_pending() {
        let table = uuid::Uuid::new_v4();
        let registry = limited(8, 3);
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        a.acquire(&registry, table, 0).await.unwrap();
        a.acquire(&registry, table, 1).await.unwrap();
        // Reentrancy consumes no additional reference.
        a.acquire(&registry, table, 0).await.unwrap();
        let mut ba = Box::pin(b.acquire(&registry, table, 0));
        assert!(poll!(&mut ba).is_pending());
        exhausted(poll!(c.acquire(&registry, table, 2).boxed()));
        assert_eq!(
            registry.0.lock().unwrap().transactions.len(),
            2,
            "failed resource admission must not consume an identity"
        );
        drop(ba);
        c.acquire(&registry, table, 2).await.unwrap();
        drop(a);
        drop(b);
        drop(c);
        assert!(registry.0.lock().unwrap().transactions.is_empty());
        assert!(registry.0.lock().unwrap().resources.is_empty());
    }

    #[tokio::test]
    async fn long_acyclic_chain_and_cancel_churn_reclaim_registry() {
        let registry = WriterLocks::default();
        let table = uuid::Uuid::new_v4();
        let mut owners: Vec<_> = (0..128).map(|_| WriterLease::default()).collect();
        for (i, owner) in owners.iter_mut().enumerate() {
            owner.acquire(&registry, table, i).await.unwrap();
        }
        let mut waits = Vec::new();
        for (i, owner) in owners.iter_mut().enumerate().skip(1) {
            let mut wait = Box::pin(owner.acquire(&registry, table, i - 1));
            assert!(
                poll!(&mut wait).is_pending(),
                "acyclic chain is not a deadlock"
            );
            waits.push(wait);
        }
        drop(waits);
        drop(owners);
        for _ in 0..128 {
            let mut owner = WriterLease::default();
            let mut waiter = WriterLease::default();
            owner.acquire(&registry, table, 0).await.unwrap();
            let mut wait = Box::pin(waiter.acquire(&registry, table, 0));
            assert!(poll!(&mut wait).is_pending());
            drop(wait);
        }
        assert!(registry.0.lock().unwrap().transactions.is_empty());
        assert!(registry.0.lock().unwrap().resources.is_empty());
    }
    #[tokio::test]
    async fn expiry_wins_grant_race_and_reclaims_unobserved_reservation() {
        let manager = super::super::MvccManager::default();
        let snapshot = manager.snapshot();
        let table = uuid::Uuid::new_v4();
        let mut owner = WriterLease::default();
        let mut waiter = WriterLease {
            lifetime: Some(snapshot.lifetime.clone()),
            identity: None,
            guards: BTreeMap::new(),
        };
        owner.acquire(&manager.writers, table, 0).await.unwrap();
        let mut wait = Box::pin(waiter.acquire(&manager.writers, table, 0));
        assert!(poll!(&mut wait).is_pending());
        drop(owner); // handoff ready, but receiver has not observed it
        assert_eq!(manager.expire_all_snapshots_for_test(), 1);
        let result = wait.await;
        assert!(
            matches!(result, Err(crate::messages::BackendMessage::ErrorResponse { ref fields }) if fields.iter().any(|f| *f == (b'C', "40001".into())))
        );
        assert!(!waiter.owns(table, 0));
        drop(waiter);
        let state = manager.writers.0.lock().unwrap();
        assert!(state.resources.is_empty());
        assert!(state.transactions.is_empty());
        assert_eq!(state.references, 0);
    }

    #[tokio::test]
    async fn middle_queue_cancellation_preserves_fifo_and_uuid_fencing() {
        let registry = WriterLocks::default();
        let old_table = uuid::Uuid::new_v4();
        let new_table = uuid::Uuid::new_v4();
        let mut a = WriterLease::default();
        let mut b = WriterLease::default();
        let mut c = WriterLease::default();
        let mut replacement = WriterLease::default();
        a.acquire(&registry, old_table, 0).await.unwrap();
        replacement.acquire(&registry, new_table, 0).await.unwrap();
        let mut ba = Box::pin(b.acquire(&registry, old_table, 0));
        let mut ca = Box::pin(c.acquire(&registry, old_table, 0));
        assert!(poll!(&mut ba).is_pending());
        assert!(poll!(&mut ca).is_pending());
        drop(ba);
        assert!(poll!(&mut ca).is_pending());
        drop(a);
        ca.await.unwrap();
        assert!(replacement.owns(new_table, 0));
        assert!(c.owns(old_table, 0));
    }
    #[tokio::test]
    async fn waiting_original_deadline_expires_without_reaper() {
        let manager =
            super::super::MvccManager::with_max_age_for_test(std::time::Duration::from_secs(1));
        let table = uuid::Uuid::new_v4();
        let mut owner = WriterLease::default();
        owner.acquire(&manager.writers, table, 0).await.unwrap();
        let snapshot = manager.snapshot();
        let mut waiter = WriterLease {
            lifetime: Some(snapshot.lifetime.clone()),
            identity: None,
            guards: BTreeMap::new(),
        };
        let mut wait = Box::pin(waiter.acquire(&manager.writers, table, 0));
        assert!(poll!(&mut wait).is_pending());
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), wait)
            .await
            .unwrap();
        assert!(
            matches!(result, Err(crate::messages::BackendMessage::ErrorResponse { ref fields }) if fields.iter().any(|f| *f == (b'C', "40001".into())))
        );
        assert!(owner.owns(table, 0), "expiry cannot steal another lease");
        assert!(!manager.writers.has_waiters_for_test());
    }
}
