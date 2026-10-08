//! Credential-verification cache for the web console's Basic-auth middleware.
//!
//! `auth_middleware` runs on every protected request, and a credential check is
//! a bcrypt `cost=12` comparison — measured at **0.18 s** per verification on a
//! Fly `performance-2x` (AMD EPYC) core. A dashboard or a scraper polling the
//! console therefore forces a fresh bcrypt per request: a 120 s `perf` capture
//! showed `bcrypt::bcrypt` at 5.1 % of a CPU-saturated node, all of it produced
//! by the load harness's own 5 s telemetry scrape (three of its four endpoints
//! sit behind the auth layer).
//!
//! Two things were wrong, and they are independent:
//!
//! 1. **It ran on the async worker.** `Schema::authenticate` is synchronous, so
//!    an inline call pins a tokio worker for the whole bcrypt. The CQL path
//!    documents this as forbidden and offloads via `spawn_blocking`
//!    (`ferrosa-cql/src/connection.rs`, `authenticate_off_runtime`); the web
//!    path never got that treatment. Fixed in `auth_middleware`.
//! 2. **It was never reused.** Every request re-verified. This module is the fix.
//!
//! # What is cached, and why it is safe
//!
//! Only **successful** verifications are stored — a wrong password always pays a
//! full bcrypt, so the cost of guessing is unchanged and there is no negative
//! cache to turn a locked account into a cheap oracle.
//!
//! A hit is only allowed while the schema snapshot the credential was verified
//! against is still the live one, checked by `Arc::ptr_eq`. Every schema mutation
//! (`Schema::inner.store(Arc::new(..))`) installs a new `Arc`, so a password
//! change, a role edit, a superuser toggle or any DDL invalidates every entry —
//! immediately, not after the TTL. The entry holds a strong reference to its
//! snapshot, so a snapshot cannot be freed and its address reused while an entry
//! still refers to it, which makes `Arc::ptr_eq` free of ABA.
//!
//! The cached value is only `AuthContext` (role name + flags). Authorization is
//! *not* cached: `has_admin_or_operator_role` re-reads the live snapshot on every
//! request, so revoking admin membership takes effect at once.
//!
//! # Keying
//!
//! The map key is `sha256(username || 0x00 || password)`. Storing a digest rather
//! than the credential means a heap dump or a `/proc/<pid>/mem` read of the cache
//! does not yield a usable password, and it is what makes the lookup a single
//! 32-byte compare instead of a secret-dependent string comparison. The digest is
//! of the *correct* credential, so observing a lookup hit reveals nothing an
//! attacker could not already prove by authenticating.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use ferrosa_schema::{AuthContext, SchemaSnapshot};
use sha2::{Digest, Sha256};

/// How long a successful verification is trusted, provided the schema has not
/// changed. Long enough that a 5 s scrape cycle verifies ~once per minute,
/// short enough to bound the life of a stale entry after an out-of-band change.
pub const DEFAULT_TTL_SECS: u64 = 60;

/// Upper bound on cached credentials. A deployment with more distinct console
/// credentials than this is pathological; the bound exists so that a hostile
/// caller cannot grow the map without limit. 4096 entries is well under a
/// megabyte of digests and contexts.
pub const DEFAULT_CAPACITY: usize = 4096;

/// Credential digest: `sha256(username || 0x00 || password)`.
type Key = [u8; 32];

struct Entry {
    context: AuthContext,
    /// The snapshot this credential was verified against. A hit requires this to
    /// still be the live snapshot (`Arc::ptr_eq`), which is what makes a password
    /// or role change invalidate the entry immediately.
    verified_against: Arc<SchemaSnapshot>,
    expires_at: Instant,
}

/// In-process cache of successful Basic-auth verifications.
///
/// Cheap to clone behind an `Arc`; all methods take `&self` and are safe to call
/// from concurrent request handlers.
pub struct AuthCache {
    entries: DashMap<Key, Entry>,
    ttl: Duration,
    capacity: usize,
    /// Test-only: number of lookups that missed. In the middleware every miss is
    /// followed by exactly one `Schema::authenticate`, so tests assert reuse by
    /// counting misses rather than instrumenting the schema. Per-instance (not a
    /// global) so parallel tests cannot interfere.
    #[cfg(test)]
    misses: std::sync::atomic::AtomicUsize,
}

impl Default for AuthCache {
    fn default() -> Self {
        Self::new(Duration::from_secs(DEFAULT_TTL_SECS), DEFAULT_CAPACITY)
    }
}

impl AuthCache {
    /// A cache with an explicit lifetime and bound. A zero TTL disables it (every
    /// lookup misses and every insert is a no-op), which is how a test — or an
    /// operator who wants the old behaviour — turns it off.
    pub fn new(ttl: Duration, capacity: usize) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            capacity,
            #[cfg(test)]
            misses: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Build from the environment, falling back to the defaults.
    ///
    /// * `FERROSA_WEB_AUTH_CACHE_TTL_SECS` — `0` disables the cache.
    /// * `FERROSA_WEB_AUTH_CACHE_CAPACITY` — entry bound.
    ///
    /// An unparseable or empty value falls back to the default rather than
    /// failing startup, matching how the other runtime tunables behave.
    pub fn from_env() -> Self {
        let ttl = std::env::var("FERROSA_WEB_AUTH_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(DEFAULT_TTL_SECS));
        let capacity = std::env::var("FERROSA_WEB_AUTH_CACHE_CAPACITY")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_CAPACITY);
        Self::new(ttl, capacity)
    }

    /// Whether lookups and inserts do anything. A zero TTL disables the cache.
    pub fn is_enabled(&self) -> bool {
        !self.ttl.is_zero() && self.capacity > 0
    }

    fn key(username: &str, password: &str) -> Key {
        let mut hasher = Sha256::new();
        hasher.update(username.as_bytes());
        hasher.update([0u8]);
        hasher.update(password.as_bytes());
        hasher.finalize().into()
    }

    /// Look up a previously-verified credential.
    ///
    /// Returns `Some` only when the entry is unexpired **and** was verified
    /// against `live`, the snapshot currently installed in the schema.
    pub fn get(
        &self,
        username: &str,
        password: &str,
        live: &Arc<SchemaSnapshot>,
        now: Instant,
    ) -> Option<AuthContext> {
        let hit = self.get_inner(username, password, live, now);
        #[cfg(test)]
        if hit.is_none() {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        hit
    }

    fn get_inner(
        &self,
        username: &str,
        password: &str,
        live: &Arc<SchemaSnapshot>,
        now: Instant,
    ) -> Option<AuthContext> {
        if !self.is_enabled() {
            return None;
        }
        let key = Self::key(username, password);
        let entry = self.entries.get(&key)?;
        if entry.expires_at > now && Arc::ptr_eq(&entry.verified_against, live) {
            return Some(entry.context.clone());
        }
        // Drop the read guard before evicting: `DashMap::remove` takes the
        // shard's write lock, which would deadlock while a read guard is held.
        drop(entry);
        // Expired, or the schema moved on: evict and report a miss.
        self.entries.remove(&key);
        None
    }

    /// Test-only: number of lookups that missed since construction. In the
    /// middleware a miss is followed by exactly one `Schema::authenticate`, so
    /// this is the observable for "the credential was re-verified".
    #[cfg(test)]
    pub fn misses(&self) -> usize {
        self.misses.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Record a successful verification. Never called for a failed one.
    pub fn insert(
        &self,
        username: &str,
        password: &str,
        verified_against: Arc<SchemaSnapshot>,
        context: AuthContext,
        now: Instant,
    ) {
        if !self.is_enabled() {
            return;
        }
        let key = Self::key(username, password);
        if self.entries.len() >= self.capacity && !self.entries.contains_key(&key) {
            self.prune(now);
            // Still full of live entries: drop an arbitrary tranche to stay
            // bounded. A miss only costs a re-verification, so this is safe.
            if self.entries.len() >= self.capacity {
                let victims: Vec<Key> = self
                    .entries
                    .iter()
                    .take(self.capacity / 4 + 1)
                    .map(|e| *e.key())
                    .collect();
                for victim in victims {
                    self.entries.remove(&victim);
                }
            }
        }
        self.entries.insert(
            key,
            Entry {
                context,
                verified_against,
                expires_at: now + self.ttl,
            },
        );
    }

    /// Number of entries currently held (expired ones included until pruned).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Drop every expired entry.
    pub fn prune(&self, now: Instant) {
        self.entries.retain(|_, entry| entry.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrosa_schema::SchemaSnapshot;
    use uuid::Uuid;

    fn snapshot() -> Arc<SchemaSnapshot> {
        // A minimal snapshot is enough: the cache only compares `Arc` identity.
        Arc::new(SchemaSnapshot {
            version: Uuid::new_v4(),
            ..SchemaSnapshot::default()
        })
    }

    fn ctx(role: &str) -> AuthContext {
        AuthContext {
            role: role.to_string(),
            is_superuser: false,
            must_change_password: false,
        }
    }

    #[test]
    fn miss_before_insert_then_hit() {
        let cache = AuthCache::new(Duration::from_secs(60), 16);
        let snap = snapshot();
        let now = Instant::now();

        assert!(
            cache.get("admin", "pw", &snap, now).is_none(),
            "an unverified credential must not be a hit"
        );

        cache.insert("admin", "pw", snap.clone(), ctx("admin"), now);
        let hit = cache.get("admin", "pw", &snap, now);
        assert_eq!(hit.map(|c| c.role), Some("admin".to_string()));
    }

    #[test]
    fn a_wrong_password_never_hits_a_cached_one() {
        let cache = AuthCache::new(Duration::from_secs(60), 16);
        let snap = snapshot();
        let now = Instant::now();
        cache.insert("admin", "correct", snap.clone(), ctx("admin"), now);

        assert!(
            cache.get("admin", "wrong", &snap, now).is_none(),
            "a different password must be a miss, never a hit on the cached one"
        );
        assert!(
            cache.get("other", "correct", &snap, now).is_none(),
            "a different username must be a miss"
        );
    }

    #[test]
    fn entry_expires_after_ttl() {
        let ttl = Duration::from_secs(30);
        let cache = AuthCache::new(ttl, 16);
        let snap = snapshot();
        let now = Instant::now();
        cache.insert("admin", "pw", snap.clone(), ctx("admin"), now);

        assert!(cache
            .get("admin", "pw", &snap, now + ttl - Duration::from_millis(1))
            .is_some());
        assert!(
            cache.get("admin", "pw", &snap, now + ttl).is_none(),
            "expiry is not inclusive"
        );
    }

    #[test]
    fn a_new_snapshot_invalidates_the_entry() {
        let cache = AuthCache::new(Duration::from_secs(3600), 16);
        let before = snapshot();
        let now = Instant::now();
        cache.insert("admin", "pw", before.clone(), ctx("admin"), now);

        // A schema mutation installs a fresh `Arc`. The credential must be
        // re-verified rather than trusted against the old snapshot.
        let after = snapshot();
        // The entry is still valid for the snapshot it was made against...
        assert!(
            cache.get("admin", "pw", &before, now).is_some(),
            "the entry is still valid for the snapshot it was verified against"
        );
        // ...but a lookup against the new snapshot is a miss, and evicts it.
        assert!(
            cache.get("admin", "pw", &after, now).is_none(),
            "a schema change must invalidate the cached verification"
        );
    }

    #[test]
    fn zero_ttl_disables_the_cache() {
        let cache = AuthCache::new(Duration::ZERO, 16);
        assert!(!cache.is_enabled());
        let snap = snapshot();
        let now = Instant::now();
        cache.insert("admin", "pw", snap.clone(), ctx("admin"), now);
        assert!(cache.get("admin", "pw", &snap, now).is_none());
        assert_eq!(cache.len(), 0, "a disabled cache stores nothing");
    }

    #[test]
    fn capacity_is_respected() {
        let cache = AuthCache::new(Duration::from_secs(3600), 4);
        let snap = snapshot();
        let now = Instant::now();
        for i in 0..40 {
            cache.insert("admin", &format!("pw{i}"), snap.clone(), ctx("admin"), now);
        }
        assert!(
            cache.len() <= 4,
            "the cache must stay within its bound, held {}",
            cache.len()
        );
    }

    #[test]
    fn prune_drops_only_expired_entries() {
        let ttl = Duration::from_secs(10);
        let cache = AuthCache::new(ttl, 16);
        let snap = snapshot();
        let now = Instant::now();
        cache.insert("old", "pw", snap.clone(), ctx("old"), now);
        cache.insert("new", "pw", snap.clone(), ctx("new"), now + ttl);
        assert_eq!(cache.len(), 2);

        cache.prune(now + ttl);
        assert_eq!(cache.len(), 1, "only the expired entry is dropped");
        assert!(cache.get("new", "pw", &snap, now + ttl).is_some());
    }
}
