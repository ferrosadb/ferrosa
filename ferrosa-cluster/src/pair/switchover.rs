//! Pair handoff and catch-up admission.
//! Correctness: withdraw old authority before granting the peer authority.
//! Last revised: 2026-10-29
//! Last changed: Keep the initiator demoted after lost or cancelled transfer replies.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use uuid::Uuid;

use ferrosa_net::codec::Lane;
use ferrosa_net::error::NetError;
use ferrosa_net::message::Message;
use ferrosa_net::peer::PeerManager;
use ferrosa_net::rpc::handler::{PeerId, RpcHandler};

use crate::error::{ClusterError, Result};
use crate::pair::PairRole;

/// Timeout for switchover RPC.
const SWITCHOVER_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the schema push keeps waiting for a `Bulk` lane that is still
/// reconnecting before it reports the peer unconfirmed.
///
/// A peer that has just restarted redials its lanes on a 1-30 s ladder, and
/// the switchover fires about a second after the peer returns, so the first
/// send is routinely refused with `Reconnecting` while the lane is 10-20 ms
/// from being ready. The budget bounds that wait rather than removing the
/// gate: a lane that never comes up still fails the switchover.
const SCHEMA_PUSH_RETRY_BUDGET: Duration = SWITCHOVER_TIMEOUT;

/// Pause between attempts while the lane is reconnecting.
const SCHEMA_PUSH_RETRY_INTERVAL: Duration = Duration::from_millis(20);

/// One wording for every way the push can fail before the peer answers.
fn schema_push_failed(peer_host_id: Uuid, e: NetError) -> ClusterError {
    ClusterError::ModeTransitionRejected(format!(
        "peer schema unconfirmed: pushing the schema to {peer_host_id} failed ({e})"
    ))
}

/// Progress of the data catch-up a promoted primary replays to a rejoined
/// peer. A switchover must not hand the primary role to a peer whose replay
/// has not finished: it would serve reads missing the replayed rows.
#[derive(Debug, Default)]
pub struct CatchUpGate(AtomicU8);

const CATCH_UP_NOT_PENDING: u8 = 0;
const CATCH_UP_IN_PROGRESS: u8 = 1;
const CATCH_UP_FAILED: u8 = 2;
const CATCH_UP_FAILED_SCHEMA: u8 = 3;

impl CatchUpGate {
    /// A replay to the peer has started.
    pub fn begin(&self) {
        self.0.store(CATCH_UP_IN_PROGRESS, Ordering::SeqCst);
    }

    /// The replay finished and every mutation was acknowledged.
    pub fn complete(&self) {
        self.0.store(CATCH_UP_NOT_PENDING, Ordering::SeqCst);
    }

    /// The replay stopped short; the peer is missing data until a new
    /// catch-up runs.
    pub fn fail(&self) {
        self.0.store(CATCH_UP_FAILED, Ordering::SeqCst);
    }

    /// Whether a replay is running right now. Distinct from `ready`, which is
    /// also `Err` for a replay that already failed.
    pub fn pending(&self) -> bool {
        self.0.load(Ordering::SeqCst) == CATCH_UP_IN_PROGRESS
    }

    /// Catch-up stopped before the replay: the peer never confirmed the
    /// primary's schema, so nothing was replayed at all.
    pub fn fail_schema(&self) {
        self.0.store(CATCH_UP_FAILED_SCHEMA, Ordering::SeqCst);
    }

    /// `Ok` when no catch-up is outstanding.
    pub fn ready(&self) -> Result<()> {
        match self.0.load(Ordering::SeqCst) {
            CATCH_UP_NOT_PENDING => Ok(()),
            CATCH_UP_IN_PROGRESS => Err(ClusterError::ModeTransitionRejected(
                "switchover refused: the data catch-up replay to the peer is still running; \
                 retry when the log shows `catch-up replay complete`"
                    .into(),
            )),
            CATCH_UP_FAILED_SCHEMA => Err(ClusterError::ModeTransitionRejected(
                "switchover refused: the peer's schema could not be confirmed, so the rejoin \
                 replay never ran and the peer is missing writes; reconnect it to rerun catch-up"
                    .into(),
            )),
            _ => Err(ClusterError::ModeTransitionRejected(
                "switchover refused: the data catch-up replay to the peer FAILED, so the peer \
                 is missing writes; reconnect the peer to rerun catch-up (see the warn line \
                 naming the failed step)"
                    .into(),
            )),
        }
    }
}

/// Push this node's schema to the peer and require the peer to acknowledge
/// with the schema version it converged to.
///
/// `PairSchemaSyncHandler` acks with the version only after the peer's
/// keyspaces and tables match the snapshot. Anything else -- an error, a
/// timeout, an empty ack from an older peer, a different version -- leaves
/// the peer's schema unconfirmed and is returned as an error.
pub async fn push_schema_and_confirm(
    peer_manager: &PeerManager,
    peer_host_id: Uuid,
    schema: &ferrosa_schema::Schema,
) -> Result<()> {
    push_schema_and_confirm_with(
        peer_host_id,
        schema,
        SCHEMA_PUSH_RETRY_BUDGET,
        |message| async {
            peer_manager
                .send_with_timeout(peer_host_id, message, Lane::Bulk, SWITCHOVER_TIMEOUT)
                .await
        },
    )
    .await
}

/// The send half of [`push_schema_and_confirm`], with the transport injected.
///
/// A peer that has just restarted still has its lanes in the reconnect ladder,
/// so the first send can be refused with `Reconnecting` before the request
/// leaves this process. The refusal is not an answer from the peer, so the
/// push waits for the lane inside a fixed budget instead of reporting the
/// switchover unconfirmable; once an attempt reaches the peer, its reply
/// decides, exactly as before. The injected sender exists so this bounded
/// wait can be exercised without a socket.
pub(crate) async fn push_schema_and_confirm_with<'a, F, Fut>(
    peer_host_id: Uuid,
    schema: &ferrosa_schema::Schema,
    budget: Duration,
    mut send: F,
) -> Result<()>
where
    F: FnMut(Message) -> Fut,
    Fut: std::future::Future<Output = ferrosa_net::error::Result<Message>> + 'a,
{
    let snapshot = schema.snapshot();
    let wire = crate::pair::ddl::WireSchemaSnapshot::from_snapshot(&snapshot);
    let body = serde_json::to_vec(&wire)
        .map_err(|e| ClusterError::Internal(format!("serialize schema snapshot: {e}")))?;
    let deadline = Instant::now() + budget;
    let resp = loop {
        match send(Message::PairSchemaSync(bytes::Bytes::from(body.clone()))).await {
            Ok(resp) => break resp,
            // The lane refused the request before it left this process, so the
            // peer has not answered: wait for the redial instead of reading
            // the refusal as "schema unconfirmed".
            Err(NetError::Reconnecting) => {
                let now = Instant::now();
                let Some(remaining) = deadline.checked_duration_since(now) else {
                    return Err(schema_push_failed(peer_host_id, NetError::Reconnecting));
                };
                tokio::time::sleep(SCHEMA_PUSH_RETRY_INTERVAL.min(remaining)).await;
            }
            Err(e) => return Err(schema_push_failed(peer_host_id, e)),
        }
    };
    match resp {
        Message::PairDdlAck(version) if version.as_ref() == snapshot.version.as_bytes() => Ok(()),
        Message::PairDdlAck(version) => Err(ClusterError::ModeTransitionRejected(format!(
            "peer schema unconfirmed: {peer_host_id} acknowledged the schema push without \
             reporting convergence to version {} (ack carried {} bytes)",
            snapshot.version,
            version.len()
        ))),
        other => Err(ClusterError::ModeTransitionRejected(format!(
            "peer schema unconfirmed: expected PairDdlAck from {peer_host_id}, got {:?}",
            other.msg_type()
        ))),
    }
}

/// Initiate switchover from the primary side.
///
/// Withdraw local authority before sending a versioned grant to the secondary.
/// A timeout or cancellation cannot prove the peer did not accept the grant,
/// so it must never restore the old primary role.
pub async fn initiate_switchover(
    peer_manager: &PeerManager,
    local_host_id: Uuid,
    peer_host_id: Uuid,
    role: &arc_swap::ArcSwap<PairRole>,
    root: &std::path::Path,
) -> Result<()> {
    if **role.load() != PairRole::Primary {
        return Err(ClusterError::NotPrimary);
    }
    if peer_manager
        .peer_capabilities(peer_host_id)
        .await
        .unwrap_or(0)
        & ferrosa_net::handshake::CAP_PAIR_AUTHORITY
        == 0
    {
        return Err(ClusterError::ReplicationFailed(
            "peer lacks versioned pair authority capability; upgrade both peers".into(),
        ));
    }
    let response = peer_manager
        .send_with_timeout(
            peer_host_id,
            wire(&AuthorityRpc::Prepare),
            Lane::Raft,
            SWITCHOVER_TIMEOUT,
        )
        .await?;
    let (challenge, peer_generation) = match decode(response) {
        Some(AuthorityRpc::Ready {
            challenge,
            generation,
        }) => (challenge, generation),
        _ => {
            return Err(ClusterError::ReplicationFailed(
                "invalid pair authority preparation".into(),
            ))
        }
    };
    let current = super::authority::PairAuthority::read(root, local_host_id)?;
    let generation = current
        .map_or(0, |r| r.generation)
        .max(peer_generation)
        .checked_add(1)
        .ok_or_else(|| ClusterError::Internal("pair authority generation exhausted".into()))?;
    // No await between withdrawal and persistence. Failure leaves this process fenced.
    role.store(Arc::new(PairRole::Secondary));
    super::authority::PairAuthority::store_generation(
        root,
        local_host_id,
        peer_host_id,
        peer_host_id,
        generation,
    )?;
    let response = peer_manager
        .send_with_timeout(
            peer_host_id,
            wire(&AuthorityRpc::Grant {
                challenge,
                generation,
            }),
            Lane::Raft,
            SWITCHOVER_TIMEOUT,
        )
        .await?;
    match decode(response) {
        Some(AuthorityRpc::Accepted {
            generation: accepted,
        }) if accepted == generation => Ok(()),
        _ => Err(ClusterError::ReplicationFailed(
            "pair authority grant unconfirmed; local writer remains fenced".into(),
        )),
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "operation", deny_unknown_fields)]
enum AuthorityRpc {
    Prepare,
    Ready { challenge: Uuid, generation: u64 },
    Grant { challenge: Uuid, generation: u64 },
    Accepted { generation: u64 },
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorityEnvelope {
    version: u32,
    payload: AuthorityRpc,
}
fn wire(message: &AuthorityRpc) -> Message {
    Message::PairAuthority(
        serde_json::to_vec(&AuthorityEnvelope {
            version: 1,
            payload: message.clone(),
        })
        .expect("serializable authority RPC")
        .into(),
    )
}
fn decode(message: Message) -> Option<AuthorityRpc> {
    match message {
        Message::PairAuthority(bytes) if bytes.len() <= 4096 => {
            let envelope: AuthorityEnvelope = serde_json::from_slice(&bytes).ok()?;
            (envelope.version == 1).then_some(envelope.payload)
        }
        _ => None,
    }
}

/// A versioned, peer-bound grant consumes one fresh receiver challenge.
/// Legacy unversioned assignments are always refused.
pub struct RoleSwapHandler {
    local_host_id: Uuid,
    role: Arc<arc_swap::ArcSwap<PairRole>>,
    durable: Option<(Uuid, std::path::PathBuf)>,
    challenge: parking_lot::Mutex<Option<Uuid>>,
    gate: Arc<tokio::sync::RwLock<()>>,
}
impl RoleSwapHandler {
    pub fn new(local_host_id: Uuid, role: Arc<arc_swap::ArcSwap<PairRole>>) -> Self {
        Self {
            local_host_id,
            role,
            durable: None,
            challenge: parking_lot::Mutex::new(None),
            gate: Arc::new(tokio::sync::RwLock::new(())),
        }
    }
    pub fn durable(
        local_host_id: Uuid,
        peer: Uuid,
        role: Arc<arc_swap::ArcSwap<PairRole>>,
        root: std::path::PathBuf,
        gate: Arc<tokio::sync::RwLock<()>>,
    ) -> Self {
        Self {
            durable: Some((peer, root)),
            gate,
            ..Self::new(local_host_id, role)
        }
    }
}
#[async_trait::async_trait]
impl RpcHandler for RoleSwapHandler {
    async fn handle(&self, from: PeerId, msg: Message) -> Option<Message> {
        let (peer, root) = self.durable.as_ref()?;
        if from.0 != *peer {
            return None;
        }
        let request = decode(msg)?;
        let _gate = self.gate.write().await;
        let record = super::authority::PairAuthority::read(root, self.local_host_id).ok()?;
        if record.as_ref().is_some_and(|r| r.peer != *peer) {
            return None;
        }
        let current = record.map_or(0, |r| r.generation);
        match request {
            AuthorityRpc::Prepare => {
                if **self.role.load() != PairRole::Secondary {
                    return None;
                }
                let challenge = Uuid::new_v4();
                *self.challenge.lock() = Some(challenge);
                Some(wire(&AuthorityRpc::Ready {
                    challenge,
                    generation: current,
                }))
            }
            AuthorityRpc::Grant {
                challenge,
                generation,
            } => {
                if generation <= current
                    || self.challenge.lock().take() != Some(challenge)
                    || **self.role.load() != PairRole::Secondary
                {
                    return None;
                }
                super::authority::PairAuthority::store_generation(
                    root,
                    self.local_host_id,
                    *peer,
                    self.local_host_id,
                    generation,
                )
                .ok()?;
                self.role.store(Arc::new(PairRole::Primary));
                Some(wire(&AuthorityRpc::Accepted { generation }))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use std::time::Instant;

    use ferrosa_net::error::NetError;

    fn test_schema() -> Arc<ferrosa_schema::Schema> {
        use ferrosa_schema::{
            AuthMethod, DeploymentMode as SchemaDeploymentMode, LogAuditSink, PasswordHasher,
            PasswordPolicy, RateLimitConfig, SchemaConfig,
        };
        let config = SchemaConfig {
            hasher: PasswordHasher::default(),
            password_policy: PasswordPolicy::permissive(),
            auth_method: AuthMethod::Password,
            rate_limit: RateLimitConfig::default(),
            audit_sink: Box::new(LogAuditSink),
            secrets: Box::new(ferrosa_schema::EnvSecretsProvider),
            mode: SchemaDeploymentMode::Development,
        };
        Arc::new(ferrosa_schema::Schema::new(config).unwrap())
    }

    fn converged_ack(schema: &ferrosa_schema::Schema) -> Message {
        let version = schema.snapshot().version;
        Message::PairDdlAck(bytes::Bytes::copy_from_slice(version.as_bytes()))
    }

    /// The nightly smoke's Phase 5: the rejoined peer is milliseconds away from
    /// a ready `Bulk` lane when the switchover's schema push arrives. The
    /// refusal is transport ("lane is reconnecting"), not an answer from the
    /// peer, so the push waits for the lane instead of failing the handoff.
    #[tokio::test]
    async fn a_reconnecting_lane_is_waited_out_rather_than_failing_the_switchover() {
        let schema = test_schema();
        let ack = converged_ack(&schema);
        let attempts = AtomicUsize::new(0);
        let result = push_schema_and_confirm_with(
            Uuid::nil(),
            &schema,
            Duration::from_millis(500),
            |_message| {
                let attempt = attempts.fetch_add(1, AtomicOrdering::SeqCst);
                let ack = ack.clone();
                async move {
                    if attempt < 2 {
                        Err(NetError::Reconnecting)
                    } else {
                        Ok(ack)
                    }
                }
            },
        )
        .await;
        assert!(
            result.is_ok(),
            "expected the retry to reach the peer: {result:?}"
        );
        assert_eq!(attempts.load(AtomicOrdering::SeqCst), 3);
    }

    /// The wait is bounded: a peer whose lane never comes up is still
    /// unconfirmed, so the switchover refuses rather than being handed a
    /// primary that cannot acknowledge its schema.
    #[tokio::test]
    async fn a_lane_that_never_comes_up_still_refuses_the_switchover() {
        let schema = test_schema();
        let started = Instant::now();
        let result = push_schema_and_confirm_with(
            Uuid::nil(),
            &schema,
            Duration::from_millis(120),
            |_message| async { Err::<Message, _>(NetError::Reconnecting) },
        )
        .await;
        let err = result.expect_err("a peer that never becomes reachable is unconfirmed");
        assert!(format!("{err}").contains("lane is reconnecting"), "{err}");
        assert!(
            started.elapsed() >= Duration::from_millis(120),
            "gave up after {:?}, before the budget was spent",
            started.elapsed()
        );
    }

    /// Only a lane that never carried the request is retried. A send that
    /// reached the transport and failed (timeout, I/O) is an answer about the
    /// peer, and repeating it would multiply the switchover's latency.
    #[tokio::test]
    async fn a_delivered_failure_is_not_retried() {
        let schema = test_schema();
        let attempts = AtomicUsize::new(0);
        let result = push_schema_and_confirm_with(
            Uuid::nil(),
            &schema,
            Duration::from_millis(500),
            |_message| {
                attempts.fetch_add(1, AtomicOrdering::SeqCst);
                async { Err::<Message, _>(NetError::Timeout("peer did not answer".into())) }
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            attempts.load(AtomicOrdering::SeqCst),
            1,
            "a send that carried the request must not be repeated"
        );
    }

    /// The confirmation gate does not move: an ack naming another schema
    /// version still refuses the switchover.
    #[tokio::test]
    async fn a_stale_ack_still_refuses_the_switchover() {
        let schema = test_schema();
        let result = push_schema_and_confirm_with(
            Uuid::nil(),
            &schema,
            Duration::from_millis(500),
            |_message| async {
                Ok::<Message, NetError>(Message::PairDdlAck(bytes::Bytes::from_static(
                    b"stale-version",
                )))
            },
        )
        .await;
        let err = result.expect_err("an ack naming another version is not confirmation");
        assert!(
            format!("{err}").contains("without reporting convergence"),
            "{err}"
        );
    }
}
