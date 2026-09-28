//! Postgres SCRAM handshake state machine (sans-IO).
//!
//! Drives `StartupMessage → AuthenticationSASL → (SASLInitialResponse) →
//! AuthenticationSASLContinue → (SASLResponse) → AuthenticationSASLFinal +
//! AuthenticationOk + ReadyForQuery` using the SCRAM-SHA-256 exchange (D4).
//!
//! No I/O happens here: the caller feeds parsed frontend payloads and emits the
//! returned backend messages, so the whole auth flow is unit-testable without a
//! socket (harness layer H1). The connection/transport layer wires this to the
//! codec and the real `ferrosa-schema` role store later.

use ferrosa_schema::AuthContext;

use crate::messages::{BackendMessage, StartupMessage};
use crate::scram::{self, ScramServerFirst, ScramVerifier};

/// The only mechanism offered/accepted in v1 (channel binding is Q4-deferred).
const MECHANISM: &str = "SCRAM-SHA-256";

/// Supplies the stored SCRAM verifier for a role (D4) and gates logins through
/// the failed-login limiter. Backed by `ferrosa-schema`'s role store
/// ([`crate::SchemaVerifierStore`]); abstracted so the handshake stays pure.
///
/// The limiter hooks have no default implementations on purpose: a store that
/// forgot them would silently bypass the lockout.
pub trait VerifierStore {
    /// The stored SCRAM verifier for `user`, or `None` when the role does not
    /// exist or has no verifier.
    fn verifier(&self, user: &str) -> Option<ScramVerifier>;

    /// Admission gate, consulted before any verifier work. `Err` carries the
    /// operator-facing reason the login is refused (backoff / lockout).
    fn admit(&self, user: &str) -> Result<(), String>;

    /// Record a failed login (unknown role or bad proof) against `user`.
    fn record_failure(&self, user: &str);

    /// Record a verified login and return the session's authorization
    /// context. `Err` refuses the login even though the proof verified (e.g.
    /// the role cannot log in).
    fn record_success(&self, user: &str) -> Result<AuthContext, String>;
}

/// A handshake failure (fail loud — never authenticate on doubt).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// No `user` parameter in the StartupMessage.
    MissingUser,
    /// The named role has no SCRAM verifier (cannot authenticate over Postgres).
    UnknownRole,
    /// The failed-login limiter refused the attempt (backoff or lockout).
    Throttled(String),
    /// The proof verified but the role store refused the login.
    LoginRefused(String),
    /// Client offered a mechanism we do not support.
    UnsupportedMechanism,
    /// A message arrived out of order for the current phase.
    UnexpectedMessage,
    /// Underlying SCRAM failure (bad proof, malformed, channel binding).
    Scram(scram::ScramError),
}

enum Phase {
    Start,
    AwaitingInitial {
        user: String,
        verifier: ScramVerifier,
    },
    AwaitingFinal {
        user: String,
        verifier: ScramVerifier,
        ctx: ScramServerFirst,
    },
    Authenticated(AuthContext),
    Failed,
}

/// Sans-IO SCRAM handshake driver.
pub struct Handshake<'a, S: VerifierStore> {
    store: &'a S,
    server_nonce: String,
    phase: Phase,
}

impl<'a, S: VerifierStore> Handshake<'a, S> {
    /// Create a handshake. `server_nonce` is this server's nonce contribution
    /// (randomly generated per connection in production; injected in tests).
    pub fn new(store: &'a S, server_nonce: impl Into<String>) -> Self {
        Self {
            store,
            server_nonce: server_nonce.into(),
            phase: Phase::Start,
        }
    }

    /// Whether the client has successfully authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self.phase, Phase::Authenticated(_))
    }

    /// The authenticated session's authorization context, once authenticated.
    pub fn auth_context(&self) -> Option<&AuthContext> {
        match &self.phase {
            Phase::Authenticated(auth) => Some(auth),
            _ => None,
        }
    }

    /// Handle the StartupMessage: resolve the role and offer SASL.
    pub fn on_startup(
        &mut self,
        startup: &StartupMessage,
    ) -> Result<Vec<BackendMessage>, HandshakeError> {
        if !matches!(self.phase, Phase::Start) {
            self.phase = Phase::Failed;
            return Err(HandshakeError::UnexpectedMessage);
        }
        let user = startup.get("user").ok_or(HandshakeError::MissingUser)?;
        // The shared failed-login limiter is consulted before any verifier
        // work, exactly as the CQL path checks it before hashing.
        self.store.admit(user).map_err(HandshakeError::Throttled)?;
        // NOTE: returning UnknownRole here is a user-enumeration oracle; a
        // hardened version runs the exchange against a dummy verifier. Tracked
        // as a follow-up (threat-model).
        let Some(verifier) = self.store.verifier(user) else {
            // An unknown role counts as a failed login, as it does over CQL.
            self.store.record_failure(user);
            return Err(HandshakeError::UnknownRole);
        };
        self.phase = Phase::AwaitingInitial {
            user: user.to_string(),
            verifier,
        };
        Ok(vec![BackendMessage::AuthenticationSasl {
            mechanisms: vec![MECHANISM.to_string()],
        }])
    }

    /// Handle a SASL payload (SASLInitialResponse first, then SASLResponse).
    pub fn on_sasl(&mut self, data: &[u8]) -> Result<Vec<BackendMessage>, HandshakeError> {
        match std::mem::replace(&mut self.phase, Phase::Failed) {
            Phase::AwaitingInitial { user, verifier } => {
                let (mechanism, client_first) = parse_sasl_initial(data)?;
                if mechanism != MECHANISM {
                    return Err(HandshakeError::UnsupportedMechanism);
                }
                let ctx = scram::server_first(&client_first, &self.server_nonce, &verifier)
                    .map_err(HandshakeError::Scram)?;
                let cont = BackendMessage::AuthenticationSaslContinue {
                    data: ctx.server_first.clone().into_bytes(),
                };
                self.phase = Phase::AwaitingFinal {
                    user,
                    verifier,
                    ctx,
                };
                Ok(vec![cont])
            }
            Phase::AwaitingFinal {
                user,
                verifier,
                ctx,
            } => {
                let proof = std::str::from_utf8(data)
                    .map_err(|_| scram::ScramError::Malformed("client-final not UTF-8"))
                    .and_then(|client_final| {
                        scram::verify_client_final(&ctx, client_final, &verifier)
                    });
                let server_final = match proof {
                    Ok(server_final) => server_final,
                    Err(error) => {
                        // Every proof that does not verify counts toward the
                        // shared lockout — a brute-force attempt has to reach
                        // this point to learn anything.
                        self.store.record_failure(&user);
                        return Err(HandshakeError::Scram(error));
                    }
                };
                let auth = self
                    .store
                    .record_success(&user)
                    .map_err(HandshakeError::LoginRefused)?;
                self.phase = Phase::Authenticated(auth);
                // The connection layer appends ParameterStatus + BackendKeyData
                // + ReadyForQuery once authentication completes.
                Ok(vec![
                    BackendMessage::AuthenticationSaslFinal {
                        data: server_final.into_bytes(),
                    },
                    BackendMessage::AuthenticationOk,
                ])
            }
            _ => Err(HandshakeError::UnexpectedMessage),
        }
    }
}

/// Parse a SASLInitialResponse body: `mechanism\0` + `i32(len)` + `client-first[len]`.
fn parse_sasl_initial(data: &[u8]) -> Result<(String, String), HandshakeError> {
    let malformed = |why: &'static str| HandshakeError::Scram(scram::ScramError::Malformed(why));
    let nul = data
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| malformed("no mechanism terminator"))?;
    let mechanism = std::str::from_utf8(&data[..nul])
        .map_err(|_| malformed("mechanism not UTF-8"))?
        .to_string();
    let rest = &data[nul + 1..];
    if rest.len() < 4 {
        return Err(malformed("missing SASL length"));
    }
    let len = i32::from_be_bytes(rest[0..4].try_into().unwrap());
    let payload = &rest[4..];
    // len == -1 means "no initial data"; otherwise it must match exactly.
    if len >= 0 && len as usize != payload.len() {
        return Err(malformed("SASL length mismatch"));
    }
    let client_first = std::str::from_utf8(payload)
        .map_err(|_| malformed("client-first not UTF-8"))?
        .to_string();
    Ok((mechanism, client_first))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};

    const SALT_B64: &str = "W22ZaJ0SNY7soEsUEjb6gQ==";
    const SERVER_NONCE: &str = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    const CLIENT_FIRST: &str = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO";
    const SERVER_FIRST: &str =
        "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
    const CLIENT_FINAL: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=";
    const SERVER_FINAL: &str = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=";

    struct MockStore {
        user: String,
        verifier: ScramVerifier,
        failures: std::cell::Cell<u32>,
        successes: std::cell::Cell<u32>,
        locked: bool,
    }
    impl VerifierStore for MockStore {
        fn verifier(&self, user: &str) -> Option<ScramVerifier> {
            (user == self.user).then(|| self.verifier.clone())
        }
        fn admit(&self, _user: &str) -> Result<(), String> {
            if self.locked {
                Err("locked out".into())
            } else {
                Ok(())
            }
        }
        fn record_failure(&self, _user: &str) {
            self.failures.set(self.failures.get() + 1);
        }
        fn record_success(&self, user: &str) -> Result<AuthContext, String> {
            self.successes.set(self.successes.get() + 1);
            Ok(AuthContext {
                role: user.to_string(),
                is_superuser: false,
                must_change_password: false,
            })
        }
    }

    fn store(password: &str) -> MockStore {
        let salt = STANDARD.decode(SALT_B64).unwrap();
        MockStore {
            user: "user".into(),
            verifier: ScramVerifier::from_password(password, &salt, 4096),
            failures: std::cell::Cell::new(0),
            successes: std::cell::Cell::new(0),
            locked: false,
        }
    }

    fn startup(user: &str) -> StartupMessage {
        StartupMessage {
            protocol_version: 196608,
            parameters: vec![
                ("user".into(), user.into()),
                ("database".into(), "ferrosa".into()),
            ],
        }
    }

    fn sasl_initial(mechanism: &str, client_first: &str) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(mechanism.as_bytes());
        v.push(0);
        v.extend_from_slice(&(client_first.len() as i32).to_be_bytes());
        v.extend_from_slice(client_first.as_bytes());
        v
    }

    #[test]
    fn full_handshake_succeeds() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);

        let r1 = hs.on_startup(&startup("user")).unwrap();
        assert_eq!(
            r1,
            vec![BackendMessage::AuthenticationSasl {
                mechanisms: vec![MECHANISM.to_string()]
            }]
        );

        let r2 = hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)).unwrap();
        assert_eq!(
            r2,
            vec![BackendMessage::AuthenticationSaslContinue {
                data: SERVER_FIRST.as_bytes().to_vec()
            }]
        );

        let r3 = hs.on_sasl(CLIENT_FINAL.as_bytes()).unwrap();
        assert_eq!(
            r3,
            vec![
                BackendMessage::AuthenticationSaslFinal {
                    data: SERVER_FINAL.as_bytes().to_vec()
                },
                BackendMessage::AuthenticationOk,
            ]
        );
        assert!(hs.is_authenticated());
        assert_eq!(s.successes.get(), 1, "a verified login is recorded");
        assert_eq!(s.failures.get(), 0);
        assert_eq!(hs.auth_context().map(|a| a.role.as_str()), Some("user"));
    }

    #[test]
    fn unknown_role_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        assert_eq!(
            hs.on_startup(&startup("nobody")),
            Err(HandshakeError::UnknownRole)
        );
        assert_eq!(s.failures.get(), 1, "an unknown role counts as a failure");
    }

    #[test]
    fn locked_out_user_is_refused_before_any_exchange() {
        let mut s = store("pencil");
        s.locked = true;
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        assert_eq!(
            hs.on_startup(&startup("user")),
            Err(HandshakeError::Throttled("locked out".into()))
        );
        assert!(!hs.is_authenticated());
    }

    #[test]
    fn missing_user_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        let su = StartupMessage {
            protocol_version: 196608,
            parameters: vec![],
        };
        assert_eq!(hs.on_startup(&su), Err(HandshakeError::MissingUser));
    }

    #[test]
    fn wrong_password_fails_auth_and_stays_unauthenticated() {
        let s = store("not-pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        hs.on_startup(&startup("user")).unwrap();
        hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)).unwrap();
        assert_eq!(
            hs.on_sasl(CLIENT_FINAL.as_bytes()),
            Err(HandshakeError::Scram(scram::ScramError::ProofMismatch))
        );
        assert!(!hs.is_authenticated());
        assert_eq!(s.failures.get(), 1, "a bad proof counts as a failure");
        assert_eq!(s.successes.get(), 0);
    }

    #[test]
    fn unsupported_mechanism_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        hs.on_startup(&startup("user")).unwrap();
        assert_eq!(
            hs.on_sasl(&sasl_initial("SCRAM-SHA-256-PLUS", CLIENT_FIRST)),
            Err(HandshakeError::UnsupportedMechanism)
        );
    }

    #[test]
    fn sasl_before_startup_is_rejected() {
        let s = store("pencil");
        let mut hs = Handshake::new(&s, SERVER_NONCE);
        assert_eq!(
            hs.on_sasl(&sasl_initial(MECHANISM, CLIENT_FIRST)),
            Err(HandshakeError::UnexpectedMessage)
        );
    }
}
